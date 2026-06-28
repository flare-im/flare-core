use super::*;

impl ConnectionManager {
    /// 创建新的连接管理器
    pub fn new() -> Self {
        Self::with_send_timeout(DEFAULT_SEND_TIMEOUT)
    }

    /// 使用指定写超时创建连接管理器
    pub fn with_send_timeout(send_timeout: Duration) -> Self {
        Self::with_limits(send_timeout, DEFAULT_FANOUT_CONCURRENCY)
    }

    /// 使用指定写超时和 fanout 并发度创建连接管理器
    pub fn with_limits(send_timeout: Duration, fanout_concurrency: usize) -> Self {
        Self::with_write_queue_limits(
            send_timeout,
            fanout_concurrency,
            DEFAULT_WRITE_QUEUE_CAPACITY,
        )
    }

    /// 使用指定写超时、fanout 并发度和每连接写队列容量创建连接管理器。
    pub fn with_write_queue_limits(
        send_timeout: Duration,
        fanout_concurrency: usize,
        write_queue_capacity: usize,
    ) -> Self {
        Self {
            connection_shards: Arc::new(
                (0..CONNECTION_SHARD_COUNT)
                    .map(|_| RwLock::new(HashMap::new()))
                    .collect(),
            ),
            user_connection_shards: Arc::new(
                (0..USER_CONNECTION_SHARD_COUNT)
                    .map(|_| RwLock::new(HashMap::new()))
                    .collect(),
            ),
            connection_count: Arc::new(AtomicUsize::new(0)),
            user_count: Arc::new(AtomicUsize::new(0)),
            send_timeout,
            fanout_concurrency: fanout_concurrency.max(1),
            write_queue_capacity: write_queue_capacity.max(1),
        }
    }

    pub(super) fn removal_registry(&self) -> ConnectionRemovalRegistry {
        ConnectionRemovalRegistry {
            connection_shards: Arc::downgrade(&self.connection_shards),
            user_connection_shards: Arc::downgrade(&self.user_connection_shards),
            connection_count: Arc::clone(&self.connection_count),
            user_count: Arc::clone(&self.user_count),
        }
    }

    pub(super) fn new_connection_entry(
        &self,
        connection_id: &str,
        connection: ConnectionHandle,
        info: ConnectionInfo,
    ) -> ConnectionEntry {
        let writer = ConnectionWriteQueue::new(
            connection_id.to_string(),
            Arc::clone(&connection),
            self.send_timeout,
            self.write_queue_capacity,
            self.removal_registry(),
        );
        (connection, writer, info)
    }

    pub(super) fn shard_index(key: &str, shard_count: usize) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as usize % shard_count
    }

    pub(super) fn connection_shard_index(&self, connection_id: &str) -> usize {
        Self::shard_index(connection_id, self.connection_shards.len())
    }

    pub(super) fn connection_shard(&self, connection_id: &str) -> &ConnectionShard {
        &self.connection_shards[self.connection_shard_index(connection_id)]
    }

    pub(super) fn user_connection_shard_index(&self, user_id: &str) -> usize {
        Self::shard_index(user_id, self.user_connection_shards.len())
    }

    pub(super) fn user_connection_shard(&self, user_id: &str) -> &UserConnectionShard {
        &self.user_connection_shards[self.user_connection_shard_index(user_id)]
    }

    pub(super) fn reserve_connection_slot(&self, max_connections: usize) -> Result<()> {
        loop {
            let current = self.connection_count.load(Ordering::Relaxed);
            if current >= max_connections {
                return Err(FlareError::connection_failed(format!(
                    "Connection limit exceeded: {}",
                    max_connections
                )));
            }

            if self
                .connection_count
                .compare_exchange_weak(current, current + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    pub(super) fn release_connection_slot(&self) {
        self.connection_count.fetch_sub(1, Ordering::Relaxed);
    }

    pub(super) fn insert_user_connection(
        &self,
        user_id: String,
        connection_id: &str,
    ) -> Result<()> {
        let mut user_connections = self
            .user_connection_shard(&user_id)
            .write()
            .map_err(|_| FlareError::general_error("Failed to lock user_connection shard"))?;
        if Self::insert_user_connection_index(&mut user_connections, user_id, connection_id) {
            self.user_count.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    pub(super) fn remove_user_connection(&self, user_id: &str, connection_id: &str) -> Result<()> {
        let mut user_connections = self
            .user_connection_shard(user_id)
            .write()
            .map_err(|_| FlareError::general_error("Failed to lock user_connection shard"))?;
        if Self::remove_user_connection_index(&mut user_connections, user_id, connection_id) {
            self.user_count.fetch_sub(1, Ordering::Relaxed);
        }
        Ok(())
    }

    pub(super) fn insert_user_connection_index(
        user_connections: &mut HashMap<String, Vec<String>>,
        user_id: String,
        connection_id: &str,
    ) -> bool {
        let is_new_user = !user_connections.contains_key(&user_id);
        let conn_ids = user_connections.entry(user_id).or_default();
        if !conn_ids.iter().any(|id| id == connection_id) {
            conn_ids.push(connection_id.to_string());
        }
        is_new_user
    }

    pub(super) fn remove_user_connection_index(
        user_connections: &mut HashMap<String, Vec<String>>,
        user_id: &str,
        connection_id: &str,
    ) -> bool {
        let Some(conn_ids) = user_connections.get_mut(user_id) else {
            return false;
        };

        conn_ids.retain(|id| id != connection_id);
        if conn_ids.is_empty() {
            user_connections.remove(user_id);
            true
        } else {
            false
        }
    }

    /// 添加连接
    ///
    /// # 参数
    /// - `connection_id`: 连接唯一标识符
    /// - `connection`: 连接实例
    /// - `user_id`: 可选的用户 ID（如果已认证）
    /// - `requires_auth`: 是否需要认证（如果为 false，连接直接标记为已验证）
    ///
    /// # 返回
    /// 如果连接 ID 已存在，返回错误
    pub fn add_connection(
        &self,
        connection_id: String,
        connection: Box<dyn Connection>,
        user_id: Option<String>,
        requires_auth: bool,
    ) -> Result<()> {
        self.add_connection_with_limit(
            connection_id,
            connection,
            user_id,
            requires_auth,
            usize::MAX,
        )
    }

    /// 添加连接，并在同一个写锁临界区内检查容量。
    ///
    /// 这个入口用于传输层新连接注册，避免先 `connection_count` 再 `add_connection`
    /// 在高并发握手完成时产生超额注册。
    pub fn add_connection_with_limit(
        &self,
        connection_id: String,
        connection: Box<dyn Connection>,
        user_id: Option<String>,
        requires_auth: bool,
        max_connections: usize,
    ) -> Result<()> {
        self.reserve_connection_slot(max_connections)?;

        let mut shard = match self.connection_shard(&connection_id).write() {
            Ok(shard) => shard,
            Err(_) => {
                self.release_connection_slot();
                return Err(FlareError::general_error("Failed to lock connection shard"));
            }
        };

        if shard.contains_key(&connection_id) {
            self.release_connection_slot();
            return Err(FlareError::protocol_error(format!(
                "Connection {} already exists",
                connection_id
            )));
        }

        let mut info = ConnectionInfo::new(connection_id.clone(), requires_auth);
        info.user_id = user_id.clone();

        let connection = Arc::new(Mutex::new(connection));
        let entry = self.new_connection_entry(&connection_id, connection, info);
        shard.insert(connection_id.clone(), entry);

        // 如果提供了用户 ID，添加到用户连接映射
        if let Some(user_id) = user_id
            && let Err(err) = self.insert_user_connection(user_id, &connection_id)
        {
            shard.remove(&connection_id);
            self.release_connection_slot();
            return Err(err);
        }

        Ok(())
    }

    /// 移除连接
    ///
    /// # 参数
    /// - `connection_id`: 要移除的连接 ID
    ///
    /// # 返回
    /// 如果连接不存在，返回错误
    pub fn remove_connection(&self, connection_id: &str) -> Result<()> {
        let mut shard = self
            .connection_shard(connection_id)
            .write()
            .map_err(|_| FlareError::general_error("Failed to lock connection shard"))?;

        let (_, writer, info) = shard.remove(connection_id).ok_or_else(|| {
            FlareError::protocol_error(format!("Connection {} not found", connection_id))
        })?;
        writer.close_underlying_in_background();
        self.release_connection_slot();

        // 如果连接关联了用户，从用户连接映射中移除
        if let Some(user_id) = info.user_id {
            self.remove_user_connection(&user_id, connection_id)?;
        }

        Ok(())
    }

    /// 获取连接
    ///
    /// # 参数
    /// - `connection_id`: 连接 ID
    ///
    /// # 返回
    /// 连接实例和连接信息的元组，如果不存在则返回 None
    #[allow(clippy::type_complexity)]
    pub fn get_connection(
        &self,
        connection_id: &str,
    ) -> Option<(ConnectionHandle, ConnectionInfo)> {
        let shard = self.connection_shard(connection_id).read().ok()?;
        let (conn, _, info) = shard.get(connection_id)?;
        let conn_clone = Arc::clone(conn);
        let info_clone = info.clone();
        drop(shard);
        Some((conn_clone, info_clone))
    }

    pub(super) fn get_connection_snapshot(
        &self,
        connection_id: &str,
    ) -> Option<ConnectionSnapshot> {
        let shard = self.connection_shard(connection_id).read().ok()?;
        let (_, writer, info) = shard.get(connection_id)?;
        Some((connection_id.to_string(), Arc::clone(writer), info.clone()))
    }

    pub(super) fn connection_handles(&self) -> Vec<ConnectionHandleSnapshot> {
        self.connection_shards
            .iter()
            .flat_map(|shard| {
                shard
                    .read()
                    .ok()
                    .map(|connections| {
                        connections
                            .iter()
                            .map(|(id, (_, writer, _))| (id.clone(), Arc::clone(writer)))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            })
            .collect()
    }

    pub(super) fn connection_handles_except(
        &self,
        exclude_connection_id: &str,
    ) -> Vec<ConnectionHandleSnapshot> {
        self.connection_shards
            .iter()
            .flat_map(|shard| {
                shard
                    .read()
                    .ok()
                    .map(|connections| {
                        connections
                            .iter()
                            .filter(|(id, _)| id.as_str() != exclude_connection_id)
                            .map(|(id, (_, writer, _))| (id.clone(), Arc::clone(writer)))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            })
            .collect()
    }

    pub(super) fn connection_handles_for_ids(
        &self,
        connection_ids: Vec<String>,
    ) -> Vec<ConnectionHandleSnapshot> {
        let mut ids_by_shard = vec![Vec::new(); self.connection_shards.len()];
        for connection_id in connection_ids {
            let shard_index = self.connection_shard_index(&connection_id);
            ids_by_shard[shard_index].push(connection_id);
        }

        ids_by_shard
            .into_iter()
            .enumerate()
            .flat_map(|(shard_index, ids)| {
                if ids.is_empty() {
                    return Vec::new();
                }

                self.connection_shards[shard_index]
                    .read()
                    .ok()
                    .map(|connections| {
                        ids.into_iter()
                            .filter_map(|id| {
                                connections
                                    .get(&id)
                                    .map(|(_, writer, _)| (id, Arc::clone(writer)))
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            })
            .collect()
    }

    pub(super) fn connection_auth_snapshots(&self) -> Vec<ConnectionAuthSnapshot> {
        self.connection_shards
            .iter()
            .flat_map(|shard| {
                shard
                    .read()
                    .ok()
                    .map(|connections| {
                        connections
                            .iter()
                            .map(|(id, (_, writer, info))| {
                                (id.clone(), Arc::clone(writer), info.authenticated)
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            })
            .collect()
    }

    pub(super) fn connection_auth_snapshots_except(
        &self,
        exclude_connection_id: &str,
    ) -> Vec<ConnectionAuthSnapshot> {
        self.connection_shards
            .iter()
            .flat_map(|shard| {
                shard
                    .read()
                    .ok()
                    .map(|connections| {
                        connections
                            .iter()
                            .filter(|(id, _)| id.as_str() != exclude_connection_id)
                            .map(|(id, (_, writer, info))| {
                                (id.clone(), Arc::clone(writer), info.authenticated)
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            })
            .collect()
    }

    pub(super) fn connection_auth_snapshots_for_ids(
        &self,
        connection_ids: Vec<String>,
    ) -> Vec<ConnectionAuthSnapshot> {
        let mut ids_by_shard = vec![Vec::new(); self.connection_shards.len()];
        for connection_id in connection_ids {
            let shard_index = self.connection_shard_index(&connection_id);
            ids_by_shard[shard_index].push(connection_id);
        }

        ids_by_shard
            .into_iter()
            .enumerate()
            .flat_map(|(shard_index, ids)| {
                if ids.is_empty() {
                    return Vec::new();
                }

                self.connection_shards[shard_index]
                    .read()
                    .ok()
                    .map(|connections| {
                        ids.into_iter()
                            .filter_map(|id| {
                                connections.get(&id).map(|(_, writer, info)| {
                                    (id, Arc::clone(writer), info.authenticated)
                                })
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            })
            .collect()
    }

    pub(super) fn connection_snapshots(&self) -> Vec<ConnectionSnapshot> {
        self.connection_shards
            .iter()
            .flat_map(|shard| {
                shard
                    .read()
                    .ok()
                    .map(|connections| {
                        connections
                            .iter()
                            .map(|(id, (_, writer, info))| {
                                (id.clone(), Arc::clone(writer), info.clone())
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            })
            .collect()
    }

    pub(super) fn connection_snapshots_except(
        &self,
        exclude_connection_id: &str,
    ) -> Vec<ConnectionSnapshot> {
        self.connection_shards
            .iter()
            .flat_map(|shard| {
                shard
                    .read()
                    .ok()
                    .map(|connections| {
                        connections
                            .iter()
                            .filter(|(id, _)| id.as_str() != exclude_connection_id)
                            .map(|(id, (_, writer, info))| {
                                (id.clone(), Arc::clone(writer), info.clone())
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            })
            .collect()
    }

    pub(super) fn connection_snapshots_for_ids(
        &self,
        connection_ids: Vec<String>,
    ) -> Vec<ConnectionSnapshot> {
        let mut ids_by_shard = vec![Vec::new(); self.connection_shards.len()];
        for connection_id in connection_ids {
            let shard_index = self.connection_shard_index(&connection_id);
            ids_by_shard[shard_index].push(connection_id);
        }

        ids_by_shard
            .into_iter()
            .enumerate()
            .flat_map(|(shard_index, ids)| {
                if ids.is_empty() {
                    return Vec::new();
                }

                self.connection_shards[shard_index]
                    .read()
                    .ok()
                    .map(|connections| {
                        ids.into_iter()
                            .filter_map(|id| {
                                connections
                                    .get(&id)
                                    .map(|(_, writer, info)| (id, Arc::clone(writer), info.clone()))
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            })
            .collect()
    }

    pub(super) fn timeout_connection_snapshots(
        &self,
        timeout: Duration,
    ) -> Vec<TimeoutConnectionSnapshot> {
        self.connection_shards
            .iter()
            .flat_map(|shard| {
                shard
                    .read()
                    .ok()
                    .map(|connections| {
                        connections
                            .iter()
                            .filter(|(_, (_, _, info))| info.is_timeout(timeout))
                            .map(|(id, (connection, _, info))| {
                                (id.clone(), Arc::clone(connection), info.user_id.clone())
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            })
            .collect()
    }

    pub(super) fn remove_connection_snapshots<I>(&self, connection_ids: I) -> Vec<String>
    where
        I: IntoIterator<Item = String>,
    {
        let mut ids_by_shard = vec![Vec::new(); self.connection_shards.len()];
        for connection_id in connection_ids {
            let shard_index = self.connection_shard_index(&connection_id);
            ids_by_shard[shard_index].push(connection_id);
        }

        let mut removed_ids = Vec::new();
        let mut removed_user_connections = Vec::new();

        for (shard_index, ids) in ids_by_shard.into_iter().enumerate() {
            if ids.is_empty() {
                continue;
            }

            let Ok(mut shard) = self.connection_shards[shard_index].write() else {
                continue;
            };

            for connection_id in ids {
                if let Some((_, writer, info)) = shard.remove(&connection_id) {
                    writer.close_underlying_in_background();
                    if let Some(user_id) = info.user_id {
                        removed_user_connections.push((user_id, connection_id.clone()));
                    }
                    removed_ids.push(connection_id);
                }
            }
        }

        if !removed_ids.is_empty() {
            self.connection_count
                .fetch_sub(removed_ids.len(), Ordering::Relaxed);
        }

        self.remove_user_connections_batch(removed_user_connections);

        removed_ids
    }

    pub(super) fn remove_user_connections_batch<I>(&self, user_connections: I)
    where
        I: IntoIterator<Item = (String, String)>,
    {
        let mut entries_by_shard = vec![Vec::new(); self.user_connection_shards.len()];
        for (user_id, connection_id) in user_connections {
            let shard_index = self.user_connection_shard_index(&user_id);
            entries_by_shard[shard_index].push((user_id, connection_id));
        }

        let removed_users = entries_by_shard
            .into_iter()
            .enumerate()
            .map(|(shard_index, entries)| {
                if entries.is_empty() {
                    return 0;
                }

                let Ok(mut shard) = self.user_connection_shards[shard_index].write() else {
                    return 0;
                };

                entries
                    .into_iter()
                    .filter(|(user_id, connection_id)| {
                        Self::remove_user_connection_index(&mut shard, user_id, connection_id)
                    })
                    .count()
            })
            .sum::<usize>();

        if removed_users > 0 {
            self.user_count.fetch_sub(removed_users, Ordering::Relaxed);
        }
    }
}
