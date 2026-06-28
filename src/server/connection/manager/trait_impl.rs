use super::*;

#[async_trait]
impl ConnectionManagerTrait for ConnectionManager {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn add_connection(
        &self,
        connection_id: String,
        connection: Arc<Mutex<Box<dyn Connection>>>,
        user_id: Option<String>,
    ) -> Result<()> {
        // 注意：trait 方法不能直接传递 requires_auth，我们需要从 ServerCore 获取
        // 但这里我们暂时使用 true（需要认证），实际值应该在调用时通过 ServerCore 的 auth_enabled() 获取
        // 由于 ConnectionManager 不知道 ServerCore，我们暂时使用 true
        // 实际应用中，连接会在 CONNECT 消息处理时被标记为已验证
        let requires_auth = true; // 默认需要认证，如果不需要认证，连接会在 CONNECT 消息处理时被标记为已验证

        // 将 Arc<Mutex<Box<dyn Connection>>> 转换为 Box<dyn Connection>
        // 注意：这需要从 Arc 中取出，但 Arc 可能被多个地方引用
        // 对于默认实现，我们需要一个不同的方式
        // 由于 ConnectionManager 内部使用 Arc<Mutex<Box<dyn Connection>>>，
        // 我们需要保持一致性
        self.reserve_connection_slot(usize::MAX)?;

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

        let entry = self.new_connection_entry(&connection_id, Arc::clone(&connection), info);
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

    async fn remove_connection(&self, connection_id: &str) -> Result<()> {
        ConnectionManager::remove_connection(self, connection_id)
    }

    async fn get_connection(
        &self,
        connection_id: &str,
    ) -> Option<(
        Arc<Mutex<Box<dyn Connection>>>,
        crate::server::connection::r#trait::ConnectionInfo,
    )> {
        ConnectionManager::get_connection(self, connection_id).map(|(conn, info)| {
            // 转换 ConnectionInfo 格式（从 Instant 转换为 Unix 时间戳）
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let created_at_secs = now.saturating_sub(info.created_at.elapsed().as_secs());
            let last_active_secs = now.saturating_sub(info.last_active.elapsed().as_secs());

            let trait_info = crate::server::connection::r#trait::ConnectionInfo {
                connection_id: info.connection_id,
                user_id: info.user_id,
                created_at: created_at_secs,
                last_active: last_active_secs,
                metadata: info.metadata,
                device_info: info.device_info.clone(),
                serialization_format: info.serialization_format,
                compression: info.compression,
                encryption: info.encryption,
                authenticated: info.authenticated,
                authenticated_at: info.authenticated_at,
                negotiation_completed: info.negotiation_completed,
                negotiation_confirmed: info.negotiation_confirmed,
                cached_parser: info.cached_parser.clone(),
                cached_pipeline: info.cached_pipeline.clone(),
            };
            (conn, trait_info)
        })
    }

    async fn get_user_connections(&self, user_id: &str) -> Vec<String> {
        ConnectionManager::get_user_connections(self, user_id)
    }

    async fn bind_user(&self, connection_id: &str, user_id: String) -> Result<()> {
        ConnectionManager::bind_user(self, connection_id, user_id)
    }

    async fn update_connection_active(&self, connection_id: &str) -> Result<()> {
        ConnectionManager::update_connection_active(self, connection_id)
    }

    async fn set_connection_authenticated(
        &self,
        connection_id: &str,
        user_id: Option<String>,
    ) -> Result<()> {
        // ConnectionManager::set_connection_authenticated 是同步方法，直接调用
        ConnectionManager::set_connection_authenticated(self, connection_id, user_id)
    }

    async fn list_connections(&self) -> Vec<String> {
        ConnectionManager::list_connections(self)
    }

    async fn connection_count(&self) -> usize {
        ConnectionManager::connection_count(self)
    }

    fn connection_count_snapshot(&self) -> usize {
        ConnectionManager::connection_count(self)
    }

    fn user_count_snapshot(&self) -> usize {
        ConnectionManager::user_count(self)
    }

    async fn cleanup_timeout_connections(&self, timeout: Duration) -> Vec<String> {
        let timeout_connections = self.timeout_connection_snapshots(timeout);

        for (_, connection, _) in &timeout_connections {
            let mut conn = connection.lock().await;
            let _ = conn.close().await;
        }

        self.remove_connection_snapshots(
            timeout_connections
                .iter()
                .map(|(connection_id, _, _)| connection_id.clone()),
        )
    }

    async fn send_to_connection(&self, connection_id: &str, data: &[u8]) -> Result<()> {
        let (_, connection, _) = self.get_connection_snapshot(connection_id).ok_or_else(|| {
            FlareError::protocol_error(format!("Connection {} not found", connection_id))
        })?;

        self.send_to_connection_handle(connection_id, connection, data)
            .await
    }

    async fn send_to_user(&self, user_id: &str, data: &[u8]) -> Result<()> {
        let connections =
            self.connection_handles_for_ids(ConnectionManager::get_user_connections(self, user_id));

        stream::iter(connections)
            .for_each_concurrent(
                self.fanout_concurrency,
                |(connection_id, connection)| async move {
                    if let Err(e) = self
                        .send_to_connection_handle(&connection_id, connection, data)
                        .await
                    {
                        tracing::warn!("Failed to send to connection {}: {:?}", connection_id, e);
                    }
                },
            )
            .await;

        Ok(())
    }

    async fn broadcast(&self, data: &[u8]) -> Result<()> {
        let connections = self.connection_handles();

        stream::iter(connections)
            .for_each_concurrent(
                self.fanout_concurrency,
                |(connection_id, connection)| async move {
                    if let Err(e) = self
                        .send_to_connection_handle(&connection_id, connection, data)
                        .await
                    {
                        tracing::warn!(
                            "Failed to broadcast to connection {}: {:?}",
                            connection_id,
                            e
                        );
                    }
                },
            )
            .await;

        Ok(())
    }

    async fn broadcast_except(&self, data: &[u8], exclude_connection_id: &str) -> Result<()> {
        let connections = self.connection_handles_except(exclude_connection_id);

        stream::iter(connections)
            .for_each_concurrent(
                self.fanout_concurrency,
                |(connection_id, connection)| async move {
                    if let Err(e) = self
                        .send_to_connection_handle(&connection_id, connection, data)
                        .await
                    {
                        tracing::warn!(
                            "Failed to broadcast to connection {}: {:?}",
                            connection_id,
                            e
                        );
                    }
                },
            )
            .await;

        Ok(())
    }

    async fn send_frame_to(
        &self,
        connection_id: &str,
        frame: &crate::common::protocol::Frame,
        parser: Option<&crate::common::MessageParser>,
    ) -> Result<()> {
        let snapshot = self.get_connection_snapshot(connection_id).ok_or_else(|| {
            FlareError::connection_failed(format!("连接 {} 不存在", connection_id))
        })?;

        self.send_frame_to_snapshot(snapshot, frame, parser).await
    }

    async fn send_frame_to_user(
        &self,
        user_id: &str,
        frame: &crate::common::protocol::Frame,
        parser: Option<&crate::common::MessageParser>,
    ) -> Result<()> {
        let connection_ids = ConnectionManager::get_user_connections(self, user_id);

        if let Some(parser) = parser {
            let connections = self.connection_auth_snapshots_for_ids(connection_ids);
            let data = match parser.serialize(frame) {
                Ok(data) => data,
                Err(e) => {
                    tracing::warn!("Failed to serialize frame for user {}: {:?}", user_id, e);
                    return Ok(());
                }
            };

            let successful_ids = Arc::new(std::sync::Mutex::new(Vec::new()));
            stream::iter(connections)
                .for_each_concurrent(self.fanout_concurrency, |snapshot| {
                    let successful_ids = Arc::clone(&successful_ids);
                    let data = data.as_slice();
                    async move {
                        let connection_id = snapshot.0.clone();
                        let result = self
                            .send_serialized_frame_to_auth_snapshot_without_active(
                                snapshot, frame, data,
                            )
                            .await;
                        match result {
                            Ok(connection_id) => {
                                Self::record_successful_connection_id(
                                    &successful_ids,
                                    connection_id,
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Failed to send frame to connection {}: {:?}",
                                    connection_id,
                                    e
                                );
                            }
                        }
                    }
                })
                .await;
            self.update_connections_active(Self::take_successful_connection_ids(successful_ids));

            return Ok(());
        }

        let connections = self.connection_snapshots_for_ids(connection_ids);
        let successful_ids = Arc::new(std::sync::Mutex::new(Vec::new()));
        stream::iter(connections)
            .for_each_concurrent(self.fanout_concurrency, |snapshot| {
                let successful_ids = Arc::clone(&successful_ids);
                async move {
                    let connection_id = snapshot.0.clone();
                    let result = self
                        .send_frame_to_snapshot_without_active(snapshot, frame, parser)
                        .await;
                    match result {
                        Ok(connection_id) => {
                            Self::record_successful_connection_id(&successful_ids, connection_id);
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Failed to send frame to connection {}: {:?}",
                                connection_id,
                                e
                            );
                        }
                    }
                }
            })
            .await;
        self.update_connections_active(Self::take_successful_connection_ids(successful_ids));

        Ok(())
    }

    async fn broadcast_frame(
        &self,
        frame: &crate::common::protocol::Frame,
        parser: Option<&crate::common::MessageParser>,
    ) -> Result<()> {
        if let Some(parser) = parser {
            let connections = self.connection_auth_snapshots();
            let data = match parser.serialize(frame) {
                Ok(data) => data,
                Err(e) => {
                    tracing::warn!("Failed to serialize broadcast frame: {:?}", e);
                    return Ok(());
                }
            };

            let successful_ids = Arc::new(std::sync::Mutex::new(Vec::new()));
            stream::iter(connections)
                .for_each_concurrent(self.fanout_concurrency, |snapshot| {
                    let successful_ids = Arc::clone(&successful_ids);
                    let data = data.as_slice();
                    async move {
                        let connection_id = snapshot.0.clone();
                        let result = self
                            .send_serialized_frame_to_auth_snapshot_without_active(
                                snapshot, frame, data,
                            )
                            .await;
                        match result {
                            Ok(connection_id) => {
                                Self::record_successful_connection_id(
                                    &successful_ids,
                                    connection_id,
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Failed to broadcast frame to connection {}: {:?}",
                                    connection_id,
                                    e
                                );
                            }
                        }
                    }
                })
                .await;
            self.update_connections_active(Self::take_successful_connection_ids(successful_ids));

            return Ok(());
        }

        let connections = self.connection_snapshots();
        let successful_ids = Arc::new(std::sync::Mutex::new(Vec::new()));
        stream::iter(connections)
            .for_each_concurrent(self.fanout_concurrency, |snapshot| {
                let successful_ids = Arc::clone(&successful_ids);
                async move {
                    let connection_id = snapshot.0.clone();
                    let result = self
                        .send_frame_to_snapshot_without_active(snapshot, frame, parser)
                        .await;
                    match result {
                        Ok(connection_id) => {
                            Self::record_successful_connection_id(&successful_ids, connection_id);
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Failed to broadcast frame to connection {}: {:?}",
                                connection_id,
                                e
                            );
                        }
                    }
                }
            })
            .await;
        self.update_connections_active(Self::take_successful_connection_ids(successful_ids));

        Ok(())
    }

    async fn broadcast_frame_except(
        &self,
        frame: &crate::common::protocol::Frame,
        exclude_connection_id: &str,
        parser: Option<&crate::common::MessageParser>,
    ) -> Result<()> {
        if let Some(parser) = parser {
            let connections = self.connection_auth_snapshots_except(exclude_connection_id);
            let data = match parser.serialize(frame) {
                Ok(data) => data,
                Err(e) => {
                    tracing::warn!("Failed to serialize broadcast frame: {:?}", e);
                    return Ok(());
                }
            };

            let successful_ids = Arc::new(std::sync::Mutex::new(Vec::new()));
            stream::iter(connections)
                .for_each_concurrent(self.fanout_concurrency, |snapshot| {
                    let successful_ids = Arc::clone(&successful_ids);
                    let data = data.as_slice();
                    async move {
                        let connection_id = snapshot.0.clone();
                        let result = self
                            .send_serialized_frame_to_auth_snapshot_without_active(
                                snapshot, frame, data,
                            )
                            .await;
                        match result {
                            Ok(connection_id) => {
                                Self::record_successful_connection_id(
                                    &successful_ids,
                                    connection_id,
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Failed to broadcast frame to connection {}: {:?}",
                                    connection_id,
                                    e
                                );
                            }
                        }
                    }
                })
                .await;
            self.update_connections_active(Self::take_successful_connection_ids(successful_ids));

            return Ok(());
        }

        let connections = self.connection_snapshots_except(exclude_connection_id);
        let successful_ids = Arc::new(std::sync::Mutex::new(Vec::new()));
        stream::iter(connections)
            .for_each_concurrent(self.fanout_concurrency, |snapshot| {
                let successful_ids = Arc::clone(&successful_ids);
                async move {
                    let connection_id = snapshot.0.clone();
                    let result = self
                        .send_frame_to_snapshot_without_active(snapshot, frame, parser)
                        .await;
                    match result {
                        Ok(connection_id) => {
                            Self::record_successful_connection_id(&successful_ids, connection_id);
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Failed to broadcast frame to connection {}: {:?}",
                                connection_id,
                                e
                            );
                        }
                    }
                }
            })
            .await;
        self.update_connections_active(Self::take_successful_connection_ids(successful_ids));

        Ok(())
    }
}
