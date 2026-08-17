use super::*;

impl ConnectionManager {
    /// 获取用户的所有连接
    ///
    /// # 参数
    /// - `user_id`: 用户 ID
    ///
    /// # 返回
    /// 该用户的所有连接 ID 列表
    pub fn get_user_connections(&self, user_id: &str) -> Vec<String> {
        self.user_connection_shard(user_id)
            .read()
            .ok()
            .and_then(|user_connections| user_connections.get(user_id).cloned())
            .unwrap_or_default()
    }

    /// 更新连接的用户 ID（用于认证后绑定用户）
    ///
    /// # 参数
    /// - `connection_id`: 连接 ID
    /// - `user_id`: 新的用户 ID
    pub fn bind_user(&self, connection_id: &str, user_id: String) -> Result<()> {
        let mut shard = self
            .connection_shard(connection_id)
            .write()
            .map_err(|_| FlareError::general_error("Failed to lock connection shard"))?;

        let (_, _, info) = shard.get_mut(connection_id).ok_or_else(|| {
            FlareError::protocol_error(format!("Connection {} not found", connection_id))
        })?;

        // 如果之前有用户 ID，先移除旧映射
        if let Some(old_user_id) = &info.user_id {
            self.remove_user_connection(old_user_id, connection_id)?;
        }

        // 更新用户 ID
        info.user_id = Some(user_id.clone());

        // 添加到新用户映射
        self.insert_user_connection(user_id, connection_id)?;

        Ok(())
    }

    /// 更新连接的最后活跃时间
    pub fn update_connection_active(&self, connection_id: &str) -> Result<()> {
        let mut shard = self
            .connection_shard(connection_id)
            .write()
            .map_err(|_| FlareError::general_error("Failed to lock connection shard"))?;

        if let Some((_, _, info)) = shard.get_mut(connection_id) {
            info.update_active();
            drop(shard);
            Ok(())
        } else {
            drop(shard);
            Err(FlareError::protocol_error(format!(
                "Connection {} not found",
                connection_id
            )))
        }
    }

    pub(super) fn update_connections_active<I>(&self, connection_ids: I) -> usize
    where
        I: IntoIterator<Item = String>,
    {
        let now = Instant::now();
        let mut ids_by_shard = vec![Vec::new(); self.connection_shards.len()];
        for connection_id in connection_ids {
            let shard_index = self.connection_shard_index(&connection_id);
            ids_by_shard[shard_index].push(connection_id);
        }

        ids_by_shard
            .into_iter()
            .enumerate()
            .map(|(shard_index, ids)| {
                if ids.is_empty() {
                    return 0;
                }

                let Ok(mut shard) = self.connection_shards[shard_index].write() else {
                    return 0;
                };

                let mut updated = 0;
                for connection_id in ids {
                    if let Some((_, _, info)) = shard.get_mut(&connection_id) {
                        info.last_active = now;
                        updated += 1;
                    }
                }
                updated
            })
            .sum()
    }

    pub(super) async fn fanout_serialized_frame_to_auth_snapshots(
        &self,
        snapshots: Vec<ConnectionAuthSnapshot>,
        frame: &crate::common::protocol::Frame,
        data: &[u8],
        fanout: &'static str,
    ) -> Vec<String> {
        stream::iter(snapshots)
            .map(|snapshot| async move {
                let connection_id = snapshot.0.clone();
                match self
                    .send_serialized_frame_to_auth_snapshot_without_active(snapshot, frame, data)
                    .await
                {
                    Ok(connection_id) => Some(connection_id),
                    Err(error) => {
                        tracing::warn!(
                            connection_id = %connection_id,
                            error = ?error,
                            fanout,
                            "Connection frame fanout failed"
                        );
                        None
                    }
                }
            })
            .buffer_unordered(self.fanout_concurrency)
            .filter_map(|connection_id| async move { connection_id })
            .collect()
            .await
    }

    /// 多连接同帧扇出：无加密连接按（序列化格式, 压缩）分组，**每组只序列化一次**共享字节；
    /// 带加密连接回退逐连接序列化（密文可能与连接态绑定，不可跨连接共享）。
    /// 群聊单节点 N 订阅者的下行从 N 次 serialize+compress → 组数次（通常 1）。
    /// 返回（成功数, 失败数），成功连接统一批量刷新活跃时间。
    pub(super) async fn fanout_frame_grouped_by_encoding(
        &self,
        connection_ids: Vec<String>,
        frame: &crate::common::protocol::Frame,
    ) -> (i32, i32) {
        use std::collections::HashMap;

        // 分段计时：实测扩散随在线连接数线性增长 ≈2.2ms/连接，但每连接的工作
        // （try_enqueue 非阻塞入队、快照按分片批量读）都极便宜，对不上这个斜率。
        // 分三段量出来，避免又一次靠读代码推断热点——这轮排查里推断已经错过四次。
        let t_snapshot = std::time::Instant::now();
        let snapshots = self.connection_snapshots_for_ids(connection_ids);
        let snapshot_us = t_snapshot.elapsed().as_micros() as u64;
        let total = snapshots.len();
        if total == 0 {
            return (0, 0);
        }
        let t_serialize = std::time::Instant::now();

        let mut plain_groups: HashMap<
            (
                crate::common::protocol::SerializationFormat,
                crate::common::compression::CompressionAlgorithm,
            ),
            Vec<ConnectionSnapshot>,
        > = HashMap::new();
        let mut encrypted: Vec<ConnectionSnapshot> = Vec::new();
        for snapshot in snapshots {
            let info = &snapshot.2;
            if info.encryption == crate::common::encryption::EncryptionAlgorithm::None {
                plain_groups
                    .entry((info.serialization_format, info.compression.clone()))
                    .or_default()
                    .push(snapshot);
            } else {
                encrypted.push(snapshot);
            }
        }

        let group_us = t_serialize.elapsed().as_micros() as u64;
        let group_count = plain_groups.len();
        let t_write = std::time::Instant::now();

        let mut successful: Vec<String> = Vec::with_capacity(total);
        for ((format, compression), group) in plain_groups {
            let parser = crate::common::MessageParser::new(
                format,
                compression,
                crate::common::encryption::EncryptionAlgorithm::None,
            );
            let data = match parser.serialize(frame) {
                Ok(data) => data,
                Err(error) => {
                    tracing::warn!(
                        error = ?error,
                        group_size = group.len(),
                        "grouped frame serialization failed; group counted as failures"
                    );
                    continue;
                }
            };
            let sent: Vec<String> = stream::iter(group)
                .map(|snapshot| {
                    let data = &data;
                    async move {
                        let (connection_id, connection, info) = snapshot;
                        if let Err(error) =
                            Self::ensure_frame_allowed_for_connection(&connection_id, &info, frame)
                        {
                            tracing::warn!(connection_id = %connection_id, error = ?error, "grouped fanout skipped");
                            return None;
                        }
                        match self
                            .send_to_connection_handle(&connection_id, connection, data)
                            .await
                        {
                            Ok(()) => Some(connection_id),
                            Err(error) => {
                                tracing::warn!(connection_id = %connection_id, error = ?error, "grouped fanout write failed");
                                None
                            }
                        }
                    }
                })
                .buffer_unordered(self.fanout_concurrency)
                .filter_map(|connection_id| async move { connection_id })
                .collect()
                .await;
            successful.extend(sent);
        }
        if !encrypted.is_empty() {
            let sent = self
                .fanout_frame_to_snapshots(encrypted, frame, None, "grouped_fanout_encrypted")
                .await;
            successful.extend(sent);
        }

        let write_us = t_write.elapsed().as_micros() as u64;
        let success = successful.len() as i32;
        let t_active = std::time::Instant::now();
        self.update_connections_active(successful);

        // 只在连接数达到一定规模时记：小扇出（单聊 2 个连接）每条消息都打一行
        // 会把日志刷爆，而斜率问题只在大扇出时才看得见。
        if total >= 16 {
            tracing::info!(
                connections = total,
                groups = group_count,
                snapshot_us,
                group_us,
                write_us,
                active_us = t_active.elapsed().as_micros() as u64,
                per_conn_us = write_us / total.max(1) as u64,
                "grouped fanout timing"
            );
        }
        (success, (total as i32).saturating_sub(success))
    }

    pub(super) async fn fanout_frame_to_snapshots(
        &self,
        snapshots: Vec<ConnectionSnapshot>,
        frame: &crate::common::protocol::Frame,
        parser: Option<&crate::common::MessageParser>,
        fanout: &'static str,
    ) -> Vec<String> {
        stream::iter(snapshots)
            .map(|snapshot| async move {
                let connection_id = snapshot.0.clone();
                match self
                    .send_frame_to_snapshot_without_active(snapshot, frame, parser)
                    .await
                {
                    Ok(connection_id) => Some(connection_id),
                    Err(error) => {
                        tracing::warn!(
                            connection_id = %connection_id,
                            error = ?error,
                            fanout,
                            "Connection frame fanout failed"
                        );
                        None
                    }
                }
            })
            .buffer_unordered(self.fanout_concurrency)
            .filter_map(|connection_id| async move { connection_id })
            .collect()
            .await
    }

    /// 设置连接为已验证状态
    pub fn set_connection_authenticated(
        &self,
        connection_id: &str,
        user_id: Option<String>,
    ) -> Result<()> {
        let mut shard = self
            .connection_shard(connection_id)
            .write()
            .map_err(|_| FlareError::general_error("Failed to lock connection shard"))?;

        let (_, _, info) = shard.get_mut(connection_id).ok_or_else(|| {
            FlareError::protocol_error(format!("Connection {} not found", connection_id))
        })?;

        // 保存旧的 user_id（在调用 set_authenticated 之前）
        let old_user_id = info.user_id.clone();

        let final_user_id = user_id.or(old_user_id.clone());

        // 设置认证状态（如果 final_user_id 是 Some，会设置 user_id）
        info.set_authenticated(final_user_id.clone());

        // 如果有 user_id（传入的或已存在的），确保用户连接映射正确
        if let Some(user_id) = final_user_id {
            // 如果 user_id 发生变化，需要更新映射
            let user_id_changed = old_user_id
                .as_ref()
                .map(|old| old != &user_id)
                .unwrap_or(true);

            if user_id_changed {
                // 如果之前有旧用户 ID，先移除旧映射
                if let Some(old_user_id) = old_user_id {
                    self.remove_user_connection(&old_user_id, connection_id)?;
                }

                // 添加新映射（检查是否已存在，避免重复）
                self.insert_user_connection(user_id, connection_id)?;
            } else {
                // user_id 没有变化，只需确保映射存在
                self.insert_user_connection(user_id, connection_id)?;
            }
        }

        Ok(())
    }

    /// 更新连接的协商信息（设备信息、序列化格式、压缩算法）
    #[allow(clippy::too_many_arguments)]
    pub fn update_connection_negotiation(
        &self,
        connection_id: &str,
        device_info: Option<crate::common::device::DeviceInfo>,
        serialization_format: crate::common::protocol::SerializationFormat,
        compression: crate::common::compression::CompressionAlgorithm,
        encryption: crate::common::encryption::EncryptionAlgorithm,
        user_id: Option<String>,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<()> {
        let mut shard = self
            .connection_shard(connection_id)
            .write()
            .map_err(|_| FlareError::general_error("Failed to lock connection shard"))?;

        let (_, _, info) = shard.get_mut(connection_id).ok_or_else(|| {
            FlareError::protocol_error(format!("Connection {} not found", connection_id))
        })?;

        // 更新协商信息（但不标记协商完成）
        // 协商完成将在 CONNECT_ACK 发送完成后由 update_connection_negotiation_with_pipeline 标记
        info.device_info = device_info;
        info.serialization_format = serialization_format;
        info.compression = compression;
        info.encryption = encryption;
        // 若有传入 metadata，将其所有键值合并到 ConnectionInfo.metadata（同 key 覆盖）
        if let Some(meta) = metadata {
            for (k, v) in meta {
                info.metadata.insert(k, v);
            }
        }
        // 注意：这里不设置 negotiation_completed = true
        // 也不创建 cached_parser，这些将在 CONNECT_ACK 发送完成后设置

        // 保存旧的 user_id（在修改之前）
        let old_user_id = info.user_id.clone();

        let user_id_to_set = user_id.clone().or(old_user_id.clone());

        // 添加调试日志
        if user_id_to_set.is_none() {
            tracing::trace!(connection_id = %connection_id,incoming_user_id = ?user_id,old_user_id = ?old_user_id,"update_connection_negotiation: user_id_to_set is None, user_id will not be set");
        }

        if let Some(user_id_val) = user_id_to_set {
            // 如果之前有用户 ID 且与新 user_id 不同，先移除旧映射
            if let Some(old_user_id) = old_user_id
                && old_user_id != user_id_val
            {
                self.remove_user_connection(&old_user_id, connection_id)?;
            }

            // 更新用户 ID
            info.user_id = Some(user_id_val.clone());

            // 添加到新用户映射
            self.insert_user_connection(user_id_val, connection_id)?;
        }

        Ok(())
    }

    /// 更新连接的协商信息（设备信息、序列化格式、压缩算法、加密方式）并设置 pipeline
    #[allow(clippy::too_many_arguments)]
    pub fn update_connection_negotiation_with_pipeline(
        &self,
        connection_id: &str,
        device_info: Option<crate::common::device::DeviceInfo>,
        serialization_format: crate::common::protocol::SerializationFormat,
        compression: crate::common::compression::CompressionAlgorithm,
        encryption: crate::common::encryption::EncryptionAlgorithm,
        user_id: Option<String>,
        parser: crate::common::MessageParser,
        pipeline: Option<std::sync::Arc<crate::common::message::pipeline::MessagePipeline>>,
    ) -> Result<()> {
        let mut shard = self
            .connection_shard(connection_id)
            .write()
            .map_err(|_| FlareError::general_error("Failed to lock connection shard"))?;

        let (_, _, info) = shard.get_mut(connection_id).ok_or_else(|| {
            FlareError::protocol_error(format!("Connection {} not found", connection_id))
        })?;

        // 更新协商信息
        info.device_info = device_info;
        info.serialization_format = serialization_format;
        info.compression = compression;
        info.encryption = encryption;
        // 标记协商已完成
        info.negotiation_completed = true;
        // 缓存 parser 和 pipeline
        info.cached_parser = Some(std::sync::Arc::new(parser));
        info.cached_pipeline = pipeline;

        // 保存旧的 user_id（在修改之前）
        let old_user_id = info.user_id.clone();

        let user_id_to_set = user_id.clone().or(old_user_id.clone());

        // 添加调试日志
        if user_id_to_set.is_none() {
            tracing::trace!(
                connection_id = %connection_id,
                incoming_user_id = ?user_id,
                old_user_id = ?old_user_id,
                "update_connection_negotiation_with_pipeline: user_id_to_set is None, user_id will not be set"
            );
        }

        if let Some(user_id_val) = user_id_to_set {
            // 如果之前有用户 ID 且与新 user_id 不同，先移除旧映射
            if let Some(old_user_id) = old_user_id
                && old_user_id != user_id_val
            {
                self.remove_user_connection(&old_user_id, connection_id)?;
            }

            // 更新用户 ID
            info.user_id = Some(user_id_val.clone());

            // 添加到新用户映射
            self.insert_user_connection(user_id_val, connection_id)?;
        }

        Ok(())
    }

    /// 标记协商已确认（客户端收到 CONNECT_ACK 后发送确认）
    pub fn mark_negotiation_confirmed(&self, connection_id: &str) -> Result<()> {
        let mut shard = self
            .connection_shard(connection_id)
            .write()
            .map_err(|_| FlareError::general_error("Failed to lock connection shard"))?;

        let (_, _, info) = shard.get_mut(connection_id).ok_or_else(|| {
            FlareError::protocol_error(format!("Connection {} not found", connection_id))
        })?;

        if !info.negotiation_completed {
            return Err(FlareError::protocol_error(format!(
                "Cannot confirm negotiation for connection {}: negotiation not completed",
                connection_id
            )));
        }

        info.negotiation_confirmed = true;
        tracing::trace!(
            "[ConnectionManager] 协商已确认: connection_id={}",
            connection_id
        );

        Ok(())
    }

    /// 获取所有连接 ID
    pub fn list_connections(&self) -> Vec<String> {
        self.connection_shards
            .iter()
            .flat_map(|shard| {
                shard
                    .read()
                    .ok()
                    .map(|connections| connections.keys().cloned().collect::<Vec<_>>())
                    .unwrap_or_default()
            })
            .collect()
    }

    /// 获取连接总数
    pub fn connection_count(&self) -> usize {
        self.connection_count.load(Ordering::Relaxed)
    }

    /// 获取当前绑定了连接的用户数
    pub fn user_count(&self) -> usize {
        self.user_count.load(Ordering::Relaxed)
    }

    /// 清理超时连接
    ///
    /// # 参数
    /// - `timeout`: 超时时间
    ///
    /// # 返回
    /// 被清理的连接 ID 列表
    pub fn cleanup_timeout_connections(&self, timeout: Duration) -> Vec<String> {
        let timeout_connections = self.timeout_connection_snapshots(timeout);
        self.remove_connection_snapshots(
            timeout_connections
                .iter()
                .map(|(connection_id, _, _)| connection_id.clone()),
        )
    }

    /// 获取连接统计信息
    pub fn stats(&self) -> TraitConnectionStats {
        let total_connections = self.connection_count();
        let total_users = self.user_count();

        TraitConnectionStats {
            total_connections,
            total_users,
        }
    }

    pub(super) fn frame_allowed_before_auth(frame: &crate::common::protocol::Frame) -> bool {
        frame
            .command
            .as_ref()
            .and_then(|cmd| {
                if let Some(crate::common::protocol::flare::core::commands::command::Type::System(
                    sys_cmd,
                )) = &cmd.r#type
                {
                    Some(
                        sys_cmd.r#type
                            == crate::common::protocol::flare::core::commands::system_command::Type::ConnectAck
                                as i32
                            || sys_cmd.r#type
                                == crate::common::protocol::flare::core::commands::system_command::Type::Ping
                                    as i32
                            || sys_cmd.r#type
                                == crate::common::protocol::flare::core::commands::system_command::Type::Pong
                                    as i32
                            || sys_cmd.r#type
                                == crate::common::protocol::flare::core::commands::system_command::Type::Error
                                    as i32
                            || sys_cmd.r#type
                                == crate::common::protocol::flare::core::commands::system_command::Type::Close
                                    as i32,
                    )
                } else {
                    None
                }
            })
            .unwrap_or(false)
    }

    pub(super) fn serialize_frame_for_connection(
        connection_id: &str,
        info: &ConnectionInfo,
        frame: &crate::common::protocol::Frame,
        parser: Option<&crate::common::MessageParser>,
    ) -> Result<Vec<u8>> {
        Self::ensure_frame_allowed_for_connection(connection_id, info, frame)?;

        if let Some(parser) = parser {
            return parser.serialize(frame);
        }

        if let Some(parser) = &info.cached_parser {
            return parser.serialize(frame);
        }

        crate::common::MessageParser::new(
            info.serialization_format,
            info.compression.clone(),
            info.encryption.clone(),
        )
        .serialize(frame)
    }

    pub(super) fn ensure_frame_allowed_for_connection(
        connection_id: &str,
        info: &ConnectionInfo,
        frame: &crate::common::protocol::Frame,
    ) -> Result<()> {
        if info.authenticated || Self::frame_allowed_before_auth(frame) {
            return Ok(());
        }

        Err(FlareError::authentication_failed(format!(
            "连接 {} 未验证，无法发送消息",
            connection_id
        )))
    }

    pub(super) async fn send_to_connection_handle(
        &self,
        connection_id: &str,
        connection: ConnectionWriteHandle,
        data: &[u8],
    ) -> Result<()> {
        match connection.try_enqueue(data) {
            Ok(()) => Ok(()),
            Err(err) => {
                connection.close_underlying_in_background();
                let _ = ConnectionManager::remove_connection(self, connection_id);
                Err(err)
            }
        }
    }

    pub(super) async fn send_frame_to_snapshot(
        &self,
        snapshot: ConnectionSnapshot,
        frame: &crate::common::protocol::Frame,
        parser: Option<&crate::common::MessageParser>,
    ) -> Result<()> {
        let connection_id = self
            .send_frame_to_snapshot_without_active(snapshot, frame, parser)
            .await?;
        ConnectionManager::update_connection_active(self, &connection_id)?;
        Ok(())
    }

    pub(super) async fn send_frame_to_snapshot_without_active(
        &self,
        snapshot: ConnectionSnapshot,
        frame: &crate::common::protocol::Frame,
        parser: Option<&crate::common::MessageParser>,
    ) -> Result<String> {
        let (connection_id, connection, info) = snapshot;
        let data = Self::serialize_frame_for_connection(&connection_id, &info, frame, parser)?;

        self.send_to_connection_handle(&connection_id, connection, &data)
            .await?;
        Ok(connection_id)
    }

    pub(super) async fn send_serialized_frame_to_auth_snapshot_without_active(
        &self,
        snapshot: ConnectionAuthSnapshot,
        frame: &crate::common::protocol::Frame,
        data: &[u8],
    ) -> Result<String> {
        let (connection_id, connection, authenticated) = snapshot;
        if !authenticated && !Self::frame_allowed_before_auth(frame) {
            return Err(FlareError::authentication_failed(format!(
                "连接 {} 未验证，无法发送消息",
                connection_id
            )));
        }

        self.send_to_connection_handle(&connection_id, connection, data)
            .await?;
        Ok(connection_id)
    }
}
