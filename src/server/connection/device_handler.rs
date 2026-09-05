//! 设备冲突处理模块
//!
//! 处理用户多设备在线时的冲突策略

use crate::common::MessageParser;
use crate::common::device::{DeviceInfo, DevicePlatform};
use crate::common::error::Result;
use crate::common::protocol::{Reliability, builder::kicked, frame_with_system_command};
use crate::server::connection::ConnectionManagerTrait;
use crate::server::device::DeviceManager;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// 设备冲突处理结果
#[derive(Debug)]
pub struct DeviceConflictResult {
    /// 需要被踢掉的连接 ID 列表
    pub conflict_connections: Vec<String>,
}

/// 处理设备冲突
///
/// 通过 user_id 和 DevicePlatform 来确定冲突规则，不再依赖完整的 DeviceInfo
///
/// # 参数
/// - `device_manager`: 设备管理器（可选）
/// - `user_id`: 用户 ID（用于确定冲突规则）
/// - `connection_id`: 新连接的连接 ID
/// - `platform`: 设备平台（用于确定冲突规则）
/// - `device_info`: 设备信息（完整信息，用于记录）
/// - `manager`: 连接管理器（用于断开冲突连接）
///
/// # 返回
/// 设备冲突处理结果，包含需要被踢掉的连接 ID 列表
pub async fn handle_device_conflict(
    device_manager: Option<Arc<DeviceManager>>,
    user_id: &str,
    connection_id: &str,
    platform: &DevicePlatform,
    device_info: &DeviceInfo,
    manager: Arc<dyn ConnectionManagerTrait>,
) -> Result<DeviceConflictResult> {
    let mut conflict_connections = Vec::new();

    info!(
        "[DeviceHandler] 开始处理设备冲突: user_id={}, connection_id={}, platform={:?}",
        user_id, connection_id, platform
    );

    let device_manager_clone = device_manager.clone();
    if let Some(device_mgr) = &device_manager {
        // 使用 user_id 和 platform 来确定冲突规则，然后添加设备
        match device_mgr
            .add_device(user_id, connection_id.to_string(), device_info.clone())
            .await
        {
            Ok(conflicts) => {
                conflict_connections = conflicts;
                if !conflict_connections.is_empty() {
                    info!(
                        "[DeviceHandler] ✅ 设备冲突检测: 用户 {} 的新平台 {:?} 将踢掉 {} 个旧连接: {:?}",
                        user_id,
                        platform,
                        conflict_connections.len(),
                        conflict_connections
                    );
                } else {
                    debug!(
                        "[DeviceHandler] 无设备冲突: 用户 {} 的新平台 {:?} 可以正常添加",
                        user_id, platform
                    );
                }
            }
            Err(e) => {
                error!("[DeviceHandler] 设备管理器错误: {}", e);
            }
        }
    } else {
        debug!("[DeviceHandler] 未配置设备管理器，跳过设备冲突检测");
    }

    // 断开冲突的连接（在断开前发送被踢消息）
    if !conflict_connections.is_empty() {
        info!(
            "[DeviceHandler] 准备踢掉 {} 个冲突连接: {:?}",
            conflict_connections.len(),
            conflict_connections
        );
    }

    if let Some(device_mgr) = device_manager_clone {
        info!(
            "[DeviceHandler] 🔍 开始处理 {} 个冲突连接，新连接ID: {}",
            conflict_connections.len(),
            connection_id
        );

        for conflict_conn_id in &conflict_connections {
            // 确保不处理新连接本身（防御性检查）
            if conflict_conn_id == connection_id {
                error!(
                    "[DeviceHandler] ❌ 严重错误：冲突连接列表包含新连接ID！跳过处理: connection_id={}",
                    connection_id
                );
                continue;
            }

            info!(
                "[DeviceHandler] 📤 准备踢掉旧连接: connection_id={} (新连接ID: {})",
                conflict_conn_id, connection_id
            );

            // 1. 获取连接信息（包括协商后的序列化格式）
            if let Some((conn, conn_info)) = manager.get_connection(conflict_conn_id).await {
                info!(
                    "[DeviceHandler] 找到冲突连接: connection_id={}, user_id={:?}, format={:?}",
                    conflict_conn_id, conn_info.user_id, conn_info.serialization_format
                );
                // 2. 创建被踢消息（使用连接的协商格式）
                let reason = format!(
                    "设备冲突：同一用户 ({}) 的同一平台 ({:?}) 已有其他设备在线，当前设备将被踢下线",
                    user_id, platform
                );

                let mut metadata = std::collections::HashMap::new();
                metadata.insert("reason".to_string(), "device_conflict".as_bytes().to_vec());
                metadata.insert(
                    "platform".to_string(),
                    format!("{:?}", platform).as_bytes().to_vec(),
                );
                metadata.insert(
                    "new_device_id".to_string(),
                    device_info.device_id.as_bytes().to_vec(),
                );

                let kick_cmd = kicked(reason.clone(), Some(metadata));
                let kick_frame = frame_with_system_command(kick_cmd, Reliability::AtLeastOnce);

                // 3. 使用连接的协商格式序列化并发送被踢消息
                let parser = MessageParser::new(
                    conn_info.serialization_format,
                    conn_info.compression,
                    crate::common::encryption::EncryptionAlgorithm::None,
                );

                match parser.serialize(&kick_frame) {
                    Ok(kick_data) => {
                        // 再次验证：确保发送给的是旧连接，不是新连接
                        if conflict_conn_id == connection_id {
                            error!(
                                "[DeviceHandler] ❌ 严重错误：尝试发送KICKED消息给新连接！跳过: connection_id={}",
                                connection_id
                            );
                            continue;
                        }

                        let mut c = conn.lock().await;
                        if let Err(e) = c.send(&kick_data).await {
                            error!(
                                "[DeviceHandler] 发送被踢消息失败给旧连接 {}: {}",
                                conflict_conn_id, e
                            );
                        } else {
                            info!(
                                "[DeviceHandler] ✅ 已发送被踢消息给旧连接 {} (新连接: {}): {}",
                                conflict_conn_id, connection_id, reason
                            );
                            info!("[DeviceHandler] 💡 客户端收到 KICKED 消息后将主动断开连接");
                        }
                    }
                    Err(e) => {
                        error!(
                            "[DeviceHandler] 序列化被踢消息失败 (旧连接: {}): {}",
                            conflict_conn_id, e
                        );
                    }
                }

                // 从设备管理器中移除旧连接的设备
                // 注意：连接断开事件会在 DefaultServerMessageObserver 中处理连接管理器的清理
                if conflict_conn_id == connection_id {
                    error!(
                        "[DeviceHandler] ❌ 严重错误：尝试从设备管理器移除新连接的设备！跳过: connection_id={}",
                        connection_id
                    );
                    continue;
                }

                match device_mgr.remove_device(user_id, conflict_conn_id).await {
                    Ok(_) => {
                        info!(
                            "[DeviceHandler] ✅ 旧连接的设备已从设备管理器移除: {} (新连接: {})",
                            conflict_conn_id, connection_id
                        );
                    }
                    Err(e) => {
                        debug!(
                            "[DeviceHandler] 旧连接的设备 {} 已不存在于设备管理器: {}",
                            conflict_conn_id, e
                        );
                    }
                }
            } else {
                warn!(
                    "[DeviceHandler] 无法获取旧连接信息: {} (连接可能已断开)",
                    conflict_conn_id
                );
            }

            // 同步从连接管理器移除旧连接，不再仅依赖断开事件的延迟清理。
            // 否则旧连接仍留在 get_user_connections / 下行投递路由集合里，
            // 下行帧（含 SendAck）会被写到已关闭的 socket 而丢失
            // （signaling-gateway "Sending after closing"）。
            match manager.remove_connection(conflict_conn_id).await {
                Ok(_) => {
                    info!(
                        "[DeviceHandler] ✅ 旧连接已从连接管理器同步移除: {} (新连接: {})",
                        conflict_conn_id, connection_id
                    );
                }
                Err(e) => {
                    debug!(
                        "[DeviceHandler] 旧连接 {} 已不在连接管理器或移除失败: {}",
                        conflict_conn_id, e
                    );
                }
            }
        }

        info!(
            "[DeviceHandler] ✅ 设备冲突处理完成: 新连接 {} 已保留，已向 {} 个旧连接发送 KICKED 消息",
            connection_id,
            conflict_connections.len()
        );
    } else {
        // 如果没有设备管理器，无法发送被踢消息（因为需要设备管理器来管理冲突）
        warn!(
            "[DeviceHandler] ⚠️  无设备管理器，无法处理设备冲突: {} 个冲突连接",
            conflict_connections.len()
        );
    }

    info!(
        "[DeviceHandler] 🎯 设备冲突处理完成: 新连接 {} 保留，已通知 {} 个旧连接断开（客户端将主动断开）",
        connection_id,
        conflict_connections.len()
    );

    Ok(DeviceConflictResult {
        conflict_connections,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::device::{DeviceConflictStrategy, DeviceInfo, DevicePlatform};
    use crate::server::connection::ConnectionManager;
    use crate::transport::connection::Connection;
    use crate::transport::events::ArcObserver;
    use async_trait::async_trait;
    use std::sync::Mutex;
    use std::time::Instant;

    struct MockConn {
        last_active: Mutex<Instant>,
    }

    impl MockConn {
        fn new() -> Self {
            Self {
                last_active: Mutex::new(Instant::now()),
            }
        }
    }

    #[async_trait]
    impl Connection for MockConn {
        fn add_observer(&mut self, _observer: ArcObserver) {}
        fn remove_observer(&mut self, _observer: ArcObserver) {}
        async fn send(&mut self, _data: &[u8]) -> Result<()> {
            Ok(())
        }
        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
        fn last_active_time(&self) -> Instant {
            *self.last_active.lock().unwrap()
        }
        fn update_active_time(&mut self) {
            *self.last_active.lock().unwrap() = Instant::now();
        }
    }

    /// 回归：设备冲突（重连）时旧连接必须**同步从连接管理器**移除，
    /// 否则旧连接仍留在 `get_user_connections`/下行投递路由集合里，
    /// 下行帧（含 SendAck）会被写到已关闭的 socket 而丢失
    /// （signaling-gateway 日志 "Sending after closing"）。
    #[tokio::test]
    async fn device_conflict_evicts_old_connection_from_connection_manager() {
        let manager = Arc::new(ConnectionManager::new());
        let device_mgr = Arc::new(DeviceManager::new(DeviceConflictStrategy::PlatformExclusive));

        // 旧连接：同用户、Web 平台，注册进连接管理器 + 设备管理器
        manager
            .add_connection(
                "old-conn".to_string(),
                Box::new(MockConn::new()),
                Some("user1".to_string()),
                true,
            )
            .unwrap();
        device_mgr
            .add_device(
                "user1",
                "old-conn".to_string(),
                DeviceInfo::new("dev-a".to_string(), DevicePlatform::Web),
            )
            .await
            .unwrap();

        // 新连接：同用户、同 Web 平台（重连），注册进连接管理器
        manager
            .add_connection(
                "new-conn".to_string(),
                Box::new(MockConn::new()),
                Some("user1".to_string()),
                true,
            )
            .unwrap();

        let mgr_dyn: Arc<dyn ConnectionManagerTrait> = manager.clone();
        let result = handle_device_conflict(
            Some(device_mgr.clone()),
            "user1",
            "new-conn",
            &DevicePlatform::Web,
            &DeviceInfo::new("dev-b".to_string(), DevicePlatform::Web),
            mgr_dyn,
        )
        .await
        .unwrap();

        assert_eq!(result.conflict_connections, vec!["old-conn".to_string()]);

        // 关键断言：旧连接必须已从连接管理器移除
        let user_conns = manager.get_user_connections("user1");
        assert!(
            !user_conns.contains(&"old-conn".to_string()),
            "设备冲突处理后旧连接应已从连接管理器移除，实际仍在: {:?}",
            user_conns
        );
        assert_eq!(user_conns, vec!["new-conn".to_string()]);
        assert!(
            manager.get_connection("old-conn").is_none(),
            "旧连接快照应已从连接管理器移除"
        );
    }
}
