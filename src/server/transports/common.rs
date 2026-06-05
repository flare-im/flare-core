//! 服务端传输协议公共模块
//!
//! 提供 WebSocket 和 QUIC 服务端共享的逻辑和辅助函数

use crate::common::generate_id;
use crate::server::config::ServerConfig;
use crate::server::connection::ConnectionManager;
use crate::server::handle::ServerHandle;
use crate::server::transports::server_core::ServerCore;
use crate::transport::connection::Connection;
use std::sync::Arc;
use tokio::sync::Mutex;

/// 服务端连接辅助函数
pub struct ServerConnectionHelper;

impl ServerConnectionHelper {
    /// 设置新连接（统一处理逻辑）
    ///
    /// 统一处理连接添加、观察者创建和注册等逻辑
    pub async fn setup_new_connection(
        connection: Box<dyn Connection>,
        manager: Arc<ConnectionManager>,
        config: &ServerConfig,
        core: Arc<ServerCore>,
    ) -> Result<String, crate::common::error::FlareError> {
        // 生成连接 ID
        let connection_id = generate_id();

        // 从 ServerCore 获取是否需要认证
        let requires_auth = core.auth_enabled();

        // 添加连接。容量检查必须和插入在同一个临界区内完成，避免并发握手超额注册。
        manager
            .add_connection_with_limit(
                connection_id.clone(),
                connection,
                None,
                requires_auth,
                config.max_connections,
            )
            .map_err(|e| {
                crate::common::error::FlareError::connection_failed(format!(
                    "Failed to add connection: {}",
                    e
                ))
            })?;

        // 创建观察者
        let observer = core.create_observer_with_core(connection_id.clone(), Arc::clone(&core));

        // 添加观察者到连接
        if let Some((conn, _)) = manager.get_connection(&connection_id) {
            let mut c = conn.lock().await;
            c.add_observer(observer);
        } else {
            return Err(crate::common::error::FlareError::connection_failed(
                "Failed to get connection after adding".to_string(),
            ));
        }

        Ok(connection_id)
    }

    /// 停止服务器（统一处理逻辑）
    ///
    /// 统一处理停止心跳、断开所有连接等逻辑
    pub async fn stop_server(
        core: &ServerCore,
        is_running: &Arc<Mutex<bool>>,
    ) -> Result<(), crate::common::error::FlareError> {
        *is_running.lock().await = false;

        // 停止心跳检测
        core.stop_heartbeat();

        // 断开所有连接
        let connection_ids = core.list_connections().await;
        for conn_id in connection_ids {
            // 先关闭连接
            let manager_trait = core.connection_manager_trait();
            if let Some((conn, _)) = manager_trait.get_connection(&conn_id).await {
                let mut c = conn.lock().await;
                let _ = c.close().await;
            }
            // 然后从连接管理器中移除
            let _ = ServerHandle::disconnect(core, &conn_id).await;
        }

        Ok(())
    }
}
