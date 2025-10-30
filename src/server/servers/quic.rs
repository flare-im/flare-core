//! QUIC服务端实现
//!
//! 提供QUIC协议的服务端支持
use crate::server::config::ServerConfig;
use crate::common::error::FlareError;
use crate::common::connections::config::ConnectionConfig;
use crate::common::connections::factory::ConnectionFactory;
use crate::server::connections::quic::QuicServerConnection;
use crate::server::manager::traits::ConnectionManager;
use crate::server::traits::ProtocolService;
use crate::server::events::server_adapter::ServerEventAdapter;
use std::sync::Arc;
use tracing::info;

/// QUIC 服务端监听骨架（服务端专有逻辑）
pub struct QuicServer {
    pub cfg: ServerConfig,
    /// 连接管理器
    connection_manager: Option<Arc<dyn ConnectionManager>>,
}

impl QuicServer {
    pub fn new(cfg: ServerConfig, connection_manager: Arc<dyn ConnectionManager>) -> Self { 
        Self { 
            cfg,
            connection_manager: Some(connection_manager),
        } 
    }
}

#[async_trait::async_trait]
impl ProtocolService for QuicServer {
    async fn start(&self, connection_manager: Arc<dyn ConnectionManager>) -> Result<(), FlareError> {
        // 构造统一连接配置并触发接受事件（最小骨架）
        let cfg_clone = self.cfg.clone();
        let handler = Arc::new(ServerEventAdapter::new());
        let connection_id = "quic_server_default".to_string();
        let mut conn_cfg: ConnectionConfig = cfg_clone
            .to_quic_connection_config(connection_id.clone())
            .unwrap_or_default();
        let listen_addr = cfg_clone.get_quic_config().map(|c| c.listen_addr.clone()).unwrap_or_else(|| "127.0.0.1:4321".to_string());
        info!("QUIC 服务端监听: {}", listen_addr);
        conn_cfg.transport = crate::common::connections::enums::Transport::Quic;
        // 创建统一服务端连接并触发接受事件
        let server_conn = Arc::new(QuicServerConnection::from_config(conn_cfg));
        
        // 添加连接到管理器（无需认证）
        connection_manager.add_connection(server_conn.clone());
        
        // 获取连接引用用于后续处理
        let conn_arc = server_conn.clone();
        
        conn_arc.set_event_handler(handler.clone());
        conn_arc.accept()?;
        Ok(())
    }

    async fn stop(&self) -> Result<(), FlareError> {
        Ok(())
    }
    
    fn name(&self) -> &str {
        "QUIC"
    }
}

impl QuicServer {
    pub fn connection_manager(&self) -> Option<&Arc<dyn ConnectionManager>> {
        self.connection_manager.as_ref()
    }
}