//! QUIC 客户端连接实现

use crate::common::connections::traits::{BaseConnection, ClientConnection, ConnectionEvent};
use crate::common::connections::config::ConnectionConfig;
use crate::common::error::FlareError;
use crate::common::connections::quic::base::QuicBaseConn;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// QUIC 客户端连接结构
pub struct QuicClientConn {
    /// 基础QUIC连接
    base: Arc<QuicBaseConn>,
}

impl QuicClientConn {
    pub fn from_config(config: ConnectionConfig) -> Self {
        let base = Arc::new(QuicBaseConn::from_config(config));
        
        Self {
            base,
        }
    }

    /// 从原生 quinn::Connection 构造服务端连接（支持真实读写桥接）
    pub fn from_quinn_connection(conn: quinn::Connection, config: ConnectionConfig) -> Self {
        let base = Arc::new(QuicBaseConn::from_quinn_connection(conn, config));
        
        Self {
            base,
        }
    }

    pub fn start_heartbeat(&mut self) -> Result<(), FlareError> {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        // 注意：这里我们需要获取base的last_activity_epoch_ms，但由于Rust的所有权规则，
        // 我们不能直接访问私有字段，需要通过方法获取
        let last_activity = BaseConnection::last_activity_epoch_ms(self.base.as_ref());
        if crate::common::connections::monitor::is_heartbeat_timeout(
            last_activity, 
            now_ms, 
            self.base.heartbeat_timeout_ms()
        ) {
            // 注意：这里我们需要调用base的handle_heartbeat_timeout方法，
            // 但由于Rust的所有权规则，我们不能直接调用可变方法
            // 这个逻辑需要在具体的连接实现中处理
        }
        Ok(())
    }
    
    pub fn stop_heartbeat(&mut self) -> Result<(), FlareError> {
        // 心跳任务的停止由基础连接处理
        Ok(())
    }

    pub fn handle_pong_rtt(&self, _rtt_ms: u32) -> Result<(), FlareError> {
        // 注意：这里我们需要调用base的handle_heartbeat_pong方法，
        // 但由于Rust的所有权规则，我们不能直接调用可变方法
        // 这个逻辑需要在具体的连接实现中处理
        Ok(())
    }
    
    /// 获取基础连接核心
    /// 
    /// # 返回值
    /// 基础连接核心的引用
    pub fn base(&self) -> &Arc<QuicBaseConn> {
        &self.base
    }
}

// 实现 BaseConnection trait（通过委托给 base）
impl crate::common::connections::traits::BaseConnection for QuicClientConn {
    fn send_bytes(&self, bytes: Vec<u8>) -> Result<(), FlareError> {
        self.base.send_bytes(bytes)
    }
    
    fn set_event_handler(&self, handler: Arc<dyn ConnectionEvent>) {
        self.base.set_event_handler(handler);
    }
    
    fn state(&self) -> crate::common::connections::enums::ConnectionState {
        self.base.state()
    }
    
    fn ready(&self) -> Result<(), FlareError> {
        self.base.ready()
    }
    
    fn connected(&self) -> Result<(), FlareError> {
        self.base.connected()
    }
    
    fn set_state(&self, state: crate::common::connections::enums::ConnectionState) -> Result<(), FlareError> {
        self.base.set_state(state)
    }
    
    fn stats(&self) -> crate::common::connections::types::ConnectionStats {
        self.base.stats()
    }
    
    fn last_activity_epoch_ms(&self) -> u64 {
        self.base.last_activity_epoch_ms()
    }
    
    fn id(&self) -> String {
        self.base.id()
    }
}

impl ClientConnection for QuicClientConn {
    fn connect(&self) -> Result<(), FlareError> {
        // 启动 QUIC 客户端连接
        if let Some(h) = self.base.get_event_handler() {
            let eh: Arc<dyn ConnectionEvent> = Arc::clone(&h);
            let remote_addr = self.base.remote_addr.clone().ok_or_else(|| FlareError::connection_failed("缺少远程地址".to_string()))?;
            let _interval_ms = self.base.heartbeat_interval_ms();
            let base_clone = Arc::clone(&self.base);
            
            let (tx, rx) = tokio::sync::mpsc::channel::<Vec<u8>>(1024);
            if let Ok(mut g) = self.base.outbound_tx.lock() { *g = Some(tx); }
            
            tokio::spawn(async move {
                // 创建默认的 QUIC 客户端配置
                let client_config = quinn::ClientConfig::with_platform_verifier();
                
                // 创建 Endpoint
                let mut endpoint = match quinn::Endpoint::client("0.0.0.0:0".parse().unwrap()) {
                    Ok(ep) => ep,
                    Err(e) => {
                        eh.on_error(FlareError::connection_failed(format!("创建QUIC端点失败: {}", e)));
                        return;
                    }
                };
                endpoint.set_default_client_config(client_config);
                
                // 连接远程地址
                let connection = match endpoint.connect(remote_addr.parse().unwrap(), "localhost").unwrap().await {
                    Ok(conn) => conn,
                    Err(e) => {
                        eh.on_error(FlareError::connection_failed(format!("QUIC连接失败: {}", e)));
                        return;
                    }
                };
                
                // 通知连接已建立
                eh.on_connected();
                
                // 启动读写任务
                let (mut send, mut recv) = match connection.open_bi().await {
                    Ok(stream) => stream,
                    Err(e) => {
                        eh.on_error(FlareError::connection_failed(format!("打开QUIC流失败: {}", e)));
                        return;
                    }
                };
                
                // 写出任务
                let mut rx_clone = rx; // 直接使用rx，而不是克隆
                let eh_clone: Arc<dyn ConnectionEvent> = Arc::clone(&eh);
                tokio::spawn(async move {
                    while let Some(bytes) = rx_clone.recv().await {
                        if let Err(e) = send.write_all(&bytes).await {
                            eh_clone.on_error(FlareError::connection_failed(format!("QUIC写入失败: {}", e)));
                            break;
                        }
                    }
                });
                
                // 读取任务：使用 MessageProcessor 处理接收到的数据
                let handler_clone: Arc<dyn ConnectionEvent> = Arc::clone(&eh);
                let base_stats_clone = Arc::clone(&base_clone);
                tokio::spawn(async move {
                    let processor = crate::common::messaging::MessageProcessor::default();
                    let mut buffer = Vec::new();
                    let mut read_buf = [0u8; 1024];
                    
                    loop {
                        match recv.read(&mut read_buf).await {
                            Ok(Some(n)) => {
                                buffer.extend_from_slice(&read_buf[..n]);
                                
                                // 使用 MessageProcessor 解析二进制数据
                                match processor.process_receive_auto(&buffer).await {
                                    Ok(frame) => {
                                        let bytes_len = buffer.len();
                                        buffer.clear();
                                        
                                        // 更新统计信息
                                        base_stats_clone.update_stats(0, 0, 1, bytes_len as u64);
                                        
                                        // 通过事件处理器传递给上层
                                        handler_clone.on_message_received(frame);
                                    }
                                    Err(_) => {
                                        // 解析失败可能是数据不完整，继续累积
                                        if buffer.len() > 1024 * 1024 {
                                            buffer.clear();
                                        }
                                    }
                                }
                            }
                            Ok(None) => {
                                // 连接关闭
                                break;
                            }
                            Err(e) => {
                                handler_clone.on_error(FlareError::connection_failed(format!("QUIC读取失败: {}", e)));
                                break;
                            }
                        }
                    }
                });
            });
        }
        Ok(())
    }
    
    fn disconnect(&self, reason: Option<String>) -> Result<(), FlareError> {
        if let Some(h) = self.base.get_event_handler() {
            h.on_disconnected(reason);
        }
        Ok(())
    }
}

