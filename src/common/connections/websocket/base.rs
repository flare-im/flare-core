//! WebSocket 基础连接结构
//!
//! 提供 WebSocket 连接的 BaseConnection 实现

use crate::common::connections::traits::{BaseConnection, ConnectionEvent};
use crate::common::connections::types::ConnectionStats;
use crate::common::connections::enums::ConnectionState;
use crate::common::connections::config::ConnectionConfig;
use crate::common::protocol::frame::Frame;
use crate::common::error::FlareError;
use crate::common::connections::monitor::compute_quality;
use crate::common::messaging::MessageProcessor;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::StreamExt;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::MaybeTlsStream;
use uuid::Uuid;

/// WebSocket 基础连接结构（只实现 BaseConnection）
pub struct WebSocketBaseConn {
    /// 连接唯一标识符
    id: String,
    /// 连接状态（使用互斥锁保护）
    state: Arc<Mutex<ConnectionState>>,
    /// 连接统计信息（使用互斥锁保护）
    stats: Arc<Mutex<ConnectionStats>>,
    /// 最后活动时间戳（毫秒）
    last_activity_epoch_ms: u64,
    /// 事件处理器（使用互斥锁保护）
    handler: Mutex<Option<Arc<dyn ConnectionEvent>>>,
    /// 心跳间隔（毫秒）
    heartbeat_interval_ms: u64,
    /// 心跳超时时间（毫秒）
    heartbeat_timeout_ms: u64,
    /// WebSocket写入端（使用互斥锁保护）
    pub write: Arc<Mutex<Option<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>>,
    /// WebSocket读取端（使用互斥锁保护）
    pub read: Arc<Mutex<Option<SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>>>>,
    /// 消息发送通道（使用互斥锁保护）
    pub outbound_tx: Mutex<Option<tokio::sync::mpsc::UnboundedSender<Vec<u8>>>>,
}

impl WebSocketBaseConn {
    /// 从配置创建WebSocket基础连接
    /// 
    /// # 参数
    /// * `config` - 连接配置信息
    /// 
    /// # 返回值
    /// 新创建的WebSocketBaseConn实例
    pub fn from_config(config: ConnectionConfig) -> Self {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        let conn_id = config.id.clone().unwrap_or_else(|| Uuid::new_v4().to_string());
        
        Self {
            id: conn_id,
            state: Arc::new(Mutex::new(ConnectionState::Initializing)),
            stats: Arc::new(Mutex::new(ConnectionStats { 
                established_epoch_ms: now_ms, 
                last_activity_epoch_ms: now_ms, 
                ..Default::default() 
            })),
            last_activity_epoch_ms: now_ms,
            handler: Mutex::new(None),
            heartbeat_interval_ms: config.heartbeat_interval_ms.unwrap_or(10000),
            heartbeat_timeout_ms: config.heartbeat_timeout_ms.unwrap_or(30000),
            write: Arc::new(Mutex::new(None)),
            read: Arc::new(Mutex::new(None)),
            outbound_tx: Mutex::new(None),
        }
    }
    
    /// 从WebSocket流创建WebSocket基础连接
    /// 
    /// # 参数
    /// * `stream` - WebSocket流
    /// * `config` - 连接配置信息
    /// 
    /// # 返回值
    /// 新创建的WebSocketBaseConn实例
    pub fn from_websocket_stream(stream: WebSocketStream<MaybeTlsStream<TcpStream>>, config: ConnectionConfig) -> Self {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        let conn_id = config.id.clone().unwrap_or_else(|| Uuid::new_v4().to_string());
        
        // 分离WebSocket流为读取端和写入端
        let (write, read) = stream.split();
        
        Self {
            id: conn_id,
            state: Arc::new(Mutex::new(ConnectionState::Initializing)),
            stats: Arc::new(Mutex::new(ConnectionStats { 
                established_epoch_ms: now_ms, 
                last_activity_epoch_ms: now_ms, 
                ..Default::default() 
            })),
            last_activity_epoch_ms: now_ms,
            handler: Mutex::new(None),
            heartbeat_interval_ms: config.heartbeat_interval_ms.unwrap_or(10000),
            heartbeat_timeout_ms: config.heartbeat_timeout_ms.unwrap_or(30000),
            write: Arc::new(Mutex::new(Some(write))),
            read: Arc::new(Mutex::new(Some(read))),
            outbound_tx: Mutex::new(None),
        }
    }
    
    /// 获取事件处理器
    ///
    /// # 返回值
    /// 事件处理器的可选引用
    pub fn get_event_handler(&self) -> Option<Arc<dyn ConnectionEvent>> {
        if let Ok(g) = self.handler.lock() {
            g.clone()
        } else {
            None
        }
    }
    
    /// 获取心跳间隔（毫秒）
    ///
    /// # 返回值
    /// 心跳间隔（毫秒）
    pub fn heartbeat_interval_ms(&self) -> u64 {
        self.heartbeat_interval_ms
    }
    
    /// 获取心跳超时时间（毫秒）
    ///
    /// # 返回值
    /// 心跳超时时间（毫秒）
    pub fn heartbeat_timeout_ms(&self) -> u64 {
        self.heartbeat_timeout_ms
    }
    
    /// 更新统计信息
    ///
    /// # 参数
    /// * `messages_sent` - 发送的消息数
    /// * `bytes_sent` - 发送的字节数
    /// * `messages_received` - 接收的消息数
    /// * `bytes_received` - 接收的字节数
    pub fn update_stats(&self, messages_sent: u64, bytes_sent: u64, messages_received: u64, bytes_received: u64) {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        if let Ok(mut s) = self.stats.lock() {
            s.messages_sent = s.messages_sent.saturating_add(messages_sent);
            s.bytes_sent = s.bytes_sent.saturating_add(bytes_sent);
            s.messages_received = s.messages_received.saturating_add(messages_received);
            s.bytes_received = s.bytes_received.saturating_add(bytes_received);
            s.last_activity_epoch_ms = now_ms;
        }
    }

    /// 处理心跳Ping
    pub fn handle_heartbeat_ping(&self) {
        if let Ok(g) = self.handler.lock() {
            if let Some(h) = &*g {
                h.on_heartbeat_ping();
            }
        }
        
        // 更新统计信息
        if let Ok(mut s) = self.stats.lock() {
            s.heartbeat_pings = s.heartbeat_pings.saturating_add(1);
            s.messages_sent = s.messages_sent.saturating_add(1);
        }
    }

    /// 处理心跳Pong
    ///
    /// # 参数
    /// * `rtt_ms` - 往返时间（毫秒）
    pub fn handle_heartbeat_pong(&self, rtt_ms: u32) {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        
        if let Ok(mut s) = self.stats.lock() {
            s.avg_rtt_ms = Some(rtt_ms);
            s.heartbeat_pongs = s.heartbeat_pongs.saturating_add(1);
            s.missed_heartbeats = 0;
            s.last_activity_epoch_ms = now_ms;
        }
        
        if let Ok(g) = self.handler.lock() {
            if let Some(h) = &*g {
                h.on_heartbeat_pong(rtt_ms);
            }
        }
        
        // 更新连接质量
        let (avg_rtt, missed) = if let Ok(s) = self.stats.lock() {
            (s.avg_rtt_ms, s.missed_heartbeats)
        } else {
            (Some(rtt_ms), 0)
        };
        let new_quality = compute_quality(avg_rtt, missed);
        let mut notify = false;
        if let Ok(mut s) = self.stats.lock() {
            if s.quality != Some(new_quality) {
                s.quality = Some(new_quality);
                notify = true;
            }
        }
        if notify {
            if let Ok(g) = self.handler.lock() {
                if let Some(h) = &*g {
                    h.on_quality_changed(new_quality);
                }
            }
        }
    }

    /// 处理心跳超时
    pub fn handle_heartbeat_timeout(&self) {
        if let Ok(mut s) = self.stats.lock() {
            s.missed_heartbeats = s.missed_heartbeats.saturating_add(1);
        }
        
        if let Ok(g) = self.handler.lock() {
            if let Some(h) = &*g {
                h.on_heartbeat_timeout();
            }
        }
        
        // 更新连接质量
        let (avg_rtt, missed) = if let Ok(s) = self.stats.lock() {
            (s.avg_rtt_ms, s.missed_heartbeats)
        } else {
            (None, 0)
        };
        let new_quality = compute_quality(avg_rtt, missed);
        let mut notify = false;
        if let Ok(mut s) = self.stats.lock() {
            if s.quality != Some(new_quality) {
                s.quality = Some(new_quality);
                notify = true;
            }
        }
        if notify {
            if let Ok(g) = self.handler.lock() {
                if let Some(h) = &*g {
                    h.on_quality_changed(new_quality);
                }
            }
        }
    }
    
    /// 启动接收消息任务
    ///
    /// 从 WebSocket 流读取二进制数据，使用 MessageProcessor 解析后通过事件处理器传递给上层。
    /// 这个方法会从 read 字段中取出 reader 并启动异步接收任务。
    pub fn start_receive_task(&self) -> Result<(), FlareError> {
        // 从 read 中取出 reader（只能取出一次）
        let mut reader_opt = {
            let mut read_guard = self.read.lock().map_err(|_| FlareError::general_error("无法获取读取端锁".to_string()))?;
            read_guard.take()
        };
        
        if reader_opt.is_none() {
            return Err(FlareError::general_error("读取端未初始化或已被使用".to_string()));
        }
        
        let mut reader = reader_opt.unwrap();
        let handler = Arc::new(Mutex::new(self.get_event_handler()));
        let base_stats = self.stats.clone();
        
        tokio::spawn(async move {
            let processor = MessageProcessor::default();
            let mut buffer = Vec::new();
            
            loop {
                match reader.next().await {
                    Some(Ok(Message::Binary(bytes))) => {
                        buffer.extend_from_slice(&bytes);
                        
                        // 使用 MessageProcessor 解析二进制数据
                        match processor.process_receive_auto(&buffer).await {
                            Ok(frame) => {
                                let bytes_len = buffer.len();
                                buffer.clear();
                                
                                // 更新统计信息
                                if let Ok(mut s) = base_stats.lock() {
                                    s.messages_received = s.messages_received.saturating_add(1);
                                    s.bytes_received = s.bytes_received.saturating_add(bytes_len as u64);
                                    s.last_activity_epoch_ms = SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_millis() as u64;
                                }
                                
                                // 通过事件处理器传递给上层
                                if let Ok(g) = handler.lock() {
                                    if let Some(h) = &*g {
                                        h.on_message_received(frame);
                                    }
                                }
                            }
                            Err(_) => {
                                // 解析失败可能是数据不完整，继续累积
                                // 如果缓冲区过大，清空以避免内存泄漏
                                if buffer.len() > 1024 * 1024 {
                                    buffer.clear();
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Text(_))) => {
                        // 文本消息暂不支持，跳过
                    }
                    Some(Ok(Message::Close(_))) => {
                        // 连接关闭
                        break;
                    }
                    Some(Ok(Message::Ping(_))) => {
                        // Ping 消息由底层处理
                    }
                    Some(Ok(Message::Pong(_))) => {
                        // Pong 消息由底层处理
                    }
                    Some(Ok(Message::Frame(_))) => {
                        // Frame 消息（旧版本tungstenite支持）
                    }
                    Some(Err(e)) => {
                        // 读取错误，通知上层
                        if let Ok(g) = handler.lock() {
                            if let Some(h) = &*g {
                                h.on_error(FlareError::connection_failed(format!("WebSocket读取错误: {}", e)));
                            }
                        }
                        break;
                    }
                    None => {
                        // 流结束
                        break;
                    }
                }
            }
        });
        
        Ok(())
    }
}

impl BaseConnection for WebSocketBaseConn {
    /// 发送二进制数据
    /// 
    /// 连接层只负责二进制数据的传输，不进行任何协议处理。
    /// 协议处理应由外部的 MessageProcessor 完成。
    /// 
    /// # 参数
    /// * `bytes` - 要发送的二进制数据（已编码和压缩的最终数据）
    /// 
    /// # 返回值
    /// 操作结果，成功返回Ok(())，失败返回相应的错误
    fn send_bytes(&self, bytes: Vec<u8>) -> Result<(), FlareError> {
        // 通过发送通道发送二进制数据
        if let Ok(g) = self.outbound_tx.lock() { 
            if let Some(tx) = &*g { 
                tx.send(bytes.clone()).map_err(|e| FlareError::connection_failed(format!("发送失败: {}", e)))?; 
            } else {
                return Err(FlareError::connection_failed("发送通道未初始化".to_string()));
            }
        } else {
            return Err(FlareError::connection_failed("无法获取发送通道锁".to_string()));
        }
        
        // 更新统计信息（发送的字节数）
        let bytes_len = bytes.len() as u64;
        self.update_stats(1, bytes_len, 0, 0);
        
        Ok(())
    }
    
    /// 设置事件处理器
    /// 
    /// # 参数
    /// * `handler` - 事件处理器实例
    fn set_event_handler(&self, handler: Arc<dyn ConnectionEvent>) {
        if let Ok(mut g) = self.handler.lock() {
            *g = Some(handler);
        }
    }
    
    /// 获取连接状态
    /// 
    /// # 返回值
    /// 当前连接状态
    fn state(&self) -> ConnectionState {
        if let Ok(s) = self.state.lock() {
            s.clone()
        } else {
            ConnectionState::Error
        }
    }
    
    /// 设置连接状态为就绪
    /// 
    /// # 返回值
    /// 操作结果，成功返回Ok(())，失败返回相应的错误
    fn ready(&self) -> Result<(), FlareError> {
        if let Ok(mut s) = self.state.lock() {
            *s = ConnectionState::Ready;
            Ok(())
        } else {
            Err(FlareError::general_error("无法设置连接状态为就绪".to_string()))
        }
    }
    
    /// 设置连接状态为已建立
    /// 
    /// # 返回值
    /// 操作结果，成功返回Ok(())，失败返回相应的错误
    fn connected(&self) -> Result<(), FlareError> {
        if let Ok(mut s) = self.state.lock() {
            *s = ConnectionState::Connected;
            Ok(())
        } else {
            Err(FlareError::general_error("无法设置连接状态为已建立".to_string()))
        }
    }
    
    /// 设置连接状态为指定状态
    /// 
    /// # 参数
    /// * `state` - 要设置的连接状态
    /// 
    /// # 返回值
    /// 操作结果，成功返回Ok(())，失败返回相应的错误
    fn set_state(&self, state: ConnectionState) -> Result<(), FlareError> {
        if let Ok(mut s) = self.state.lock() {
            *s = state;
            Ok(())
        } else {
            Err(FlareError::general_error("无法设置连接状态".to_string()))
        }
    }
    
    /// 获取统计信息
    /// 
    /// # 返回值
    /// 连接的统计信息
    fn stats(&self) -> ConnectionStats {
        if let Ok(g) = self.stats.lock() {
            g.clone()
        } else {
            Default::default()
        }
    }
    
    /// 获取最后活动时间
    /// 
    /// # 返回值
    /// 最后活动时间戳（毫秒）
    fn last_activity_epoch_ms(&self) -> u64 {
        self.last_activity_epoch_ms
    }
    
    /// 获取连接ID
    /// 
    /// # 返回值
    /// 连接的唯一标识符字符串
    fn id(&self) -> String {
        self.id.clone()
    }
}

