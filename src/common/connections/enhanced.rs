//! 增强型通用连接实现
//!
//! 该模块提供了一个增强型的通用连接实现，整合了WebSocket和QUIC连接的共性功能，
//! 提供了标准化的长连接抽象层。

use crate::common::connections::traits::{BaseConnection, ConnectionEvent};
use crate::common::connections::types::ConnectionStats;
use crate::common::connections::enums::ConnectionState;
use crate::common::connections::config::ConnectionConfig;
use crate::common::protocol::frame::Frame;
use crate::common::error::FlareError;
use crate::common::connections::monitor::{compute_quality, is_heartbeat_timeout};
use crate::common::parsing::parser::MessageParser;
use crate::common::parsing::codec::PayloadCodec;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use tokio::time::interval;
use uuid::Uuid;

/// 增强型通用连接结构
///
/// 提供跨协议的增强型通用连接功能，整合了WebSocket和QUIC连接的共性功能
pub struct EnhancedConnection {
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
    /// 心跳任务句柄（使用互斥锁保护）
    heartbeat_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// 心跳间隔（毫秒）
    heartbeat_interval_ms: u64,
    /// 消息解析器
    parser: MessageParser,
    /// 消息发送通道（使用互斥锁保护）
    outbound_tx: Mutex<Option<tokio::sync::mpsc::UnboundedSender<Vec<u8>>>>,
}

impl EnhancedConnection {
    /// 创建新的增强型通用连接实例
    ///
    /// # 参数
    /// * `config` - 连接配置
    ///
    /// # 返回值
    /// 新创建的增强型通用连接实例
    pub fn new(config: ConnectionConfig) -> Self {
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
            heartbeat_handle: Mutex::new(None),
            heartbeat_interval_ms: config.heartbeat_interval_ms.unwrap_or(10000),
            parser: MessageParser::new(config.serialization_codec.unwrap_or(PayloadCodec::Json)),
            outbound_tx: Mutex::new(None),
        }
    }

    /// 获取连接ID
    ///
    /// # 返回值
    /// 连接的唯一标识符字符串
    pub fn id(&self) -> String {
        self.id.clone()
    }

    /// 获取统计信息
    ///
    /// # 返回值
    /// 连接的统计信息副本
    pub fn stats(&self) -> ConnectionStats {
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
    pub fn last_activity_epoch_ms(&self) -> u64 {
        self.last_activity_epoch_ms
    }

    /// 设置事件处理器
    ///
    /// # 参数
    /// * `handler` - 事件处理器实例
    pub fn set_event_handler(&self, handler: Arc<dyn ConnectionEvent>) {
        if let Ok(mut g) = self.handler.lock() {
            *g = Some(handler);
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

    /// 发送消息
    ///
    /// # 参数
    /// * `frame` - 要发送的消息帧
    ///
    /// # 返回值
    /// 操作结果，成功返回Ok(())，失败返回相应的错误
    pub fn send_message(&self, frame: Frame) -> Result<(), FlareError> {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        
        // 使用 MessageParser 编码 Frame
        let bytes = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.parser.encode_frame(&frame).await
            })
        })?;
        
        if let Ok(mut s) = self.stats.lock() {
            s.messages_sent = s.messages_sent.saturating_add(1);
            s.bytes_sent = s.bytes_sent.saturating_add(bytes.len() as u64);
            s.last_activity_epoch_ms = now_ms;
        }
        
        // 通过发送通道发送消息
        if let Ok(g) = self.outbound_tx.lock() { 
            if let Some(tx) = &*g { 
                tx.send(bytes).map_err(|e| FlareError::connection_failed(format!("发送失败: {}", e)))?; 
            } 
        }
        
        if let Ok(g) = self.handler.lock() { 
            if let Some(h) = &*g { 
                h.on_message_sent(frame.clone()); 
            } 
        }
        Ok(())
    }

    /// 获取连接状态
    ///
    /// # 返回值
    /// 当前连接状态的副本
    pub fn state(&self) -> ConnectionState {
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
    pub fn ready(&self) -> Result<(), FlareError> {
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
    pub fn connected(&self) -> Result<(), FlareError> {
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
    pub fn set_state(&self, state: ConnectionState) -> Result<(), FlareError> {
        if let Ok(mut s) = self.state.lock() {
            *s = state;
            Ok(())
        } else {
            Err(FlareError::general_error("无法设置连接状态".to_string()))
        }
    }

    /// 启动心跳任务
    ///
    /// # 返回值
    /// 操作结果，成功返回Ok(())，失败返回相应的错误
    pub fn start_heartbeat_task(&self) -> Result<(), FlareError> {
        if let Ok(g) = self.handler.lock() {
            if let Some(h) = &*g {
                let eh: Arc<dyn ConnectionEvent> = Arc::clone(h);
                let interval_ms = self.heartbeat_interval_ms;
                let stats_arc: Arc<Mutex<ConnectionStats>> = Arc::clone(&self.stats);
                let handle = tokio::spawn(async move {
                    let mut interval = interval(Duration::from_millis(interval_ms));
                    let mut missed: u32 = 0;
                    loop {
                        interval.tick().await;
                        // 发送心跳Ping帧
                        eh.on_heartbeat_ping();
                        
                        // 更新统计（心跳与消息）
                        if let Ok(mut s) = stats_arc.lock() {
                            s.heartbeat_pings = s.heartbeat_pings.saturating_add(1);
                            s.messages_sent = s.messages_sent.saturating_add(1);
                        }
                        
                        // 超时与质量更新（占位：未接收Pong时计数）
                        missed = missed.saturating_add(1);
                        eh.on_heartbeat_timeout();
                        let q = compute_quality(None, missed);
                        eh.on_quality_changed(q);
                    }
                });
                if let Ok(mut hh) = self.heartbeat_handle.lock() { *hh = Some(handle); }
            }
        }
        Ok(())
    }

    /// 停止心跳任务
    ///
    /// # 返回值
    /// 操作结果，成功返回Ok(())，失败返回相应的错误
    pub fn stop_heartbeat_task(&self) -> Result<(), FlareError> {
        if let Ok(mut g) = self.heartbeat_handle.lock() { 
            if let Some(handle) = g.take() { 
                handle.abort(); 
            } 
        }
        Ok(())
    }

    /// 设置发送通道
    ///
    /// # 参数
    /// * `tx` - 消息发送通道
    pub fn set_outbound_tx(&self, tx: tokio::sync::mpsc::UnboundedSender<Vec<u8>>) {
        if let Ok(mut g) = self.outbound_tx.lock() { 
            *g = Some(tx); 
        }
    }

    /// 获取发送通道
    ///
    /// # 返回值
    /// 消息发送通道的可选引用
    pub fn get_outbound_tx(&self) -> Option<tokio::sync::mpsc::UnboundedSender<Vec<u8>>> {
        if let Ok(g) = self.outbound_tx.lock() { 
            g.clone() 
        } else { 
            None 
        }
    }

    /// 处理接收到的消息
    ///
    /// # 参数
    /// * `data` - 接收到的数据
    ///
    /// # 返回值
    /// 操作结果，成功返回Ok(())，失败返回相应的错误
    pub fn handle_received_message(&self, data: Vec<u8>) -> Result<(), FlareError> {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        
        // 更新统计信息
        if let Ok(mut s) = self.stats.lock() {
            s.messages_received = s.messages_received.saturating_add(1);
            s.bytes_received = s.bytes_received.saturating_add(data.len() as u64);
            s.last_activity_epoch_ms = now_ms;
        }
        
        // 使用 MessageParser 解码数据
        let frame = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.parser.parse_bytes(&data).await
            })
        })?;
        
        // 通知事件处理器
        if let Ok(g) = self.handler.lock() { 
            if let Some(h) = &*g { 
                h.on_message_received(frame); 
            } 
        }
        
        Ok(())
    }

    /// 处理心跳Pong
    ///
    /// # 参数
    /// * `rtt_ms` - 往返时间（毫秒）
    ///
    /// # 返回值
    /// 操作结果，成功返回Ok(())，失败返回相应的错误
    pub fn handle_pong(&self, rtt_ms: u32) -> Result<(), FlareError> {
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
        
        Ok(())
    }

    /// 检查心跳超时
    ///
    /// # 返回值
    /// 操作结果，成功返回Ok(())，失败返回相应的错误
    pub fn check_heartbeat_timeout(&self) -> Result<(), FlareError> {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        if is_heartbeat_timeout(self.last_activity_epoch_ms, now_ms, self.heartbeat_interval_ms * 3) {
            if let Ok(mut s) = self.stats.lock() { 
                s.missed_heartbeats = s.missed_heartbeats.saturating_add(1); 
            }
            if let Ok(g) = self.handler.lock() { 
                if let Some(h) = &*g { 
                    h.on_heartbeat_timeout(); 
                } 
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
        
        Ok(())
    }
}

impl BaseConnection for EnhancedConnection {
    fn send_bytes(&self, bytes: Vec<u8>) -> Result<(), FlareError> {
        // 直接通过发送通道发送二进制数据
        if let Ok(g) = self.outbound_tx.lock() { 
            if let Some(tx) = &*g { 
                tx.send(bytes.clone()).map_err(|e| FlareError::connection_failed(format!("发送失败: {}", e)))?; 
            } else {
                return Err(FlareError::connection_failed("发送通道未初始化".to_string()));
            }
        } else {
            return Err(FlareError::connection_failed("无法获取发送通道锁".to_string()));
        }
        
        // 更新统计信息
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        if let Ok(mut s) = self.stats.lock() {
            s.messages_sent = s.messages_sent.saturating_add(1);
            s.bytes_sent = s.bytes_sent.saturating_add(bytes.len() as u64);
            s.last_activity_epoch_ms = now_ms;
        }
        
        Ok(())
    }

    fn set_event_handler(&self, handler: Arc<dyn ConnectionEvent>) {
        self.set_event_handler(handler);
    }

    fn state(&self) -> ConnectionState {
        self.state()
    }

    fn ready(&self) -> Result<(), FlareError> {
        self.ready()
    }

    fn connected(&self) -> Result<(), FlareError> {
        self.connected()
    }

    fn set_state(&self, state: ConnectionState) -> Result<(), FlareError> {
        self.set_state(state)
    }

    fn stats(&self) -> ConnectionStats {
        self.stats()
    }

    fn last_activity_epoch_ms(&self) -> u64 {
        self.last_activity_epoch_ms()
    }

    fn id(&self) -> String {
        self.id()
    }
}

/// 增强型通用连接工厂
///
/// 用于创建增强型通用连接实例
pub struct EnhancedConnectionFactory;

impl EnhancedConnectionFactory {
    /// 创建增强型通用连接
    ///
    /// # 参数
    /// * `config` - 连接配置
    ///
    /// # 返回值
    /// 增强型通用连接实例
    pub fn create_connection(config: ConnectionConfig) -> EnhancedConnection {
        EnhancedConnection::new(config)
    }
}