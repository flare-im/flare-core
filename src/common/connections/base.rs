//! 基础连接结构
//!
//! 该模块定义了所有连接类型的基础结构，包含WebSocket和QUIC连接的公共功能。

use crate::common::connections::traits::{BaseConnection, ConnectionEvent};
use crate::common::connections::types::ConnectionStats;
use crate::common::connections::enums::ConnectionState;
use crate::common::connections::config::ConnectionConfig;
use crate::common::protocol::frame::Frame;
use crate::common::error::FlareError;
use crate::common::connections::monitor::compute_quality;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// 基础连接结构
///
/// 包含所有连接类型的核心功能：
/// - 连接状态管理
/// - 统计信息收集
/// - 事件处理机制
pub struct BaseConn {
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
    /// 最大允许丢失的心跳数
    max_missed_heartbeats: u32,
}

impl BaseConn {
    /// 创建新的基础连接实例
    ///
    /// # 参数
    /// * `config` - 连接配置信息
    ///
    /// # 返回值
    /// 新创建的基础连接实例
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
            heartbeat_interval_ms: config.heartbeat_interval_ms.unwrap_or(10000),
            heartbeat_timeout_ms: config.heartbeat_timeout_ms.unwrap_or(30000),
            max_missed_heartbeats: config.max_missed_heartbeats.unwrap_or(3),
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
        // 注意：由于Rust的所有权规则，我们不能直接修改self.last_activity_epoch_ms
        // 这个字段会在需要时通过stats.last_activity_epoch_ms获取
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
        
        // 注意：由于Rust的所有权规则，我们不能直接修改self.last_activity_epoch_ms
        // 这个字段会在需要时通过stats.last_activity_epoch_ms获取
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
        
        // 注意：由于Rust的所有权规则，我们不能直接修改self.last_activity_epoch_ms
        // 这个字段会在需要时通过stats.last_activity_epoch_ms获取
    }
}

impl BaseConnection for BaseConn {
    fn send_bytes(&self, _bytes: Vec<u8>) -> Result<(), FlareError> {
        // 基础连接不实现具体的二进制发送逻辑，由具体的连接类型实现
        Err(FlareError::general_error("基础连接不实现二进制发送功能".to_string()))
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