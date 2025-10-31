//! 心跳管理模块
//! 
//! 提供心跳机制的实现，保持连接活跃

use crate::common::error::Result;
use crate::common::message_parser::MessageParser;
use crate::transport::connection::Connection;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use std::time::Duration;
use tokio::time::interval;

/// 心跳管理器
pub struct HeartbeatManager {
    interval: Duration,
    timeout: Duration,
    // 使用 std::sync::Mutex，因为 record_pong 可能从同步上下文调用
    last_pong: Arc<std::sync::Mutex<Option<std::time::Instant>>>,
    stop_tx: Option<mpsc::Sender<()>>,
}

impl HeartbeatManager {
    /// 创建新的心跳管理器
    /// 
    /// # 参数
    /// - `interval`: 心跳发送间隔
    /// - `timeout`: 等待 PONG 的超时时间
    pub fn new(interval: Duration, timeout: Duration) -> Self {
        Self {
            interval,
            timeout,
            last_pong: Arc::new(std::sync::Mutex::new(None)),
            stop_tx: None,
        }
    }

    /// 启动心跳
    /// 
    /// # 参数
    /// - `connection`: 连接实例
    /// - `parser`: 消息解析器（用于序列化 ping 消息）
    /// 
    /// # 返回
    /// 停止心跳的发送端
    pub fn start(
        &mut self,
        connection: Arc<Mutex<Box<dyn Connection>>>,
        parser: MessageParser,
    ) {
        let (tx, mut rx) = mpsc::channel(1);
        self.stop_tx = Some(tx);
        
        let interval_duration = self.interval;
        let timeout_duration = self.timeout;
        let last_pong = Arc::clone(&self.last_pong);
        
        tokio::spawn(async move {
            let mut interval_timer = interval(interval_duration);
            
            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        // 发送心跳
                        let ping_frame = crate::common::protocol::frame_with_system_command(
                            crate::common::protocol::ping(),
                            crate::common::protocol::Reliability::AtLeastOnce,
                        );
                        
                        let data = match parser.serialize(&ping_frame) {
                            Ok(d) => d,
                            Err(_) => continue,
                        };
                        
                        // 使用 tokio::sync::Mutex，支持跨 await
                        let send_result = {
                            let mut conn = connection.lock().await;
                            conn.send(&data).await
                        };
                        
                        if send_result.is_ok() {
                            // 检查是否超时（在锁外检查）
                            let should_close = {
                                let last = last_pong.lock().unwrap();
                                if let Some(last_pong_time) = *last {
                                    last_pong_time.elapsed() > timeout_duration
                                } else {
                                    false
                                }
                            };
                            
                            if should_close {
                                // 心跳超时，触发断开
                                {
                                    let mut conn = connection.lock().await;
                                    let _ = conn.close().await;
                                }
                                break;
                            }
                        }
                    }
                    _ = rx.recv() => {
                        break;
                    }
                }
            }
        });
    }

    /// 停止心跳
    pub fn stop(&mut self) {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }
    }

    /// 记录收到 PONG
    pub fn record_pong(&self) {
        if let Ok(mut last) = self.last_pong.lock() {
            *last = Some(std::time::Instant::now());
        }
    }

    /// 检查心跳是否超时
    pub fn is_timeout(&self) -> bool {
        if let Ok(last) = self.last_pong.lock() {
            if let Some(last_pong_time) = *last {
                return last_pong_time.elapsed() > self.timeout;
            }
        }
        // 如果从未收到 PONG，且启动后已超过超时时间，则认为超时
        false
    }
}

