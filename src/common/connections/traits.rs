//! 连接抽象接口定义
//!
//! # 设计原则
//!
//! ## 1. 统一连接抽象
//! 提供统一的连接接口，屏蔽底层协议（WebSocket、QUIC等）的差异。
//!
//! ## 2. 长连接标准
//! 定义长连接的标准化接口和行为规范，包括：
//! - 连接状态管理
//! - 心跳机制
//! - 统计信息收集
//! - 消息处理流程
//! - 错误处理机制
//!
//! ## 3. 协议无关性
//! 连接抽象不依赖于特定的传输协议，可以适配不同的传输协议。

use crate::common::connections::enums::ConnectionState;
use crate::common::connections::types::ConnectionStats;
use crate::common::error::FlareError;
use std::sync::Arc;
use crate::common::protocol::frame::Frame;

// ============================================================================
// 事件回调接口（观察者模式）
// ============================================================================

/// 连接事件回调接口
///
/// 实现此 trait 以监听连接的各种事件（连接、断开、消息、心跳等）。
/// 所有方法都有默认空实现，只需实现关心的事件即可。
///
/// # 线程安全
/// 事件回调可能在任意线程调用，因此必须实现 `Send + Sync`。
pub trait ConnectionEvent: Send + Sync + 'static {
    /// 连接建立成功时触发
    fn on_connected(&self) {}
    
    /// 连接断开时触发
    ///
    /// # 参数
    /// - `reason`: 断开原因（None 表示正常断开）
    fn on_disconnected(&self, _reason: Option<String>) {}
    
    /// 发生错误时触发
    fn on_error(&self, _err: FlareError) {}
    
    /// 接收到消息时触发
    fn on_message_received(&self, _frame: Frame) {}
    
    /// 消息发送成功时触发
    fn on_message_sent(&self, _frame: Frame) {}
    
    /// 发送心跳 Ping 时触发
    fn on_heartbeat_ping(&self) {}
    
    /// 接收到心跳 Pong 时触发
    ///
    /// # 参数
    /// - `rtt_ms`: 往返时间（毫秒）
    fn on_heartbeat_pong(&self, _rtt_ms: u32) {}
    
    /// 心跳超时时触发
    fn on_heartbeat_timeout(&self) {}
    
    /// 连接质量变化时触发
    ///
    /// # 参数
    /// - `quality`: 质量评分（0-100）
    fn on_quality_changed(&self, _quality: u8) {}
    
    /// 统计信息更新时触发
    fn on_statistics_updated(&self, _stats: ConnectionStats) {}
    
    /// 开始重连时触发（客户端）
    fn on_reconnect_started(&self) {}
    
    /// 重连成功时触发（客户端）
    fn on_reconnected(&self) {}
    
    /// 重连失败时触发（客户端）
    fn on_reconnect_failed(&self, _err: FlareError) {}
    
    /// 为类型转换提供支持
    fn as_any(&self) -> &dyn std::any::Any where Self: Sized {
        self as &dyn std::any::Any
    }
}

// ============================================================================
// 统一连接接口
// ============================================================================

/// 连接基础能力接口
///
/// 此 trait 定义了连接的通用能力：
/// - 二进制数据传输（核心功能）
/// - 状态查询
/// - 统计信息
/// - 事件订阅
///
/// # 设计原则
/// - **统一抽象**：屏蔽底层协议差异
/// - **通用性**：适用于各种传输协议
/// - **职责单一**：连接层只负责二进制传输，协议处理由上层统一管理
///
/// # 架构说明
///
/// 连接层专注于二进制数据传输，协议处理（编码、压缩、解析）由外部的 `MessageProcessor` 统一处理。
/// 
/// 典型使用流程：
/// ```rust,no_run
/// use flare_core::common::messaging::MessageProcessor;
/// use flare_core::common::protocol::frame::Frame;
///
/// // 1. 上层构建 Frame
/// let frame = Frame::new(...);
///
/// // 2. 使用 MessageProcessor 处理（编码 + 压缩）
/// let processor = MessageProcessor::default();
/// let bytes = processor.process_send(&frame).await?;
///
/// // 3. 连接层只负责传输二进制数据
/// connection.send_bytes(bytes)?;
/// ```
pub trait BaseConnection: Send + Sync {
    /// 发送二进制数据（核心方法）
    ///
    /// 连接层只负责二进制数据的传输，不进行任何协议处理。
    /// 协议处理（编码、压缩）应由外部的 `MessageProcessor` 完成。
    ///
    /// # 参数
    /// - `bytes`: 要发送的二进制数据（已编码和压缩的最终数据）
    ///
    /// # 返回
    /// - `Ok(())`: 数据已成功加入发送队列
    /// - `Err(FlareError)`: 发送失败
    ///
    /// # 示例
    /// ```rust,ignore
    /// let bytes = vec![1, 2, 3, 4, 5];
    /// connection.send_bytes(bytes)?;
    /// ```
    fn send_bytes(&self, bytes: Vec<u8>) -> Result<(), FlareError>;
    
    /// 设置事件处理器
    ///
    /// # 参数
    /// - `handler`: 事件回调实现
    fn set_event_handler(&self, handler: Arc<dyn ConnectionEvent>);
    
    /// 获取当前连接状态
    ///
    /// # 返回
    /// 连接状态枚举
    fn state(&self) -> ConnectionState;
    
    /// 标记连接为就绪状态
    ///
    /// # 返回
    /// 操作结果
    fn ready(&self) -> Result<(), FlareError>;
    
    /// 标记连接为已建立状态
    ///
    /// # 返回
    /// 操作结果
    fn connected(&self) -> Result<(), FlareError>;
    
    /// 设置连接状态为指定状态
    ///
    /// # 参数
    /// - `state`: 要设置的连接状态
    ///
    /// # 返回
    /// 操作结果
    fn set_state(&self, state: ConnectionState) -> Result<(), FlareError>;
    
    /// 获取统计信息
    ///
    /// # 返回
    /// 连接的统计数据
    fn stats(&self) -> ConnectionStats;
    
    /// 获取最后活动时间
    ///
    /// # 返回
    /// Unix 时间戳（毫秒），表示最后一次收发消息的时间
    fn last_activity_epoch_ms(&self) -> u64;
    
    /// 获取连接ID
    ///
    /// # 返回
    /// 唯一标识此连接的字符串
    fn id(&self) -> String;
}

// ============================================================================
// 对外接口层（客户端 & 服务端）
// ============================================================================

/// 客户端连接接口
///
/// 定义客户端连接的完整能力：
/// - 主动连接服务器（`connect`）
/// - 主动断开连接（`disconnect`）
/// - 继承基础能力（消息收发、状态查询等，来自 `BaseConnection`）
///
/// # 生命周期
/// ```text
/// Idle -> connect() -> Connecting -> Connected -> disconnect() -> Disconnected
///                          ↓ 失败         ↓ 异常断开
///                      Disconnected -> reconnect (可选)
/// ```
///
/// # 与 ServerConnection 的区别
/// - **ClientConnection**: 主动发起连接（`connect`），支持重连
/// - **ServerConnection**: 被动接受连接（`accept`），无重连逻辑
///
/// # 实现者
/// - `WebSocketClient` (在 client 模块)
/// - `QuicClient` (在 client 模块)
/// - 未来的 `Http3Client`, `GrpcClient` 等
///
/// # 示例
/// ```rust,no_run
/// use flare_core::common::connections::config::ConnectionConfig;
/// use flare_core::common::connections::traits::{ClientConnection, ConnectionEvent};
/// use flare_core::common::connections::enums::Transport;
/// use std::sync::Arc;
///
/// struct MyHandler;
/// impl ConnectionEvent for MyHandler {
///     fn on_connected(&self) {
///         println!("Connected to server!");
///     }
/// }
///
/// fn connect_to_server(client: Arc<dyn ClientConnection>) {
///     // 1. 设置事件处理器
///     client.set_event_handler(Arc::new(MyHandler));
///     
///     // 2. 连接服务器
///     client.connect().expect("Failed to connect");
///     
///     // 3. 发送消息
///     // client.send_message(...);
///     
///     // 4. 断开连接
///     client.disconnect(Some("User logout".to_string())).ok();
/// }
/// ```
pub trait ClientConnection: BaseConnection {
    /// 连接到服务器
    ///
    /// 此方法会**异步启动**连接任务，通常不会阻塞。
    /// 连接结果通过 `ConnectionEvent::on_connected` 或 `on_error` 通知。
    ///
    /// # 返回
    /// - `Ok(())`: 连接任务已启动
    /// - `Err(FlareError)`: 启动失败（参数错误、资源不足等）
    ///
    /// # 状态转换
    /// `Idle` → `Connecting` → `Connected` (成功) / `Disconnected` (失败)
    ///
    /// # 示例
    /// ```rust,ignore
    /// client.connect()?;
    /// // 等待 on_connected 事件...
    /// ```
    fn connect(&self) -> Result<(), FlareError>;
    
    /// 断开连接
    ///
    /// 主动断开与服务器的连接。
    ///
    /// # 参数
    /// - `reason`: 断开原因（可选），会传递给事件处理器
    ///
    /// # 返回
    /// - `Ok(())`: 断开任务已启动
    /// - `Err(FlareError)`: 断开失败（已经断开等）
    ///
    /// # 状态转换
    /// `Connected` → `Disconnecting` → `Disconnected`
    ///
    /// # 示例
    /// ```rust,ignore
    /// client.disconnect(Some("User logout".to_string()))?;
    /// ```
    fn disconnect(&self, reason: Option<String>) -> Result<(), FlareError>;
    
    /// 发送消息（便利方法）
    ///
    /// 使用 MessageProcessor 处理 Frame（编码+压缩），然后通过 send_bytes 发送。
    /// 这是向后兼容的便利方法，建议直接使用 send_bytes 以获得更好的控制。
    ///
    /// # 参数
    /// - `frame`: 要发送的消息帧
    ///
    /// # 返回
    /// - `Ok(())`: 消息已成功发送
    /// - `Err(FlareError)`: 发送失败
    fn send_message(&self, frame: Frame) -> Result<(), FlareError> {
        use crate::common::messaging::MessageProcessor;
        
        // 使用 MessageProcessor 处理 Frame → 二进制
        let processor = MessageProcessor::default();
        let bytes = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                processor.process_send(&frame).await
            })
        })?;
        
        // 连接层只负责发送二进制数据
        self.send_bytes(bytes)
    }
}

/// 服务端连接接口
///
/// 定义服务端连接的完整能力：
/// - 接受客户端连接（`accept`）
/// - 关闭连接（`close`）
/// - 继承基础能力（消息收发、状态查询等，来自 `BaseConnection`）
///
/// # 生命周期
/// ```text
/// Pending -> accept() -> Accepting -> Connected -> close() -> Closed
///                           ↓ 失败         ↓ 异常断开
///                         Closed        Closed
/// ```
///
/// # 与 ClientConnection 的区别
/// - **ServerConnection**: 被动接受连接（`accept`），由监听器创建
/// - **ClientConnection**: 主动发起连接（`connect`），由用户创建
///
/// # 实现者
/// - `WebSocketServerConn` (在 server 模块)
/// - `QuicServerConn` (在 server 模块)
/// - 未来的 `Http3ServerConn`, `GrpcServerConn` 等
///
/// # 示例
/// ```rust,no_run
/// use flare_core::common::connections::traits::{ServerConnection, ConnectionEvent};
/// use std::sync::Arc;
///
/// struct ServerHandler;
/// impl ConnectionEvent for ServerHandler {
///     fn on_connected(&self) {
///         println!("Client connected!");
///     }
/// }
///
/// fn handle_client(conn: Arc<dyn ServerConnection>) {
///     // 1. 设置事件处理器
///     conn.set_event_handler(Arc::new(ServerHandler));
///     
///     // 2. 接受连接（完成握手）
///     conn.accept().expect("Failed to accept");
///     
///     // 3. 处理消息（通过事件回调）
///     // ...
///     
///     // 4. 关闭连接
///     conn.close(Some("Server shutdown".to_string())).ok();
/// }
/// ```
pub trait ServerConnection: BaseConnection {
    /// 接受客户端连接
    ///
    /// 完成握手并启动读写任务。此方法通常由监听器在接收到新连接后调用。
    ///
    /// # 返回
    /// - `Ok(())`: 连接已接受，读写任务已启动
    /// - `Err(FlareError)`: 接受失败（握手失败、资源不足等）
    ///
    /// # 状态转换
    /// `Pending` → `Accepting` → `Connected` (成功) / `Closed` (失败)
    ///
    /// # 注意
    /// 通常在设置事件处理器后立即调用此方法。
    ///
    /// # 示例
    /// ```rust,ignore
    /// conn.accept()?;
    /// // 开始接收消息...
    /// ```
    fn accept(&self) -> Result<(), FlareError>;
    
    /// 关闭连接
    ///
    /// 主动关闭与客户端的连接。
    ///
    /// # 参数
    /// - `reason`: 关闭原因（可选），会传递给事件处理器
    ///
    /// # 返回
    /// - `Ok(())`: 关闭任务已启动
    /// - `Err(FlareError)`: 关闭失败（已经关闭等）
    ///
    /// # 状态转换
    /// `Connected` → `Closing` → `Closed`
    ///
    /// # 示例
    /// ```rust,ignore
    /// conn.close(Some("Idle timeout".to_string()))?;
    /// ```
    fn close(&self, reason: Option<String>) -> Result<(), FlareError>;
    
    /// 发送消息（便利方法）
    ///
    /// 使用 MessageProcessor 处理 Frame（编码+压缩），然后通过 send_bytes 发送。
    /// 这是向后兼容的便利方法，建议直接使用 send_bytes 以获得更好的控制。
    ///
    /// # 参数
    /// - `frame`: 要发送的消息帧
    ///
    /// # 返回
    /// - `Ok(())`: 消息已成功发送
    /// - `Err(FlareError)`: 发送失败
    fn send_message(&self, frame: Frame) -> Result<(), FlareError> {
        use crate::common::messaging::MessageProcessor;
        
        // 使用 MessageProcessor 处理 Frame → 二进制
        let processor = MessageProcessor::default();
        let bytes = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                processor.process_send(&frame).await
            })
        })?;
        
        // 连接层只负责发送二进制数据
        self.send_bytes(bytes)
    }
}
