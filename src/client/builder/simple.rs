//! 简单模式客户端构建器
//! 
//! 使用闭包定义消息处理逻辑

use crate::common::error::Result;
use crate::common::protocol::Frame;
use crate::client::{ClientConfig, HybridClient, Client};
use crate::transport::events::{ConnectionEvent, ConnectionObserver};
use std::sync::Arc;
use tokio::sync::Mutex;

/// 客户端消息处理函数类型
pub type ClientMessageHandler = Box<dyn Fn(&Frame) -> Result<()> + Send + Sync>;

/// 客户端事件处理函数类型
pub type ClientEventHandler = Box<dyn Fn(&ConnectionEvent) + Send + Sync>;

/// 简化的客户端观察者
struct SimpleClientObserver {
    message_handler: Option<ClientMessageHandler>,
    event_handler: Option<ClientEventHandler>,
}

impl ConnectionObserver for SimpleClientObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Message(data) => {
                // 解析消息
                if let Ok(frame) = crate::common::MessageParser::new(
                    crate::common::protocol::SerializationFormat::Protobuf,
                    crate::common::compression::CompressionAlgorithm::None,
                ).parse(data) {
                    if let Some(ref handler) = self.message_handler {
                        if let Err(e) = handler(&frame) {
                            tracing::error!("消息处理错误: {:?}", e);
                        }
                    }
                }
            }
            _ => {
                if let Some(ref handler) = self.event_handler {
                    handler(event);
                }
            }
        }
    }
}

/// 简化的客户端实例
pub struct SimpleClient {
    client: Arc<Mutex<HybridClient>>,
    observer: Arc<SimpleClientObserver>,
}

impl SimpleClient {
    /// 连接到服务器
    pub async fn connect(&mut self) -> Result<()> {
        // 先添加观察者
        {
            let mut client = self.client.lock().await;
            client.add_observer(self.observer.clone() as Arc<dyn ConnectionObserver>);
        }
        
        // 然后连接
        let mut client = self.client.lock().await;
        client.connect().await
    }

    /// 断开连接
    pub async fn disconnect(&mut self) -> Result<()> {
        let mut client = self.client.lock().await;
        client.disconnect().await
    }

    /// 发送消息
    pub async fn send_frame(&mut self, frame: &Frame) -> Result<()> {
        let mut client = self.client.lock().await;
        client.send_frame(frame).await
    }

    /// 检查连接状态
    pub fn is_connected(&self) -> bool {
        tokio::task::block_in_place(|| {
            let client = self.client.blocking_lock();
            client.is_connected()
        })
    }

    /// 获取连接 ID
    pub fn connection_id(&self) -> Option<String> {
        tokio::task::block_in_place(|| {
            let client = self.client.blocking_lock();
            client.connection_id()
        })
    }

    /// 获取活动协议
    pub fn active_protocol(&self) -> crate::common::config_types::TransportProtocol {
        tokio::task::block_in_place(|| {
            let client = self.client.blocking_lock();
            client.active_protocol()
        })
    }
}

/// 简单模式客户端构建器
/// 
/// 使用闭包定义消息处理逻辑
pub struct ClientBuilder {
    config: ClientConfig,
    message_handler: Option<ClientMessageHandler>,
    event_handler: Option<ClientEventHandler>,
}

impl ClientBuilder {
    /// 创建新的客户端构建器
    /// 
    /// # 参数
    /// - `server_url`: 服务器地址，例如 "ws://127.0.0.1:8080" 或 "quic://127.0.0.1:8080"
    pub fn new(server_url: impl Into<String>) -> Self {
        Self {
            config: ClientConfig::new(server_url.into()),
            message_handler: None,
            event_handler: None,
        }
    }

    /// 设置消息处理函数
    /// 
    /// # 参数
    /// - `handler`: 消息处理函数，接收 Frame
    pub fn on_message<F>(mut self, handler: F) -> Self
    where
        F: Fn(&Frame) -> Result<()> + Send + Sync + 'static,
    {
        self.message_handler = Some(Box::new(handler));
        self
    }

    /// 设置事件处理函数
    /// 
    /// # 参数
    /// - `handler`: 事件处理函数，接收 ConnectionEvent
    pub fn on_event<F>(mut self, handler: F) -> Self
    where
        F: Fn(&ConnectionEvent) + Send + Sync + 'static,
    {
        self.event_handler = Some(Box::new(handler));
        self
    }

    /// 设置传输协议
    pub fn with_protocol(mut self, protocol: crate::common::config_types::TransportProtocol) -> Self {
        self.config.transport = protocol;
        self
    }

    /// 启用多协议竞速
    pub fn with_protocol_race(mut self, protocols: Vec<crate::common::config_types::TransportProtocol>) -> Self {
        self.config = self.config.with_protocol_race(protocols);
        self
    }

    /// 设置用户 ID
    pub fn with_user_id(mut self, user_id: String) -> Self {
        self.config = self.config.with_user_id(user_id);
        self
    }

    /// 设置序列化格式
    pub fn with_format(mut self, format: crate::common::protocol::SerializationFormat) -> Self {
        self.config = self.config.with_format(format);
        self
    }

    /// 设置压缩算法
    pub fn with_compression(mut self, compression: crate::common::compression::CompressionAlgorithm) -> Self {
        self.config = self.config.with_compression(compression);
        self
    }

    /// 设置心跳配置
    pub fn with_heartbeat(mut self, heartbeat: crate::common::config_types::HeartbeatConfig) -> Self {
        self.config = self.config.with_heartbeat(heartbeat);
        self
    }

    /// 设置 TLS 配置
    pub fn with_tls(mut self, tls: crate::common::config_types::TlsConfig) -> Self {
        self.config = self.config.with_tls(tls);
        self
    }

    /// 设置连接超时
    pub fn with_connect_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.config = self.config.with_connect_timeout(timeout);
        self
    }

    /// 设置重连间隔
    pub fn with_reconnect_interval(mut self, interval: std::time::Duration) -> Self {
        self.config = self.config.with_reconnect_interval(interval);
        self
    }

    /// 设置最大重连次数
    pub fn with_max_reconnect_attempts(mut self, max: Option<u32>) -> Self {
        self.config = self.config.with_max_reconnect_attempts(max);
        self
    }

    /// 构建客户端
    /// 
    /// # 返回
    /// 返回配置好的 SimpleClient 实例
    pub fn build(self) -> Result<SimpleClient> {
        let observer = Arc::new(SimpleClientObserver {
            message_handler: self.message_handler,
            event_handler: self.event_handler,
        });

        let client = HybridClient::new(self.config)?;
        let client_arc = Arc::new(Mutex::new(client));

        Ok(SimpleClient {
            client: client_arc,
            observer,
        })
    }
}

