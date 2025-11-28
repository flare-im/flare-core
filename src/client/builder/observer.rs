//! 观察者模式客户端构建器（基本装修）
//! 
//! 提供基本实现，用户可以自定义观察器和处理器
//! 
//! ## 特点
//! - ✅ 自定义观察器：实现 `ConnectionObserver` trait 自定义消息处理
//! - ✅ 事件处理：支持自定义事件处理器
//! - ✅ 消息路由：支持消息路由功能
//! - ✅ 灵活扩展：可以添加自定义的观察器和处理器
//! 
//! ## 适用场景
//! - 需要自定义消息处理逻辑
//! - 需要事件驱动的架构
//! - 需要消息路由功能

use crate::common::error::Result;
use crate::common::protocol::Frame;
use crate::client::{HybridClient, Client};
use crate::transport::events::ConnectionObserver;
use crate::client::builder::{BaseClientBuilderConfig, ClientWrapper};
use std::sync::Arc;
use tokio::sync::Mutex;

/// 观察者模式客户端构建器
/// 
/// 使用实现了 ConnectionObserver trait 的观察者
pub struct ObserverClientBuilder {
    base: BaseClientBuilderConfig,
    observer: Option<Arc<dyn ConnectionObserver>>,
    event_handler: Option<Arc<dyn crate::client::events::handler::ClientEventHandler>>,
}

impl ObserverClientBuilder {
    /// 创建新的观察者模式构建器
    pub fn new(server_url: impl Into<String>) -> Self {
        Self {
            base: BaseClientBuilderConfig::new(server_url),
            observer: None,
            event_handler: None,
        }
    }
    
    /// 设置事件处理器（可选，用于自定义业务逻辑）
    pub fn with_event_handler(mut self, event_handler: Arc<dyn crate::client::events::handler::ClientEventHandler>) -> Self {
        self.event_handler = Some(event_handler);
        self
    }

    /// 设置观察者（必须）
    pub fn with_observer(mut self, observer: Arc<dyn ConnectionObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// 设置传输协议
    pub fn with_protocol(mut self, protocol: crate::common::config_types::TransportProtocol) -> Self {
        self.base = self.base.with_protocol(protocol);
        self
    }

    /// 启用多协议竞速
    /// 
    /// 协议列表的顺序就是优先级顺序，前面的协议优先级更高
    pub fn with_protocol_race(mut self, protocols: Vec<crate::common::config_types::TransportProtocol>) -> Self {
        self.base = self.base.with_protocol_race(protocols);
        self
    }

    /// 为特定协议设置服务器地址
    pub fn with_protocol_url(mut self, protocol: crate::common::config_types::TransportProtocol, url: String) -> Self {
        self.base = self.base.with_protocol_url(protocol, url);
        self
    }

    /// 设置用户 ID
    pub fn with_user_id(mut self, user_id: String) -> Self {
        self.base = self.base.with_user_id(user_id);
        self
    }

    /// 设置序列化格式
    pub fn with_format(mut self, format: crate::common::protocol::SerializationFormat) -> Self {
        self.base = self.base.with_format(format);
        self
    }

    /// 设置压缩算法
    pub fn with_compression(mut self, compression: crate::common::compression::CompressionAlgorithm) -> Self {
        self.base = self.base.with_compression(compression);
        self
    }

    /// 设置心跳配置
    pub fn with_heartbeat(mut self, heartbeat: crate::common::config_types::HeartbeatConfig) -> Self {
        self.base = self.base.with_heartbeat(heartbeat);
        self
    }

    /// 设置 TLS 配置
    pub fn with_tls(mut self, tls: crate::common::config_types::TlsConfig) -> Self {
        self.base = self.base.with_tls(tls);
        self
    }

    /// 设置连接超时
    pub fn with_connect_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.base = self.base.with_connect_timeout(timeout);
        self
    }

    /// 设置重连间隔
    pub fn with_reconnect_interval(mut self, interval: std::time::Duration) -> Self {
        self.base = self.base.with_reconnect_interval(interval);
        self
    }

    /// 设置最大重连次数
    pub fn with_max_reconnect_attempts(mut self, max: Option<u32>) -> Self {
        self.base = self.base.with_max_reconnect_attempts(max);
        self
    }
    
    /// 启用消息路由
    /// 
    /// 启用后，可以通过 ClientCore 的 router 方法注册消息处理器
    pub fn enable_router(mut self) -> Self {
        self.base = self.base.enable_router();
        self
    }
    
    /// 设置设备信息（用于协商和设备管理）
    pub fn with_device_info(mut self, device_info: crate::common::device::DeviceInfo) -> Self {
        self.base = self.base.with_device_info(device_info);
        self
    }
    
    /// 设置 Token（用于认证，如果服务端启用认证，必须提供）
    pub fn with_token(mut self, token: String) -> Self {
        self.base = self.base.with_token(token);
        self
    }

    /// 构建客户端（使用协议竞速）
    pub async fn build_with_race(self) -> Result<ObserverClient> {
        let observer = self.observer.ok_or_else(|| {
            crate::common::error::FlareError::general_error("Observer is required")
        })?;

        let client = HybridClient::connect_with_race(self.base.config).await?;
        let wrapper = ClientWrapper::new(client);
        
        // 设置事件处理器（如果提供）
        if let Some(event_handler) = self.event_handler {
            let mut client = wrapper.client().lock().await;
            client.core_mut().set_event_handler(Some(event_handler));
        }
        
        // 添加观察者
        {
            let observer_clone = Arc::clone(&observer);
            let mut client = wrapper.client().lock().await;
            client.add_observer(observer_clone);
        }

        Ok(ObserverClient {
            wrapper,
            observer: Some(observer),
        })
    }

    /// 构建客户端
    pub fn build(self) -> Result<ObserverClient> {
        let observer = self.observer.ok_or_else(|| {
            crate::common::error::FlareError::general_error("Observer is required")
        })?;

        let mut client = HybridClient::new(self.base.config)?;
        
        // 设置事件处理器（如果提供）
        if let Some(event_handler) = self.event_handler {
            client.core_mut().set_event_handler(Some(event_handler));
        }
        
        let wrapper = ClientWrapper::new(client);

        Ok(ObserverClient {
            wrapper,
            observer: Some(observer),
        })
    }
}

/// 观察者模式客户端实例
pub struct ObserverClient {
    wrapper: ClientWrapper,
    observer: Option<Arc<dyn ConnectionObserver>>,
}

impl ObserverClient {
    /// 连接到服务器
    pub async fn connect(&mut self) -> Result<()> {
        // 先添加观察者（如果还未添加）
        if let Some(observer) = self.observer.take() {
            let mut client = self.wrapper.client().lock().await;
            client.add_observer(observer);
        }
        
        // 然后连接
        self.wrapper.connect().await
    }

    /// 断开连接
    pub async fn disconnect(&mut self) -> Result<()> {
        self.wrapper.disconnect().await
    }

    /// 发送消息
    pub async fn send_frame(&mut self, frame: &Frame) -> Result<()> {
        self.wrapper.send_frame(frame).await
    }

    /// 检查连接状态
    pub fn is_connected(&self) -> bool {
        self.wrapper.is_connected()
    }

    /// 获取连接 ID
    pub fn connection_id(&self) -> Option<String> {
        self.wrapper.connection_id()
    }

    /// 获取活动协议
    pub fn active_protocol(&self) -> crate::common::config_types::TransportProtocol {
        self.wrapper.active_protocol()
    }
}

