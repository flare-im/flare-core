//! 观察者模式客户端构建器
//! 
//! 使用实现了 ConnectionObserver trait 的观察者

use crate::common::error::Result;
use crate::common::protocol::Frame;
use crate::client::{ClientConfig, HybridClient, Client};
use crate::transport::events::ConnectionObserver;
use std::sync::Arc;
use tokio::sync::Mutex;

/// 观察者模式客户端构建器
/// 
/// 使用实现了 ConnectionObserver trait 的观察者
pub struct ObserverClientBuilder {
    config: ClientConfig,
    observer: Option<Arc<dyn ConnectionObserver>>,
}

impl ObserverClientBuilder {
    /// 创建新的观察者模式构建器
    pub fn new(server_url: impl Into<String>) -> Self {
        Self {
            config: ClientConfig::new(server_url.into()),
            observer: None,
        }
    }

    /// 设置观察者（必须）
    pub fn with_observer(mut self, observer: Arc<dyn ConnectionObserver>) -> Self {
        self.observer = Some(observer);
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

    /// 构建客户端（使用协议竞速）
    pub async fn build_with_race(self) -> Result<ObserverClient> {
        let observer = self.observer.ok_or_else(|| {
            crate::common::error::FlareError::general_error("Observer is required")
        })?;

        let client = HybridClient::connect_with_race(self.config).await?;
        let client_arc = Arc::new(Mutex::new(client));
        
        // 添加观察者
        {
            let observer_clone = Arc::clone(&observer);
            let mut client = client_arc.lock().await;
            client.add_observer(observer_clone);
        }

        Ok(ObserverClient {
            client: client_arc,
            observer: Some(observer),
        })
    }

    /// 构建客户端
    pub fn build(self) -> Result<ObserverClient> {
        let observer = self.observer.ok_or_else(|| {
            crate::common::error::FlareError::general_error("Observer is required")
        })?;

        let client = HybridClient::new(self.config)?;
        let client_arc = Arc::new(Mutex::new(client));

        Ok(ObserverClient {
            client: client_arc,
            observer: Some(observer),
        })
    }
}

/// 观察者模式客户端实例
pub struct ObserverClient {
    client: Arc<Mutex<HybridClient>>,
    observer: Option<Arc<dyn ConnectionObserver>>,
}

impl ObserverClient {
    /// 连接到服务器
    pub async fn connect(&mut self) -> Result<()> {
        // 先添加观察者（如果还未添加）
        if let Some(observer) = self.observer.take() {
            let mut client = self.client.lock().await;
            client.add_observer(observer);
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

