//! 服务端消息观察者工厂
//!
//! 提供观察者创建接口，支持用户自定义观察者实现

use crate::common::MessageParser;
use crate::server::connection::ConnectionManager;
use crate::server::events::{ServerMessageWrapper, observer::ConnectionHandlerObserverAdapter};
use crate::transport::events::ConnectionObserver;
use std::sync::Arc;
use tracing::error;

/// ServerCore 的轻量级引用
///
/// 用于在工厂中访问 ServerCore 的组件，避免循环引用
pub struct ServerCoreRef {
    pub device_manager: Option<Arc<crate::server::device::DeviceManager>>,
    pub event_handler: Option<Arc<dyn crate::server::events::handler::ServerEventHandler>>,
}

/// 服务端消息观察者工厂
///
/// 用于创建连接观察者，支持用户自定义实现
///
/// # 设计原则
/// - **依赖倒置**：transports 依赖工厂接口，不依赖具体实现
/// - **开闭原则**：对扩展开放，对修改关闭
/// - **单一职责**：只负责创建观察者
///
/// # 使用示例
/// ```rust,no_run
/// use flare_core::server::events::factory::ServerMessageObserverFactory;
///
/// struct MyCustomObserverFactory;
///
/// impl ServerMessageObserverFactory for MyCustomObserverFactory {
///     fn create_observer(
///         &self,
///         manager: Arc<ConnectionManager>,
///         parser: MessageParser,
///         event_handler: Arc<dyn crate::server::events::handler::ServerEventHandler>,
///         connection_id: String,
///         core_ref: Arc<ServerCoreRef>,
///         core: Arc<crate::server::transports::server_core::ServerCore>,
///     ) -> Arc<dyn ConnectionObserver> {
///         // 创建自定义观察者
///         Arc::new(MyCustomObserver::new(...))
///     }
/// }
/// ```
pub trait ServerMessageObserverFactory: Send + Sync {
    /// 为指定连接创建观察者
    ///
    /// # 参数
    /// - `manager`: 连接管理器
    /// - `parser`: 消息解析器
    /// - `event_handler`: 事件处理器（必需）
    /// - `connection_id`: 连接 ID
    /// - `core_ref`: 服务器核心的轻量级引用
    /// - `core`: 服务器核心的完整引用（用于需要完整功能的情况）
    ///
    /// # 返回
    /// 创建的观察者实例
    fn create_observer(
        &self,
        manager: Arc<ConnectionManager>,
        parser: MessageParser,
        event_handler: Arc<dyn crate::server::events::handler::ServerEventHandler>,
        connection_id: String,
        core_ref: Arc<ServerCoreRef>,
        core: Arc<crate::server::transports::server_core::ServerCore>,
    ) -> Arc<dyn ConnectionObserver>;
}

/// 默认观察者工厂
///
/// 使用 `DefaultServerMessageObserver` 创建观察者
///
/// # 使用示例
/// ```rust,no_run
/// use flare_core::server::events::factory::DefaultServerMessageObserverFactory;
///
/// // 使用默认工厂
/// let factory = DefaultServerMessageObserverFactory::new();
///
/// // 或配置设备管理器和事件处理器
/// let factory = DefaultServerMessageObserverFactory::new()
///     .with_device_manager(Some(device_manager))
///     .with_event_handler(Some(event_handler));
/// ```
pub struct DefaultServerMessageObserverFactory {
    /// 设备管理器（可选）
    device_manager: Option<Arc<crate::server::device::DeviceManager>>,
    /// 事件处理器（可选）
    event_handler: Option<Arc<dyn crate::server::events::handler::ServerEventHandler>>,
}

impl DefaultServerMessageObserverFactory {
    /// 创建新的默认工厂
    pub fn new() -> Self {
        Self {
            device_manager: None,
            event_handler: None,
        }
    }

    /// 设置设备管理器
    pub fn with_device_manager(
        mut self,
        device_manager: Option<Arc<crate::server::device::DeviceManager>>,
    ) -> Self {
        self.device_manager = device_manager;
        self
    }

    /// 设置事件处理器
    pub fn with_event_handler(
        mut self,
        event_handler: Option<Arc<dyn crate::server::events::handler::ServerEventHandler>>,
    ) -> Self {
        self.event_handler = event_handler;
        self
    }
}

impl Default for DefaultServerMessageObserverFactory {
    fn default() -> Self {
        Self::new()
    }
}

/// 服务端观察者工厂实现
impl ServerMessageObserverFactory for DefaultServerMessageObserverFactory {
    fn create_observer(
        &self,
        manager: Arc<ConnectionManager>,
        parser: MessageParser,
        event_handler: Arc<dyn crate::server::events::handler::ServerEventHandler>,
        connection_id: String,
        core_ref: Arc<ServerCoreRef>,
        _core: Arc<crate::server::transports::server_core::ServerCore>,
    ) -> Arc<dyn ConnectionObserver> {
        // 优先使用工厂配置，如果没有则使用 core_ref 中的配置
        let device_manager = self
            .device_manager
            .clone()
            .or_else(|| core_ref.device_manager.clone());

        // 优先使用传入的 event_handler，如果没有则使用工厂配置，最后使用 core_ref 中的配置
        let event_handler = Some(event_handler)
            .or_else(|| self.event_handler.clone())
            .or_else(|| core_ref.event_handler.clone())
            .ok_or_else(|| {
                error!("[DefaultServerMessageObserverFactory] ServerEventHandler is required but not provided");
                "ServerEventHandler is required"
            })
            .expect("ServerEventHandler is required");

        // 创建 ServerMessageWrapper（实现 ConnectionHandler）
        let wrapper = Arc::new(ServerMessageWrapper::new(
            event_handler,
            Some(Arc::clone(&manager)),
            device_manager,
            parser.clone(), // 克隆 parser 用于适配器
        ));

        // 将 ConnectionHandler 适配为 ConnectionObserver
        // 不传递 parser，而是从连接信息中动态获取协商结果来创建 parser
        Arc::new(ConnectionHandlerObserverAdapter::new(
            wrapper,
            connection_id,
            manager,
            Some(_core),
        ))
    }
}

/// 观察者链工厂
///
/// 支持创建多个观察者，按顺序处理事件
///
/// # 使用场景
/// - 需要多个观察者协同工作
/// - 需要按优先级处理事件
/// - 需要组合不同的观察者功能
///
/// # 使用示例
/// ```rust,no_run
/// use flare_core::server::events::factory::{ChainedObserverFactory, ServerMessageObserverFactory};
///
/// let factory1 = Arc::new(MyObserverFactory1::new());
/// let factory2 = Arc::new(MyObserverFactory2::new());
///
/// let chained = ChainedObserverFactory::new()
///     .add_factory(factory1)
///     .add_factory(factory2);
/// ```
pub struct ChainedObserverFactory {
    factories: Vec<Arc<dyn ServerMessageObserverFactory>>,
}

impl ChainedObserverFactory {
    /// 创建新的链式工厂
    pub fn new() -> Self {
        Self {
            factories: Vec::new(),
        }
    }

    /// 添加工厂到链中
    pub fn add_factory(mut self, factory: Arc<dyn ServerMessageObserverFactory>) -> Self {
        self.factories.push(factory);
        self
    }
}

impl Default for ChainedObserverFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl ServerMessageObserverFactory for ChainedObserverFactory {
    fn create_observer(
        &self,
        manager: Arc<ConnectionManager>,
        parser: MessageParser,
        event_handler: Arc<dyn crate::server::events::handler::ServerEventHandler>,
        connection_id: String,
        core_ref: Arc<ServerCoreRef>,
        core: Arc<crate::server::transports::server_core::ServerCore>,
    ) -> Arc<dyn ConnectionObserver> {
        // 如果只有一个工厂，直接返回
        if self.factories.len() == 1 {
            return self.factories[0].create_observer(
                manager,
                parser,
                event_handler,
                connection_id,
                core_ref,
                core,
            );
        }

        // 创建多个观察者并组合
        let observers: Vec<Arc<dyn ConnectionObserver>> = self
            .factories
            .iter()
            .map(|factory| {
                factory.create_observer(
                    Arc::clone(&manager),
                    parser.clone(),
                    Arc::clone(&event_handler),
                    connection_id.clone(),
                    Arc::clone(&core_ref),
                    Arc::clone(&core),
                )
            })
            .collect();

        // 创建链式观察者包装器
        Arc::new(ChainedObserver { observers })
    }
}

/// 链式观察者包装器
///
/// 按顺序调用所有观察者
struct ChainedObserver {
    observers: Vec<Arc<dyn ConnectionObserver>>,
}

impl crate::transport::events::ConnectionObserver for ChainedObserver {
    fn on_event(&self, event: &crate::transport::events::ConnectionEvent) {
        for observer in &self.observers {
            observer.on_event(event);
        }
    }
}
