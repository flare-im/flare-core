//! 传输层事件模块
//!
//! 定义连接事件类型和观察者模式接口，用于在传输层和上层之间传递事件

use crate::common::error::FlareError;
use std::sync::{Arc, Mutex};

/// 连接事件
///
/// 表示连接上发生的各种事件，由 `ConnectionObserver` 使用以响应连接状态和数据接收。
#[derive(Debug, Clone)]
pub enum ConnectionEvent {
    /// 连接成功建立时触发
    Connected,
    /// 连接关闭时触发
    ///
    /// 参数提供断开连接的原因
    Disconnected(String),
    /// 收到新消息时触发
    ///
    /// 负载是字节向量
    Message(Vec<u8>),
    /// 连接因错误而终止时触发（如读出错、协议错误）
    ///
    /// 与 `Disconnected` 一样是终止事件：传输先标记关闭、取走全部观察者再上报，此后不再上报
    /// 任何事件，也不会再补一个 `Disconnected`。观察者要按断开处理（服务端会移出连接表、清设备）。
    Error(FlareError),
}

impl ConnectionEvent {
    /// 检查是否为连接事件
    pub fn is_connected(&self) -> bool {
        matches!(self, Self::Connected)
    }

    /// 检查是否为断开连接事件
    pub fn is_disconnected(&self) -> bool {
        matches!(self, Self::Disconnected(_))
    }

    /// 检查是否为消息事件
    pub fn is_message(&self) -> bool {
        matches!(self, Self::Message(_))
    }

    /// 检查是否为错误事件
    pub fn is_error(&self) -> bool {
        matches!(self, Self::Error(_))
    }

    /// 获取断开连接的原因（如果是断开连接事件）
    pub fn disconnect_reason(&self) -> Option<&str> {
        match self {
            Self::Disconnected(reason) => Some(reason),
            _ => None,
        }
    }

    /// 获取消息数据（如果是消息事件）
    pub fn message_data(&self) -> Option<&[u8]> {
        match self {
            Self::Message(data) => Some(data),
            _ => None,
        }
    }

    /// 获取错误（如果是错误事件）
    pub fn error(&self) -> Option<&FlareError> {
        match self {
            Self::Error(err) => Some(err),
            _ => None,
        }
    }
}

/// 连接事件观察者
///
/// 实现此 trait 以响应连接建立、断开和接收消息等事件。
/// 观察者通过 `Connection` 实例注册。
pub trait ConnectionObserver: Send + Sync {
    /// 当连接上发生事件时由 `Connection` 调用
    ///
    /// # 参数
    /// - `event`: 发生的事件
    fn on_event(&self, event: &ConnectionEvent);
}

/// 线程安全的、引用计数的观察者类型别名
pub type ArcObserver = Arc<dyn ConnectionObserver>;

/// 观察者列表的快照，供锁外逐个回调。
///
/// 列表只做 push、retain 与 clone，锁被毒化（别处持锁时 panic）不代表列表不一致，照常取用；
/// 按毒化丢弃的话，此后所有通知——包括踢线原因——都会被静默丢掉。
pub(crate) fn observer_snapshot(observers: &Mutex<Vec<ArcObserver>>) -> Vec<ArcObserver> {
    observers
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

/// 把一个事件逐个交给观察者，调用方不得持有观察者列表的锁。
///
/// 某个观察者 panic 时记一条 ERROR 后继续：其余观察者照常收到这次事件，panic 也不会沿调用方
/// （读循环、重连任务）上抛。
pub(crate) fn deliver_to_observers(
    observers: &[ArcObserver],
    event: &ConnectionEvent,
    source: &'static str,
) {
    for observer in observers {
        deliver_to_observer(observer, event, source);
    }
}

/// 把一个事件交给单个观察者：它 panic 时记一条 ERROR 后返回，panic 不沿调用方上抛。
pub(crate) fn deliver_to_observer(
    observer: &ArcObserver,
    event: &ConnectionEvent,
    source: &'static str,
) {
    let delivered =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observer.on_event(event)));
    if let Err(panic) = delivered {
        let message = panic
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("non-string panic payload");
        tracing::error!(
            source,
            panic = message,
            "connection observer panicked; the other observers still receive the event"
        );
    }
}

/// 传输 `add_observer` 的共用部分：连接未关闭时先告诉新观察者 Connected 再挂上它，返回 true，
/// 调用方随后唤醒等第一个观察者的读任务；已经关闭时只告诉它 Disconnected，返回 false。
///
/// 已经关闭的连接不能再报 Connected：否则上层会在一条死连接上发 CONNECT 并干等 CONNECT_ACK。
/// 挂上与读任务的「先标记关闭、再取走观察者」配对：持锁时仍未关闭，读任务之后一定会通知到它；
/// 已经关闭，就由这里补一次 Disconnected。
///
/// 这几次直接回调新观察者与读循环一样隔离 panic（见 [`deliver_to_observer`]）。原先直接回调，
/// 观察者处理 Connected 时 panic 会沿 `add_observer` 上抛：它没挂上、读任务也没被唤醒，已到的帧
/// 与之后的断开都没人收。
#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "websocket", feature = "quic", feature = "tcp")
))]
pub(crate) fn attach_observer(
    observers: &Mutex<Vec<ArcObserver>>,
    is_closed: &std::sync::atomic::AtomicBool,
    observer: ArcObserver,
    closed_reason: &str,
    source: &'static str,
) -> bool {
    use std::sync::atomic::Ordering;

    let closed = || ConnectionEvent::Disconnected(closed_reason.to_string());
    if is_closed.load(Ordering::SeqCst) {
        deliver_to_observer(&observer, &closed(), source);
        return false;
    }
    deliver_to_observer(&observer, &ConnectionEvent::Connected, source);
    let registered = match observers.lock() {
        Ok(mut observers) if !is_closed.load(Ordering::SeqCst) => {
            observers.push(Arc::clone(&observer));
            true
        }
        _ => false,
    };
    if !registered {
        deliver_to_observer(&observer, &closed(), source);
    }
    registered
}

/// 传输读循环、重连任务上报一个事件：按快照在锁外逐个回调，观察者 panic 被隔离（见
/// [`deliver_to_observers`]）。原先直接回调，一个观察者 panic 会沿读循环上抛：读任务就此结束，
/// 排在它后面的观察者收不到这次事件，之后的帧也不再有人读、连连接断开都不会再上报。
pub(crate) fn notify_observers(
    observers_arc: &Arc<Mutex<Vec<ArcObserver>>>,
    event: &ConnectionEvent,
    source: &'static str,
) {
    let observers = observer_snapshot(observers_arc);
    deliver_to_observers(&observers, event, source);
}

/// 终止事件：取走全部观察者（锁被毒化也照常取走）再逐个回调，观察者 panic 同样被隔离。
pub(crate) fn notify_observers_and_clear(
    observers_arc: &Arc<Mutex<Vec<ArcObserver>>>,
    event: &ConnectionEvent,
    source: &'static str,
) {
    let observers = std::mem::take(
        &mut *observers_arc
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    );
    deliver_to_observers(&observers, event, source);
}

// ============================================================================
// 便利观察者实现
// ============================================================================

/// 空观察者（不执行任何操作）
///
/// 用于需要观察者但不需要实际处理的情况
pub struct NoOpObserver;

impl ConnectionObserver for NoOpObserver {
    fn on_event(&self, _event: &ConnectionEvent) {
        // 不执行任何操作
    }
}

impl NoOpObserver {
    /// 创建新的空观察者
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> ArcObserver {
        Arc::new(Self)
    }
}

/// 日志观察者（记录所有事件到日志）
///
/// 用于调试和监控连接事件
pub struct LoggingObserver {
    prefix: String,
}

impl LoggingObserver {
    /// 创建新的日志观察者
    ///
    /// # 参数
    /// - `prefix`: 日志前缀，用于标识不同的观察者实例
    #[allow(clippy::new_ret_no_self)]
    pub fn new(prefix: impl Into<String>) -> ArcObserver {
        Arc::new(Self {
            prefix: prefix.into(),
        })
    }
}

impl ConnectionObserver for LoggingObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Connected => {
                tracing::info!("[{}] Connection established", self.prefix);
            }
            ConnectionEvent::Disconnected(reason) => {
                tracing::info!("[{}] Connection disconnected: {}", self.prefix, reason);
            }
            ConnectionEvent::Message(data) => {
                tracing::debug!("[{}] Message received: {} bytes", self.prefix, data.len());
            }
            ConnectionEvent::Error(err) => {
                tracing::error!("[{}] Connection error: {:?}", self.prefix, err);
            }
        }
    }
}

/// 组合观察者（将事件转发给多个观察者）
///
/// 用于需要多个观察者处理同一事件的情况
pub struct CompositeObserver {
    observers: Vec<ArcObserver>,
}

impl CompositeObserver {
    /// 创建新的组合观察者
    pub fn new() -> Self {
        Self {
            observers: Vec::new(),
        }
    }

    /// 添加观察者
    pub fn add(&mut self, observer: ArcObserver) {
        self.observers.push(observer);
    }

    /// 创建 Arc 包装的组合观察者
    pub fn into_arc(self) -> ArcObserver {
        Arc::new(self)
    }
}

impl ConnectionObserver for CompositeObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        for observer in &self.observers {
            observer.on_event(event);
        }
    }
}

impl Default for CompositeObserver {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingObserver {
        calls: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    }

    impl ConnectionObserver for CountingObserver {
        fn on_event(&self, _event: &ConnectionEvent) {
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl Drop for CountingObserver {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn terminal_notify_clears_and_drops_observers() {
        let calls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let observers = Arc::new(Mutex::new(Vec::<ArcObserver>::new()));
        observers.lock().unwrap().push(Arc::new(CountingObserver {
            calls: Arc::clone(&calls),
            drops: Arc::clone(&drops),
        }));

        notify_observers_and_clear(
            &observers,
            &ConnectionEvent::Disconnected("test".to_string()),
            "test observers",
        );

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(observers.lock().unwrap().is_empty());
    }

    #[test]
    fn non_terminal_notify_keeps_observers_registered() {
        let calls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let observers = Arc::new(Mutex::new(Vec::<ArcObserver>::new()));
        observers.lock().unwrap().push(Arc::new(CountingObserver {
            calls: Arc::clone(&calls),
            drops: Arc::clone(&drops),
        }));

        notify_observers(&observers, &ConnectionEvent::Connected, "test observers");

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(observers.lock().unwrap().len(), 1);
    }

    struct PanickingObserver;

    impl ConnectionObserver for PanickingObserver {
        fn on_event(&self, _event: &ConnectionEvent) {
            panic!("observer bug");
        }
    }

    /// 锁被毒化后照样取到快照；排在前面的观察者 panic，后面的照常收到事件，panic 不上抛。
    #[test]
    fn snapshot_survives_a_poisoned_lock_and_delivery_isolates_panics() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observers = Arc::new(Mutex::new(Vec::<ArcObserver>::new()));
        observers.lock().unwrap().push(Arc::new(PanickingObserver));
        observers.lock().unwrap().push(Arc::new(CountingObserver {
            calls: Arc::clone(&calls),
            drops: Arc::new(AtomicUsize::new(0)),
        }));
        let poisoner = Arc::clone(&observers);
        let _ = std::thread::spawn(move || {
            let _guard = poisoner.lock().unwrap();
            panic!("poison the observer list");
        })
        .join();
        assert!(observers.is_poisoned());

        let snapshot = observer_snapshot(&observers);
        assert_eq!(snapshot.len(), 2);
        deliver_to_observers(&snapshot, &ConnectionEvent::Connected, "test observers");
        deliver_to_observers(&snapshot, &ConnectionEvent::Connected, "test observers");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    /// 传输读循环用的两个通知入口同样隔离观察者 panic：排在后面的观察者照常收到，panic 不沿
    /// 读循环上抛；终止通知在锁被毒化时也照常取走全部观察者。
    #[test]
    fn transport_notifications_isolate_a_panicking_observer() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observers = Arc::new(Mutex::new(Vec::<ArcObserver>::new()));
        observers.lock().unwrap().push(Arc::new(PanickingObserver));
        observers.lock().unwrap().push(Arc::new(CountingObserver {
            calls: Arc::clone(&calls),
            drops: Arc::new(AtomicUsize::new(0)),
        }));

        notify_observers(
            &observers,
            &ConnectionEvent::Message(b"frame".to_vec()),
            "test observers",
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(observers.lock().unwrap().len(), 2);

        let poisoner = Arc::clone(&observers);
        let _ = std::thread::spawn(move || {
            let _guard = poisoner.lock().unwrap();
            panic!("poison the observer list");
        })
        .join();
        assert!(observers.is_poisoned());
        notify_observers_and_clear(
            &observers,
            &ConnectionEvent::Disconnected("peer closed".to_string()),
            "test observers",
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(
            observers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty()
        );
    }

    #[cfg(all(
        not(target_arch = "wasm32"),
        any(feature = "websocket", feature = "quic", feature = "tcp")
    ))]
    mod attach {
        use super::*;

        /// 处理 Connected 或 Disconnected 时 panic、其余事件照常记数的观察者。
        struct PanicOnConnectionChange {
            others: Arc<AtomicUsize>,
        }

        impl ConnectionObserver for PanicOnConnectionChange {
            fn on_event(&self, event: &ConnectionEvent) {
                if event.is_connected() || event.is_disconnected() {
                    panic!("observer bug while handling a connection change");
                }
                self.others.fetch_add(1, Ordering::SeqCst);
            }
        }

        /// 传输 `add_observer` 直接回调新观察者 Connected：它 panic 时 panic 不沿
        /// `add_observer` 上抛，它照样挂上、之后的帧照常收到，后挂的观察者照常收到
        /// Connected。原先 panic 上抛，观察者没挂上，读任务也不被唤醒。
        #[test]
        fn attach_isolates_a_panic_on_connected() {
            let observers = Mutex::new(Vec::<ArcObserver>::new());
            let is_closed = std::sync::atomic::AtomicBool::new(false);
            let others = Arc::new(AtomicUsize::new(0));
            let calls = Arc::new(AtomicUsize::new(0));

            assert!(attach_observer(
                &observers,
                &is_closed,
                Arc::new(PanicOnConnectionChange {
                    others: Arc::clone(&others),
                }),
                "closed",
                "test attach",
            ));
            assert!(attach_observer(
                &observers,
                &is_closed,
                Arc::new(CountingObserver {
                    calls: Arc::clone(&calls),
                    drops: Arc::new(AtomicUsize::new(0)),
                }),
                "closed",
                "test attach",
            ));
            assert_eq!(calls.load(Ordering::SeqCst), 1, "Connected for the second");
            assert_eq!(observers.lock().unwrap().len(), 2);

            let snapshot = observer_snapshot(&observers);
            deliver_to_observers(
                &snapshot,
                &ConnectionEvent::Message(b"frame".to_vec()),
                "test attach",
            );
            assert_eq!(
                others.load(Ordering::SeqCst),
                1,
                "the first still gets frames"
            );
            assert_eq!(calls.load(Ordering::SeqCst), 2);
        }

        /// 连接已经关闭（挂之前或挂的那一刻）：只补一次 Disconnected，观察者在这里 panic
        /// 同样不上抛，也不挂上。
        #[test]
        fn attach_isolates_a_panic_on_disconnected_once_closed() {
            let observers = Mutex::new(Vec::<ArcObserver>::new());
            let is_closed = std::sync::atomic::AtomicBool::new(true);
            let others = Arc::new(AtomicUsize::new(0));

            assert!(!attach_observer(
                &observers,
                &is_closed,
                Arc::new(PanicOnConnectionChange {
                    others: Arc::clone(&others),
                }),
                "closed",
                "test attach",
            ));
            assert!(observers.lock().unwrap().is_empty());
            assert_eq!(others.load(Ordering::SeqCst), 0);
        }
    }
}
