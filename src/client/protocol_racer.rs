use crate::common::connections::config::ConnectionConfig;
use crate::common::connections::config::ProtocolConfig;
use crate::common::connections::enums::{Transport, ConnectionState};
use crate::common::connections::traits::{ClientConnection, ConnectionEvent};
use crate::common::error::FlareError;
use crate::common::connections::factory::ConnectionFactory;
use std::sync::Arc;
use tracing::{debug, warn};
use rand::seq::SliceRandom;
use rand::rng;
use tokio::time::{timeout, Duration};

pub struct ProtocolRacer;

impl ProtocolRacer {
    /// 并行发起连接，先成功者获胜并返回该连接
    pub async fn race(
        base_config: &ConnectionConfig,
        server_addresses: &[String],
        protocols: &[Transport],
        handler: Option<Arc<dyn ConnectionEvent>>,
    ) -> Result<Arc<dyn ClientConnection>, FlareError> {
        if protocols.is_empty() || server_addresses.is_empty() {
            return Err(FlareError::general_error("协议或地址列表为空"));
        }

        // 随机化地址与协议的尝试顺序，减少热点与尾延迟
        let mut addrs: Vec<String> = server_addresses.to_vec();
        addrs.shuffle(&mut rng());
        let mut protos: Vec<Transport> = protocols.to_vec();
        protos.shuffle(&mut rng());

        let capacity = protos.len() * addrs.len();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Arc<dyn ClientConnection>>(capacity.max(1));

        // 连接超时（默认 5 秒）
        let connect_timeout_ms: u64 = 5000;

        for p in protos {
            for addr in &addrs {
                let mut cfg = base_config.clone();
                cfg.transport = p.clone();
                cfg.remote_addr = Some(addr.clone());

                // 确保配置有必要的字段
                if cfg.protocol_config.is_none() {
                    cfg.protocol_config = Some(ProtocolConfig::default());
                }

                let tx_clone = tx.clone();
                let handler_clone = handler.clone();
                tokio::spawn(async move {
                    let res = match handler_clone {
                        Some(h) => ConnectionFactory::create_client_with_handler(cfg, h),
                        None => ConnectionFactory::create_client(cfg),
                    };
                    if let Ok(client_box) = res {
                        let client_arc: Arc<dyn ClientConnection> = Arc::from(client_box);
                        // 带超时的连接尝试
                        match timeout(Duration::from_millis(connect_timeout_ms), async {
                            client_arc.connect()
                        }).await {
                            Ok(Ok(())) => {
                                let _ = tx_clone.send(client_arc).await;
                            }
                            Ok(Err(_e)) => {
                                // 连接错误，忽略
                            }
                            Err(_elapsed) => {
                                // 超时，忽略
                            }
                        }
                    }
                });
            }
        }
        drop(tx);

        // 选取第一个成功的连接
        if let Some(conn) = rx.recv().await {
            return Ok(conn);
        }
        Err(FlareError::connection_failed("协议竞速全部失败"))
    }
}

/// 返回一个可更新的句柄，内部自管理重连与竞速
pub struct RacingHandle {
    base_config: ConnectionConfig,
    addresses: Vec<String>,
    protocols: Vec<Transport>,
    handler: Option<Arc<dyn ConnectionEvent>>,
    current: tokio::sync::Mutex<Option<Arc<dyn ClientConnection>>>,
    check_interval_ms: u64,
    // 回退策略
    backoff_initial_ms: u64,
    backoff_factor: u32,
    backoff_max_ms: u64,
    max_total_retry_ms: u64,
}

impl RacingHandle {
    pub fn new(
        base_config: ConnectionConfig,
        addresses: Vec<String>,
        protocols: Vec<Transport>,
        handler: Option<Arc<dyn ConnectionEvent>>,
        check_interval_ms: u64,
    ) -> Arc<Self> {
        Arc::new(Self {
            base_config,
            addresses,
            protocols,
            handler,
            current: tokio::sync::Mutex::new(None),
            check_interval_ms,
            backoff_initial_ms: 1000,
            backoff_factor: 2,
            backoff_max_ms: 10000,
            max_total_retry_ms: 30000,
        })
    }

    async fn attempt_race_with_backoff(&self) -> Option<Arc<dyn ClientConnection>> {
        use tokio::time::{sleep, Duration, Instant};
        let start = Instant::now();
        let mut delay = self.backoff_initial_ms;
        loop {
            // 超过最大重试总时长则退出
            if start.elapsed() >= Duration::from_millis(self.max_total_retry_ms) {
                return None;
            }
            match ProtocolRacer::race(&self.base_config, &self.addresses, &self.protocols, self.handler.clone()).await {
                Ok(conn) => return Some(conn),
                Err(e) => {
                    warn!("协议竞速失败: {:?}", e);
                    sleep(Duration::from_millis(delay)).await;
                    delay = (delay.saturating_mul(self.backoff_factor as u64)).min(self.backoff_max_ms);
                }
            }
        }
    }

    pub async fn start(self: &Arc<Self>) -> Result<(), FlareError> {
        // 首次竞速
        let conn = ProtocolRacer::race(&self.base_config, &self.addresses, &self.protocols, self.handler.clone()).await?;
        {
            let mut g = self.current.lock().await;
            *g = Some(conn.clone());
        }
        // 启动后台重连监控
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(this.check_interval_ms));
            loop {
                interval.tick().await;
                // 检查当前连接状态
                let need_retry = {
                    if let Some(conn) = this.current.lock().await.as_ref().cloned() {
                        let st = conn.state();
                        !(matches!(st, ConnectionState::Connected | ConnectionState::Ready))
                    } else { true }
                };
                if need_retry {
                    debug!("检测到连接断开，开始重连...");
                    if let Some(new_conn) = this.attempt_race_with_backoff().await {
                        let mut g = this.current.lock().await;
                        *g = Some(new_conn);
                        debug!("重连成功");
                    } else {
                        warn!("重连失败，达到最大重试次数");
                    }
                }
            }
        });
        Ok(())
    }

    pub async fn get_connection(&self) -> Option<Arc<dyn ClientConnection>> {
        self.current.lock().await.clone()
    }
}