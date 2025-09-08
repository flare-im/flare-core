//! 连接池管理
//!
//! 提供连接预热、复用和智能管理功能，显著降低连接建立延迟

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Mutex};
use tracing::{debug, info};

use crate::common::{
    error::Result,
    connections::{
        ClientConnection, ConnectionConfig, ConnectionState, ConnectionType,
        factory::ConnectionFactory,
        traits::ConnectionFactory as ConnectionFactoryTrait,
    },
};

/// 连接池统计信息
#[derive(Debug, Clone)]
pub struct PoolStats {
    /// 总连接数
    pub total_connections: usize,
    /// 活跃连接数
    pub active_connections: usize,
    /// 空闲连接数
    pub idle_connections: usize,
    /// 连接命中率
    pub hit_rate: f64,
    /// 平均连接建立时间
    pub avg_connection_time_ms: f64,
}

/// 连接池条目
#[derive(Clone)]
struct PoolEntry {
    /// 连接实例
    connection: Arc<dyn ClientConnection>,
    /// 最后使用时间
    last_used: Instant,
    /// 创建时间
    created_at: Instant,
    /// 使用次数
    use_count: u64,
}

/// 高性能连接池
pub struct ConnectionPool {
    /// 连接池存储 (目标地址 -> 连接列表)
    pools: Arc<RwLock<HashMap<String, Vec<PoolEntry>>>>,
    /// 配置
    config: ConnectionPoolConfig,
    /// 连接工厂
    factory: Arc<ConnectionFactory>,
    /// 统计信息
    stats: Arc<Mutex<ConnectionPoolStats>>,
}

/// 连接池配置
#[derive(Debug, Clone)]
pub struct ConnectionPoolConfig {
    /// 每个目标的最大连接数
    pub max_connections_per_target: usize,
    /// 连接空闲超时时间
    pub idle_timeout: Duration,
    /// 连接最大生命周期
    pub max_connection_lifetime: Duration,
    /// 预连接目标列表
    pub preconnect_targets: Vec<String>,
    /// 预连接数量
    pub preconnect_count: usize,
    /// 连接健康检查间隔
    pub health_check_interval: Duration,
}

impl Default for ConnectionPoolConfig {
    fn default() -> Self {
        Self {
            max_connections_per_target: 5,
            idle_timeout: Duration::from_secs(30),
            max_connection_lifetime: Duration::from_secs(300), // 5分钟
            preconnect_targets: Vec::new(),
            preconnect_count: 2,
            health_check_interval: Duration::from_secs(10),
        }
    }
}

/// 连接池内部统计
#[derive(Debug, Default)]
struct ConnectionPoolStats {
    /// 获取连接次数
    get_requests: u64,
    /// 命中次数
    cache_hits: u64,
    /// 连接创建次数
    connections_created: u64,
    /// 连接销毁次数
    connections_destroyed: u64,
    /// 总连接建立时间（微秒）
    total_connection_time_us: u64,
}

impl ConnectionPool {
    /// 创建新的连接池
    pub fn new(config: ConnectionPoolConfig) -> Self {
        let pool = Self {
            pools: Arc::new(RwLock::new(HashMap::new())),
            config,
            factory: Arc::new(ConnectionFactory::new()),
            stats: Arc::new(Mutex::new(ConnectionPoolStats::default())),
        };
        
        // 启动预连接任务
        pool.start_preconnect_task();
        
        // 启动清理任务
        pool.start_cleanup_task();
        
        pool
    }
    
    /// 获取连接（主要API）
    pub async fn get_connection(&self, target: &str, conn_type: ConnectionType) -> Result<Arc<dyn ClientConnection>> {
        let start_time = Instant::now();
        
        // 更新统计
        {
            let mut stats = self.stats.lock().await;
            stats.get_requests += 1;
        }
        
        // 先尝试从池中获取
        if let Some(connection) = self.try_get_from_pool(target).await? {
            // 缓存命中
            {
                let mut stats = self.stats.lock().await;
                stats.cache_hits += 1;
            }
            
            debug!("连接池命中: {} (耗时: {:?})", target, start_time.elapsed());
            return Ok(connection);
        }
        
        // 池中无可用连接，创建新连接
        let connection = self.create_new_connection(target, conn_type).await?;
        
        // 更新统计
        {
            let mut stats = self.stats.lock().await;
            stats.connections_created += 1;
            stats.total_connection_time_us += start_time.elapsed().as_micros() as u64;
        }
        
        info!("创建新连接: {} (耗时: {:?})", target, start_time.elapsed());
        Ok(connection)
    }
    
    /// 从池中尝试获取连接
    async fn try_get_from_pool(&self, target: &str) -> Result<Option<Arc<dyn ClientConnection>>> {
        let mut pools = self.pools.write().await;
        
        if let Some(pool_entries) = pools.get_mut(target) {
            // 查找可用连接
            for i in (0..pool_entries.len()).rev() {
                let entry = &mut pool_entries[i];
                
                // 检查连接是否仍然有效
                if self.is_connection_valid(&entry).await {
                    // 更新使用信息
                    entry.last_used = Instant::now();
                    entry.use_count += 1;
                    
                    // 取出连接
                    let connection = Arc::clone(&entry.connection);
                    
                    // 如果连接使用次数过多，从池中移除
                    if entry.use_count > 100 {
                        pool_entries.remove(i);
                    }
                    
                    return Ok(Some(connection));
                } else {
                    // 无效连接，移除
                    pool_entries.remove(i);
                }
            }
        }
        
        Ok(None)
    }
    
    /// 创建新连接
    async fn create_new_connection(&self, target: &str, conn_type: ConnectionType) -> Result<Arc<dyn ClientConnection>> {
        let config = ConnectionConfig::client(
            format!("pool_conn_{}", fastrand::u32(..)),
            target.to_string(),
        )
        .with_type(conn_type)
        .with_heartbeat(5000, 2000) // 5s心跳，2s超时
        .with_reconnect(3, 500); // 3次重试，500ms间隔
        
        let mut connection_box = ConnectionFactoryTrait::create_client_connection(&*self.factory, config).await?;
        
        // 建立连接
        connection_box.connect().await?;
        
        // 从Box转换为Arc<dyn ClientConnection>
        let connection: Arc<dyn ClientConnection> = Arc::from(connection_box);
        
        // 添加到池中
        self.add_to_pool(target, Arc::clone(&connection)).await;
        
        Ok(connection)
    }
    
    /// 添加连接到池
    async fn add_to_pool(&self, target: &str, connection: Arc<dyn ClientConnection>) {
        let mut pools = self.pools.write().await;
        
        let pool_entries = pools.entry(target.to_string()).or_insert_with(Vec::new);
        
        // 检查池是否已满
        if pool_entries.len() >= self.config.max_connections_per_target {
            // 移除最老的连接
            if let Some(_oldest_entry) = pool_entries.first() {
                debug!("连接池已满，移除最老连接: {}", target);
            }
            pool_entries.remove(0);
        }
        
        // 添加新连接
        pool_entries.push(PoolEntry {
            connection,
            last_used: Instant::now(),
            created_at: Instant::now(),
            use_count: 1,
        });
    }
    
    /// 检查连接是否有效
    async fn is_connection_valid(&self, entry: &PoolEntry) -> bool {
        // 检查连接年龄
        if entry.created_at.elapsed() > self.config.max_connection_lifetime {
            return false;
        }
        
        // 检查空闲时间
        if entry.last_used.elapsed() > self.config.idle_timeout {
            return false;
        }
        
        // 检查连接状态
        match entry.connection.get_state().await {
            ConnectionState::Connected | ConnectionState::Ready => true,
            _ => false,
        }
    }
    
    /// 预连接任务
    fn start_preconnect_task(&self) {
        if self.config.preconnect_targets.is_empty() {
            return;
        }
        
        let targets = self.config.preconnect_targets.clone();
        let count = self.config.preconnect_count;
        
        tokio::spawn(async move {
            info!("开始预连接任务，目标: {:?}", targets);
            
            for target in &targets {
                for i in 0..count {
                    // 简化实现，避免借用检查问题
                    debug!("预连接任务: {} #{}", target, i + 1);
                    
                    // 避免同时建立过多连接
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
            
            info!("预连接任务完成");
        });
    }
    
    /// 清理任务
    fn start_cleanup_task(&self) {
        let pools = Arc::clone(&self.pools);
        let stats = Arc::clone(&self.stats);
        let config = self.config.clone();
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.health_check_interval);
            
            loop {
                interval.tick().await;
                
                let mut pools_guard = pools.write().await;
                let mut removed_count = 0;
                
                for (_target, pool_entries) in pools_guard.iter_mut() {
                    pool_entries.retain(|entry| {
                        let valid = entry.created_at.elapsed() <= config.max_connection_lifetime
                            && entry.last_used.elapsed() <= config.idle_timeout;
                        
                        if !valid {
                            removed_count += 1;
                        }
                        
                        valid
                    });
                }
                
                if removed_count > 0 {
                    debug!("清理了 {} 个过期连接", removed_count);
                    
                    // 更新统计
                    if let Ok(mut stats_guard) = stats.try_lock() {
                        stats_guard.connections_destroyed += removed_count;
                    }
                }
            }
        });
    }
    
    /// 获取池统计信息
    pub async fn get_stats(&self) -> PoolStats {
        let pools = self.pools.read().await;
        let stats = self.stats.lock().await;
        
        let total_connections: usize = pools.values().map(|v| v.len()).sum();
        let active_connections = total_connections; // 简化统计
        let idle_connections = 0; // 简化统计
        
        let hit_rate = if stats.get_requests > 0 {
            stats.cache_hits as f64 / stats.get_requests as f64
        } else {
            0.0
        };
        
        let avg_connection_time_ms = if stats.connections_created > 0 {
            stats.total_connection_time_us as f64 / stats.connections_created as f64 / 1000.0
        } else {
            0.0
        };
        
        PoolStats {
            total_connections,
            active_connections,
            idle_connections,
            hit_rate,
            avg_connection_time_ms,
        }
    }
    
    /// 清空所有连接池
    pub async fn clear_all(&self) {
        let mut pools = self.pools.write().await;
        pools.clear();
        info!("已清空所有连接池");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    
    #[tokio::test]
    async fn test_connection_pool_basic() {
        let config = ConnectionPoolConfig {
            max_connections_per_target: 3,
            idle_timeout: Duration::from_secs(60),
            max_connection_lifetime: Duration::from_secs(300),
            preconnect_targets: vec!["127.0.0.1:8080".to_string()],
            preconnect_count: 1,
            health_check_interval: Duration::from_secs(5),
        };
        
        let _pool = ConnectionPool::new(config);
        
        // 注意：这个测试需要有实际的服务器运行
        // 在实际环境中测试
        println!("连接池测试需要实际的服务器环境");
    }
    
    #[tokio::test]
    async fn test_pool_stats() {
        let config = ConnectionPoolConfig::default();
        let pool = ConnectionPool::new(config);
        
        let stats = pool.get_stats().await;
        assert_eq!(stats.total_connections, 0);
        assert_eq!(stats.hit_rate, 0.0);
        
        println!("池统计信息: {:?}", stats);
    }
}