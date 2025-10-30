use crate::common::connections::traits::ServerConnection;
use crate::common::connections::enums::ConnectionState;
use crate::common::error::FlareError;
use crate::server::manager::traits::{ConnectionManager as ConnectionManagerTrait, AggregatedStats, BroadcastStats, ConnectionInfo};
use crate::server::events::handler::EventHandlerAdapter;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::HashMap;
use tracing::{info, warn};

/// 连接管理器
/// 
/// 负责管理所有服务端连接的生命周期，包括：
/// - 连接的添加、删除和查询
/// - 连接状态监控和统计信息收集
/// - 连接过期清理
pub struct ConnectionManagerImpl {
    /// 按连接ID索引的连接映射
    by_id: DashMap<String, Arc<dyn ServerConnection>>,
    /// 事件处理器适配器
    event_handler_adapter: Arc<tokio::sync::RwLock<Option<Arc<dyn crate::server::events::handler::EnhancedEventHandler>>>>,
}

impl Default for ConnectionManagerImpl {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionManagerImpl {
    /// 创建新的连接管理器
    pub fn new() -> Self {
        Self { 
            by_id: DashMap::new(), 
            event_handler_adapter: Arc::new(tokio::sync::RwLock::new(None)),
        }
    }
    
    /// 设置事件处理器适配器
    pub async fn set_event_handler(&self, handler: Arc<dyn crate::server::events::handler::EnhancedEventHandler>) {
        let mut event_handler = self.event_handler_adapter.write().await;
        *event_handler = Some(handler);
    }
    
    /// 获取事件处理器适配器
    pub async fn get_event_handler_adapter(&self) -> EventHandlerAdapter {
        let event_handler = self.event_handler_adapter.read().await;
        if let Some(handler) = &*event_handler {
            EventHandlerAdapter::with_handler(handler.clone())
        } else {
            EventHandlerAdapter::new()
        }
    }
}

#[async_trait::async_trait]
impl ConnectionManagerTrait for ConnectionManagerImpl {
    /// 添加连接（不关联用户ID，适用于无需认证的场景）
    /// 
    /// # 参数
    /// * `conn` - 要添加的连接
    /// 
    /// # 返回值
    /// 如果是新连接返回 true，如果已存在返回 false
    fn add_connection(&self, conn: Arc<dyn ServerConnection>) -> bool {
        let id = conn.id();
        
        // 记录日志
        info!("添加连接: ID={}", id);
        
        // 添加到ID映射
        let is_new = !self.by_id.contains_key(&id);
        self.by_id.insert(id, conn);
        
        is_new
    }

    /// 移除连接
    /// 
    /// # 参数
    /// * `id` - 要移除的连接ID
    /// 
    /// # 返回值
    /// 如果找到并移除连接则返回该连接，否则返回 None
    fn remove_connection(&self, id: &str) -> Option<Arc<dyn ServerConnection>> {
        // 从ID映射中移除
        let conn = self.by_id.remove(id).map(|(_, v)| v);
        
        if conn.is_some() {
            info!("移除连接: ID={}", id);
        }
        
        conn
    }

    /// 获取连接
    /// 
    /// # 参数
    /// * `id` - 连接ID
    /// 
    /// # 返回值
    /// 如果找到连接则返回该连接的克隆，否则返回 None
    fn get_connection(&self, id: &str) -> Option<Arc<dyn ServerConnection>> {
        self.by_id.get(id).map(|r| Arc::clone(&*r))
    }

    /// 获取聚合统计信息
    /// 
    /// # 返回值
    /// 当前所有连接的聚合统计信息
    fn stats_snapshot(&self) -> AggregatedStats {
        let total = self.by_id.len();
        
        let mut active = 0;
        let mut failed = 0;
        let mut reconnecting = 0;
        let mut total_quality = 0u64;
        let mut quality_count = 0u64;
        let mut by_state: HashMap<ConnectionState, usize> = HashMap::with_capacity(16);
        
        // 遍历所有连接收集统计信息
        for item in self.by_id.iter() {
            let conn = item.value();
            let stats = conn.stats();
            let state = conn.state().clone();
            
            // 统计各状态连接数
            *by_state.entry(state.clone()).or_insert(0) += 1;
            
            // 检查连接状态
            match &state {
                ConnectionState::Connected => active += 1,
                ConnectionState::Failed => failed += 1,
                ConnectionState::Reconnecting => reconnecting += 1,
                _ => {} // 其他状态不计入特定计数
            }
            
            // 统计连接质量
            if let Some(quality) = stats.quality {
                total_quality += quality as u64;
                quality_count += 1;
            }
        }
        
        let avg_quality = if quality_count > 0 {
            Some((total_quality / quality_count) as u8)
        } else {
            None
        };
        
        AggregatedStats { 
            total, 
            active, 
            failed, 
            reconnecting, 
            msg_send_rate: None, 
            msg_recv_rate: None, 
            avg_quality,
            by_state,
        }
    }

    /// 清理过期连接
    /// 
    /// # 参数
    /// * `heartbeat_monitor_timeout_ms` - 心跳监控超时时间（毫秒）
    /// 
    /// # 返回值
    /// 操作结果
    fn cleanup(&self, heartbeat_monitor_timeout_ms: u64) -> Result<(), FlareError> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
            
        let mut to_remove: Vec<String> = Vec::new();
        
        // 查找过期连接
        for item in self.by_id.iter() {
            let id = item.key();
            let conn = item.value();
            let last_activity = conn.last_activity_epoch_ms();
            
            if now_ms.saturating_sub(last_activity) > heartbeat_monitor_timeout_ms {
                to_remove.push(id.clone());
            }
        }
        
        // 移除过期连接
        let removed_count = to_remove.len();
        for id in to_remove {
            if let Some(conn) = self.remove_connection(&id) {
                warn!("清理过期连接: ID={}, 超时时间={}ms", id, heartbeat_monitor_timeout_ms);
                
                // 关闭连接
                if let Err(e) = conn.close(Some("heartbeat monitor timeout".into())) {
                    warn!("关闭过期连接时出错: ID={}, 错误={:?}", id, e);
                }
            }
        }
        
        if removed_count > 0 {
            info!("共清理 {} 个过期连接", removed_count);
        }
        
        Ok(())
    }
    
    /// 获取连接总数
    /// 
    /// # 返回值
    /// 当前管理的连接总数
    fn connection_count(&self) -> usize {
        self.by_id.len()
    }
    
    /// 获取所有连接ID列表
    /// 
    /// # 返回值
    /// 所有连接ID的列表
    fn all_connection_ids(&self) -> Vec<String> {
        self.by_id.iter().map(|item| item.key().clone()).collect()
    }
    
    /// 广播消息给所有连接
    /// 
    /// # 参数
    /// * `frame` - 要广播的消息帧
    /// 
    /// # 返回值
    /// 发送结果的统计信息
    fn broadcast_message(&self, frame: crate::common::protocol::frame::Frame) -> Result<BroadcastStats, FlareError> {
        let mut stats = BroadcastStats::default();
        
        for item in self.by_id.iter() {
            let conn = item.value();
            match conn.send_message(frame.clone()) {
                Ok(()) => {
                    stats.success += 1;
                }
                Err(e) => {
                    stats.failed += 1;
                    warn!("广播消息失败: 连接ID={}, 错误={:?}", item.key(), e);
                }
            }
        }
        
        Ok(stats)
    }
    
    /// 按连接状态过滤连接
    /// 
    /// # 参数
    /// * `state` - 连接状态
    /// 
    /// # 返回值
    /// 指定状态的连接ID列表
    fn get_connections_by_state(&self, state: ConnectionState) -> Vec<String> {
        self.by_id
            .iter()
            .filter(|item| {
                let conn_state = item.value().state().clone();
                conn_state == state
            })
            .map(|item| item.key().clone())
            .collect()
    }
    
    /// 获取连接的详细信息
    /// 
    /// # 参数
    /// * `id` - 连接ID
    /// 
    /// # 返回值
    /// 连接的详细信息，如果连接不存在则返回None
    fn get_connection_info(&self, id: &str) -> Option<ConnectionInfo> {
        self.by_id.get(id).map(|item| {
            let conn = item.value();
            let stats = conn.stats();
            
            ConnectionInfo {
                id: id.to_string(),
                state: conn.state().clone(),
                user_id: None, // 基础连接管理器不处理用户认证
                established_at: stats.established_epoch_ms,
                last_activity_at: stats.last_activity_epoch_ms,
                quality: stats.quality,
                messages_sent: stats.messages_sent,
                messages_received: stats.messages_received,
            }
        })
    }
    
    /// 获取所有连接的详细信息
    /// 
    /// # 返回值
    /// 所有连接的详细信息列表
    fn all_connection_info(&self) -> Vec<ConnectionInfo> {
        self.by_id
            .iter()
            .map(|item| {
                let conn = item.value();
                let stats = conn.stats();
                
                ConnectionInfo {
                    id: item.key().clone(),
                    state: conn.state().clone(),
                    user_id: None, // 基础连接管理器不处理用户认证
                    established_at: stats.established_epoch_ms,
                    last_activity_at: stats.last_activity_epoch_ms,
                    quality: stats.quality,
                    messages_sent: stats.messages_sent,
                    messages_received: stats.messages_received,
                }
            })
            .collect()
    }
    
    /// 获取事件处理器适配器
    async fn get_event_handler_adapter(&self) -> EventHandlerAdapter {
        let event_handler = self.event_handler_adapter.read().await;
        if let Some(handler) = &*event_handler {
            EventHandlerAdapter::with_handler(handler.clone())
        } else {
            EventHandlerAdapter::new()
        }
    }
    
    /// 设置事件处理器
    async fn set_event_handler(&self, handler: Arc<dyn crate::server::events::handler::EnhancedEventHandler>) {
        let mut event_handler = self.event_handler_adapter.write().await;
        *event_handler = Some(handler);
    }
}