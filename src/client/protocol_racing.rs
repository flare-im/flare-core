//! 协议竞速管理器
//! 
//! 实现智能协议选择和动态切换算法

use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{Duration, Instant};
use tracing::{info, warn, debug};
use serde::{Deserialize, Serialize};

use crate::common::{
    error::{Result, FlareError},
    protocol::{ProtocolSelection, ConnectionQuality},
    connections::types::ConnectionMetrics,
};

/// 协议优先级策略
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProtocolPriority {
    /// QUIC 优先
    QuicFirst,
    /// WebSocket 优先
    WebSocketFirst,
    /// 自动选择
    Auto,
}

use super::config::{ClientConfig, ProtocolRacingConfig, QualityWeights};

/// 协议性能指标
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolMetrics {
    /// 协议类型
    pub protocol: ProtocolSelection,
    /// 连接时间（毫秒）
    pub connection_time_ms: u32,
    /// 延迟（毫秒）
    pub latency_ms: u32,
    /// 抖动（毫秒）
    pub jitter_ms: u32,
    /// 丢包率（百分比）
    pub packet_loss_percent: f32,
    /// 带宽（字节/秒）
    pub bandwidth_bps: u64,
    /// 稳定性评分（0-100）
    pub stability_score: u8,
    /// 最后更新时间（时间戳，毫秒）
    pub last_updated: u64,
    /// 测试次数
    pub test_count: u32,
    /// 成功率
    pub success_rate: f32,
}

impl ProtocolMetrics {
    /// 创建新的协议指标
    pub fn new(protocol: ProtocolSelection) -> Self {
        Self {
            protocol,
            connection_time_ms: 0,
            latency_ms: 0,
            jitter_ms: 0,
            packet_loss_percent: 0.0,
            bandwidth_bps: 0,
            stability_score: 100,
            last_updated: chrono::Utc::now().timestamp_millis() as u64,
            test_count: 0,
            success_rate: 1.0,
        }
    }
    
    /// 计算综合评分
    pub fn calculate_score(&self, weights: &QualityWeights) -> f32 {
        // 延迟评分（越低越好，转换为0-100分）
        let latency_score = (1000.0 / (self.latency_ms as f32 + 1.0)).min(100.0);
        
        // 稳定性评分（0-100分）
        let stability_score = self.stability_score as f32;
        
        // 连接时间评分（越低越好，转换为0-100分）
        let connection_score = (1000.0 / (self.connection_time_ms as f32 + 1.0)).min(100.0);
        
        // 丢包率评分（越低越好，转换为0-100分）
        let packet_loss_score = (100.0 - self.packet_loss_percent).max(0.0);
        
        // 带宽评分（越高越好，标准化到0-100分）
        let bandwidth_score = (self.bandwidth_bps as f32 / 1_000_000.0).min(100.0); // 假设100Mbps为满分
        
        // 成功率评分
        let success_score = self.success_rate * 100.0;
        
        // 加权计算综合评分
        let total_score = 
            latency_score * weights.latency_weight +
            stability_score * weights.stability_weight +
            connection_score * weights.connection_time_weight +
            packet_loss_score * 0.15 +      // 15%权重
            bandwidth_score * 0.1 +         // 10%权重
            success_score * 0.1;            // 10%权重
        
        total_score
    }
    
    /// 更新指标
    pub fn update(&mut self, metrics: &ConnectionMetrics, connection_time: Duration, success: bool) {
        self.connection_time_ms = connection_time.as_millis() as u32;
        self.latency_ms = metrics.latency_ms;
        self.jitter_ms = metrics.jitter_ms;
        self.packet_loss_percent = metrics.packet_loss_percent;
        self.bandwidth_bps = metrics.bandwidth_bps;
        self.stability_score = metrics.stability_score;
        self.last_updated = chrono::Utc::now().timestamp_millis() as u64;
        self.test_count += 1;
        
        // 更新成功率
        if success {
            self.success_rate = (self.success_rate * (self.test_count - 1) as f32 + 1.0) / self.test_count as f32;
        } else {
            self.success_rate = (self.success_rate * (self.test_count - 1) as f32) / self.test_count as f32;
        }
    }
    
    /// 检查指标是否过期
    pub fn is_expired(&self, max_age: Duration) -> bool {
        let now = chrono::Utc::now().timestamp_millis() as u64;
        let elapsed = now.saturating_sub(self.last_updated);
        elapsed > max_age.as_millis() as u64
    }
}

/// 协议竞速管理器
pub struct ProtocolRacingManager {
    /// 配置
    config: ProtocolRacingConfig,
    /// 协议指标缓存
    protocol_metrics: Arc<RwLock<std::collections::HashMap<ProtocolSelection, ProtocolMetrics>>>,
    /// 当前选择的协议
    current_protocol: Arc<Mutex<ProtocolSelection>>,
    /// 最后竞速测试时间
    last_racing_test: Arc<Mutex<Instant>>,
    /// 竞速测试任务句柄
    racing_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl ProtocolRacingManager {
    /// 创建新的协议竞速管理器
    pub fn new(config: ProtocolRacingConfig) -> Self {
        Self {
            config,
            protocol_metrics: Arc::new(RwLock::new(std::collections::HashMap::new())),
            current_protocol: Arc::new(Mutex::new(ProtocolSelection::QuicOnly)), // QUIC 优先
            last_racing_test: Arc::new(Mutex::new(Instant::now())),
            racing_task: Arc::new(Mutex::new(None)),
        }
    }
    
    /// 启动协议竞速
    pub async fn start(&self) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }
        
        info!("启动协议竞速管理器");
        
        // 启动竞速测试任务
        let config = self.config.clone();
        let protocol_metrics = Arc::clone(&self.protocol_metrics);
        let current_protocol = Arc::clone(&self.current_protocol);
        let last_racing_test = Arc::clone(&self.last_racing_test);
        
        let task = tokio::spawn(async move {
            Self::racing_task_loop(
                config,
                protocol_metrics,
                current_protocol,
                last_racing_test,
            ).await;
        });
        
        {
            let mut racing_task = self.racing_task.lock().await;
            *racing_task = Some(task);
        }
        
        Ok(())
    }
    
    /// 停止协议竞速
    pub async fn stop(&self) -> Result<()> {
        info!("停止协议竞速管理器");
        
        if let Some(task) = self.racing_task.lock().await.take() {
            task.abort();
        }
        
        Ok(())
    }
    
    /// 竞速测试任务循环
    async fn racing_task_loop(
        config: ProtocolRacingConfig,
        protocol_metrics: Arc<RwLock<std::collections::HashMap<ProtocolSelection, ProtocolMetrics>>>,
        current_protocol: Arc<Mutex<ProtocolSelection>>,
        last_racing_test: Arc<Mutex<Instant>>,
    ) {
        let mut interval = tokio::time::interval(Duration::from_millis(config.test_interval_ms as u64));
        
        loop {
            interval.tick().await;
            
            // 检查是否需要竞速测试
            {
                let last_test = *last_racing_test.lock().await;
                if last_test.elapsed() < Duration::from_millis(config.test_interval_ms as u64) {
                    continue;
                }
            }
            
            // 执行竞速测试
            if let Err(e) = Self::perform_racing_test(
                &config,
                &protocol_metrics,
                &current_protocol,
            ).await {
                warn!("协议竞速测试失败: {}", e);
            }
            
            // 更新最后测试时间
            {
                let mut last_test = last_racing_test.lock().await;
                *last_test = Instant::now();
            }
        }
    }
    
    /// 执行竞速测试
    async fn perform_racing_test(
        config: &ProtocolRacingConfig,
        protocol_metrics: &Arc<RwLock<std::collections::HashMap<ProtocolSelection, ProtocolMetrics>>>,
        current_protocol: &Arc<Mutex<ProtocolSelection>>,
    ) -> Result<()> {
        debug!("开始协议竞速测试");
        
        // 获取当前所有协议的指标
        let metrics = protocol_metrics.read().await;
        
        if metrics.is_empty() {
            debug!("没有可用的协议指标");
            return Ok(());
        }
        
        // 计算每个协议的综合评分
        let mut protocol_scores = Vec::new();
        for (protocol, metrics) in metrics.iter() {
            let score = metrics.calculate_score(&config.quality_weights);
            protocol_scores.push((*protocol, score));
        }
        
        // 按评分排序
        protocol_scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        
        // 获取当前协议和最佳协议
        let current = *current_protocol.lock().await;
        let best_protocol = protocol_scores[0].0;
        let best_score = protocol_scores[0].1;
        
        // 查找当前协议的评分
        let current_score = protocol_scores
            .iter()
            .find(|(p, _)| *p == current)
            .map(|(_, s)| *s)
            .unwrap_or(0.0);
        
        // QUIC 优先策略：只有在 QUIC 性能明显差于 WebSocket 时才切换
        let should_switch = if current == ProtocolSelection::QuicOnly {
            // 如果当前是 QUIC，只有在性能差异很大时才切换到 WebSocket
            let quic_score = current_score;
            let websocket_score = protocol_scores
                .iter()
                .find(|(p, _)| *p == ProtocolSelection::WebSocketOnly)
                .map(|(_, s)| *s)
                .unwrap_or(0.0);
            
            // WebSocket 需要比 QUIC 好 20% 以上才切换
            websocket_score > quic_score * 1.2
        } else if current == ProtocolSelection::WebSocketOnly {
            // 如果当前是 WebSocket，QUIC 只要稍微好一点就切换回去
            let websocket_score = current_score;
            let quic_score = protocol_scores
                .iter()
                .find(|(p, _)| *p == ProtocolSelection::QuicOnly)
                .map(|(_, s)| *s)
                .unwrap_or(0.0);
            
            // QUIC 只要比 WebSocket 好 5% 就切换回去
            quic_score > websocket_score * 1.05
        } else {
            // Auto 模式：使用标准阈值
            let score_difference = (best_score - current_score) / best_score;
            score_difference > config.switch_threshold
        };
        
        if should_switch {
            let target_protocol = if current == ProtocolSelection::QuicOnly {
                ProtocolSelection::WebSocketOnly
            } else {
                ProtocolSelection::QuicOnly
            };
            
            info!("QUIC 优先策略：切换到 {:?} (当前: {:?}, 评分: {:.1})", 
                  target_protocol, current, current_score);
            
            // 更新当前协议
            {
                let mut current = current_protocol.lock().await;
                *current = target_protocol;
            }
            
            info!("已切换到协议: {:?}", target_protocol);
        } else {
            debug!("保持当前协议 {:?} (评分: {:.1})", current, current_score);
        }
        
        // 输出竞速结果
        info!("协议竞速测试结果 (QUIC 优先策略):");
        for (protocol, score) in protocol_scores.iter().take(3) {
            let status = if *protocol == best_protocol { "🏆" } else if *protocol == current { "📍" } else { "  " };
            let priority = if *protocol == ProtocolSelection::QuicOnly { "🔥" } else { "  " };
            info!("  {} {} {:?}: {:.1}/100", status, priority, protocol, score);
        }
        
        Ok(())
    }
    
    /// 更新协议指标
    pub async fn update_protocol_metrics(
        &self,
        protocol: ProtocolSelection,
        metrics: &ConnectionMetrics,
        connection_time: Duration,
        success: bool,
    ) -> Result<()> {
        let mut protocol_metrics = self.protocol_metrics.write().await;
        
        let protocol_metric = protocol_metrics
            .entry(protocol)
            .or_insert_with(|| ProtocolMetrics::new(protocol));
        
        protocol_metric.update(metrics, connection_time, success);
        
        debug!("更新协议 {:?} 指标: 延迟={}ms, 稳定性={}/100, 成功率={:.1}%", 
               protocol, metrics.latency_ms, metrics.stability_score, protocol_metric.success_rate * 100.0);
        
        Ok(())
    }
    
    /// 获取当前选择的协议
    pub async fn get_current_protocol(&self) -> ProtocolSelection {
        *self.current_protocol.lock().await
    }
    
    /// 手动切换协议
    pub async fn switch_protocol(&self, protocol: ProtocolSelection) -> Result<()> {
        info!("手动切换到协议: {:?}", protocol);
        
        {
            let mut current = self.current_protocol.lock().await;
            *current = protocol;
        }
        
        Ok(())
    }
    
    /// 设置协议优先级策略
    pub async fn set_protocol_priority(&self, priority: ProtocolPriority) -> Result<()> {
        info!("设置协议优先级策略: {:?}", priority);
        
        let target_protocol = match priority {
            ProtocolPriority::QuicFirst => ProtocolSelection::QuicOnly,
            ProtocolPriority::WebSocketFirst => ProtocolSelection::WebSocketOnly,
            ProtocolPriority::Auto => ProtocolSelection::Auto,
        };
        
        self.switch_protocol(target_protocol).await
    }
    
    /// 强制使用 QUIC（忽略性能差异）
    pub async fn force_quic(&self) -> Result<()> {
        info!("强制使用 QUIC 协议");
        self.switch_protocol(ProtocolSelection::QuicOnly).await
    }
    
    /// 强制使用 WebSocket（忽略性能差异）
    pub async fn force_websocket(&self) -> Result<()> {
        info!("强制使用 WebSocket 协议");
        self.switch_protocol(ProtocolSelection::WebSocketOnly).await
    }
    
    /// 获取协议指标
    pub async fn get_protocol_metrics(&self, protocol: ProtocolSelection) -> Option<ProtocolMetrics> {
        let metrics = self.protocol_metrics.read().await;
        metrics.get(&protocol).cloned()
    }
    
    /// 获取所有协议指标
    pub async fn get_all_protocol_metrics(&self) -> Vec<ProtocolMetrics> {
        let metrics = self.protocol_metrics.read().await;
        metrics.values().cloned().collect()
    }
    
    /// 清理过期的指标
    pub async fn cleanup_expired_metrics(&self, max_age: Duration) -> Result<()> {
        let mut metrics = self.protocol_metrics.write().await;
        let expired_keys: Vec<_> = metrics
            .iter()
            .filter(|(_, m)| m.is_expired(max_age))
            .map(|(k, _)| *k)
            .collect();
        
        let expired_count = expired_keys.len();
        for key in expired_keys {
            metrics.remove(&key);
        }
        
        if expired_count > 0 {
            debug!("清理了 {} 个过期的协议指标", expired_count);
        }
        
        Ok(())
    }
}
