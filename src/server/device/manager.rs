//! 设备管理器
//!
//! 管理用户设备的在线状态，处理设备冲突

use crate::common::device::{DeviceConflictStrategy, DeviceInfo, DevicePlatform};
use crate::common::error::Result;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// 用户设备信息
#[derive(Debug, Clone)]
struct UserDevice {
    /// 连接 ID
    connection_id: String,
    /// 设备信息
    device_info: DeviceInfo,
}

/// 设备管理器
///
/// 管理用户设备的在线状态，根据冲突策略处理设备登录
pub struct DeviceManager {
    /// 用户ID -> 设备列表（连接ID -> 设备信息）
    user_devices: Arc<RwLock<HashMap<String, HashMap<String, UserDevice>>>>,
    /// 冲突策略
    conflict_strategy: DeviceConflictStrategy,
}

impl DeviceManager {
    /// 创建新的设备管理器
    pub fn new(conflict_strategy: DeviceConflictStrategy) -> Self {
        Self {
            user_devices: Arc::new(RwLock::new(HashMap::new())),
            conflict_strategy,
        }
    }

    /// 添加设备（检查冲突）
    ///
    /// # 参数
    /// - `user_id`: 用户 ID
    /// - `connection_id`: 连接 ID
    /// - `device_info`: 设备信息
    ///
    /// # 返回
    /// - `Ok(Vec<String>)`: 需要被踢掉的连接 ID 列表（可能为空）
    /// - `Err`: 如果添加失败
    pub async fn add_device(
        &self,
        user_id: &str,
        connection_id: String,
        device_info: DeviceInfo,
    ) -> Result<Vec<String>> {
        let mut user_devices = self.user_devices.write().await;

        // 获取用户现有设备
        let user_device_map = user_devices
            .entry(user_id.to_string())
            .or_insert_with(HashMap::new);

        // 收集现有设备的平台类型
        let existing_platforms: HashSet<DevicePlatform> = user_device_map
            .values()
            .map(|d| d.device_info.platform.clone())
            .collect();

        info!(
            "[DeviceManager] 添加设备: user_id={}, connection_id={}, platform={:?}, 现有设备数={}, 现有平台={:?}",
            user_id,
            connection_id,
            device_info.platform,
            user_device_map.len(),
            existing_platforms
        );

        // 检查冲突（先克隆 platform，避免移动 device_info）
        let platform = device_info.platform.clone();
        let conflicts = match self
            .conflict_strategy
            .check_conflict(platform.clone(), &existing_platforms)
        {
            Ok(_) => {
                debug!("[DeviceManager] 无冲突，允许添加设备");
                Vec::new()
            }
            Err(conflict_platforms) => {
                info!(
                    "[DeviceManager] 检测到冲突: 新平台={:?}, 冲突平台={:?}",
                    platform, conflict_platforms
                );
                // 找到需要踢掉的连接（确保不包含新连接本身）
                let mut conflict_connections = Vec::new();
                for (conn_id, device) in user_device_map.iter() {
                    // 确保不包含新连接本身
                    if conn_id == &connection_id {
                        continue;
                    }
                    if conflict_platforms.contains(&device.device_info.platform) {
                        info!(
                            "[DeviceManager] 发现冲突连接: connection_id={}, platform={:?} (新连接ID: {})",
                            conn_id, device.device_info.platform, connection_id
                        );
                        conflict_connections.push(conn_id.clone());
                    }
                }
                if conflict_connections.is_empty() {
                    warn!(
                        "[DeviceManager] 警告：检测到冲突但未找到冲突连接，现有设备: {:?}",
                        user_device_map.keys().collect::<Vec<_>>()
                    );
                }
                conflict_connections
            }
        };

        // 移除冲突的设备
        for conn_id in &conflicts {
            user_device_map.remove(conn_id);
            info!("[DeviceManager] 已移除冲突设备: connection_id={}", conn_id);
        }

        // 添加新设备
        user_device_map.insert(
            connection_id.clone(),
            UserDevice {
                connection_id: connection_id.clone(),
                device_info,
            },
        );
        info!(
            "[DeviceManager] 新设备已添加: user_id={}, connection_id={}, 当前设备数={}",
            user_id,
            connection_id,
            user_device_map.len()
        );

        Ok(conflicts)
    }

    /// 移除设备
    pub async fn remove_device(&self, user_id: &str, connection_id: &str) -> Result<()> {
        let mut user_devices = self.user_devices.write().await;

        if let Some(devices) = user_devices.get_mut(user_id) {
            devices.remove(connection_id);

            // 如果用户没有设备了，移除用户
            if devices.is_empty() {
                user_devices.remove(user_id);
            }
        }

        Ok(())
    }

    /// 获取用户的所有设备
    pub async fn get_user_devices(&self, user_id: &str) -> Vec<DeviceInfo> {
        let user_devices = self.user_devices.read().await;

        user_devices
            .get(user_id)
            .map(|devices| devices.values().map(|d| d.device_info.clone()).collect())
            .unwrap_or_default()
    }

    /// 获取用户的所有连接 ID
    pub async fn get_user_connections(&self, user_id: &str) -> Vec<String> {
        let user_devices = self.user_devices.read().await;

        user_devices
            .get(user_id)
            .map(|devices| devices.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// 获取设备的连接 ID
    pub async fn get_device_connection(&self, user_id: &str, device_id: &str) -> Option<String> {
        let user_devices = self.user_devices.read().await;

        user_devices.get(user_id).and_then(|devices| {
            devices
                .values()
                .find(|d| d.device_info.device_id == device_id)
                .map(|d| d.connection_id.clone())
        })
    }

    /// 更新冲突策略
    pub fn update_strategy(&mut self, strategy: DeviceConflictStrategy) {
        self.conflict_strategy = strategy;
    }
}
