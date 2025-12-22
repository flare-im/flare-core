//! 设备平台和设备信息管理
//!
//! 支持多端设备管理，包括平台类型、设备标识、冲突策略等

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// 设备平台类型
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DevicePlatform {
    /// Web 浏览器
    Web,
    /// PC 桌面应用（Windows、macOS、Linux）
    PC,
    /// H5 移动网页
    H5,
    /// Android 应用
    Android,
    /// iOS 应用
    IOS,
    /// 鸿蒙应用
    HarmonyOS,
    /// 其他平台
    Other(String),
}

impl DevicePlatform {
    /// 从字符串转换为平台类型
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        match s {
            s if s.eq_ignore_ascii_case("web") => DevicePlatform::Web,
            s if s.eq_ignore_ascii_case("pc") || s.eq_ignore_ascii_case("desktop") => {
                DevicePlatform::PC
            }
            s if s.eq_ignore_ascii_case("h5") || s.eq_ignore_ascii_case("mobile_web") => {
                DevicePlatform::H5
            }
            s if s.eq_ignore_ascii_case("android") => DevicePlatform::Android,
            s if s.eq_ignore_ascii_case("ios")
                || s.eq_ignore_ascii_case("iphone")
                || s.eq_ignore_ascii_case("ipad") =>
            {
                DevicePlatform::IOS
            }
            s if s.eq_ignore_ascii_case("harmonyos")
                || s.eq_ignore_ascii_case("harmony")
                || s.eq_ignore_ascii_case("openharmony") =>
            {
                DevicePlatform::HarmonyOS
            }
            other => DevicePlatform::Other(other.to_string()),
        }
    }

    /// 转换为字符串
    pub fn as_str(&self) -> &str {
        match self {
            DevicePlatform::Web => "web",
            DevicePlatform::PC => "pc",
            DevicePlatform::H5 => "h5",
            DevicePlatform::Android => "android",
            DevicePlatform::IOS => "ios",
            DevicePlatform::HarmonyOS => "harmonyos",
            DevicePlatform::Other(s) => s,
        }
    }

    /// 判断是否为移动端平台
    pub fn is_mobile(&self) -> bool {
        matches!(
            self,
            DevicePlatform::H5
                | DevicePlatform::Android
                | DevicePlatform::IOS
                | DevicePlatform::HarmonyOS
        )
    }
}

/// 设备信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceInfo {
    /// 设备唯一标识符（由客户端生成）
    pub device_id: String,
    /// 设备平台类型
    pub platform: DevicePlatform,
    /// 设备型号（可选，如 "iPhone 14", "Samsung Galaxy S23"）
    pub model: Option<String>,
    /// 应用版本（可选）
    pub app_version: Option<String>,
    /// 系统版本（可选）
    pub system_version: Option<String>,
    /// 其他自定义元数据
    pub metadata: std::collections::HashMap<String, String>,
}

impl DeviceInfo {
    /// 创建新的设备信息
    pub fn new(device_id: String, platform: DevicePlatform) -> Self {
        Self {
            device_id,
            platform,
            model: None,
            app_version: None,
            system_version: None,
            metadata: std::collections::HashMap::new(),
        }
    }

    /// 设置设备型号
    pub fn with_model(mut self, model: String) -> Self {
        self.model = Some(model);
        self
    }

    /// 设置应用版本
    pub fn with_app_version(mut self, version: String) -> Self {
        self.app_version = Some(version);
        self
    }

    /// 设置系统版本
    pub fn with_system_version(mut self, version: String) -> Self {
        self.system_version = Some(version);
        self
    }

    /// 添加元数据
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }
}

/// 设备冲突处理策略
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum DeviceConflictStrategy {
    /// 允许所有设备同时在线（不检查冲突）
    #[default]
    AllowAll,
    /// 移动端互斥（同一用户只能有一个移动端设备在线）
    /// 例如：Android 和 iOS 互斥，但 PC 和移动端可以同时在线
    MobileExclusive,
    /// 平台互斥（同一平台只能有一个设备在线）
    /// 例如：同一用户的 Android 设备之间互斥，但 Android 和 iOS 可以同时在线
    PlatformExclusive,
    /// 完全互斥（同一用户只能有一个设备在线）
    FullyExclusive,
    /// 移动端和PC端共存（移动端之间互斥，PC端之间互斥，但移动端和PC端可以同时在线）
    /// 例如：同一用户可以有 1 个移动端 + 1 个 PC 同时在线
    MobileAndPcCoexist,
    /// 自定义规则（指定哪些平台可以同时在线）
    Custom {
        /// 允许同时在线 platform 组合列表
        /// 例如：[{Web, PC}] 表示 Web 和 PC 可以同时在线
        allowed_combinations: Vec<HashSet<DevicePlatform>>,
        /// 互斥的平台组（组内互斥）
        /// 例如：[{Android, IOS, HarmonyOS}] 表示移动端互斥
        exclusive_groups: Vec<HashSet<DevicePlatform>>,
    },
}

impl DeviceConflictStrategy {
    /// 检查新设备是否可以与现有设备同时在线
    ///
    /// # 参数
    /// - `new_device`: 新设备的平台类型
    /// - `existing_devices`: 现有设备的平台类型集合
    ///
    /// # 返回
    /// - `Ok(())`: 可以同时在线
    /// - `Err(Vec<DevicePlatform>)`: 需要被踢掉的设备平台列表
    pub fn check_conflict(
        &self,
        new_device: DevicePlatform,
        existing_devices: &HashSet<DevicePlatform>,
    ) -> Result<(), Vec<DevicePlatform>> {
        match self {
            DeviceConflictStrategy::AllowAll => Ok(()),

            DeviceConflictStrategy::MobileExclusive => {
                if new_device.is_mobile() {
                    let mobile_conflicts = self.get_mobile_devices(existing_devices);
                    if !mobile_conflicts.is_empty() {
                        return Err(mobile_conflicts);
                    }
                }
                Ok(())
            }

            DeviceConflictStrategy::PlatformExclusive => {
                if existing_devices.contains(&new_device) {
                    return Err(vec![new_device]);
                }
                Ok(())
            }

            DeviceConflictStrategy::FullyExclusive => {
                if !existing_devices.is_empty() {
                    return Err(existing_devices.iter().cloned().collect());
                }
                Ok(())
            }

            DeviceConflictStrategy::MobileAndPcCoexist => {
                // 移动端和PC端共存策略
                if new_device.is_mobile() {
                    let mobile_conflicts = self.get_mobile_devices(existing_devices);
                    if !mobile_conflicts.is_empty() {
                        return Err(mobile_conflicts);
                    }
                } else if matches!(new_device, DevicePlatform::Web | DevicePlatform::PC) {
                    let pc_conflicts: Vec<DevicePlatform> = existing_devices
                        .iter()
                        .filter(|p| matches!(p, DevicePlatform::Web | DevicePlatform::PC))
                        .cloned()
                        .collect();
                    if !pc_conflicts.is_empty() {
                        return Err(pc_conflicts);
                    }
                } else {
                    // 其他平台（如 Other），检查是否有相同平台
                    if existing_devices.contains(&new_device) {
                        return Err(vec![new_device]);
                    }
                }
                Ok(())
            }

            DeviceConflictStrategy::Custom {
                allowed_combinations,
                exclusive_groups,
            } => {
                // 检查互斥组
                for group in exclusive_groups {
                    if group.contains(&new_device) {
                        let conflicts: Vec<DevicePlatform> = existing_devices
                            .iter()
                            .filter(|p| group.contains(p))
                            .cloned()
                            .collect();
                        if !conflicts.is_empty() {
                            return Err(conflicts);
                        }
                    }
                }

                // 检查允许的组合
                let mut combined = existing_devices.clone();
                combined.insert(new_device.clone());

                if combined.len() > 1 {
                    let is_allowed = allowed_combinations
                        .iter()
                        .any(|allowed| combined.is_subset(allowed));

                    if !is_allowed {
                        return Err(existing_devices.iter().cloned().collect());
                    }
                }

                Ok(())
            }
        }
    }

    /// 获取所有移动端设备（辅助方法）
    fn get_mobile_devices(&self, devices: &HashSet<DevicePlatform>) -> Vec<DevicePlatform> {
        devices.iter().filter(|p| p.is_mobile()).cloned().collect()
    }
}

/// 设备冲突策略构建器
pub struct DeviceConflictStrategyBuilder {
    strategy: DeviceConflictStrategy,
}

impl DeviceConflictStrategyBuilder {
    /// 创建新的构建器（默认允许所有）
    pub fn new() -> Self {
        Self {
            strategy: DeviceConflictStrategy::AllowAll,
        }
    }

    /// 设置移动端互斥
    pub fn mobile_exclusive(mut self) -> Self {
        self.strategy = DeviceConflictStrategy::MobileExclusive;
        self
    }

    /// 设置平台互斥
    pub fn platform_exclusive(mut self) -> Self {
        self.strategy = DeviceConflictStrategy::PlatformExclusive;
        self
    }

    /// 设置完全互斥
    pub fn fully_exclusive(mut self) -> Self {
        self.strategy = DeviceConflictStrategy::FullyExclusive;
        self
    }

    /// 设置移动端和PC端共存
    ///
    /// 移动端之间互斥，PC端之间互斥，但移动端和PC端可以同时在线
    /// 例如：同一用户可以有 1 个移动端（Android/iOS/HarmonyOS/H5） + 1 个 PC（Web/PC）同时在线
    pub fn mobile_and_pc_coexist(mut self) -> Self {
        self.strategy = DeviceConflictStrategy::MobileAndPcCoexist;
        self
    }

    /// 设置自定义规则
    pub fn custom(
        mut self,
        allowed_combinations: Vec<HashSet<DevicePlatform>>,
        exclusive_groups: Vec<HashSet<DevicePlatform>>,
    ) -> Self {
        self.strategy = DeviceConflictStrategy::Custom {
            allowed_combinations,
            exclusive_groups,
        };
        self
    }

    /// 构建策略
    pub fn build(self) -> DeviceConflictStrategy {
        self.strategy
    }
}

impl Default for DeviceConflictStrategyBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mobile_exclusive() {
        let strategy = DeviceConflictStrategy::MobileExclusive;
        let mut existing = HashSet::new();
        existing.insert(DevicePlatform::Android);

        // 新设备是移动端，应该冲突
        assert!(
            strategy
                .check_conflict(DevicePlatform::IOS, &existing)
                .is_err()
        );

        // 新设备是 PC，应该允许
        assert!(
            strategy
                .check_conflict(DevicePlatform::PC, &existing)
                .is_ok()
        );
    }

    #[test]
    fn test_platform_exclusive() {
        let strategy = DeviceConflictStrategy::PlatformExclusive;
        let mut existing = HashSet::new();
        existing.insert(DevicePlatform::Web);

        // 相同平台应该冲突
        assert!(
            strategy
                .check_conflict(DevicePlatform::Web, &existing)
                .is_err()
        );

        // 不同平台应该允许
        assert!(
            strategy
                .check_conflict(DevicePlatform::PC, &existing)
                .is_ok()
        );
    }
}
