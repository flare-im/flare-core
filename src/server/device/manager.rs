//! 设备管理器
//!
//! 管理用户设备的在线状态，处理设备冲突。
//!
//! 设备按（租户，用户）分桶：冲突检测、策略上限带来的逐出、查询与移除都只在同一个
//! [`DeviceOwner`] 内进行。两个租户里 user_id 相同的用户是两个人，不会互相顶掉设备，
//! 也不会占用对方的设备名额。

use crate::common::device::{DeviceConflictStrategy, DeviceInfo, DevicePlatform};
use crate::common::error::Result;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// 未携带租户的连接所归入的默认租户。
///
/// 与 flare-im 的默认租户约定一致（缺省租户即 `"0"`）。缺失或全空白的租户都落到这里，
/// 所以它们只会和同样属于默认租户的连接共桶，不会并入任何其他租户。
pub const DEFAULT_TENANT_ID: &str = "0";

/// 设备归属：（租户，用户）。
///
/// 这是设备管理器的分桶键。它是结构化的二元组而不是拼接字符串，
/// 所以不同的（租户，用户）组合不会因为分隔符而撞到同一个桶里。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeviceOwner {
    tenant_id: String,
    user_id: String,
}

impl DeviceOwner {
    /// 创建设备归属
    ///
    /// `tenant_id` 缺失或全空白时使用 [`DEFAULT_TENANT_ID`]；其余值原样保留，
    /// 与连接元数据中的租户保持一致。
    pub fn new(tenant_id: Option<&str>, user_id: impl Into<String>) -> Self {
        let tenant_id = tenant_id
            .filter(|tenant_id| !tenant_id.trim().is_empty())
            .unwrap_or(DEFAULT_TENANT_ID);
        Self {
            tenant_id: tenant_id.to_string(),
            user_id: user_id.into(),
        }
    }

    /// 租户 ID（已套用默认租户）
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// 用户 ID
    pub fn user_id(&self) -> &str {
        &self.user_id
    }
}

/// 用户设备信息
#[derive(Debug, Clone)]
struct UserDevice {
    /// 连接 ID
    connection_id: String,
    /// 设备信息
    device_info: DeviceInfo,
}

/// 设备表：分桶数据与连接反查索引放在同一把锁下，保证两者一致。
#[derive(Default)]
struct DeviceRegistry {
    /// （租户，用户）-> 设备列表（连接 ID -> 设备信息）
    devices: HashMap<DeviceOwner, HashMap<String, UserDevice>>,
    /// 连接 ID -> 归属，断开时按连接精确清理
    owners: HashMap<String, DeviceOwner>,
}

impl DeviceRegistry {
    /// 从指定归属的桶里移除一个连接；桶空了就删掉桶。
    fn detach(&mut self, owner: &DeviceOwner, connection_id: &str) -> bool {
        let Some(devices) = self.devices.get_mut(owner) else {
            return false;
        };
        let removed = devices.remove(connection_id).is_some();
        if devices.is_empty() {
            self.devices.remove(owner);
        }
        removed
    }
}

/// 设备管理器
///
/// 管理用户设备的在线状态，根据冲突策略处理设备登录
pub struct DeviceManager {
    registry: Arc<RwLock<DeviceRegistry>>,
    /// 冲突策略
    conflict_strategy: DeviceConflictStrategy,
}

impl DeviceManager {
    /// 创建新的设备管理器
    pub fn new(conflict_strategy: DeviceConflictStrategy) -> Self {
        Self {
            registry: Arc::new(RwLock::new(DeviceRegistry::default())),
            conflict_strategy,
        }
    }

    /// 添加设备（检查冲突）
    ///
    /// 只和同一 [`DeviceOwner`] 下的设备比较，其他租户的同名用户不参与冲突判定。
    ///
    /// # 参数
    /// - `owner`: 设备归属（租户，用户）
    /// - `connection_id`: 连接 ID
    /// - `device_info`: 设备信息
    ///
    /// # 返回
    /// - `Ok(Vec<String>)`: 需要被踢掉的连接 ID 列表（可能为空，且只含同一归属的连接）
    /// - `Err`: 如果添加失败
    pub async fn add_device(
        &self,
        owner: &DeviceOwner,
        connection_id: String,
        device_info: DeviceInfo,
    ) -> Result<Vec<String>> {
        let mut registry = self.registry.write().await;

        // 同一连接此前登记在别的归属下（重复 CONNECT），先从旧桶摘掉，避免一条连接占两个桶
        if let Some(previous) = registry.owners.get(&connection_id).cloned()
            && previous != *owner
        {
            registry.detach(&previous, &connection_id);
        }

        let registry = &mut *registry;
        let user_device_map = registry.devices.entry(owner.clone()).or_default();

        // 收集现有设备的平台类型
        let existing_platforms: HashSet<DevicePlatform> = user_device_map
            .iter()
            .filter(|(conn_id, _)| **conn_id != connection_id)
            .map(|(_, d)| d.device_info.platform.clone())
            .collect();

        info!(
            "[DeviceManager] 添加设备: tenant_id={}, user_id={}, connection_id={}, platform={:?}, 现有设备数={}, 现有平台={:?}",
            owner.tenant_id(),
            owner.user_id(),
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
            registry.owners.remove(conn_id);
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
        let device_count = user_device_map.len();
        registry.owners.insert(connection_id.clone(), owner.clone());
        info!(
            "[DeviceManager] 新设备已添加: tenant_id={}, user_id={}, connection_id={}, 当前设备数={}",
            owner.tenant_id(),
            owner.user_id(),
            connection_id,
            device_count
        );

        Ok(conflicts)
    }

    /// 移除设备
    ///
    /// 只在该连接确实登记在 `owner` 名下时才移除，不会动到其他租户或其他用户的设备。
    pub async fn remove_device(&self, owner: &DeviceOwner, connection_id: &str) -> Result<()> {
        let mut registry = self.registry.write().await;

        if registry.owners.get(connection_id) == Some(owner) {
            registry.owners.remove(connection_id);
            registry.detach(owner, connection_id);
        }

        Ok(())
    }

    /// 按连接 ID 移除设备（连接断开时使用）
    ///
    /// 通过反查索引定位归属，不依赖连接信息是否还在连接管理器里。
    ///
    /// # 返回
    /// 被移除设备的归属；连接未登记时返回 `None`
    pub async fn remove_connection(&self, connection_id: &str) -> Option<DeviceOwner> {
        let mut registry = self.registry.write().await;

        let owner = registry.owners.remove(connection_id)?;
        registry.detach(&owner, connection_id);
        Some(owner)
    }

    /// 获取用户的所有设备（仅限该租户）
    pub async fn get_user_devices(&self, owner: &DeviceOwner) -> Vec<DeviceInfo> {
        let registry = self.registry.read().await;

        registry
            .devices
            .get(owner)
            .map(|devices| devices.values().map(|d| d.device_info.clone()).collect())
            .unwrap_or_default()
    }

    /// 获取用户的所有连接 ID（仅限该租户）
    pub async fn get_user_connections(&self, owner: &DeviceOwner) -> Vec<String> {
        let registry = self.registry.read().await;

        registry
            .devices
            .get(owner)
            .map(|devices| devices.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// 获取设备的连接 ID（仅限该租户）
    pub async fn get_device_connection(
        &self,
        owner: &DeviceOwner,
        device_id: &str,
    ) -> Option<String> {
        let registry = self.registry.read().await;

        registry.devices.get(owner).and_then(|devices| {
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

#[cfg(test)]
mod tests {
    use super::*;

    const TENANT_A: &str = "1200000000000000333";
    const TENANT_B: &str = "1200000000000000349";
    const SHARED_USER: &str = "shared-user";

    fn device(id: &str, platform: DevicePlatform) -> DeviceInfo {
        DeviceInfo::new(id.to_string(), platform)
    }

    fn owner(tenant: &str) -> DeviceOwner {
        DeviceOwner::new(Some(tenant), SHARED_USER)
    }

    fn sorted(mut connections: Vec<String>) -> Vec<String> {
        connections.sort();
        connections
    }

    /// W3-01 §3.3 的复现：B 先登 Android/IOS/PC 三台，A 再以同样的平台登录，
    /// B 的三台必须原样保留，A 也不应看到 B 的设备。
    #[tokio::test]
    async fn same_user_in_other_tenant_is_never_kicked_by_platform_conflict() {
        let manager = DeviceManager::new(DeviceConflictStrategy::PlatformExclusive);
        let platforms = [
            ("d1", DevicePlatform::Android),
            ("d2", DevicePlatform::IOS),
            ("d3", DevicePlatform::PC),
        ];

        for (id, platform) in &platforms {
            let kicked = manager
                .add_device(
                    &owner(TENANT_B),
                    format!("b-{id}"),
                    device(id, platform.clone()),
                )
                .await
                .unwrap();
            assert!(
                kicked.is_empty(),
                "B/{id} must not kick anything: {kicked:?}"
            );
        }
        for (id, platform) in &platforms {
            let kicked = manager
                .add_device(
                    &owner(TENANT_A),
                    format!("a-{id}"),
                    device(id, platform.clone()),
                )
                .await
                .unwrap();
            assert!(
                kicked.is_empty(),
                "A/{id} must not kick the other tenant's same-platform device: {kicked:?}"
            );
        }

        assert_eq!(
            sorted(manager.get_user_connections(&owner(TENANT_B)).await),
            vec!["b-d1", "b-d2", "b-d3"]
        );
        assert_eq!(
            sorted(manager.get_user_connections(&owner(TENANT_A)).await),
            vec!["a-d1", "a-d2", "a-d3"]
        );
        assert_eq!(manager.get_user_devices(&owner(TENANT_A)).await.len(), 3);
        assert_eq!(
            manager.get_device_connection(&owner(TENANT_A), "d1").await,
            Some("a-d1".to_string())
        );
        assert_eq!(
            manager.get_device_connection(&owner(TENANT_B), "d1").await,
            Some("b-d1".to_string())
        );
    }

    /// 同一租户内仍按策略冲突：同平台的新连接顶掉旧连接，只顶掉同租户的那一条。
    #[tokio::test]
    async fn same_tenant_same_platform_still_conflicts() {
        let manager = DeviceManager::new(DeviceConflictStrategy::PlatformExclusive);
        manager
            .add_device(
                &owner(TENANT_B),
                "b-old".into(),
                device("b", DevicePlatform::Android),
            )
            .await
            .unwrap();
        manager
            .add_device(
                &owner(TENANT_A),
                "a-old".into(),
                device("a", DevicePlatform::Android),
            )
            .await
            .unwrap();

        let kicked = manager
            .add_device(
                &owner(TENANT_A),
                "a-new".into(),
                device("a2", DevicePlatform::Android),
            )
            .await
            .unwrap();

        assert_eq!(kicked, vec!["a-old".to_string()]);
        assert_eq!(
            manager.get_user_connections(&owner(TENANT_A)).await,
            vec!["a-new".to_string()]
        );
        assert_eq!(
            manager.get_user_connections(&owner(TENANT_B)).await,
            vec!["b-old".to_string()]
        );
    }

    /// 策略带来的名额上限按（租户，用户）各自计算：
    /// 单设备策略下，A 占满自己的名额不影响 B，B 也不会因为 A 的设备被逐出。
    #[tokio::test]
    async fn strategy_caps_are_counted_per_tenant() {
        for strategy in [
            DeviceConflictStrategy::FullyExclusive,
            DeviceConflictStrategy::MobileExclusive,
        ] {
            let manager = DeviceManager::new(strategy.clone());
            let kicked = manager
                .add_device(
                    &owner(TENANT_A),
                    "a-1".into(),
                    device("a1", DevicePlatform::IOS),
                )
                .await
                .unwrap();
            assert!(kicked.is_empty(), "{strategy:?}");

            let kicked = manager
                .add_device(
                    &owner(TENANT_B),
                    "b-1".into(),
                    device("b1", DevicePlatform::Android),
                )
                .await
                .unwrap();
            assert!(
                kicked.is_empty(),
                "{strategy:?}: B's first device must not count against A's cap: {kicked:?}"
            );

            // 各自超出名额时只逐出本租户的旧设备
            let kicked = manager
                .add_device(
                    &owner(TENANT_A),
                    "a-2".into(),
                    device("a2", DevicePlatform::Android),
                )
                .await
                .unwrap();
            assert_eq!(kicked, vec!["a-1".to_string()], "{strategy:?}");
            let kicked = manager
                .add_device(
                    &owner(TENANT_B),
                    "b-2".into(),
                    device("b2", DevicePlatform::IOS),
                )
                .await
                .unwrap();
            assert_eq!(kicked, vec!["b-1".to_string()], "{strategy:?}");

            assert_eq!(
                manager.get_user_connections(&owner(TENANT_A)).await,
                vec!["a-2".to_string()],
                "{strategy:?}"
            );
            assert_eq!(
                manager.get_user_connections(&owner(TENANT_B)).await,
                vec!["b-2".to_string()],
                "{strategy:?}"
            );
        }
    }

    /// 缺失或空白的租户落到默认租户，只与默认租户共桶，不会与其他租户互踢。
    #[tokio::test]
    async fn missing_or_blank_tenant_uses_only_the_default_tenant_bucket() {
        assert_eq!(DeviceOwner::new(None, "u").tenant_id(), DEFAULT_TENANT_ID);
        assert_eq!(
            DeviceOwner::new(Some(""), "u").tenant_id(),
            DEFAULT_TENANT_ID
        );
        assert_eq!(
            DeviceOwner::new(Some("  "), "u").tenant_id(),
            DEFAULT_TENANT_ID
        );
        assert_eq!(DeviceOwner::new(None, "u"), DeviceOwner::new(Some(""), "u"));
        assert_ne!(
            DeviceOwner::new(None, "u"),
            DeviceOwner::new(Some(TENANT_A), "u")
        );

        let manager = DeviceManager::new(DeviceConflictStrategy::PlatformExclusive);
        manager
            .add_device(
                &owner(TENANT_A),
                "a".into(),
                device("a", DevicePlatform::Web),
            )
            .await
            .unwrap();
        let kicked = manager
            .add_device(
                &DeviceOwner::new(None, SHARED_USER),
                "none".into(),
                device("n", DevicePlatform::Web),
            )
            .await
            .unwrap();
        assert!(kicked.is_empty(), "{kicked:?}");

        let kicked = manager
            .add_device(
                &DeviceOwner::new(Some(" "), SHARED_USER),
                "blank".into(),
                device("b", DevicePlatform::Web),
            )
            .await
            .unwrap();
        assert_eq!(kicked, vec!["none".to_string()]);
        assert_eq!(
            manager.get_user_connections(&owner(TENANT_A)).await,
            vec!["a".to_string()]
        );
    }

    /// 移除只作用于登记所在的归属；按连接移除走反查索引。
    #[tokio::test]
    async fn removal_is_scoped_to_the_owner_that_registered_the_connection() {
        let manager = DeviceManager::new(DeviceConflictStrategy::PlatformExclusive);
        manager
            .add_device(
                &owner(TENANT_A),
                "a".into(),
                device("a", DevicePlatform::Web),
            )
            .await
            .unwrap();
        manager
            .add_device(
                &owner(TENANT_B),
                "b".into(),
                device("b", DevicePlatform::Web),
            )
            .await
            .unwrap();

        // 用错误的租户移除不会生效
        manager.remove_device(&owner(TENANT_B), "a").await.unwrap();
        assert_eq!(
            manager.get_user_connections(&owner(TENANT_A)).await,
            vec!["a".to_string()]
        );

        manager.remove_device(&owner(TENANT_A), "a").await.unwrap();
        assert!(
            manager
                .get_user_connections(&owner(TENANT_A))
                .await
                .is_empty()
        );

        assert_eq!(manager.remove_connection("b").await, Some(owner(TENANT_B)));
        assert!(
            manager
                .get_user_connections(&owner(TENANT_B))
                .await
                .is_empty()
        );
        assert_eq!(manager.remove_connection("b").await, None);
    }

    /// 同一连接重复 CONNECT 换了归属时，旧桶里的登记被摘掉。
    #[tokio::test]
    async fn re_registering_a_connection_moves_it_to_the_new_owner() {
        let manager = DeviceManager::new(DeviceConflictStrategy::PlatformExclusive);
        manager
            .add_device(
                &owner(TENANT_A),
                "c".into(),
                device("d", DevicePlatform::Web),
            )
            .await
            .unwrap();
        let kicked = manager
            .add_device(
                &owner(TENANT_B),
                "c".into(),
                device("d", DevicePlatform::Web),
            )
            .await
            .unwrap();

        assert!(kicked.is_empty());
        assert!(
            manager
                .get_user_connections(&owner(TENANT_A))
                .await
                .is_empty()
        );
        assert_eq!(
            manager.get_user_connections(&owner(TENANT_B)).await,
            vec!["c".to_string()]
        );
        assert_eq!(manager.remove_connection("c").await, Some(owner(TENANT_B)));
    }
}
