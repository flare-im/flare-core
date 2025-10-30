//! 通用连接管理器
//!
//! 该模块提供了一个通用连接管理器，用于管理通用连接的生命周期，
//! 包括连接的状态监控等功能。

use crate::common::connections::enhanced::EnhancedConnection;
use crate::common::connections::config::ConnectionConfig;
use crate::common::connections::enums::ConnectionState;
use crate::common::error::FlareError;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;

/// 通用连接管理器
///
/// 用于管理通用连接的状态监控等功能
pub struct ConnectionManager {
    /// 连接映射表（连接ID -> 连接实例）
    connections: Arc<Mutex<HashMap<String, Arc<EnhancedConnection>>>>,
}

impl ConnectionManager {
    /// 创建新的连接管理器实例
    ///
    /// # 返回值
    /// 新创建的连接管理器实例
    pub fn new() -> Self {
        Self {
            connections: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// 添加连接
    ///
    /// # 参数
    /// * `connection` - 连接实例
    ///
    /// # 返回值
    /// 操作结果，成功返回Ok(())，失败返回相应的错误
    pub fn add_connection(&self, connection: Arc<EnhancedConnection>) -> Result<(), FlareError> {
        if let Ok(mut connections) = self.connections.lock() {
            connections.insert(connection.id(), connection);
            Ok(())
        } else {
            Err(FlareError::general_error("无法获取连接映射表锁".to_string()))
        }
    }

    /// 根据ID获取连接
    ///
    /// # 参数
    /// * `id` - 连接ID
    ///
    /// # 返回值
    /// 连接实例的可选引用
    pub fn get_connection(&self, id: &str) -> Option<Arc<EnhancedConnection>> {
        if let Ok(connections) = self.connections.lock() {
            connections.get(id).cloned()
        } else {
            None
        }
    }

    /// 删除连接
    ///
    /// # 参数
    /// * `id` - 连接ID
    ///
    /// # 返回值
    /// 操作结果，成功返回Ok(())，失败返回相应的错误
    pub fn remove_connection(&self, id: &str) -> Result<(), FlareError> {
        if let Ok(mut connections) = self.connections.lock() {
            connections.remove(id);
            Ok(())
        } else {
            Err(FlareError::general_error("无法获取连接映射表锁".to_string()))
        }
    }

    /// 获取所有连接
    ///
    /// # 返回值
    /// 所有连接实例的向量
    pub fn get_all_connections(&self) -> Vec<Arc<EnhancedConnection>> {
        if let Ok(connections) = self.connections.lock() {
            connections.values().cloned().collect()
        } else {
            Vec::new()
        }
    }

    /// 获取指定状态的连接
    ///
    /// # 参数
    /// * `state` - 连接状态
    ///
    /// # 返回值
    /// 指定状态的连接实例的向量
    pub fn get_connections_by_state(&self, state: ConnectionState) -> Vec<Arc<EnhancedConnection>> {
        if let Ok(connections) = self.connections.lock() {
            connections
                .values()
                .filter(|conn| conn.state() == state)
                .cloned()
                .collect()
        } else {
            Vec::new()
        }
    }

    /// 获取连接数量
    ///
    /// # 返回值
    /// 当前管理的连接数量
    pub fn get_connection_count(&self) -> usize {
        if let Ok(connections) = self.connections.lock() {
            connections.len()
        } else {
            0
        }
    }

    /// 清空所有连接
    ///
    /// # 返回值
    /// 操作结果，成功返回Ok(())，失败返回相应的错误
    pub fn clear_all_connections(&self) -> Result<(), FlareError> {
        if let Ok(mut connections) = self.connections.lock() {
            connections.clear();
            Ok(())
        } else {
            Err(FlareError::general_error("无法获取连接映射表锁".to_string()))
        }
    }
}

impl Default for ConnectionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// 全局连接管理器实例
///
/// 提供全局访问的连接管理器实例
static mut GLOBAL_CONNECTION_MANAGER: Option<Arc<ConnectionManager>> = None;
static GLOBAL_CONNECTION_MANAGER_INIT: std::sync::Once = std::sync::Once::new();

/// 获取全局连接管理器实例
///
/// # 返回值
/// 全局连接管理器实例
pub fn get_global_connection_manager() -> Arc<ConnectionManager> {
    unsafe {
        GLOBAL_CONNECTION_MANAGER_INIT.call_once(|| {
            GLOBAL_CONNECTION_MANAGER = Some(Arc::new(ConnectionManager::new()));
        });
        GLOBAL_CONNECTION_MANAGER.as_ref().unwrap().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::connections::enums::Transport;
    use std::sync::Arc;

    #[test]
    fn test_connection_manager_add_connection() {
        let manager = ConnectionManager::new();
        let config = ConnectionConfig::default();
        
        let connection = Arc::new(EnhancedConnection::new(config));
        let result = manager.add_connection(connection);
        assert!(result.is_ok());
    }

    #[test]
    fn test_connection_manager_get_connection() {
        let manager = ConnectionManager::new();
        let config = ConnectionConfig::default();
        
        let connection = Arc::new(EnhancedConnection::new(config));
        let id = connection.id();
        
        let result = manager.add_connection(connection);
        assert!(result.is_ok());
        
        let retrieved = manager.get_connection(&id);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().id(), id);
    }

    #[test]
    fn test_connection_manager_remove_connection() {
        let manager = ConnectionManager::new();
        let config = ConnectionConfig::default();
        
        let connection = Arc::new(EnhancedConnection::new(config));
        let id = connection.id();
        
        let result = manager.add_connection(connection);
        assert!(result.is_ok());
        
        assert!(manager.get_connection(&id).is_some());
        
        let result = manager.remove_connection(&id);
        assert!(result.is_ok());
        
        assert!(manager.get_connection(&id).is_none());
    }

    #[test]
    fn test_connection_manager_get_all_connections() {
        let manager = ConnectionManager::new();
        
        let config1 = ConnectionConfig::default();
        let config2 = ConnectionConfig {
            transport: Transport::WebSocket,
            ..Default::default()
        };
        
        let conn1 = Arc::new(EnhancedConnection::new(config1));
        let conn2 = Arc::new(EnhancedConnection::new(config2));
        
        let _ = manager.add_connection(conn1);
        let _ = manager.add_connection(conn2);
        
        let connections = manager.get_all_connections();
        assert_eq!(connections.len(), 2);
    }

    #[test]
    fn test_connection_manager_get_connection_count() {
        let manager = ConnectionManager::new();
        
        assert_eq!(manager.get_connection_count(), 0);
        
        let config = ConnectionConfig::default();
        let connection = Arc::new(EnhancedConnection::new(config));
        let _ = manager.add_connection(connection);
        
        assert_eq!(manager.get_connection_count(), 1);
    }

    #[test]
    fn test_connection_manager_clear_all_connections() {
        let manager = ConnectionManager::new();
        
        let config = ConnectionConfig::default();
        let connection = Arc::new(EnhancedConnection::new(config));
        let _ = manager.add_connection(connection);
        
        assert_eq!(manager.get_connection_count(), 1);
        
        let result = manager.clear_all_connections();
        assert!(result.is_ok());
        
        assert_eq!(manager.get_connection_count(), 0);
    }
}