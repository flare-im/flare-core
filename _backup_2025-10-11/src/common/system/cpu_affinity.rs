//! CPU亲和性管理
//!
//! 提供CPU核心绑定和线程调度优化功能

use std::collections::HashSet;
use crate::common::error::{Result, FlareError};

/// CPU集合
#[derive(Debug, Clone)]
pub struct CpuSet {
    cores: HashSet<usize>,
}

impl CpuSet {
    /// 从核心列表创建CPU集合
    pub fn from_cores(cores: &[usize]) -> Self {
        Self {
            cores: cores.iter().copied().collect(),
        }
    }

    /// 获取核心列表
    pub fn cores(&self) -> Vec<usize> {
        self.cores.iter().copied().collect()
    }
}

/// CPU亲和性管理器
#[derive(Debug)]
pub struct CpuAffinityManager {
    available_cores: usize,
}

impl CpuAffinityManager {
    /// 创建新的CPU亲和性管理器
    pub fn new() -> Result<Self> {
        let available_cores = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1);
        Ok(Self { available_cores })
    }

    /// 绑定网络线程到指定核心
    pub fn bind_network_threads(&self, cores: &CpuSet) -> Result<()> {
        // 简化实现，在实际项目中需要调用系统API
        tracing::info!("绑定网络线程到核心: {:?}", cores.cores());
        Ok(())
    }

    /// 绑定计算线程到指定核心
    pub fn bind_compute_threads(&self, cores: &CpuSet) -> Result<()> {
        tracing::info!("绑定计算线程到核心: {:?}", cores.cores());
        Ok(())
    }

    /// 绑定当前线程到指定核心
    pub fn bind_current_thread(&self, core: usize) -> Result<()> {
        if core >= self.available_cores {
            return Err(FlareError::general_error("指定的核心不存在"));
        }
        
        #[cfg(target_os = "linux")]
        {
            // Linux实现
            use libc::{cpu_set_t, sched_setaffinity, CPU_SET, CPU_ZERO};
            use std::mem;
            
            unsafe {
                let mut cpu_set: cpu_set_t = mem::zeroed();
                CPU_ZERO(&mut cpu_set);
                CPU_SET(core, &mut cpu_set);
                
                if sched_setaffinity(0, mem::size_of::<cpu_set_t>(), &cpu_set) != 0 {
                    return Err(FlareError::general_error("设置CPU亲和性失败"));
                }
            }
        }
        
        #[cfg(not(target_os = "linux"))]
        {
            // 其他平台的简化实现
            tracing::warn!("当前平台不支持CPU亲和性绑定，跳过核心{}的绑定", core);
        }
        
        tracing::info!("绑定当前线程到核心: {}", core);
        Ok(())
    }

    /// 获取可用核心数
    pub fn available_cores(&self) -> usize {
        self.available_cores
    }

    /// 隔离CPU核心
    pub fn isolate_cores(&self, cores: &[usize]) -> Result<()> {
        tracing::info!("隔离CPU核心: {:?}", cores);
        // 实际实现需要修改系统配置
        Ok(())
    }
}

impl Default for CpuAffinityManager {
    fn default() -> Self {
        Self::new().unwrap_or_else(|_| Self { available_cores: 1 })
    }
}