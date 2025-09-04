//! 系统级优化模块
//!
//! 提供CPU亲和性、内存对齐、NUMA优化等系统级性能优化

pub mod cpu_affinity;
pub mod memory_optimization;
pub mod numa_awareness;

pub use cpu_affinity::CpuAffinityManager;
pub use memory_optimization::{AlignedBuffer, MemoryOptimizer};
pub use numa_awareness::NumaOptimizer;