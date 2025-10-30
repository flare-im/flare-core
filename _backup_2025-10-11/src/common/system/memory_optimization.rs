//! 内存优化模块
//!
//! 提供内存对齐、预分配、零拷贝等内存优化技术

use std::alloc::{alloc, dealloc, Layout};
/// 缓存行对齐的缓冲区
#[repr(C, align(64))] // 64字节缓存行对齐
pub struct AlignedBuffer {
    data: Vec<u8>,
    capacity: usize,
}

impl AlignedBuffer {
    /// 创建对齐的缓冲区
    pub fn new(capacity: usize) -> Self {
        // 确保内存对齐
        let data = unsafe {
            let layout = Layout::from_size_align(capacity, 64).unwrap();
            let ptr = alloc(layout);
            if ptr.is_null() {
                panic!("内存分配失败");
            }
            
            Vec::from_raw_parts(ptr, 0, capacity)
        };
        
        Self { data, capacity }
    }
    
    /// 获取可变切片
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(self.data.as_mut_ptr(), self.capacity)
        }
    }
    
    /// 获取只读切片
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe {
            let layout = Layout::from_size_align(self.capacity, 64).unwrap();
            dealloc(self.data.as_mut_ptr(), layout);
        }
    }
}

/// 内存优化器
pub struct MemoryOptimizer {
    buffer_pools: Vec<AlignedBuffer>,
}

impl MemoryOptimizer {
    pub fn new() -> Self {
        Self {
            buffer_pools: Vec::new(),
        }
    }
    
    /// 预分配缓冲区池
    pub fn preallocate_buffers(&mut self, count: usize, size: usize) {
        for _ in 0..count {
            self.buffer_pools.push(AlignedBuffer::new(size));
        }
    }
    
    /// 获取优化的缓冲区
    pub fn get_buffer(&mut self, _min_size: usize) -> Option<AlignedBuffer> {
        self.buffer_pools.pop()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aligned_buffer() {
        let mut buffer = AlignedBuffer::new(1024);
        let slice = buffer.as_mut_slice();
        slice[0] = 42;
        assert_eq!(slice[0], 42);
    }
    
    #[test]  
    fn test_memory_optimizer() {
        let mut optimizer = MemoryOptimizer::new();
        optimizer.preallocate_buffers(10, 1024);
        
        let buffer = optimizer.get_buffer(512);
        assert!(buffer.is_some());
    }
}