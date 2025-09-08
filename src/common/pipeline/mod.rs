//! 异步处理管道
//!
//! 实现消息处理的流水线并行化，显著降低总体延迟

use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

use crate::common::{
    error::{Result, FlareError},
    protocol::Frame,
    serialization::FrameSerializer,
    compression::Compressor,
};

/// 管道消息
#[derive(Debug)]
pub struct PipelineMessage {
    pub frame: Frame,
    pub start_time: Instant,
    pub serialized_data: Option<Vec<u8>>,
    pub compressed_data: Option<Vec<u8>>,
}

impl PipelineMessage {
    pub fn new(frame: Frame) -> Self {
        Self {
            frame,
            start_time: Instant::now(),
            serialized_data: None,
            compressed_data: None,
        }
    }
}

/// 异步处理管道
pub struct AsyncMessagePipeline {
    serialize_tx: mpsc::Sender<PipelineMessage>,
    /// 微批处理大小（默认为2，适合实时通信场景）
    _batch_size: usize,
}

impl AsyncMessagePipeline {
    /// 创建新的异步管道（默认配置）
    pub fn new(
        serializer: Arc<dyn FrameSerializer>,
        compressor: Arc<dyn Compressor>,
    ) -> Self {
        Self::with_config(serializer, compressor, 2) // 默认微批处理大小为2
    }
    
    /// 创建带配置的异步管道
    pub fn with_config(
        serializer: Arc<dyn FrameSerializer>,
        compressor: Arc<dyn Compressor>,
        batch_size: usize,
    ) -> Self {
        let (serialize_tx, mut serialize_rx) = mpsc::channel::<PipelineMessage>(100);
        let (compress_tx, mut compress_rx) = mpsc::channel::<PipelineMessage>(100);
        let (result_tx, mut result_rx) = mpsc::channel::<PipelineMessage>(100);

        // 序列化阶段
        let ser_serializer = Arc::clone(&serializer);
        tokio::spawn(async move {
            while let Some(mut msg) = serialize_rx.recv().await {
                if let Ok(data) = ser_serializer.serialize(&msg.frame).await {
                    msg.serialized_data = Some(data);
                    let _ = compress_tx.send(msg).await;
                }
            }
        });

        // 压缩阶段
        let comp_compressor = Arc::clone(&compressor);
        tokio::spawn(async move {
            while let Some(mut msg) = compress_rx.recv().await {
                if let Some(ref data) = msg.serialized_data {
                    if let Ok(result) = comp_compressor.compress(data).await {
                        msg.compressed_data = Some(result.data);
                        let _ = result_tx.send(msg).await;
                    }
                }
            }
        });

        // 结果处理阶段
        tokio::spawn(async move {
            while let Some(msg) = result_rx.recv().await {
                let total_time = msg.start_time.elapsed();
                println!("管道处理完成，总耗时: {:?}", total_time);
            }
        });

        Self { 
            serialize_tx,
            _batch_size: batch_size,
        }
    }
    
    /// 创建超低延迟配置（适合<3ms实时通信）
    pub fn ultra_low_latency(
        serializer: Arc<dyn FrameSerializer>,
        compressor: Arc<dyn Compressor>,
    ) -> Self {
        Self::with_config(serializer, compressor, 2) // 微批处理大小为2
    }

    /// 异步处理消息
    pub async fn process_async(&self, frame: Frame) -> Result<()> {
        let message = PipelineMessage::new(frame);
        self.serialize_tx.send(message).await
            .map_err(|_| FlareError::general_error("管道已关闭"))?;
        Ok(())
    }
}
