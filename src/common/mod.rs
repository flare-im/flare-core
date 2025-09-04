//! 通用模块
//! 
//! 提供核心功能、错误处理、协议定义等

pub mod connections;
pub mod error;
pub mod protocol;
pub mod serialization;
pub mod compression;
pub mod messaging;
pub mod pipeline;
pub mod system;

// 重新导出主要类型
pub use connections::{
    Connection, ClientConnection, ServerConnection, 
    ConnectionFactory, ConnectionManager,
    ConnectionType, ConnectionRole, ConnectionState, ConnectionConfig, DefConnectionEventHandler ,
    QuicConnection, WebSocketConnection,
};
pub use error::{Result, FlareError};
pub use protocol::{Frame, MessageType, Reliability, ProtocolSelection};
pub use serialization::{
    FrameSerializer, SerializationFormat, SerializationConfig,
    JsonSerializer, SerializerFactory,
};
pub use compression::{
    Compressor, CompressionFormat, CompressionConfig,
    Lz4Compressor, SnappyCompressor, GzipCompressor, CompressorFactory,
}; 