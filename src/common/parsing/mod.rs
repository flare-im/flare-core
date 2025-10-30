/// 统一的消息解析模块
/// 
/// 该模块提供了一套统一的消息解析架构，支持：
/// 1. Frame 与业务数据的双向转换
/// 2. 可扩展的序列化格式（JSON、Protobuf、MsgPack等）
/// 3. 协议无关的解析接口（适用于 WebSocket、QUIC 等）
/// 4. 用户自定义序列化器的注册机制
///
/// # 设计思路
/// 
/// 采用枚举封装模式而非 trait object，避免泛型方法导致的 dyn 兼容性问题。
/// 
/// ## 核心组件
/// 
/// 1. **PayloadCodec**：序列化器枚举，支持多种格式
/// 2. **FrameCodec**：Frame 编解码器，处理协议层细节
/// 3. **MessageParser**：统一的消息解析器，整合上述两者
/// 
/// ## 使用示例
/// 
/// ```rust,no_run
/// use flare_core::common::parsing::MessageParser;
/// use flare_core::common::serialization::SerializationFormat;
/// 
/// // 创建 JSON 格式的解析器
/// let parser = MessageParser::new(SerializationFormat::Json);
/// 
/// // 序列化并发送
/// // let data = MyStruct { id: 42, name: "test".to_string() };
/// // let frame = parser.build_frame(&data, "msg-123".to_string()).await?;
/// // let bytes = parser.encode_frame(&frame).await?;
/// 
/// // 接收并反序列化
/// // let received_frame = parser.parse_bytes(&bytes).await?;
/// // let received_data: MyStruct = parser.parse_payload(&received_frame).await?;
/// ```

pub mod codec;
pub mod parser;

pub use codec::{PayloadCodec, FrameCodec, DefaultFrameCodec};
pub use parser::{MessageParser, ParserStats};
