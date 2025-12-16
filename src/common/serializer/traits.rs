//! 序列化器 Trait 定义
//!
//! 定义标准序列化接口，方便用户实现自定义序列化格式

use crate::common::error::Result;
use crate::common::protocol::Frame;

/// 序列化器标准接口
///
/// 实现此 trait 以支持自定义序列化格式
///
/// # 示例
///
/// ```rust
/// use flare_core::common::serializer::{Serializer, SerializationFormat};
/// use flare_core::common::protocol::Frame;
/// use flare_core::common::error::Result;
///
/// struct MyCustomSerializer {
///     format_name: String,
/// }
///
/// impl Serializer for MyCustomSerializer {
///     fn serialize(&self, frame: &Frame) -> Result<Vec<u8>> {
///         // 实现序列化逻辑
///         Ok(vec![])
///     }
///     
///     fn deserialize(&self, data: &[u8]) -> Result<Frame> {
///         // 实现反序列化逻辑
///         todo!()
///     }
///     
///     fn format(&self) -> SerializationFormat {
///         SerializationFormat::Protobuf
///     }
///     
///     fn name(&self) -> &'static str {
///         "my_custom"
///     }
///     
///     fn can_detect(&self, data: &[u8]) -> bool {
///         // 实现格式检测逻辑
///         false
///     }
/// }
/// ```
pub trait Serializer: Send + Sync {
    /// 序列化 Frame 为字节数组
    ///
    /// # 参数
    /// - `frame`: 要序列化的 Frame
    ///
    /// # 返回
    /// 序列化后的字节数组
    fn serialize(&self, frame: &Frame) -> Result<Vec<u8>>;

    /// 反序列化字节数组为 Frame
    ///
    /// # 参数
    /// - `data`: 要反序列化的字节数组
    ///
    /// # 返回
    /// 反序列化后的 Frame
    fn deserialize(&self, data: &[u8]) -> Result<Frame>;

    /// 获取序列化格式类型
    fn format(&self) -> crate::common::protocol::SerializationFormat;

    /// 获取序列化器名称（用于注册和查找）
    ///
    /// 名称应该是唯一的，用于在注册表中标识序列化器
    fn name(&self) -> &'static str;

    /// 检测数据是否使用此序列化格式
    ///
    /// # 参数
    /// - `data`: 待检测的数据（通常是数据的前几个字节）
    ///
    /// # 返回
    /// 如果数据可能是由此序列化器序列化的，返回 `true`
    fn can_detect(&self, _data: &[u8]) -> bool {
        false
    }
}
