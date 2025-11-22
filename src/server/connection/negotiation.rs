//! 连接协商模块
//! 
//! 处理连接建立时的序列化格式、压缩算法和设备信息协商

use crate::common::device::DeviceInfo;
use crate::common::protocol::{Frame, SystemCommand};
use crate::common::protocol::flare::core::commands::system_command::SerializationFormat;
use crate::common::compression::CompressionAlgorithm;
use crate::common::error::Result;
use std::collections::HashMap;

/// 连接协商结果
#[derive(Debug, Clone)]
pub struct NegotiationResult {
    /// 序列化格式（客户端请求的格式）
    pub serialization_format: SerializationFormat,
    /// 压缩算法（客户端请求的压缩方式）
    pub compression: CompressionAlgorithm,
    /// 是否强制指定格式（客户端强制模式，服务端必须使用客户端指定的格式）
    pub is_forced: bool,
    /// 设备信息（如果客户端提供）
    pub device_info: Option<DeviceInfo>,
    /// 用户 ID（如果客户端在 CONNECT 中提供）
    pub user_id: Option<String>,
}

impl NegotiationResult {
    /// 创建默认协商结果（JSON + 不压缩）
    pub fn default() -> Self {
        Self {
            serialization_format: SerializationFormat::Json,
            compression: CompressionAlgorithm::None,
            is_forced: false,
            device_info: None,
            user_id: None,
        }
    }
}

/// 解析 CONNECT 消息，提取客户端协商信息
/// 
/// # 参数
/// - `frame`: CONNECT 消息的 Frame
/// 
/// # 返回
/// 协商结果，包含序列化格式、压缩算法、设备信息等
pub fn parse_connect_message(frame: &Frame) -> Result<NegotiationResult> {
    let mut result = NegotiationResult::default();
    
    // 检查是否是 CONNECT 命令
    if let Some(cmd) = &frame.command {
        if let Some(crate::common::protocol::flare::core::commands::command::Type::System(sys_cmd)) = &cmd.r#type {
            use crate::common::protocol::flare::core::commands::system_command::Type as SystemType;
            if sys_cmd.r#type == SystemType::Connect as i32 {
                // 解析序列化格式（使用 TryFrom 替代已弃用的 from_i32）
                use std::convert::TryFrom;
                result.serialization_format = SerializationFormat::try_from(sys_cmd.format)
                    .unwrap_or(SerializationFormat::Json);
                
                // 解析压缩算法（从 metadata 中）
                if let Some(compression_bytes) = sys_cmd.metadata.get("compression") {
                    if let Ok(compression_str) = String::from_utf8(compression_bytes.clone()) {
                        result.compression = CompressionAlgorithm::from_str(&compression_str)
                            .unwrap_or(CompressionAlgorithm::None);
                    }
                }
                
                // 解析设备信息（从 metadata 中）
                if let Some(device_id_bytes) = sys_cmd.metadata.get("device_id") {
                    if let Ok(device_id) = String::from_utf8(device_id_bytes.clone()) {
                        // 解析平台类型
                        let platform = if let Some(platform_bytes) = sys_cmd.metadata.get("platform") {
                            if let Ok(platform_str) = String::from_utf8(platform_bytes.clone()) {
                                crate::common::device::DevicePlatform::from_str(&platform_str)
                            } else {
                                crate::common::device::DevicePlatform::Other("unknown".to_string())
                            }
                        } else {
                            crate::common::device::DevicePlatform::Other("unknown".to_string())
                        };
                        
                        let mut device_info = DeviceInfo::new(device_id, platform);
                        
                        // 解析可选的设备信息
                        if let Some(model_bytes) = sys_cmd.metadata.get("model") {
                            if let Ok(model) = String::from_utf8(model_bytes.clone()) {
                                device_info = device_info.with_model(model);
                            }
                        }
                        if let Some(app_version_bytes) = sys_cmd.metadata.get("app_version") {
                            if let Ok(app_version) = String::from_utf8(app_version_bytes.clone()) {
                                device_info = device_info.with_app_version(app_version);
                            }
                        }
                        if let Some(system_version_bytes) = sys_cmd.metadata.get("system_version") {
                            if let Ok(system_version) = String::from_utf8(system_version_bytes.clone()) {
                                device_info = device_info.with_system_version(system_version);
                            }
                        }
                        
                        // 解析其他元数据
                        for (key, value) in &sys_cmd.metadata {
                            if !matches!(key.as_str(), "compression" | "device_id" | "platform" | "model" | "app_version" | "system_version" | "user_id") {
                                if let Ok(value_str) = String::from_utf8(value.clone()) {
                                    device_info = device_info.with_metadata(key.clone(), value_str);
                                }
                            }
                        }
                        
                        result.device_info = Some(device_info);
                    }
                }
                
                // 解析用户 ID（如果客户端在 CONNECT 中提供，用于预认证）
                if let Some(user_id_bytes) = sys_cmd.metadata.get("user_id") {
                    if let Ok(user_id) = String::from_utf8(user_id_bytes.clone()) {
                        result.user_id = Some(user_id);
                    }
                }
                
                // 解析是否强制指定格式（客户端强制模式）
                if let Some(force_bytes) = sys_cmd.metadata.get("force_format") {
                    if let Ok(force_str) = String::from_utf8(force_bytes.clone()) {
                        result.is_forced = force_str == "true";
                    }
                }
            }
        }
    }
    
    Ok(result)
}

/// 创建 CONNECT_ACK 响应
/// 
/// # 参数
/// - `format`: 确认使用的序列化格式
/// - `compression`: 确认使用的压缩算法
/// - `additional_metadata`: 额外的元数据（如设备冲突信息等）
/// 
/// # 返回
/// CONNECT_ACK 命令
pub fn create_connect_ack(
    format: SerializationFormat,
    compression: CompressionAlgorithm,
    additional_metadata: Option<HashMap<String, Vec<u8>>>,
) -> SystemCommand {
    let mut metadata = HashMap::new();
    
    // 添加压缩算法信息
    metadata.insert("compression".to_string(), compression.as_str().as_bytes().to_vec());
    
    // 添加额外的元数据
    if let Some(extra) = additional_metadata {
        for (key, value) in extra {
            metadata.insert(key, value);
        }
    }
    
    crate::common::protocol::connect_ack(format, metadata)
}

