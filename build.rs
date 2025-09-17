use std::io::Result;

fn main() -> Result<()> {
    // 创建输出目录
    std::fs::create_dir_all("src/common/protocol")?;
    
    // 配置prost-build输出路径
    let mut config = prost_build::Config::new();
    config.out_dir("src/common/protocol");
    
    // 为所有生成的结构添加serde支持
    config.type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");
    
    // 定义统一的JSON key格式，使用snake_case命名方式
    config.type_attribute(".", "#[serde(rename_all = \"snake_case\")]");
    
    // 移除默认的skip_serializing_if配置，这样Option类型的字段会序列化为null
    // 字符串和其他类型字段会保留默认值
    
    // 配置外部路径映射，解决模块引用问题
    config.extern_path(".flare.core.commands", "crate::common::protocol::flare_proto::commands");
    
    // 编译proto文件
    config.compile_protos(&["proto/frame.proto", "proto/commands.proto"], &["proto/"])?;
    Ok(())
}