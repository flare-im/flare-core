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
    
    // 移除 extern_path 配置，让 Protobuf 生成的代码自包含
    
    // 编译proto文件
    config.compile_protos(&["proto/frame.proto", "proto/commands.proto"], &["proto/"])?;
    
    // 修复生成的 flare.core.rs 文件中的引用
    let flare_core_path = "src/common/protocol/flare.core.rs";
    if std::path::Path::new(flare_core_path).exists() {
        let content = std::fs::read_to_string(flare_core_path)?;
        let fixed_content = content.replace(
            "pub command: ::core::option::Option<commands::Command>,",
            "pub command: ::core::option::Option<super::flare_core_commands::Command>,"
        );
        std::fs::write(flare_core_path, fixed_content)?;
    }
    
    println!("cargo:rerun-if-changed=proto/frame.proto");
    println!("cargo:rerun-if-changed=proto/commands.proto");
    
    Ok(())
}