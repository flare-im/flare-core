use std::io::Result;

fn main() -> Result<()> {
    // 创建输出目录
    std::fs::create_dir_all("src/common/protocol")?;
    
    // 配置prost-build输出路径
    let mut config = prost_build::Config::new();
    config.out_dir("src/common/protocol");
    
    // 为所有生成的结构添加serde支持（支持JSON序列化）
    config.type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");
    
    // 定义统一的JSON key格式，使用snake_case命名方式
    config.type_attribute(".", "#[serde(rename_all = \"snake_case\")]");
    
    // 编译proto文件（自动生成protobuf序列化支持）
    config.compile_protos(&["frame.proto", "commands.proto"], &["proto/"])?;
    
    // 修复生成的 flare.core.rs 文件中的命令引用
    let flare_core_path = "src/common/protocol/flare.core.rs";
    if std::path::Path::new(flare_core_path).exists() {
        let content = std::fs::read_to_string(flare_core_path)?;
        // 将 commands::Command 替换为 super::commands::Command
        let fixed_content = content.replace(
            "commands::Command",
            "super::commands::Command"
        );
        std::fs::write(flare_core_path, fixed_content)?;
    }
    
    println!("cargo:rerun-if-changed=proto/frame.proto");
    println!("cargo:rerun-if-changed=proto/commands.proto");
    
    Ok(())
}