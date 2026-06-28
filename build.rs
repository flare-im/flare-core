use std::io::Result;

fn main() -> Result<()> {
    println!("cargo:rerun-if-env-changed=PROTOC");
    println!("cargo:rerun-if-env-changed=FLARE_CORE_REGENERATE_PROTO");

    // 创建输出目录
    std::fs::create_dir_all("src/common/protocol")?;

    // 检查是否已存在生成的文件
    let flare_core_path = "src/common/protocol/flare.core.rs";
    let commands_path = "src/common/protocol/flare.core.commands.rs";
    let files_exist = std::path::Path::new(flare_core_path).exists()
        && std::path::Path::new(commands_path).exists();
    let regenerate_proto = std::env::var_os("FLARE_CORE_REGENERATE_PROTO").is_some();

    if files_exist && !regenerate_proto {
        println!("cargo:rerun-if-changed=proto/frame.proto");
        println!("cargo:rerun-if-changed=proto/commands.proto");
        return Ok(());
    }

    // 检查是否有 PROTOC 环境变量
    let has_protoc = std::env::var("PROTOC").is_ok();
    let has_protoc_on_path = std::process::Command::new("protoc")
        .arg("--version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false);

    // 如果文件已存在且没有可用 protoc，跳过编译（使用已生成的文件）
    if files_exist && !regenerate_proto && !has_protoc && !has_protoc_on_path {
        println!("cargo:warning=protoc not found, using pre-generated protobuf files");
        println!("cargo:warning=If you modify proto files, install protoc: brew install protobuf");
        println!("cargo:rerun-if-changed=proto/frame.proto");
        println!("cargo:rerun-if-changed=proto/commands.proto");
        return Ok(());
    }

    // 如果有 protoc 或文件不存在，尝试编译
    if regenerate_proto || has_protoc || has_protoc_on_path || !files_exist {
        // 配置prost-build输出路径
        let mut config = prost_build::Config::new();
        config.out_dir("src/common/protocol");

        // 为所有生成的结构添加serde支持（支持JSON序列化）
        config.type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");

        // 定义统一的JSON key格式，使用snake_case命名方式
        config.type_attribute(".", "#[serde(rename_all = \"snake_case\")]");

        // 编译proto文件（自动生成protobuf序列化支持）
        if let Err(e) = config.compile_protos(&["frame.proto", "commands.proto"], &["proto/"]) {
            if files_exist {
                // 如果编译失败但文件已存在，使用已生成的文件
                println!(
                    "cargo:warning=protoc compilation failed: {}, using pre-generated files",
                    e
                );
                println!(
                    "cargo:warning=If you modify proto files, ensure protoc is installed: brew install protobuf"
                );
            } else {
                // 如果文件不存在且编译失败，返回错误
                return Err(std::io::Error::other(format!(
                    "Failed to compile protobuf files: {}. Please install protoc: brew install protobuf",
                    e
                )));
            }
        }
    }

    // 修复生成的 flare.core.rs 文件中的命令引用
    if std::path::Path::new(flare_core_path).exists() {
        let content = std::fs::read_to_string(flare_core_path)?;
        // 将各种形式的 commands::Command 引用替换为 super::commands::Command
        let fixed_content = content
            .replace(
                "super::super::commands::Command",
                "super::commands::Command",
            )
            .replace("commands::Command", "super::commands::Command");
        if fixed_content != content {
            std::fs::write(flare_core_path, fixed_content)?;
        }
    }

    println!("cargo:rerun-if-changed=proto/frame.proto");
    println!("cargo:rerun-if-changed=proto/commands.proto");

    Ok(())
}
