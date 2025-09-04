use std::io::Result;

fn main() -> Result<()> {
    // 创建输出目录
    std::fs::create_dir_all("src/common/protobuf")?;
    
    // 配置prost-build输出路径
    let mut config = prost_build::Config::new();
    config.out_dir("src/common/protobuf");
    
    // 编译proto文件
    config.compile_protos(&["proto/frame.proto"], &["proto/"])?;
    Ok(())
}