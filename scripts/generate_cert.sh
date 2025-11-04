#!/bin/bash

# 生成 TLS 证书脚本
# 使用 rcgen 生成自签名证书，用于 QUIC 和 WebSocket TLS 连接

set -e

# 获取脚本所在目录和项目根目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# 证书输出目录
CERT_DIR="certs"
CERT_FILE="$CERT_DIR/server.crt"
KEY_FILE="$CERT_DIR/server.key"
TEMP_DIR=$(mktemp -d)
TEMP_RS_FILE="$TEMP_DIR/generate_cert_temp.rs"
TEMP_CARGO_TOML="$TEMP_DIR/Cargo.toml"

# 创建证书目录
mkdir -p "$CERT_DIR"

# 检查是否已安装 cargo
if ! command -v cargo &> /dev/null; then
    echo "错误: 未找到 cargo，请先安装 Rust 工具链"
    exit 1
fi

# 清理函数
cleanup() {
    # 删除临时目录及其所有内容
    rm -rf "$TEMP_DIR"
}

# 注册清理函数，确保脚本退出时清理
trap cleanup EXIT

# 创建临时 Rust 程序来生成证书
cat > "$TEMP_RS_FILE" << 'EOF'
use std::fs;
use std::path::Path;

fn main() {
    // 证书保存到项目根目录的 certs 目录
    let project_root = std::env::var("PROJECT_ROOT").expect("PROJECT_ROOT not set");
    let cert_dir = Path::new(&project_root).join("certs");
    fs::create_dir_all(&cert_dir).expect("Failed to create certs directory");
    
    let cert_file = cert_dir.join("server.crt");
    let key_file = cert_dir.join("server.key");
    
    // 生成证书
    let subject_alt_names = vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
        "::1".to_string(),
    ];
    
    let certified_key = rcgen::generate_simple_self_signed(subject_alt_names)
        .expect("Failed to generate certificate");
    
    let cert_der = certified_key.cert.der().to_vec();
    let key_der = certified_key.signing_key.serialize_der();
    
    // 保存证书
    fs::write(&cert_file, &cert_der)
        .expect("Failed to write certificate file");
    
    // 保存私钥
    fs::write(&key_file, &key_der)
        .expect("Failed to write private key file");
    
    println!("证书已生成:");
    println!("  证书文件: {}", cert_file.display());
    println!("  私钥文件: {}", key_file.display());
    println!("\n证书信息:");
    println!("  支持的域名: localhost, 127.0.0.1, ::1");
    println!("  格式: DER");
    println!("  用途: QUIC 和 WebSocket TLS");
}
EOF

# 创建临时 Cargo.toml
cat > "$TEMP_CARGO_TOML" << EOF
[package]
name = "generate_cert_temp"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "generate_cert_temp"
path = "generate_cert_temp.rs"

[dependencies]
rcgen = "0.14"
EOF

# 运行生成脚本（在临时目录中运行，但证书保存到项目根目录的 certs 目录）
echo "正在生成证书..."
cd "$TEMP_DIR"
PROJECT_ROOT="$PROJECT_ROOT" cargo run --quiet --bin generate_cert_temp
cd "$PROJECT_ROOT"

# 清理函数会在 EXIT 时自动执行

echo ""
echo "✅ 证书生成完成！"
echo ""
echo "证书文件位置:"
echo "  - 证书: $CERT_FILE"
echo "  - 私钥: $KEY_FILE"
echo ""
echo "这些证书可用于:"
echo "  - QUIC 服务器/客户端"
echo "  - WebSocket TLS (wss://) 服务器/客户端"

