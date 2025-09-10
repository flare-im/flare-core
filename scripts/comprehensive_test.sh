#!/bin/bash

# Flare Core 综合测试脚本
# 测试 WebSocket、QUIC 和协议竞速功能

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查依赖
check_dependencies() {
    log_info "检查依赖..."
    
    if ! command -v cargo &> /dev/null; then
        log_error "Cargo 未安装"
        exit 1
    fi
    
    log_success "依赖检查通过"
}

# 构建项目
build_project() {
    log_info "构建项目..."
    
    cargo build --examples --release
    
    log_success "项目构建完成"
}

# 确保证书存在
ensure_certificates() {
    log_info "检查 TLS 证书..."
    
    if [ ! -f "certs/server.crt" ] || [ ! -f "certs/server.key" ]; then
        log_warn "证书不存在，正在生成..."
        ./scripts/generate_certs.sh
    fi
    
    log_success "证书检查完成"
}

# 测试 WebSocket 单独连接
test_websocket_only() {
    log_info "=== 测试 WebSocket 单独连接 ==="
    
    # 启动 WebSocket 服务端
    log_info "启动 WebSocket 服务端..."
    cargo run --release --example websocket_server &
    WS_SERVER_PID=$!
    
    # 等待服务端启动
    sleep 3
    
    if ! kill -0 $WS_SERVER_PID 2>/dev/null; then
        log_error "WebSocket 服务端启动失败"
        return 1
    fi
    
    log_success "WebSocket 服务端已启动 (PID: $WS_SERVER_PID)"
    
    # 运行 WebSocket 客户端测试
    log_info "运行 WebSocket 客户端测试..."
    timeout 15 cargo run --release --example websocket_client <<< "test message
quit" || true
    
    # 停止服务端
    kill $WS_SERVER_PID 2>/dev/null || true
    wait $WS_SERVER_PID 2>/dev/null || true
    
    log_success "WebSocket 单独连接测试完成"
}

# 测试 QUIC 单独连接
test_quic_only() {
    log_info "=== 测试 QUIC 单独连接 ==="
    
    # 确保证书存在
    ensure_certificates
    
    # 启动 QUIC 服务端（需要创建一个简单的 QUIC 服务端示例）
    log_info "创建临时 QUIC 服务端示例..."
    
    # 创建临时的 QUIC 服务端示例文件
    cat > examples/server/quic_server.rs << 'EOF'
//! QUIC 服务器示例
//!
//! 展示如何创建和运行一个 QUIC 服务器

use std::sync::Arc;
use flare_core::{
    server::{
        Server, ServerConfig, ConnectionBasedManager,
        EchoMessageHandler,
    },
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化 TLS 加密提供程序
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("无法初始化 TLS 加密提供程序");
    
    // 初始化日志
    tracing_subscriber::fmt::init();
    
    // 创建连接管理器
    let connection_manager = Arc::new(ConnectionBasedManager::new());
    
    // 创建服务器配置（仅启用 QUIC）
    let config = ServerConfig {
        websocket_addr: None,
        quic_addr: Some("127.0.0.1:4433".to_string()),
        enable_tls: true,
        tls_cert_path: Some("certs/server.crt".to_string()),
        tls_key_path: Some("certs/server.key".to_string()),
        max_connections: 1000,
        connection_timeout_ms: 30000,
        heartbeat_interval_ms: 10000,
        enable_auto_cleanup: true,
    };
    
    // 创建服务器实例
    let mut server = Server::new(config, connection_manager);
    
    // 注册消息处理器
    let echo_handler = Arc::new(EchoMessageHandler);
    server.register_message_handler(echo_handler).await;
    
    // 启动服务器
    server.start().await?;
    
    println!("QUIC 服务器已启动:");
    println!("  QUIC地址: 127.0.0.1:4433");
    println!("按 Ctrl+C 停止服务器");
    
    // 等待中断信号
    tokio::signal::ctrl_c().await?;
    
    // 停止服务器
    server.stop().await?;
    
    println!("服务器已停止");
    Ok(())
}
EOF

    # 在 Cargo.toml 中添加 QUIC 服务端示例
    if ! grep -q "quic_server" Cargo.toml; then
        sed -i '' '/im_gateway/a\
[[example]]\
name = "quic_server"\
path = "examples/server/quic_server.rs"' Cargo.toml
    fi
    
    # 启动 QUIC 服务端
    log_info "启动 QUIC 服务端..."
    cargo run --release --example quic_server &
    QUIC_SERVER_PID=$!
    
    # 等待服务端启动
    sleep 3
    
    if ! kill -0 $QUIC_SERVER_PID 2>/dev/null; then
        log_error "QUIC 服务端启动失败"
        return 1
    fi
    
    log_success "QUIC 服务端已启动 (PID: $QUIC_SERVER_PID)"
    
    # 运行 QUIC 客户端测试
    log_info "运行 QUIC 客户端测试..."
    timeout 15 cargo run --release --example quic_client <<< "test message
quit" || true
    
    # 停止服务端
    kill $QUIC_SERVER_PID 2>/dev/null || true
    wait $QUIC_SERVER_PID 2>/dev/null || true
    
    # 清理临时文件
    rm -f examples/server/quic_server.rs
    sed -i '' '/quic_server/d' Cargo.toml
    
    log_success "QUIC 单独连接测试完成"
}

# 测试协议竞速
test_protocol_racing() {
    log_info "=== 测试协议竞速 ==="
    
    # 确保证书存在
    ensure_certificates
    
    # 启动 WebSocket 和 QUIC 服务端
    log_info "启动 WebSocket 服务端..."
    cargo run --release --example websocket_server &
    WS_SERVER_PID=$!
    
    sleep 2
    
    log_info "启动 QUIC 服务端..."
    # 创建临时的 QUIC 服务端示例文件
    cat > examples/server/quic_server.rs << 'EOF'
//! QUIC 服务器示例
//!
//! 展示如何创建和运行一个 QUIC 服务器

use std::sync::Arc;
use flare_core::{
    server::{
        Server, ServerConfig, ConnectionBasedManager,
        EchoMessageHandler,
    },
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化 TLS 加密提供程序
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("无法初始化 TLS 加密提供程序");
    
    // 初始化日志
    tracing_subscriber::fmt::init();
    
    // 创建连接管理器
    let connection_manager = Arc::new(ConnectionBasedManager::new());
    
    // 创建服务器配置（仅启用 QUIC）
    let config = ServerConfig {
        websocket_addr: None,
        quic_addr: Some("127.0.0.1:4433".to_string()),
        enable_tls: true,
        tls_cert_path: Some("certs/server.crt".to_string()),
        tls_key_path: Some("certs/server.key".to_string()),
        max_connections: 1000,
        connection_timeout_ms: 30000,
        heartbeat_interval_ms: 10000,
        enable_auto_cleanup: true,
    };
    
    // 创建服务器实例
    let mut server = Server::new(config, connection_manager);
    
    // 注册消息处理器
    let echo_handler = Arc::new(EchoMessageHandler);
    server.register_message_handler(echo_handler).await;
    
    // 启动服务器
    server.start().await?;
    
    println!("QUIC 服务器已启动:");
    println!("  QUIC地址: 127.0.0.1:4433");
    println!("按 Ctrl+C 停止服务器");
    
    // 等待中断信号
    tokio::signal::ctrl_c().await?;
    
    // 停止服务器
    server.stop().await?;
    
    println!("服务器已停止");
    Ok(())
}
EOF

    # 在 Cargo.toml 中添加 QUIC 服务端示例
    if ! grep -q "quic_server" Cargo.toml; then
        sed -i '' '/im_gateway/a\
[[example]]\
name = "quic_server"\
path = "examples/server/quic_server.rs"' Cargo.toml
    fi
    
    cargo run --release --example quic_server &
    QUIC_SERVER_PID=$!
    
    # 等待服务端启动
    sleep 3
    
    if ! kill -0 $WS_SERVER_PID 2>/dev/null; then
        log_error "WebSocket 服务端启动失败"
        return 1
    fi
    
    if ! kill -0 $QUIC_SERVER_PID 2>/dev/null; then
        log_error "QUIC 服务端启动失败"
        return 1
    fi
    
    log_success "WebSocket 和 QUIC 服务端均已启动"
    
    # 创建协议竞速客户端测试
    log_info "创建协议竞速客户端测试..."
    
    cat > examples/client/protocol_racing_test.rs << 'EOF'
//! 协议竞速测试客户端
//!
//! 测试客户端协议竞速功能

use flare_core::{
    client::{Client, ClientConfig},
    common::protocol::{Frame, MessageType},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    // 创建客户端配置，启用协议竞速
    let config = ClientConfig::new(
        "ws://127.0.0.1:8080".to_string(),  // WebSocket地址
        "127.0.0.1:4433".to_string()       // QUIC地址
    )
    .with_auto_protocol_selection()  // 启用协议竞速
    .with_heartbeat(5000, 30000);    // 5秒心跳，30秒超时
    
    // 创建客户端实例
    let mut client = Client::new(config);
    
    // 连接到服务器（协议竞速）
    println!("正在进行协议竞速...");
    client.connect().await?;
    println!("连接成功!");
    
    // 发送测试消息
    println!("正在发送测试消息...");
    let test_message = b"Hello, Protocol Racing!";
    let frame = Frame::new(
        MessageType::Data,
        1,
        flare_core::common::protocol::Reliability::AtLeastOnce,
        test_message.to_vec(),
    );
    
    client.send_message(frame).await?;
    println!("测试消息已发送: {}", String::from_utf8_lossy(test_message));
    
    // 等待一段时间以观察结果
    println!("等待5秒以观察服务器响应...");
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    
    // 断开连接
    println!("正在断开连接...");
    client.disconnect().await?;
    println!("连接已断开");
    
    Ok(())
}
EOF

    # 在 Cargo.toml 中添加协议竞速测试客户端示例
    if ! grep -q "protocol_racing_test" Cargo.toml; then
        sed -i '' '/im_client/a\
[[example]]\
name = "protocol_racing_test"\
path = "examples/client/protocol_racing_test.rs"' Cargo.toml
    fi
    
    # 运行协议竞速客户端测试
    log_info "运行协议竞速客户端测试..."
    timeout 20 cargo run --release --example protocol_racing_test || true
    
    # 停止服务端
    kill $WS_SERVER_PID 2>/dev/null || true
    kill $QUIC_SERVER_PID 2>/dev/null || true
    wait $WS_SERVER_PID 2>/dev/null || true
    wait $QUIC_SERVER_PID 2>/dev/null || true
    
    # 清理临时文件
    rm -f examples/server/quic_server.rs
    rm -f examples/client/protocol_racing_test.rs
    sed -i '' '/quic_server/d' Cargo.toml
    sed -i '' '/protocol_racing_test/d' Cargo.toml
    
    log_success "协议竞速测试完成"
}

# 测试 IM 网关
test_im_gateway() {
    log_info "=== 测试 IM 网关 ==="
    
    # 确保证书存在
    ensure_certificates
    
    # 启动 IM 网关
    log_info "启动 IM 网关..."
    cargo run --release --example im_gateway &
    IM_GATEWAY_PID=$!
    
    # 等待服务端启动
    sleep 5
    
    if ! kill -0 $IM_GATEWAY_PID 2>/dev/null; then
        log_error "IM 网关启动失败"
        return 1
    fi
    
    log_success "IM 网关已启动 (PID: $IM_GATEWAY_PID)"
    
    # 运行 IM 客户端测试
    log_info "运行 IM 客户端测试..."
    timeout 20 cargo run --release --example im_client <<< "test message
quit" || true
    
    # 停止 IM 网关
    kill $IM_GATEWAY_PID 2>/dev/null || true
    wait $IM_GATEWAY_PID 2>/dev/null || true
    
    log_success "IM 网关测试完成"
}

# 主函数
main() {
    case "${1:-}" in
        -h|--help)
            echo "Flare Core 综合测试脚本"
            echo ""
            echo "用法: $0 [选项]"
            echo ""
            echo "选项:"
            echo "  -h, --help         显示此帮助信息"
            echo "  --websocket        仅测试 WebSocket"
            echo "  --quic             仅测试 QUIC"
            echo "  --racing           仅测试协议竞速"
            echo "  --im-gateway       仅测试 IM 网关"
            echo ""
            echo "默认运行所有测试"
            exit 0
            ;;
        --websocket)
            check_dependencies
            build_project
            test_websocket_only
            ;;
        --quic)
            check_dependencies
            build_project
            ensure_certificates
            test_quic_only
            ;;
        --racing)
            check_dependencies
            build_project
            ensure_certificates
            test_protocol_racing
            ;;
        --im-gateway)
            check_dependencies
            build_project
            ensure_certificates
            test_im_gateway
            ;;
        "")
            # 默认运行所有测试
            check_dependencies
            build_project
            ensure_certificates
            
            test_websocket_only
            test_quic_only
            test_protocol_racing
            test_im_gateway
            
            log_success "所有测试完成！"
            ;;
        *)
            log_error "未知选项: $1"
            echo "使用 --help 查看帮助信息"
            exit 1
            ;;
    esac
}

# 运行主函数
main "$@"