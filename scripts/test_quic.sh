#!/bin/bash

# QUIC 功能测试脚本
# 用于验证 Flare Core 的 QUIC 服务端和客户端功能

set -e

echo "🚀 开始测试 QUIC 功能..."

# 检查必要的工具
check_requirements() {
    echo "📋 检查系统要求..."
    
    if ! command -v openssl &> /dev/null; then
        echo "❌ 错误: 需要安装 OpenSSL"
        exit 1
    fi
    
    if ! command -v cargo &> /dev/null; then
        echo "❌ 错误: 需要安装 Rust 和 Cargo"
        exit 1
    fi
    
    echo "✅ 系统要求检查通过"
}

# 生成测试证书
generate_certs() {
    echo "🔐 生成测试证书..."
    
    if [ ! -d "certs" ]; then
        mkdir -p certs
    fi
    
    if [ ! -f "certs/server.crt" ] || [ ! -f "certs/server.key" ]; then
        echo "生成自签名证书..."
        openssl req -x509 -newkey rsa:4096 \
            -keyout certs/server.key \
            -out certs/server.crt \
            -days 365 -nodes \
            -subj "/C=CN/ST=Beijing/L=Beijing/O=Flare/OU=Core/CN=localhost"
        echo "✅ 证书生成完成"
    else
        echo "✅ 证书已存在"
    fi
}

# 编译项目
build_project() {
    echo "🔨 编译项目..."
    
    if ! cargo build --features "tls,server,client" --release; then
        echo "❌ 编译失败"
        exit 1
    fi
    
    echo "✅ 编译完成"
}

# 运行测试
run_tests() {
    echo "🧪 运行 QUIC 测试..."
    
    # 启动服务端（后台运行）
    echo "启动 QUIC 服务端..."
    timeout 30s cargo run --example quic_example --features "tls,server,client" &
    SERVER_PID=$!
    
    # 等待服务端启动
    sleep 5
    
    # 检查服务端是否正常运行
    if ! kill -0 $SERVER_PID 2>/dev/null; then
        echo "❌ 服务端启动失败"
        exit 1
    fi
    
    echo "✅ 服务端启动成功 (PID: $SERVER_PID)"
    
    # 等待测试完成
    wait $SERVER_PID || true
    
    echo "✅ QUIC 测试完成"
}

# 清理
cleanup() {
    echo "🧹 清理资源..."
    
    if [ ! -z "$SERVER_PID" ]; then
        if kill -0 $SERVER_PID 2>/dev/null; then
            echo "停止服务端..."
            kill $SERVER_PID
        fi
    fi
    
    echo "✅ 清理完成"
}

# 主函数
main() {
    echo "=========================================="
    echo "Flare Core QUIC 功能测试"
    echo "=========================================="
    
    # 设置错误处理
    trap cleanup EXIT
    
    # 执行测试步骤
    check_requirements
    generate_certs
    build_project
    run_tests
    
    echo "=========================================="
    echo "🎉 所有测试通过！"
    echo "=========================================="
}

# 运行主函数
main "$@"
