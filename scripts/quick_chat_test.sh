#!/bin/bash

# 快速聊天室测试脚本
# 测试QUIC聊天室的基本功能

echo "🚀 快速聊天室测试..."

# 检查证书文件
if [ ! -f "certs/server.crt" ] || [ ! -f "certs/server.key" ]; then
    echo "❌ 证书文件不存在，正在生成..."
    ./scripts/generate_certs.sh
fi

# 编译项目
echo "🔨 编译项目..."
cargo build --examples

if [ $? -ne 0 ]; then
    echo "❌ 编译失败"
    exit 1
fi

echo "✅ 编译成功"
echo ""
echo "📋 测试步骤："
echo "1. 在一个终端运行: cargo run --example simple_quic_chat_server"
echo "2. 在另一个终端运行: cargo run --example simple_quic_chat_client Alice"
echo "3. 在第三个终端运行: cargo run --example simple_quic_chat_client Bob"
echo "4. 在Alice的客户端输入消息，检查Bob是否收到"
echo "5. 在Bob的客户端输入消息，检查Alice是否收到"
echo ""
echo "🔍 观察要点："
echo "- 服务端应该显示连接建立和消息接收日志"
echo "- 客户端应该显示连接成功和消息接收日志"
echo "- 消息应该在所有客户端之间正确广播"
echo ""
echo "✅ 准备就绪，请按照上述步骤进行测试"
