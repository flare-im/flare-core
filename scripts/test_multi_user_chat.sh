#!/bin/bash

# 多用户聊天室测试脚本
# 测试QUIC聊天室的多用户消息广播功能

echo "🚀 开始多用户聊天室测试..."

# 检查证书文件是否存在
if [ ! -f "certs/server.crt" ] || [ ! -f "certs/server.key" ]; then
    echo "❌ 证书文件不存在，正在生成..."
    ./scripts/generate_certs.sh
fi

# 启动服务端（后台运行）
echo "📡 启动聊天室服务端..."
cargo run --example simple_quic_chat_server &
SERVER_PID=$!

# 等待服务端启动
echo "⏳ 等待服务端启动..."
sleep 3

# 检查服务端是否成功启动
if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo "❌ 服务端启动失败"
    exit 1
fi

echo "✅ 服务端启动成功，PID: $SERVER_PID"

# 启动多个客户端进行测试
echo "👥 启动多个客户端..."

# 客户端1
echo "启动客户端1 (Alice)..."
cargo run --example simple_quic_chat_client Alice &
CLIENT1_PID=$!

# 等待客户端1连接
sleep 2

# 客户端2  
echo "启动客户端2 (Bob)..."
cargo run --example simple_quic_chat_client Bob &
CLIENT2_PID=$!

# 等待客户端2连接
sleep 2

# 客户端3
echo "启动客户端3 (Charlie)..."
cargo run --example simple_quic_chat_client Charlie &
CLIENT3_PID=$!

# 等待客户端3连接
sleep 2

echo "✅ 所有客户端已启动"
echo "📝 测试说明："
echo "1. 在客户端1 (Alice) 中输入消息"
echo "2. 检查客户端2 (Bob) 和客户端3 (Charlie) 是否收到消息"
echo "3. 在客户端2 (Bob) 中输入消息"
echo "4. 检查客户端1 (Alice) 和客户端3 (Charlie) 是否收到消息"
echo "5. 测试完成后，在任意客户端输入 '/quit' 退出"
echo ""
echo "🔍 观察日志输出，确认消息广播是否正常工作"
echo ""

# 等待用户手动测试
echo "按 Enter 键开始自动测试，或按 Ctrl+C 手动测试..."
read -r

# 自动测试：发送测试消息
echo "🤖 开始自动测试..."

# 发送测试消息到客户端1
echo "测试消息1: Hello from Alice" | timeout 5 cargo run --example simple_quic_chat_client Alice &
sleep 3

# 发送测试消息到客户端2  
echo "测试消息2: Hello from Bob" | timeout 5 cargo run --example simple_quic_chat_client Bob &
sleep 3

# 发送测试消息到客户端3
echo "测试消息3: Hello from Charlie" | timeout 5 cargo run --example simple_quic_chat_client Charlie &
sleep 3

echo "✅ 自动测试完成"
echo ""

# 清理函数
cleanup() {
    echo "🧹 清理进程..."
    
    # 终止客户端
    kill $CLIENT1_PID $CLIENT2_PID $CLIENT3_PID 2>/dev/null || true
    
    # 终止服务端
    kill $SERVER_PID 2>/dev/null || true
    
    # 等待进程结束
    sleep 2
    
    # 强制终止（如果还在运行）
    kill -9 $CLIENT1_PID $CLIENT2_PID $CLIENT3_PID $SERVER_PID 2>/dev/null || true
    
    echo "✅ 清理完成"
}

# 设置清理陷阱
trap cleanup EXIT INT TERM

echo "📋 测试完成！"
echo "💡 提示：如果看到消息在多个客户端之间正确广播，说明聊天室功能正常"
echo "🛑 按 Ctrl+C 退出测试"
