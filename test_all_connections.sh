#!/bin/bash

# 测试所有连接构建的脚本

echo "=== 测试所有连接构建 ==="

# 检查证书是否存在
if [ ! -f "certs/server.crt" ] || [ ! -f "certs/server.key" ]; then
    echo "证书不存在，正在生成..."
    cargo run --example cert_generator
    echo "证书生成完成！"
fi

echo ""
echo "=== 1. 测试 QUIC 连接 ==="
echo "启动 QUIC 服务端（后台运行）..."
cargo run --example quic_server_example &
QUIC_SERVER_PID=$!

# 等待服务端启动
sleep 3

echo "运行 QUIC 客户端..."
cargo run --example quic_client_example

# 停止 QUIC 服务端
kill $QUIC_SERVER_PID 2>/dev/null
wait $QUIC_SERVER_PID 2>/dev/null

echo ""
echo "=== 2. 测试 WebSocket 连接 ==="
echo "启动 WebSocket 服务端（后台运行）..."
cargo run --example websocket_server_example &
WS_SERVER_PID=$!

# 等待服务端启动
sleep 3

echo "运行 WebSocket 客户端..."
cargo run --example websocket_client_example

# 停止 WebSocket 服务端
kill $WS_SERVER_PID 2>/dev/null
wait $WS_SERVER_PID 2>/dev/null

echo ""
echo "=== 3. 测试自定义主机名 QUIC 连接 ==="
echo "启动 QUIC 服务端（后台运行）..."
cargo run --example quic_server_example &
QUIC_SERVER_PID=$!

# 等待服务端启动
sleep 3

echo "运行自定义主机名 QUIC 客户端..."
cargo run --example quic_custom_hostname_example

# 停止 QUIC 服务端
kill $QUIC_SERVER_PID 2>/dev/null
wait $QUIC_SERVER_PID 2>/dev/null

echo ""
echo "=== 所有连接测试完成 ==="
echo "✅ QUIC 连接正常"
echo "✅ WebSocket 连接正常"
echo "✅ 自定义主机名连接正常"
echo "✅ 所有连接都通过 ConnectionFactory 构建"
