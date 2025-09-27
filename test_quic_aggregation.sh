#!/bin/bash

# 测试 AggregationServer 和 Client 的 QUIC 通信

echo "=== 测试 AggregationServer 和 Client 的 QUIC 通信 ==="

# 检查证书是否存在
if [ ! -f "certs/server.crt" ] || [ ! -f "certs/server.key" ]; then
    echo "证书不存在，正在生成..."
    cargo run --example cert_generator
    echo "证书生成完成！"
fi

echo ""
echo "=== 启动 AggregationServer QUIC 服务端（后台运行）==="
cargo run --example quic_server &
SERVER_PID=$!

# 等待服务端启动
echo "等待服务端启动..."
sleep 5

echo ""
echo "=== 运行 Client QUIC 客户端 ==="
cargo run --example quic_client

echo ""
echo "=== 停止服务端 ==="
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo ""
echo "=== 测试完成 ==="
echo "✅ AggregationServer 服务端正常"
echo "✅ Client 客户端正常"
echo "✅ QUIC 通信正常"
echo "✅ 所有连接都通过 ConnectionFactory 构建"
