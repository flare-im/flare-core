#!/bin/bash

# QUIC通信测试脚本

echo "开始QUIC通信测试..."

# 编译项目
echo "编译项目..."
cargo build --examples

if [ $? -ne 0 ]; then
    echo "编译失败！"
    exit 1
fi

echo "编译成功！"

# 启动服务端（后台运行）
echo "启动QUIC服务端..."
cargo run --example quic_server_example &
SERVER_PID=$!

# 等待服务端启动
echo "等待服务端启动..."
sleep 3

# 启动客户端
echo "启动QUIC客户端..."
cargo run --example quic_client_example

# 等待客户端完成
sleep 2

# 停止服务端
echo "停止服务端..."
kill $SERVER_PID 2>/dev/null

echo "测试完成！"
