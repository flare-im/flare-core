#!/bin/bash

# Flare 聊天室测试脚本
# 测试两个用户相互聊天

set -e

echo "=========================================="
echo "Flare 聊天室测试"
echo "=========================================="
echo ""

# 设置日志级别
export RUST_LOG=info

# 颜色定义
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}步骤 1: 启动服务器...${NC}"
echo ""

# 在后台启动服务器
cargo run --example flare_chat_server > /tmp/flare_chat_server.log 2>&1 &
SERVER_PID=$!

# 等待服务器启动
sleep 3

# 检查服务器是否启动成功
if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo -e "${YELLOW}⚠️  服务器启动失败，查看日志:${NC}"
    cat /tmp/flare_chat_server.log
    exit 1
fi

echo -e "${GREEN}✅ 服务器已启动 (PID: $SERVER_PID)${NC}"
echo ""

echo -e "${BLUE}步骤 2: 启动第一个客户端 (用户: alice)...${NC}"
echo ""

# 启动第一个客户端（用户 alice）
# 使用 echo 命令模拟用户输入
(
    sleep 2  # 等待连接建立
    echo "Hello, this is Alice!"
    sleep 1
    echo "How are you?"
    sleep 1
    echo "quit"
) | cargo run --example flare_chat_client -- alice > /tmp/flare_chat_alice.log 2>&1 &
ALICE_PID=$!

echo -e "${GREEN}✅ 第一个客户端已启动 (PID: $ALICE_PID)${NC}"
echo ""

sleep 2

echo -e "${BLUE}步骤 3: 启动第二个客户端 (用户: bob)...${NC}"
echo ""

# 启动第二个客户端（用户 bob）
(
    sleep 2  # 等待连接建立
    echo "Hi Alice, this is Bob!"
    sleep 1
    echo "I'm doing great, thanks!"
    sleep 1
    echo "quit"
) | cargo run --example flare_chat_client -- bob > /tmp/flare_chat_bob.log 2>&1 &
BOB_PID=$!

echo -e "${GREEN}✅ 第二个客户端已启动 (PID: $BOB_PID)${NC}"
echo ""

echo -e "${YELLOW}等待客户端完成...${NC}"
echo ""

# 等待客户端完成
wait $ALICE_PID 2>/dev/null || true
wait $BOB_PID 2>/dev/null || true

sleep 2

echo ""
echo "=========================================="
echo "测试结果"
echo "=========================================="
echo ""

echo -e "${BLUE}服务器日志:${NC}"
echo "----------------------------------------"
tail -30 /tmp/flare_chat_server.log | grep -E "(💬|✅|❌|📝|新连接|用户断开)" || echo "（无相关日志）"
echo ""

echo -e "${BLUE}Alice 客户端日志:${NC}"
echo "----------------------------------------"
tail -20 /tmp/flare_chat_alice.log | grep -E "(消息|连接|用户ID)" || echo "（无相关日志）"
echo ""

echo -e "${BLUE}Bob 客户端日志:${NC}"
echo "----------------------------------------"
tail -20 /tmp/flare_chat_bob.log | grep -E "(消息|连接|用户ID)" || echo "（无相关日志）"
echo ""

# 清理
echo -e "${YELLOW}清理进程...${NC}"
kill $SERVER_PID 2>/dev/null || true
sleep 1

echo ""
echo -e "${GREEN}✅ 测试完成！${NC}"
echo ""
echo "提示：要手动测试，请："
echo "1. 在一个终端运行: cargo run --example flare_chat_server"
echo "2. 在另一个终端运行: cargo run --example flare_chat_client -- user1"
echo "3. 在第三个终端运行: cargo run --example flare_chat_client -- user2"
echo "4. 在两个客户端中输入消息，观察消息是否正常发送和接收"

