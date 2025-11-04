#!/bin/bash
# 聊天室测试脚本 - 多人聊天测试

# 获取项目根目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=========================================="
echo "    WebSocket 聊天室多人测试脚本"
echo "=========================================="
echo ""

# 编译项目
echo "正在编译项目..."
cargo build --examples 2>&1 | grep -E "error|Finished|Compiling" || {
    echo "❌ 编译失败！请检查错误信息"
    exit 1
}

echo "✅ 编译成功！"
echo ""

# 检查是否已安装 tmux（用于多终端）
if ! command -v tmux &> /dev/null; then
    echo "⚠️  未检测到 tmux，将使用手动测试方式"
    USE_TMUX=false
else
    echo "✅ 检测到 tmux，将自动创建多终端窗口"
    USE_TMUX=true
fi

echo ""
echo "=========================================="
echo "开始测试多人聊天功能"
echo "=========================================="
echo ""

if [ "$USE_TMUX" = true ]; then
    # 使用 tmux 创建多个窗口
    echo "正在创建 tmux 会话..."
    
    # 杀死可能存在的旧会话
    tmux kill-session -t chatroom_test 2>/dev/null
    
    # 创建新的 tmux 会话
    tmux new-session -d -s chatroom_test -n server
    
    # 在第一个窗口启动服务器
    tmux send-keys -t chatroom_test:server "cd $PROJECT_ROOT && cargo run --example websocket_server" Enter
    
    echo "✅ 服务端已在 tmux 窗口 'server' 中启动"
    sleep 2
    
    # 创建客户端窗口
    echo "正在创建客户端窗口..."
    for i in {1..3}; do
        tmux new-window -t chatroom_test -n "client$i"
        tmux send-keys -t chatroom_test:client$i "cd $PROJECT_ROOT && echo '客户端 $i - 请输入用户名后开始聊天' && cargo run --example websocket_client" Enter
        sleep 1
    done
    
    echo ""
    echo "✅ 已创建测试环境："
    echo "   - 服务端：tmux 窗口 'server'"
    echo "   - 客户端1：tmux 窗口 'client1'"
    echo "   - 客户端2：tmux 窗口 'client2'"
    echo "   - 客户端3：tmux 窗口 'client3'"
    echo ""
    echo "使用方法："
    echo "  1. 运行: tmux attach -t chatroom_test"
    echo "  2. 使用 Ctrl+b 然后按窗口号 (0,1,2,3) 切换窗口"
    echo "  3. 在每个客户端窗口输入用户名和消息进行聊天"
    echo "  4. 退出: Ctrl+b 然后按 d 分离会话，或输入 tmux kill-session -t chatroom_test"
    echo ""
    
    # 自动附加到会话
    read -p "是否立即附加到 tmux 会话？(y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        tmux attach -t chatroom_test
    fi
else
    # 手动测试模式
    echo "手动测试模式："
    echo ""
    echo "步骤 1: 在一个终端窗口启动服务端"
    echo "  运行: cd $PROJECT_ROOT && cargo run --example websocket_server"
    echo ""
    echo "步骤 2: 在另外的终端窗口运行客户端（可以开多个）"
    echo "  运行: cd $PROJECT_ROOT && cargo run --example websocket_client"
    echo ""
    echo "步骤 3: 在每个客户端："
    echo "  - 输入用户名"
    echo "  - 输入消息进行聊天"
    echo "  - 输入 /quit 退出"
    echo ""
    echo "提示: 可以打开多个终端窗口，每个窗口运行一个客户端，"
    echo "      这样就能测试多人聊天功能了！"
    echo ""
fi
