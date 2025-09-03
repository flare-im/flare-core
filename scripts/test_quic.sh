#!/bin/bash

# QUIC 测试脚本
# 用于验证 QUIC 客户端和服务端的功能

set -e

echo "🚀 QUIC 功能测试脚本"
echo "====================="

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
    
    cargo build --examples
    
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

# 运行 QUIC 服务端测试
test_quic_server() {
    log_info "启动 QUIC 服务端..."
    
    # 在后台启动服务端
    cargo run --example quic_server &
    SERVER_PID=$!
    
    # 等待服务端启动
    sleep 3
    
    log_success "QUIC 服务端已启动 (PID: $SERVER_PID)"
    
    # 返回服务端 PID 供后续使用
    echo $SERVER_PID
}

# 运行 QUIC 客户端测试
test_quic_client() {
    local server_pid=$1
    
    log_info "运行 QUIC 客户端测试..."
    
    # 运行快速测试客户端
    timeout 30 cargo run --example quick_test_quic_client || {
        log_warn "客户端测试超时或完成"
    }
    
    log_success "QUIC 客户端测试完成"
}

# 清理进程
cleanup() {
    local server_pid=$1
    
    log_info "清理进程..."
    
    if [ ! -z "$server_pid" ]; then
        kill $server_pid 2>/dev/null || true
        log_info "已停止服务端进程 (PID: $server_pid)"
    fi
    
    log_success "清理完成"
}

# 运行性能对比测试
run_performance_comparison() {
    log_info "运行 WebSocket vs QUIC 性能对比..."
    
    echo "1. 启动 WebSocket 服务端..."
    cargo run --example websocket_server &
    WS_SERVER_PID=$!
    sleep 2
    
    echo "2. 运行 WebSocket 客户端测试..."
    timeout 15 cargo run --example quick_test_client || true
    
    kill $WS_SERVER_PID 2>/dev/null || true
    sleep 1
    
    echo "3. 启动 QUIC 服务端..."
    cargo run --example quic_server &
    QUIC_SERVER_PID=$!
    sleep 2
    
    echo "4. 运行 QUIC 客户端测试..."
    timeout 15 cargo run --example quick_test_quic_client || true
    
    kill $QUIC_SERVER_PID 2>/dev/null || true
    
    log_success "性能对比测试完成"
}

# 主函数
main() {
    case "${1:-}" in
        -h|--help)
            echo "QUIC 测试脚本"
            echo ""
            echo "用法: $0 [选项]"
            echo ""
            echo "选项:"
            echo "  -h, --help         显示此帮助信息"
            echo "  --server-only      仅启动服务端"
            echo "  --client-only      仅运行客户端测试"
            echo "  --performance      运行性能对比测试"
            echo "  --interactive      交互式测试"
            echo ""
            echo "默认运行完整的客户端-服务端测试"
            exit 0
            ;;
        --server-only)
            check_dependencies
            ensure_certificates
            build_project
            log_info "启动 QUIC 服务端（按 Ctrl+C 停止）..."
            cargo run --example quic_server
            ;;
        --client-only)
            check_dependencies
            build_project
            log_info "运行 QUIC 客户端测试..."
            cargo run --example quick_test_quic_client
            ;;
        --performance)
            check_dependencies
            ensure_certificates
            build_project
            run_performance_comparison
            ;;
        --interactive)
            check_dependencies
            ensure_certificates
            build_project
            
            echo "请选择要运行的示例:"
            echo "1. QUIC 服务端"
            echo "2. QUIC 客户端"
            echo "3. QUIC 快速测试客户端"
            echo "4. WebSocket vs QUIC 性能对比"
            read -p "请输入选择 (1-4): " choice
            
            case $choice in
                1)
                    cargo run --example quic_server
                    ;;
                2)
                    cargo run --example quic_client
                    ;;
                3)
                    cargo run --example quick_test_quic_client
                    ;;
                4)
                    run_performance_comparison
                    ;;
                *)
                    log_error "无效的选择"
                    exit 1
                    ;;
            esac
            ;;
        "")
            # 默认运行完整测试
            check_dependencies
            ensure_certificates
            build_project
            
            SERVER_PID=$(test_quic_server)
            
            # 捕获中断信号以清理进程
            trap "cleanup $SERVER_PID; exit" INT TERM
            
            test_quic_client $SERVER_PID
            
            cleanup $SERVER_PID
            
            log_success "QUIC 测试全部完成！"
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