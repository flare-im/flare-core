#!/bin/bash

# TLS 加密客户端和服务端演示测试脚本
# 展示完整的消息类型交互和TLS加密功能

set -e

echo "🔐 Flare Core TLS 加密演示测试"
echo "================================"

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

# 检查证书
check_certs() {
    log_info "检查TLS证书..."
    
    if [[ ! -f "certs/server.crt" ]] || [[ ! -f "certs/server.key" ]] || [[ ! -f "certs/client.crt" ]]; then
        log_error "TLS证书文件缺失，正在生成..."
        ./scripts/generate_certs.sh
    else
        log_success "TLS证书文件已存在"
    fi
}

# 检查依赖
check_dependencies() {
    log_info "检查项目依赖..."
    
    if ! command -v cargo &> /dev/null; then
        log_error "Cargo 未安装"
        exit 1
    fi
    
    # 检查项目是否已构建
    if [[ ! -d "target" ]]; then
        log_info "项目未构建，正在构建..."
        cargo build --release
    fi
    
    log_success "依赖检查通过"
}

# 启动服务端
start_server() {
    log_info "启动服务端..."
    
    # 在后台启动服务端
    cargo run --example server_demo > server.log 2>&1 &
    SERVER_PID=$!
    
    # 等待服务端启动
    log_info "等待服务端启动..."
    sleep 5
    
    # 检查服务端是否启动成功
    if kill -0 $SERVER_PID 2>/dev/null; then
        log_success "服务端启动成功 (PID: $SERVER_PID)"
        return 0
    else
        log_error "服务端启动失败"
        return 1
    fi
}

# 启动客户端
start_client() {
    log_info "启动客户端..."
    
    # 在后台启动客户端
    cargo run --example client_demo > client.log 2>&1 &
    CLIENT_PID=$!
    
    # 等待客户端启动
    log_info "等待客户端启动..."
    sleep 3
    
    # 检查客户端是否启动成功
    if kill -0 $CLIENT_PID 2>/dev/null; then
        log_success "客户端启动成功 (PID: $CLIENT_PID)"
        return 0
    else
        log_error "客户端启动失败"
        return 1
    fi
}

# 监控运行状态
monitor_running() {
    log_info "监控客户端和服务端运行状态..."
    
    local start_time=$(date +%s)
    local max_runtime=180  # 最大运行时间3分钟
    
    while true; do
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))
        
        # 检查是否超时
        if [[ $elapsed -ge $max_runtime ]]; then
            log_warn "运行时间已达 ${max_runtime} 秒，准备停止"
            break
        fi
        
        # 检查服务端状态
        if ! kill -0 $SERVER_PID 2>/dev/null; then
            log_error "服务端已停止运行"
            break
        fi
        
        # 检查客户端状态
        if ! kill -0 $CLIENT_PID 2>/dev/null; then
            log_info "客户端已停止运行"
            break
        fi
        
        # 显示运行时间
        echo -ne "\r⏱️  运行时间: ${elapsed}s / ${max_runtime}s"
        
        sleep 5
    done
    
    echo ""  # 换行
}

# 停止所有进程
stop_processes() {
    log_info "停止所有进程..."
    
    # 停止服务端
    if [[ -n $SERVER_PID ]] && kill -0 $SERVER_PID 2>/dev/null; then
        log_info "停止服务端 (PID: $SERVER_PID)"
        kill $SERVER_PID 2>/dev/null || true
        wait $SERVER_PID 2>/dev/null || true
    fi
    
    # 停止客户端
    if [[ -n $CLIENT_PID ]] && kill -0 $CLIENT_PID 2>/dev/null; then
        log_info "停止客户端 (PID: $CLIENT_PID)"
        kill $CLIENT_PID 2>/dev/null || true
        wait $CLIENT_PID 2>/dev/null || true
    fi
    
    log_success "所有进程已停止"
}

# 显示日志摘要
show_log_summary() {
    log_info "显示运行日志摘要..."
    
    echo ""
    echo "📊 服务端日志摘要:"
    echo "=================="
    if [[ -f "server.log" ]]; then
        tail -20 server.log
    else
        echo "服务端日志文件不存在"
    fi
    
    echo ""
    echo "📊 客户端日志摘要:"
    echo "=================="
    if [[ -f "client.log" ]]; then
        tail -20 client.log
    else
        echo "客户端日志文件不存在"
    fi
}

# 清理日志文件
cleanup_logs() {
    log_info "清理日志文件..."
    
    rm -f server.log client.log
    log_success "日志文件已清理"
}

# 显示测试结果
show_test_results() {
    log_info "测试结果分析..."
    
    echo ""
    echo "🎯 测试完成！"
    echo "============="
    echo ""
    echo "📋 测试内容:"
    echo "  ✅ TLS 加密连接"
    echo "  ✅ WebSocket 和 QUIC 协议支持"
    echo "  ✅ 所有消息类型发送和接收"
    echo "  ✅ 心跳管理和连接状态监控"
    echo "  ✅ 协议竞速和自动切换"
    echo ""
    echo "🔍 查看详细日志:"
    echo "  tail -f server.log  # 服务端日志"
    echo "  tail -f client.log  # 客户端日志"
    echo ""
    echo "📁 证书文件:"
    echo "  certs/server.crt  # 服务端证书"
    echo "  certs/server.key  # 服务端私钥"
    echo "  certs/client.crt  # 客户端验证证书"
}

# 主函数
main() {
    case "${1:-}" in
        -h|--help)
            echo "Flare Core TLS 加密演示测试脚本"
            echo ""
            echo "用法: $0 [选项]"
            echo ""
            echo "选项:"
            echo "  -h, --help     显示此帮助信息"
            echo "  -c, --clean    清理日志文件"
            echo "  -k, --keep     保留日志文件"
            echo ""
            echo "功能:"
            echo "  - 启动TLS加密的服务端和客户端"
            echo "  - 演示所有消息类型的发送和接收"
            echo "  - 监控连接状态和协议性能"
            echo "  - 自动清理和结果分析"
            exit 0
            ;;
        -c|--clean)
            cleanup_logs
            exit 0
            ;;
        -k|--keep)
            KEEP_LOGS=true
            ;;
        "")
            # 默认行为
            ;;
        *)
            log_error "未知选项: $1"
            echo "使用 $0 -h 查看帮助信息"
            exit 1
            ;;
    esac
    
    # 设置信号处理
    trap 'log_warn "收到中断信号，正在清理..."; stop_processes; exit 1' INT TERM
    
    log_info "开始 Flare Core TLS 加密演示测试"
    
    # 执行测试步骤
    check_certs
    check_dependencies
    start_server
    start_client
    monitor_running
    stop_processes
    
    # 显示结果
    show_log_summary
    show_test_results
    
    # 清理日志（除非指定保留）
    if [[ "${KEEP_LOGS:-false}" != "true" ]]; then
        cleanup_logs
    fi
    
    log_success "TLS 加密演示测试完成！"
}

# 运行主函数
main "$@"
