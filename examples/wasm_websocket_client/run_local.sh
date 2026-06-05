#!/usr/bin/env bash
# 本地 E2E：先安全终止旧 flare_chat_server / http.server，再启动新实例。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
EXAMPLE="$(cd "$(dirname "$0")" && pwd)"
PORT="${FLARE_HTTP_PORT:-3000}"
SELF=$$
LOCKDIR="$EXAMPLE/.run_local.lock.d"

if [[ ! -f "$EXAMPLE/pkg/flare_core_wasm_example.js" ]]; then
  echo ">>> 先构建 WASM: cd $EXAMPLE && ./build.sh"
  exit 1
fi

if ! mkdir "$LOCKDIR" 2>/dev/null; then
  echo ">>> 已有 run_local 在运行（若确认无进程，删除: rm -rf $LOCKDIR）"
  exit 1
fi

SERVER_PID=""
HTTP_PID=""

cleanup() {
  trap - EXIT INT TERM
  if [[ -n "${SERVER_PID}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill -TERM "$SERVER_PID" 2>/dev/null || true
  fi
  if [[ -n "${HTTP_PID}" ]] && kill -0 "$HTTP_PID" 2>/dev/null; then
    kill -TERM "$HTTP_PID" 2>/dev/null || true
  fi
  rm -rf "$LOCKDIR"
}

trap cleanup EXIT INT TERM

# 仅终止我们识别的进程；其它占用者报错退出，避免 kill -9 误伤。
stop_port() {
  local port="$1"
  local name_pattern="$2"
  local pids cmd

  pids="$(lsof -nP -iTCP:"$port" -sTCP:LISTEN -t 2>/dev/null || true)"
  [[ -z "$pids" ]] && return 0

  for pid in $pids; do
    [[ "$pid" -eq "$SELF" ]] && continue
    cmd="$(ps -p "$pid" -o command= 2>/dev/null || echo "")"
    if [[ "$cmd" != *"$name_pattern"* ]]; then
      echo ">>> 错误: :${port} 被非预期进程占用 (pid=${pid})"
      echo "    ${cmd}"
      echo ">>> 请手动停止该进程，或设置 FLARE_HTTP_PORT 后重试"
      exit 1
    fi
    echo ">>> 停止 :${port} pid=${pid}"
    kill -TERM "$pid" 2>/dev/null || true
  done

  sleep 1

  pids="$(lsof -nP -iTCP:"$port" -sTCP:LISTEN -t 2>/dev/null || true)"
  for pid in $pids; do
    [[ "$pid" -eq "$SELF" ]] && continue
    cmd="$(ps -p "$pid" -o command= 2>/dev/null || echo "")"
    [[ "$cmd" != *"$name_pattern"* ]] && continue
    echo ">>> 强制停止 :${port} pid=${pid}"
    kill -KILL "$pid" 2>/dev/null || true
  done
  sleep 0.5
}

echo ">>> 清理旧服务 ..."
stop_port 8080 "flare_chat_server"
stop_port "$PORT" "http.server"

echo ">>> 启动 flare_chat_server (FLARE_WS_ONLY=1, WS :8080) ..."
cd "$ROOT"
FLARE_WS_ONLY=1 RUST_LOG=info cargo run --example flare_chat_server &
SERVER_PID=$!

echo ">>> 等待 :8080 (pid=${SERVER_PID}) ..."
ready=0
for _ in $(seq 1 120); do
  if lsof -nP -iTCP:8080 -sTCP:LISTEN >/dev/null 2>&1; then
    ready=1
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo ">>> flare_chat_server 已退出，请查看上方 cargo 日志"
    wait "$SERVER_PID" || true
    exit 1
  fi
  sleep 1
done

if [[ "$ready" -ne 1 ]]; then
  echo ">>> flare_chat_server 未在 :8080 监听（超时）"
  exit 1
fi

echo ">>> 启动静态页 :${PORT} ..."
cd "$EXAMPLE"
python3 -m http.server "$PORT" &
HTTP_PID=$!
sleep 1

if ! lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo ">>> 静态服务未在 :${PORT} 监听"
  exit 1
fi

echo ""
echo "=============================================="
echo "  浏览器: http://127.0.0.1:${PORT}/index.html"
echo "  WebSocket: ws://127.0.0.1:8080"
echo "  密钥: 01234567890123456789012345678901"
echo "  硬刷新: Cmd+Shift+R"
echo "  Ctrl+C 停止本脚本及子进程"
echo "=============================================="
echo ""

# 只 wait 我们启动的两个进程，避免误 wait 其它后台任务
wait "$SERVER_PID" "$HTTP_PID"
