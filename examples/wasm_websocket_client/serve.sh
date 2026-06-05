#!/usr/bin/env bash
# 启动 WASM 静态页（默认 :3000）。与 build.sh 端口一致，避免 3000/3001 混用。
set -euo pipefail
cd "$(dirname "$0")"
PORT="${1:-3000}"
if [[ ! -f pkg/flare_core_wasm_example.js ]]; then
  echo "pkg/ 不存在，先运行: ./build.sh"
  exit 1
fi
echo "静态服务: http://127.0.0.1:${PORT}/index.html"
exec python3 -m http.server "$PORT"
