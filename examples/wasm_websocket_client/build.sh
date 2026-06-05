#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if ! command -v wasm-pack >/dev/null 2>&1; then
  echo "wasm-pack 未安装，正在通过 cargo install 安装…"
  cargo install wasm-pack --locked
fi

if ! rustup target list --installed | grep -q wasm32-unknown-unknown; then
  rustup target add wasm32-unknown-unknown
fi

# 本地开发默认 --dev（当前 toolchain 下 release 可能触发 wasm-bindgen build.rs 问题）
PROFILE="${1:-dev}"
if [[ "$PROFILE" == "release" ]]; then
  wasm-pack build --target web --out-dir pkg
else
  wasm-pack build --dev --target web --out-dir pkg
fi

echo ""
echo "构建完成: $(pwd)/pkg"
echo "启动静态服务: python3 -m http.server 3000"
echo "浏览器打开: http://127.0.0.1:3000/index.html"
