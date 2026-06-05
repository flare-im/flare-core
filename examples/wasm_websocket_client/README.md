# flare-core WASM WebSocket 客户端示例

演示在浏览器中通过 `wasm32-unknown-unknown` 使用 `flare-core` 的 **`FlareClientBuilder`**，**对接 Native `flare_chat_server`**。

## 与服务端配对

| 项目 | Native `flare_chat_server` | 本 WASM 示例 |
|------|---------------------------|--------------|
| 传输 | WebSocket `:8080` + QUIC `:8081` | 仅 WebSocket `:8080` |
| 协商 | Protobuf + Gzip + AES-256-GCM | 跟随服务端协商（不强制 JSON） |
| 加密 | 默认 AES（演示密钥） | 注册相同演示密钥 |
| 设备 | 平台互斥策略 | `DevicePlatform::Web` |
| QUIC | 支持 | 浏览器不可用（正常） |

**结论：可以**用 `flare_chat_server` 作为服务端；WASM 客户端走 WebSocket 分支，与 Native `flare_chat_client` 在 WS 路径上行为一致。

## 前置条件

1. **Rust wasm32 目标**

```bash
rustup target add wasm32-unknown-unknown
```

2. **wasm-pack**（任选一种安装方式）

```bash
# 推荐
cargo install wasm-pack --locked

# 或 macOS Homebrew
brew install wasm-pack
```

安装后若仍提示 `command not found`，把 `~/.cargo/bin` 加入 PATH：

```bash
export PATH="$HOME/.cargo/bin:$PATH"
```

## 构建

```bash
cd examples/wasm_websocket_client

# 方式 A：一键脚本（默认 dev 构建，本地开发足够）
./build.sh

# 方式 B：手动 dev 构建（推荐本地调试）
wasm-pack build --dev --target web --out-dir pkg

# 方式 C：release 构建（体积更小；若遇 wasm-bindgen 编译错误，先用 --dev）
wasm-pack build --target web --out-dir pkg
```

## 运行

**推荐一键启动**（会先终止 :8080 / :3000 旧进程，再启动服务端 + 静态页）：

```bash
cd examples/wasm_websocket_client
./build.sh          # 若 pkg/ 已存在可跳过
./run_local.sh      # WS :8080 + http://127.0.0.1:3000/index.html
```

或分两个终端手动启动：

1. 启动 Flare 聊天室服务端（推荐）：

```bash
cd ../..   # flare-core 根目录
RUST_LOG=info cargo run --example flare_chat_server

# WASM E2E 推荐：仅 WebSocket，避免 QUIC :8081 被占用导致启动失败
FLARE_WS_ONLY=1 RUST_LOG=info cargo run --example flare_chat_server
```

2. 静态服务 + 浏览器：

```bash
cd examples/wasm_websocket_client
./serve.sh          # 默认 :3000，与 build.sh 一致
# 或: python3 -m http.server 3000
# 打开 http://127.0.0.1:3000/index.html  （不要用 3001，除非你改了端口）
```

3. 连接 `ws://127.0.0.1:8080`，发送消息；可与 Native `flare_chat_client` 同时在线（不同平台可共存）。

## 常见问题

| 现象 | 原因 | 处理 |
|------|------|------|
| 终端 `GET /.well-known/appspecific/com.chrome.devtools.json 404` | Chrome DevTools 自动探测 | **可忽略**，与 IM 无关 |
| 点击「连接」无网络请求 | 旧版 `#[wasm_bindgen] async fn` + `run_async` 双重调度导致 WASM 不执行 | `./build.sh` 重建后 **硬刷新**（Cmd+Shift+R） |
| 页面打不开 | 静态端口与浏览器 URL 不一致（3000 vs 3001） | 用 `./run_local.sh` 或 `./serve.sh`，URL 与终端打印一致 |
| CONNECT 超时 | 未启动 `flare_chat_server` 或用了 `simple_server` | `FLARE_WS_ONLY=1 cargo run --example flare_chat_server` |
| `./run_local.sh` 显示 `Killed: 9` | 旧版脚本 `kill -9` 误杀 / 重复启动 / 锁目录残留 | 更新脚本后 `rm -rf .run_local.lock.d` 再执行；勿对 `run_local` 用 `kill -9` |
| `已有 run_local 在运行` | 上次异常退出未删锁 | `rm -rf examples/wasm_websocket_client/.run_local.lock.d` |
| `:8080 被非预期进程占用` | 端口被其它程序占用（非 flare_chat_server） | `lsof -nP -iTCP:8080 -sTCP:LISTEN` 查看并手动停止 |

## JavaScript API

| 函数 | 说明 |
|------|------|
| `flare_connect(url, username, encryption_key?)` | 连接；可选第三参数传入 32 字节密钥 |
| `flare_set_encryption_key(key)` | 连接前注入 UTF-8 密钥 |
| `flare_set_encryption_key_hex(hex)` | 连接前注入 hex 密钥（64 字符） |
| `flare_clear_encryption_key()` | 清除运行时密钥 |
| `flare_has_encryption_key()` | 是否已注入密钥 |
| `flare_encryption_key_len()` | 密钥长度（32） |
| `flare_disconnect()` | 断开 |
| `flare_send(text)` | 发送聊天消息 |
| `flare_is_connected()` | 连接状态 |
| `flare_wall_clock_ms()` | 墙钟毫秒（WASM：`Date.now()`） |
| `flare_now_rfc3339()` | UTC RFC3339 时间字符串 |
| `flare_runtime_id()` | 实例 ID（WASM：`wasm-{random}`） |

## WASM 平台工具（flare-core 库内）

业务代码可使用 `flare_core::common::platform`：

- **time**：`wall_clock_ms`、`monotonic_now`、`format_now_rfc3339`
- **env**：`runtime_instance_id`、`optional_env`、`web_device_info`、`default_local_ws_url`

Native 与 WASM 差异在此集中封装，避免业务层散落 `cfg(wasm32)`。

## 架构说明

- `flare-core` WASM 仅编译 WebSocket 客户端栈；`FlareClientBuilder::build_with_race()` 内部走 `WebSocketClient` + `ClientCore` 协商。
- 浏览器 `onmessage` 为同步回调：入站帧经 `ClientCore::push_wasm_inbound` 入队，在 `wait_for_negotiation` / `drain_wasm_inbound` 于 LocalSet 内处理（避免 CONNECT_ACK 丢失）。
- Tokio driver 与 `flare-im-core-sdk` 共用 `flare_core::client::wasm_tokio`（`run_async` + `yield_to_event_loop`）。
- **WASM 导出**：使用 `future_to_promise(run_async(...))`，不要用 `#[wasm_bindgen] async fn` 再包一层 `run_async`（会导致点击连接无 WebSocket 请求）。
