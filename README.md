# Flare Core

[![Crates.io](https://img.shields.io/crates/v/flare-core.svg)](https://crates.io/crates/flare-core)
[![Documentation](https://docs.rs/flare-core/badge.svg)](https://docs.rs/flare-core)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.94%2B-orange.svg)](https://www.rust-lang.org/)

Rust 长连接通信库，面向即时通讯与实时推送场景。支持 WebSocket、QUIC、协议竞速、序列化与压缩协商、设备冲突策略及 Token 认证。

英文 API 文档见 [docs.rs/flare-core](https://docs.rs/flare-core)。

## 许可

[MIT License](LICENSE)。可自由使用、修改与再分发；分发时请保留版权声明与许可全文。

## 特性

- **传输**：WebSocket（含 TLS）、QUIC；客户端可多协议竞速
- **协商**：Protobuf / JSON；Gzip / Zstd 压缩
- **连接**：心跳、活跃检测、多设备策略
- **构建模式**：`ClientBuilder` / `ServerBuilder`（闭包）、Observer、Flare（生产推荐）

## 安装

```toml
[dependencies]
flare-core = "0.1.3"
```

## 快速开始（Flare 模式）

```rust
use async_trait::async_trait;
use flare_core::common::protocol::{Frame, PayloadCommand};
use flare_core::server::events::handler::ServerEventHandler;
use flare_core::server::FlareServerBuilder;
use std::sync::Arc;

struct Handler;

#[async_trait]
impl ServerEventHandler for Handler {
    async fn handle_message(
        &self,
        _command: &PayloadCommand,
        _connection_id: &str,
    ) -> flare_core::common::error::Result<Option<Frame>> {
        Ok(None)
    }
}

#[tokio::main]
async fn main() -> flare_core::common::error::Result<()> {
    let server = FlareServerBuilder::new("0.0.0.0:8080", Arc::new(Handler)).build()?;
    server.run().await
}
```

更多示例见 `examples/` 与 `doc/`。

## 架构

```
Application (ServerEventHandler, Authenticator)
        ↓
Core (ServerCore / ClientCore, ConnectionManager)
        ↓
Transport (HybridServer / HybridClient, WebSocket, QUIC)
```

## 文档

| 资源 | 说明 |
|------|------|
| [docs.rs](https://docs.rs/flare-core) | API 参考 |
| `examples/` | 可运行示例 |
| `doc/` | 各构建模式说明 |

## 版本

当前 crate 版本见 `Cargo.toml`。历史变更见仓库 tag 与 release 说明。

## 联系

技术问题可通过仓库 Issue 或邮件 `flare1522@163.com` 反馈。
