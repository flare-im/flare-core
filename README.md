# Flare Core

[![Crates.io](https://img.shields.io/crates/v/flare-core.svg)](https://crates.io/crates/flare-core)
[![Documentation](https://docs.rs/flare-core/badge.svg)](https://docs.rs/flare-core)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-blue.svg)](https://www.rust-lang.org/)

**Flare Core** 是一个高性能、可靠的即时通讯长连接工具包，专为 Rust 设计。提供了简洁的 API 和强大的功能，让开发者能够轻松构建实时通信应用。

> **注意**: 本文档使用中文编写。For English documentation, please refer to [docs.rs/flare-core](https://docs.rs/flare-core).

## ✨ 核心特性

### 🚀 多协议支持
- **WebSocket** - 基于标准 WebSocket 协议，支持 Web 和移动端
- **QUIC** - 基于 UDP 的现代传输协议，提供更低的延迟和更好的性能
- **协议竞速** - 客户端可以同时尝试多个协议，自动选择最快的连接

### 🔐 认证与安全
- **Token 认证** - 支持可配置的 token 验证机制
- **自定义认证器** - 实现 `Authenticator` trait 提供自定义验证逻辑
- **认证超时** - 可配置的认证超时时间

### 📱 多设备管理
- **设备冲突策略** - 支持平台互斥、移动端互斥、完全互斥、移动端和PC共存、完全开放等多种策略
- **设备信息** - 支持设备ID、平台、型号、版本等信息

### 🔄 序列化协商
- **多格式支持** - 支持 Protobuf 和 JSON 序列化格式
- **压缩算法** - 支持 None、Gzip、Zstd 压缩算法
- **自动协商** - 客户端和服务端自动协商最优的序列化格式和压缩算法

### 💓 心跳检测
- **自动心跳** - 服务端自动检测连接超时
- **客户端心跳** - 客户端自动发送心跳保持连接
- **可配置** - 心跳间隔和超时时间可配置

## 🏗️ 三种构建模式

Flare Core 提供三种构建模式，从简单到完整，按需选择：

| 模式 | 构建器 | 实现方式 | 适用场景 |
|------|--------|---------|---------|
| **简单模式** | `ClientBuilder` / `ServerBuilder` | 闭包（Closure） | 快速原型、学习测试 |
| **观察者模式** | `ObserverClientBuilder` / `ObserverServerBuilder` | Trait 实现 | 自定义处理、事件驱动 |
| **Flare 模式** | `FlareClientBuilder` / `FlareServerBuilder` | 完整功能集 | **生产环境（推荐）** |

### Flare 模式（推荐）

**服务端**：只需实现 `ServerEventHandler` trait，框架自动处理消息路由、ACK、错误响应等。

```rust
use async_trait::async_trait;
use flare_core::server::events::handler::ServerEventHandler;
use flare_core::common::protocol::{MessageCommand, Frame};
use flare_core::common::*;
use flare_core::server::*;

struct MyHandler;

#[async_trait]
impl ServerEventHandler for MyHandler {
    async fn handle_message(
        &self,
        command: &MessageCommand,
        connection_id: &str,
    ) -> Result<Option<Frame>> {
        // 处理消息
        Ok(None) // 返回 None 表示使用自动 ACK
    }

    async fn on_connect(&self, connection_id: &str) -> Result<()> {
        println!("连接建立: {}", connection_id);
        Ok(())
    }
}

let server = FlareServerBuilder::new("0.0.0.0:8080", Arc::new(MyHandler))
    .enable_auth()
    .with_authenticator(authenticator)
    .build()?;
```

## 📦 安装

```toml
[dependencies]
flare-core = "0.1.3"
```

## 🚀 快速开始

### 服务端示例

- [服务端 Flare 模式示例](doc/server-flare-mode-example.md) - **推荐**
- [服务端观察者模式示例](doc/server-observer-mode-example.md)
- [服务端简单模式示例](doc/server-simple-mode-example.md)

### 客户端示例

- [客户端 Flare 模式示例](doc/client-flare-mode-example.md) - **推荐**
- [客户端观察者模式示例](doc/client-observer-mode-example.md)
- [客户端简单模式示例](doc/client-simple-mode-example.md)

## 🔧 常用功能

- **设备管理** - 支持多种设备冲突策略，详见 [API 文档](https://docs.rs/flare-core)
- **序列化协商** - 自动协商 Protobuf/JSON 和压缩算法，详见 [API 文档](https://docs.rs/flare-core)
- **认证机制** - 支持 Token 认证和自定义认证器，详见 [API 文档](https://docs.rs/flare-core)

## 🏗️ 架构设计

```
┌─────────────────────────────────────────┐
│          应用层 (Application)            │
│  - ServerEventHandler                   │
│  - Authenticator                        │
└─────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────┐
│          核心层 (Core)                    │
│  - ServerCore / ClientCore               │
│  - ConnectionManager                     │
│  - DeviceManager                         │
└─────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────┐
│          协议层 (Protocol)                │
│  - HybridServer / HybridClient           │
└─────────────────────────────────────────┘
```

## 📊 性能特性

- **异步架构** - 基于 Tokio 的高性能异步运行时
- **零拷贝** - 尽可能减少数据拷贝
- **连接复用** - 高效的连接管理

### 性能指标

| 指标 | 目标值 |
|------|--------|
| **消息处理延迟** | P99 < 50ms |
| **连接建立延迟** | P99 < 100ms |
| **内存占用** | < 2GB/10K连接 |
| **吞吐量** | 10万+ TPS/实例 |

## 📖 文档

- **API 文档**: [docs.rs/flare-core](https://docs.rs/flare-core)
- **示例代码**: 位于 `examples/` 目录，详见各示例文档

## 🔒 安全性

- **认证机制** - 支持可配置的 token 验证
- **连接状态** - 只有已验证的连接才能收发业务消息
- **TLS 支持** - 支持 TLS/SSL 加密（WebSocket 和 QUIC）

## 📦 发布

```bash
cargo add flare-core
```

详细发布指南请查看 [PUBLISH.md](PUBLISH.md)。

## 📝 版本历史

### 0.1.3 (当前版本)

- 优化 Flare 模式实现
- 改进代码结构和可读性
- 完善文档

### 0.1.0

- ✨ 初始发布
- 支持 WebSocket 和 QUIC 协议
- 支持协议竞速、序列化协商、压缩算法
- 支持多设备管理和 Token 认证
- 提供三种构建模式（简单、观察者、Flare）

---

**Flare Core** - 让实时通信变得简单 🚀
