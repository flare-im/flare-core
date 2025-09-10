# Flare IM 网关服务器概览

## 简介

Flare IM 网关服务器是一个专为即时通讯应用设计的开箱即用服务器解决方案。它集成了认证、多端控制、消息广播等核心功能，为开发者提供了一个完整的IM后端基础。

## 核心特性

### 1. 双协议支持
- **WebSocket**：兼容性好，适用于Web应用
- **QUIC**：低延迟、高可靠性，适用于移动应用

### 2. 两阶段认证
- 连接建立后进行身份认证
- 支持Token认证和平台信息认证

### 3. 多端在线控制
- 支持用户在多个设备上同时在线
- 基于用户的连接管理策略

### 4. 消息广播
- 支持点对点、群聊和广播消息
- 可扩展的消息处理链

### 5. 心跳检测
- 自动检测和清理超时连接
- 可配置的心跳间隔

### 6. 高性能架构
- 基于Tokio异步运行时
- 支持大量并发连接

## 快速开始

### 运行服务器

```bash
cargo run --example im_gateway
```

### 运行客户端测试

```bash
cargo run --example im_client
```

## 目录结构

```
flare-core/
├── examples/
│   ├── server/
│   │   ├── im_gateway.rs          # IM网关服务器示例
│   │   ├── README.md              # 服务器使用说明
│   │   └── ...                    # 其他服务器示例
│   └── client/
│       ├── im_client.rs           # IM客户端示例
│       └── ...                    # 其他客户端示例
├── docs/
│   ├── im_gateway_user_guide.md   # 用户指南
│   ├── im_gateway_architecture.md # 架构设计文档
│   └── im_gateway_usage_examples.md # 使用示例
├── scripts/
│   └── test_im_gateway.sh         # 测试脚本
└── src/
    └── server/                    # 服务器核心代码
        ├── auth.rs                # 认证管理器
        ├── websocket.rs           # WebSocket服务器
        ├── quic.rs                # QUIC服务器
        ├── manager/               # 连接管理器
        ├── service.rs             # 消息服务
        └── server.rs              # 服务器主类
```

## 主要组件

### 1. IM网关服务器 (im_gateway.rs)
- 集成了所有核心功能的完整服务器实现
- 使用UserBasedManager支持多端在线
- 包含完整的消息处理链

### 2. IM客户端 (im_client.rs)
- 用于测试IM网关服务器的客户端示例
- 演示了认证、消息发送等基本操作

### 3. 认证管理器 (auth.rs)
- 处理两阶段认证流程
- 支持平台信息认证
- 管理用户在线状态

### 4. 连接管理器 (manager/)
- ConnectionBasedManager: 基于连接的管理器
- UserBasedManager: 基于用户的管理器（支持多端在线）

### 5. 消息处理器 (service.rs)
- LoggingMessageHandler: 日志记录处理器
- EnhancedMessageHandler: 增强消息处理器
- BroadcastMessageHandler: 广播消息处理器

## 配置选项

### 服务器配置

```rust
let config = ServerConfig {
    websocket_addr: Some("127.0.0.1:8080".to_string()),
    quic_addr: Some("127.0.0.1:8081".to_string()),
    enable_tls: false,
    tls_cert_path: None,
    tls_key_path: None,
    max_connections: 1000,
    connection_timeout_ms: 30000,
    heartbeat_interval_ms: 5000,
    enable_auto_cleanup: true,
};
```

## 扩展功能

### 自定义认证
- 实现AuthHandler trait创建自定义认证逻辑
- 支持带平台信息的认证

### 自定义消息处理
- 实现MessageHandler trait创建自定义消息处理器
- 支持消息处理链

### 自定义连接管理
- 实现ConnectionManager trait创建自定义连接管理器

## 安全特性

- TLS加密支持
- Token认证机制
- 连接频率限制
- 访问控制

## 性能优化

- 异步架构支持高并发
- 心跳检测自动清理超时连接
- 可配置的连接超时时间
- 内存优化设计

## 部署建议

### 开发环境
```bash
cargo run --example im_gateway
```

### 生产环境
```bash
cargo build --release
./target/release/examples/im_gateway
```

### 系统服务部署
创建systemd服务文件实现自动重启和管理。

## 文档资源

1. [用户指南](docs/im_gateway_user_guide.md) - 详细的使用说明
2. [架构设计](docs/im_gateway_architecture.md) - 系统架构和技术细节
3. [使用示例](docs/im_gateway_usage_examples.md) - 实际应用示例

## 贡献

欢迎提交Issue和Pull Request来改进Flare IM网关服务器。

### 开发流程
1. Fork项目
2. 创建功能分支
3. 提交更改
4. 发起Pull Request

## 许可证

MIT许可证