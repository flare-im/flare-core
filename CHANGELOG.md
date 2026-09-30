# 变更记录

本文件只记录**已发布到 crates.io 的版本**。版本号遵循
[语义化版本](https://semver.org/lang/zh-CN/)。

## 1.1.1 — 未发布

安全修复版本。1.1.0 发布于 2026-08-03，此后修掉的公告都在这里。
**建议所有 1.1.0 用户升级**。

### 安全

- 升级传输层依赖，修复 **13 项 RUSTSEC 公告**。
- 升级 `time`，修复 **RUSTSEC-2026-0009**（栈耗尽 DoS，评分 6.8）。
- 迁离 `rustls-pemfile` —— 它已停止维护且**没有修复版本**
  （RUSTSEC-2025-0134）。PEM 解析改用其继任者 `rustls-pki-types`，
  它本就在依赖树里，不引入新依赖。
- 设备冲突改按（租户，用户）判定。此前 `DeviceManager` 只按 user_id 分桶，两个租户里
  user_id 相同、平台相同的用户会在平台互斥策略下互相踢掉设备，策略带来的设备名额也跨租户
  计算。租户取自认证结果（`AuthResult::with_tenant_id`，记在 `user_metadata` 的
  `tenant_id` 下），并原样写进连接元数据；认证器没给租户、或未启用认证且客户端也没报时，
  归入默认租户 `"0"`。
- 启用认证时，设备冲突不再使用客户端在 CONNECT 里自报的 `user_id`，改用认证结果里的
  用户。此前持有效令牌的客户端报上别人的 user_id，就能把对方同平台的设备踢下线。

### 不兼容变更

- `DeviceManager` 的 `add_device`、`remove_device`、`get_user_devices`、
  `get_user_connections`、`get_device_connection` 以及 `handle_device_conflict`
  改为接收 `DeviceOwner`（租户，用户），不再接收裸 user_id。新增
  `DeviceManager::remove_connection`，按连接 ID 移除设备。
- `NegotiationResult` 新增 `tenant_id` 字段（客户端自报，只在未启用认证时采用）。
  用结构体字面量构造它的代码需要补上这个字段或改用 `..Default::default()`。

### 修复

- 缺少事件处理器时返回配置错误，不再 panic。服务端在启动配置不完整时
  应当给出可读的错误，而不是崩掉。
- 服务端拒绝 CONNECT（认证失败）时，关连接前先发一个 ERROR 系统帧，编码与
  CONNECT_ACK 相同（协商前 JSON）。认证器给出类型化错误码（如 `TENANT_UNAVAILABLE`）
  时原样作为 message，其余一律为 `AUTHENTICATION_FAILED`；metadata `code` 为
  `AUTHENTICATION_FAILED`。此前连接被直接关掉，客户端分不清令牌失效和租户被停用。

### 其他

- 补齐 crate 的 `license` 声明。此前 `Cargo.toml` 里没有该字段，
  SBOM 与合规扫描会把它报成 "unlicensed"。
- 修正文档中的许可证表述：本 crate 是 **Apache-2.0**，此前部分文档写成 MIT。

## 1.1.0 — 2026-08-03

与契约层 `flare-proto` 2.0.1 对齐的实现层发布。

## 1.0.3 — 2026-07-05

见 git tag `1.0.3`。
