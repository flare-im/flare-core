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
- 连接管理器的用户索引改按（租户，用户）建立。此前按裸 user_id 建索引：`send_to_user`
  会把帧写到其他租户里 user_id 相同的用户的连接上，`get_user_connections` 也返回其他
  租户的连接，用户数把两个租户的同名用户算成一个。现在按连接元数据里的租户（认证结果写入）
  限定，没有租户的连接归入默认租户 `"0"`；租户在用户之后才写入时，索引随之迁移。
- 启用认证时，认证器返回「通过」却没有给出用户（或给了空用户）就拒绝 CONNECT，客户端收到
  `AUTHENTICATION_FAILED`。此前回落到客户端在 CONNECT 里自报的 `user_id`，持任意有效
  令牌就能把连接绑到别人名下。

### 不兼容变更

- `DeviceManager` 的 `add_device`、`remove_device`、`get_user_devices`、
  `get_user_connections`、`get_device_connection` 以及 `handle_device_conflict`
  改为接收 `DeviceOwner`（租户，用户），不再接收裸 user_id。新增
  `DeviceManager::remove_connection`，按连接 ID 移除设备。
- `NegotiationResult` 新增 `tenant_id` 字段（客户端自报，只在未启用认证时采用）。
  用结构体字面量构造它的代码需要补上这个字段或改用 `..Default::default()`。
- `AuthResult` 新增 `retryable` 字段（见下文「修复」中的暂时性拒绝）。用结构体字面量构造
  它的代码需要补上这个字段，或改用 `success` / `failure` / `retryable_failure` 构造。
- 按用户寻址的接口改为接收 `&DeviceOwner`（租户，用户），不再接收裸 user_id：
  `ConnectionManager::get_user_connections`，`ConnectionManagerTrait` 的
  `get_user_connections`、`send_to_user`、`send_frame_to_user`，`ServerHandle::send_to_user`，
  以及 `ServerCore`、`FlareServer`、`ObserverServer`、`SimpleServer`、`MessageContext`、
  `ServerWrapper` 的 `send_to_user`。单租户部署传 `DeviceOwner::new(None, user_id)`。
  `bind_user` 签名不变，用户绑在连接当前的租户下；`user_count` 与
  `ConnectionStats::total_users` 按（租户，用户）计数。新增 `ConnectionInfo::owner`。
- `DefaultAuthenticator` 改为采用客户端在 CONNECT 元数据 `user_id` 里自报的用户（它本就
  不校验令牌），客户端没报时按认证失败拒绝。此前它返回不带用户的「通过」，由服务端回落到
  自报值，没报 `user_id` 的客户端会以无用户的身份接入。

### 修复

- 客户端（QUIC）：被服务端踢下线或移除之后，下一次发送不再复用旧连接。此前惰性重连把
  CONNECT 发在服务端已经不读的旧流上，传输层照样报 Connected，客户端却等不到 CONNECT_ACK，
  要等心跳在旧连接上失败（约 24 s）才真正重连。TCP 客户端有同样的问题，一并修复。
- QUIC 传输在对端结束流或读出错后即标记为关闭：此后发送立即失败，新挂的观察者只会收到
  Disconnected。读任务等第一个观察者挂上才开始读，服务端不再因观察者挂得晚而丢掉客户端
  刚发来的 CONNECT。
- 客户端：连接断开时停掉绑在这条连接上的心跳。此前旧心跳留在死连接上继续发 PING，失败时
  再关一次连接并上报断开，还占着槽位，下一条连接协商完成后起不了自己的心跳。
- 客户端：CONNECT_ACK 之前连接被关，按服务端先下发的 ERROR 帧分类：`TENANT_UNAVAILABLE`
  报 `PERMISSION_DENIED`，`AUTHENTICATION_FAILED`、`TOKEN_EXPIRED` 等错误码原样上报；
  服务端没给原因时报 `CONNECTION_FAILED`。此前一律当成令牌被拒，心跳失败、踢线后的断开
  都被误报成鉴权失败。新增 `connect_rejection_error` 与 `TENANT_UNAVAILABLE_REASON`，
  去掉未发布过的 `TOKEN_REJECTED_PREFIX`。
- 客户端：入站帧按读出那一刻的协商状态选解析器。服务端先发 KICKED 再关流时，关流事件常常
  先把协商标志复位，KICKED 随后被当成协商前的 JSON 解析失败而整帧丢掉，上层看不到踢线原因。
- WebSocket、TCP 传输与 QUIC 一样，读任务等第一个观察者挂上才开始读。此前服务端握手完成后
  立即开读，观察者还没挂上时到达的 CONNECT 被读出后丢掉，客户端只能干等 CONNECT_ACK；
  这段时间里对端断开也没人收到，连接停在「已连接」。连接关闭（对端关闭、读出错或本端关闭）
  之后发送立即失败，新挂的观察者只收到 Disconnected。
- 缺少事件处理器时返回配置错误，不再 panic。服务端在启动配置不完整时
  应当给出可读的错误，而不是崩掉。
- 服务端拒绝 CONNECT（认证失败）时，关连接前先发一个 ERROR 系统帧，编码与
  CONNECT_ACK 相同（协商前 JSON）。认证器给出类型化错误码（如 `TENANT_UNAVAILABLE`）
  时原样作为 message，其余一律为 `AUTHENTICATION_FAILED`；metadata `code` 为
  `AUTHENTICATION_FAILED`。此前连接被直接关掉，客户端分不清令牌失效和租户被停用。
- CONNECT_ACK 发出后 `ServerEventHandler::on_connect` 失败时（例如认证时租户缓存还是
  active，业务层登记会话时租户已被停用），关连接前先发一个 KICKED 系统帧，按本次协商的
  格式编码。错误原因是类型化错误码时原样作为 message 和 metadata `reason`，否则用错误码
  本身（如 `SERVICE_UNAVAILABLE`）；metadata `code` 为错误码。此前客户端刚收到 ACK 就被
  无原因断开，无法与网络故障区分。`PermissionDenied` 的 CONNECT 失败改记 warn。
- 认证器可以给出暂时性拒绝：`AuthResult::retryable_failure`，用于认证依赖暂时读不到
  （例如租户状态库抖动）、并没有对令牌或租户下结论的情况。服务端以 `SERVICE_UNAVAILABLE`
  拒绝 CONNECT，ERROR 帧的 metadata `code` 也是它，客户端拿到的错误码为
  `SERVICE_UNAVAILABLE`（可重试）。此前认证器只能报失败，客户端拿到的不是鉴权失败就是
  `TENANT_UNAVAILABLE`，前者让客户端去换令牌，后者让它终止会话、不再重连，一次数据库
  抖动就会让这段时间里连进来的客户端一直离线。

### 其他

- 补齐 crate 的 `license` 声明。此前 `Cargo.toml` 里没有该字段，
  SBOM 与合规扫描会把它报成 "unlicensed"。
- 修正文档中的许可证表述：本 crate 是 **Apache-2.0**，此前部分文档写成 MIT。

## 1.1.0 — 2026-08-03

与契约层 `flare-proto` 2.0.1 对齐的实现层发布。

## 1.0.3 — 2026-07-05

见 git tag `1.0.3`。
