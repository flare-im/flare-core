# Flare IM 性能报告

> 本文的目的不是展示漂亮数字，而是让你**自己跑出可比较的数字**。
> 所有数据都附带复现命令与环境；对不能代表生产的部分，本文直接说明。

**测量日期**：2026-08-03

---

## 一、先说这份数据不能证明什么

企业选型时最容易被误导的就是厂商给的性能数字。所以先划清边界：

| 本文**能**回答 | 本文**不能**回答 |
|---|---|
| 单机上编解码、连接管理、事件分发的量级 | 集群吞吐、跨机房延迟 |
| 各环节的相对开销（哪里是瓶颈） | 真实用户在弱网下的体感 |
| 改动是否引入性能回退（可作回归基线） | 你的业务负载下的容量规划 |

**下面所有数字都来自一台 MacBook 的进程内基准测试，不含网络、不含数据库、
不含多机协调。** 拿它做容量规划会严重高估。它的正确用途是：
①判断架构量级是否合理 ②作为性能回归的基线。

要做容量规划，请在与生产同构的环境上跑第四节的端到端压测。

## 二、测量环境

| 项 | 值 |
|---|---|
| CPU | Apple M1 Pro（10 核） |
| 内存 | 16 GB |
| 系统 | macOS 26.5.1 |
| Rust | 1.94.1 |
| 构建 | `--release`（cargo bench 默认） |

> ⚠️ **ARM 笔记本与生产用的 x86 服务器不可直接比较。** 同一份基准在云上
> 通用型实例通常慢 1.5–3 倍（更低的单核频率、共享 CPU、虚拟化开销）。

## 三、传输层（flare-core）

复现：

```bash
cd flare-core
cargo bench --bench perf_baseline --features "server,compression-gzip"
```

| 指标 | ops/s | ns/op | 说明 |
|---|---:|---:|---|
| `codec.protobuf.round_trip.256b` | 1,488,965 | 672 | Protobuf 编解码往返 |
| `pipeline.process_raw.validate_no_response.256b` | 1,383,957 | 723 | 完整管线处理（校验，不产生响应） |
| `connection_manager.add_update_remove` | 1,104,655 | 905 | 单连接生命周期（分片管理器） |
| `codec.json.round_trip.256b` | 200,549 | 4,986 | JSON 编解码往返 |
| `codec.protobuf_gzip.round_trip.1kb` | 59,800 | 16,722 | Protobuf + gzip（1KB） |
| `connection_manager.broadcast.1000x256b` | 1,521 | 657,320 | 向 1000 个内存连接广播（一次广播为一 op） |
| `connection_manager.broadcast_frame_explicit_parser.1000x256b` | 1,720 | 581,536 | 同上，但帧只序列化一次 |
| `connection_manager.cleanup_timeout_trait.1000` | 1,080 | 926,250 | 快照+关闭+批量移除 1000 个超时连接 |

### 从数据里能读出的三件事

**① Protobuf 比 JSON 快 7.4 倍**（672ns vs 4,986ns）。协商时默认选 Protobuf 是对的，
JSON 通道只应用于调试与协商前握手。

**② gzip 的代价是 25 倍**（16,722ns vs 672ns）。压缩只对大载荷划算 ——
小文本消息开压缩是净亏。当前默认策略按体积阈值开启，与这个数据一致。

**③ 广播时预序列化能省 12%**（581µs vs 657µs，1000 连接）。差距会随连接数放大，
超大群场景应始终走 `broadcast_frame_explicit_parser` 那条路径。

> 广播那三项的 ops/s 看起来很低，是因为**一次 op 等于向 1000 个连接各发一次**。
> 换算成单连接投递约 580–660 ns/连接。

## 四、客户端核心（flare-im-core-sdk）

复现：

```bash
cd flare-im-core-sdk
cargo bench --bench perf_baseline
```

该套件用 criterion，输出含置信区间与相对上次运行的变化率 —— 适合做回归检测。
关键量级：

| 指标 | 吞吐 |
|---|---|
| `protocol_codec/decode_data_packet` | ~1.27 GiB/s（66.7 ns） |
| `event_bus_publish_steady_state/0`（无订阅者） | 1.31 M/s |
| `event_bus_publish_steady_state/100`（100 订阅者） | 15.1 K/s |
| `message_send/prepare_text_message` | 1.06 M/s |
| `message_send/memory_store_save_batch_100` | 808 K 条/s |
| `event_json_serialization/sync_messages_1000_payload` | 298 K 条/s |

**事件总线随订阅者数近似线性劣化**（0 → 100 订阅者，吞吐降约 87 倍）。
这符合扇出的本质，但意味着**单进程内挂大量订阅者会成为瓶颈** ——
多设备/多视图场景应复用订阅而非各自订阅。

## 五、端到端压测（做容量规划请用这个）

上面都是进程内基准。真实容量必须连着服务端与数据库测：

```bash
# 1) 起全栈（见 flare-im-core/QUICKSTART.md）
cd flare-im-core && docker compose -f deploy/docker-compose.yml up -d && ./scripts/start_server.sh

# 2) 两用户端到端时延与吞吐
cd flare-im-core-sdk && cargo run --release --example two_user_latency_throughput

# 3) 群场景时延与吞吐
cargo run --release --example group_latency_throughput
```

**报告端到端数字时必须同时给出**：服务端与客户端是否同机、数据库规格、
网络 RTT、并发连接数。缺任何一项，数字都无法被验证或比较。

## 六、把它当回归基线用

性能回退通常不是一次跳崖，而是每次改动慢 3%、半年后慢一倍。建议：

```bash
# 改动前
cargo bench --bench perf_baseline -- --save-baseline before
# 改动后
cargo bench --bench perf_baseline -- --baseline before
```

criterion 会直接标出显著变化（含 p 值）。**超过 5% 的劣化应当在 PR 里给出解释。**

本文数字未接入 CI 门禁 —— 基准测试在共享 CI 机器上噪声极大，
拿它当红绿灯会制造大量假警报。建议在固定物理机上定期跑并归档趋势。

---

## 附：本报告的诚实声明

- 数字来自**一台 ARM 笔记本的进程内基准**，非生产环境实测
- 广播类指标为**内存连接**，不含真实 socket 与网络栈开销
- 未做长时间稳定性测试（内存增长、连接泄漏需另行验证）
- 端到端压测（第五节）**本报告未执行**，只给出复现方法
