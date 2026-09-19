# 真在线压测工具包

模拟大规模**真实认证在线用户**（真 token + 真 WS 认证连接 + 心跳保活），用于容量压测。
裸 TCP / 未认证 WS 会被网关秒级回收，**必须真认证**才算在线。

## 组成

| 文件 | 作用 |
|---|---|
| `../loadconns.rs` | cargo example：读 token 文件，建 N 个已认证 WS 连接并保持（SDK 自带心跳） |
| `mint_tokens.sh` | 批量签 token（走 api-gateway 服务端签发端点，与 web app SDK 托管一致） |
| `safemon.sh` | 内存安全闸：可用内存低于阈值自动杀 loadconns，杜绝 thrash 压死机器 |

## 用法（在服务器本机跑，别从笔记本跑）

```bash
# 1. 签 25000 个 token
bash mint_tokens.sh 25000 /root/tokens.txt

# 2. 起内存安全闸（必挂）
FLOOR=2800 DUR=1800 bash safemon.sh &

# 3. 建 25000 真在线连接（多目标 IP 绕单源 IP ~28k 临时端口上限）
#    关键：ulimit -n 提高 fd；勿用 setsid（会把 fd 上限重置回 1024）
bash -c 'ulimit -n 300000; \
  TOKENS=/root/tokens.txt \
  HOSTS=127.0.0.1,127.0.0.2,127.0.0.3,127.0.0.4,127.0.0.5,127.0.0.6 PORT=60051 \
  N=25000 DURATION=1800 CONNECT_CONC=500 \
  target/release/examples/loadconns'
```

## 实测结论（8核/15.6G 单机，2026-09-08）

- **稳定舒适区 ~2.5 万真在线**（留 4.3G 内存余量）；30 分钟稳跑 56.5 万消息 0 错误。
- **安全边缘 ~2.8 万**；**32k 能建立但内存降到 ~2.7G（安全线边缘，非稳定）**。
- 硬限：①临时端口 ~28k/源IP（多目标 IP 绕过）②网关内存 ~130KB/连接。
- 稳定持 32k+ 需 24G+ 内存机。

## 坑

- **setsid 重置 fd soft limit 到 1024** → 连接卡 ~1015。用 `bash -c "ulimit -n 300000; ..."`，勿套 setsid。
- **单源 IP 临时端口 ~28k 上限** → 用 `HOSTS` 轮转多个目标 IP（网关绑 0.0.0.0，127.0.0.1-6 都到它，各独立端口空间）。
- **CONNECT 帧必须带 device_id**，否则网关 `INVALID_PARAMETER device_id is required` 秒踢。
- **pkill -f 自匹配**：同命令内任何地方（含 echo 文字/其他路径）出现 pattern 明文都会误杀 shell，用括号 `[l]oadconns` 且不出现明文。
