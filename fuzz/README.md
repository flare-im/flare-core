# flare-core 模糊测试

覆盖不可信输入的解析路径 —— 网络原始字节进入系统的第一道门。

## ⚠️ 当前状态

**这些 target 尚未实际跑过。** 它们的代码是照 cargo-fuzz 标准布局写的，但
`cargo fuzz` 需要 nightly 工具链（依赖 `-Z sanitizer`），落地时的开发机上
只有 stable，因此无法验证。**首次运行前请当作未验证代码对待**，编译不过是
预期内的，按报错修即可。

已经实际跑过并全绿的是 stable 上的属性测试：

```bash
cargo test --test parse_untrusted_input
```

那套用例做了 40 万次解析调用（8 条 × 50000 例）零 panic，覆盖随机字节、
合法帧变异、变异+截断、粘包尾随垃圾。见
[`../tests/parse_untrusted_input.rs`](../tests/parse_untrusted_input.rs)。

## 两者的关系

属性测试和模糊测试解决的不是同一个问题，缺一不可：

| | 属性测试（已跑） | 模糊测试（待跑） |
|---|---|---|
| 工具链 | stable | **nightly** |
| 进 CI | 是，每个 PR | 否，长跑任务 |
| 输入生成 | 随机 + 定点变异 | **覆盖率反馈引导** |
| 强项 | 快、可重放、能进门禁 | 能自己「学」出穿过格式检查的输入 |

纯随机字节几乎不可能凑成合法帧头，多半在第一道检查就被拒。属性测试靠「以合法帧
为种子做变异」部分绕开了这个问题，但 libFuzzer 的覆盖率反馈能走得更深。

## 运行

```bash
rustup toolchain install nightly
cargo install cargo-fuzz
cargo +nightly fuzz run parse_frame
```

跑一段固定时长（CI 夜跑用）：

```bash
cargo +nightly fuzz run parse_frame -- -max_total_time=300
```

## Target

- **`parse_frame`** —— `MessageParser::parse` 全路径（解密 → 解压 → 反序列化），
  容错与严格两种模式都过一遍。不变量：任何字节都不允许 panic。

- **`decompress`** —— gzip 解压。不变量：输出不得超过 16MB 上限，否则解压炸弹
  防护被绕过，几 KB 的恶意包就能把服务端打到 OOM。

## 为什么 panic 是安全问题

服务端解析不可信输入时 panic，等于该连接的处理任务被撕掉。如果 panic 发生在持有
锁的临界区，还会毒化（poison）那把锁，影响面就从单个连接扩大到整个进程 —— 一个
畸形包放倒一台服务器。所以「解析不可信输入时不 panic」不是代码洁癖，是可用性边界。

## 发现 crash 之后

crash 输入会落在 `fuzz/artifacts/<target>/`。重放：

```bash
cargo +nightly fuzz run parse_frame fuzz/artifacts/parse_frame/crash-<hash>
```

修完之后，**把那个输入固化成 `tests/parse_untrusted_input.rs` 里的一条普通用例**，
这样它就进了 CI 门禁，不会再退化回去。
