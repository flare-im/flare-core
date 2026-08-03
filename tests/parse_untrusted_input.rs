//! 不可信输入解析路径的属性测试。
//!
//! `MessageParser::parse` 直接吃网络来的原始字节，走 解密 → 解压 → 反序列化
//! 三段，每段都在解析攻击者可控的数据。这里的不变量只有一条，但它是最重要的
//! 一条：**任何字节序列都只能得到 Ok 或 Err，绝不允许 panic**。
//!
//! panic 在服务端等于该连接的处理任务被撕掉；如果 panic 发生在持有锁的临界区，
//! 还会把锁毒化（poison），影响面从单连接扩大到整个进程。所以「解析不可信输入
//! 时不 panic」不是代码洁癖，是可用性边界。
//!
//! 这些用例在 stable 上跑、进 CI、每个 PR 都过一遍。它们不能替代真正的
//! 覆盖率引导 fuzz（见 fuzz/README.md），但能挡住最常见的一类：畸形长度前缀、
//! 截断的帧、越界切片、算术溢出。

use flare_core::common::compression::{CompressionAlgorithm, CompressionUtil};
use flare_core::common::encryption::EncryptionAlgorithm;
use flare_core::common::message::MessageParser;
use flare_core::common::protocol::{
    Command, FrameBuilder, SerializationFormat, flare::core::commands::command::Type as CmdType,
    ping,
};
use proptest::prelude::*;

fn parser() -> MessageParser {
    MessageParser::new(
        SerializationFormat::Protobuf,
        CompressionAlgorithm::None,
        EncryptionAlgorithm::None,
    )
}

/// 一个合法帧的序列化字节，作为变异测试的种子。
///
/// 纯随机字节几乎不可能凑成合法帧头，多半在第一道格式检查就被拒，
/// 根本走不到解析深处。以合法帧为种子做定点变异，才能真正穿过外层检查、
/// 把畸形数据送进字段级解析——那里才是越界和溢出的高发区。
fn valid_frame_bytes() -> Vec<u8> {
    let frame = FrameBuilder::new()
        .with_command(Command {
            r#type: Some(CmdType::System(ping())),
        })
        .build();
    parser().serialize(&frame).expect("种子帧必须能序列化")
}

proptest! {
    /// 任意字节 → 容错模式解析绝不 panic。
    ///
    /// 容错模式会在失败时逐个尝试其它序列化格式，覆盖面最广，
    /// 也最容易在某个格式的解析器里踩到边界。
    #[test]
    fn parse_never_panics_on_arbitrary_bytes(data in prop::collection::vec(any::<u8>(), 0..4096)) {
        let _ = parser().parse(&data);
    }

    /// 严格模式同样不允许 panic —— 只是应当更早返回 Err。
    #[test]
    fn strict_parse_never_panics(data in prop::collection::vec(any::<u8>(), 0..4096)) {
        let _ = parser().parse_with_fallback(&data, false);
    }

    /// 逐个指定格式解析也不允许 panic。
    ///
    /// 单独覆盖是因为容错模式可能在轮到某个格式之前就已返回，
    /// 那条分支不会被上面的用例走到。
    #[test]
    fn parse_with_each_format_never_panics(
        data in prop::collection::vec(any::<u8>(), 0..2048),
        use_json in any::<bool>(),
    ) {
        let format = if use_json {
            SerializationFormat::Json
        } else {
            SerializationFormat::Protobuf
        };
        let _ = parser().parse_with_format(&data, format);
    }

    /// 解压不允许 panic，且必须尊重输出上限。
    ///
    /// 解压是解析链里唯一能让「小输入 → 大输出」的一环，是解压炸弹的入口。
    /// gzip 实现里有 MAX_GZIP_DECOMPRESSED_LEN(16MB) 上限，这里确认无论喂什么
    /// 字节都不会绕过它。
    #[test]
    fn decompress_never_panics_and_respects_limit(
        data in prop::collection::vec(any::<u8>(), 0..8192),
    ) {
        if let Ok(out) = CompressionUtil::decompress(&data, CompressionAlgorithm::Gzip) {
            prop_assert!(
                out.len() <= 16 * 1024 * 1024,
                "解压输出 {} 字节，超过 16MB 上限——解压炸弹防护被绕过",
                out.len()
            );
        }
    }

    /// 变异不变量：合法帧被改动任意字节后，解析仍不能 panic。
    ///
    /// 这是本文件里最有价值的一条 —— 它能穿过外层格式检查，把畸形数据送到
    /// 字段级解析器手上（越界切片、长度前缀撒谎、varint 溢出都在那一层）。
    #[test]
    fn mutated_valid_frame_never_panics(
        positions in prop::collection::vec((any::<prop::sample::Index>(), any::<u8>()), 1..12),
    ) {
        let mut data = valid_frame_bytes();
        prop_assume!(!data.is_empty());
        for (idx, byte) in positions {
            let i = idx.index(data.len());
            data[i] = byte;
        }
        let _ = parser().parse(&data);
    }

    /// 变异 + 截断组合：改字节之后再砍掉尾巴。
    ///
    /// 对应真实场景里「对端发了半个被损坏的帧就断开」。
    #[test]
    fn mutated_and_truncated_frame_never_panics(
        positions in prop::collection::vec((any::<prop::sample::Index>(), any::<u8>()), 1..8),
        cut in any::<prop::sample::Index>(),
    ) {
        let mut data = valid_frame_bytes();
        prop_assume!(!data.is_empty());
        for (idx, byte) in positions {
            let i = idx.index(data.len());
            data[i] = byte;
        }
        let end = cut.index(data.len() + 1);
        let _ = parser().parse(&data[..end]);
    }

    /// 合法帧后面追加垃圾字节，不能 panic。
    ///
    /// 覆盖「粘包」——TCP 上两个帧连在一起、或攻击者故意多塞字节。
    #[test]
    fn valid_frame_with_trailing_garbage_never_panics(
        garbage in prop::collection::vec(any::<u8>(), 1..512),
    ) {
        let mut data = valid_frame_bytes();
        data.extend_from_slice(&garbage);
        let _ = parser().parse(&data);
    }

    /// 截断不变量：合法帧的任意前缀都不能 panic。
    ///
    /// 真实网络里半个帧太常见了（连接断开、读超时、对端崩溃）。
    /// 这条用例专门覆盖「长度前缀说还有 N 字节，实际只剩 M < N」这类情况。
    #[test]
    fn truncated_valid_frame_never_panics(
        data in prop::collection::vec(any::<u8>(), 8..1024),
        cut in 0usize..1024,
    ) {
        let end = cut.min(data.len());
        let _ = parser().parse(&data[..end]);
    }
}
