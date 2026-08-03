//! 对 `MessageParser::parse` 做覆盖率引导的模糊测试。
//!
//! 这是不可信输入进入系统的第一道门：网络原始字节 → 解密 → 解压 → 反序列化。
//! 不变量只有一条 —— 不论输入什么字节，都不允许 panic。
//!
//! 与 `tests/parse_untrusted_input.rs` 的属性测试互补：属性测试在 stable 上跑、
//! 进 CI、每个 PR 都过；这里靠 libFuzzer 的覆盖率反馈，能自己「学」出能穿过
//! 格式检查的输入，走到属性测试的随机字节到不了的深处。
#![no_main]

use flare_core::common::compression::CompressionAlgorithm;
use flare_core::common::encryption::EncryptionAlgorithm;
use flare_core::common::message::MessageParser;
use flare_core::common::protocol::SerializationFormat;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let parser = MessageParser::new(
        SerializationFormat::Protobuf,
        CompressionAlgorithm::None,
        EncryptionAlgorithm::None,
    );

    // 容错模式：失败时会逐个尝试其它序列化格式，分支最多。
    let _ = parser.parse(data);

    // 严格模式：应当更早失败，但同样不允许 panic。
    let _ = parser.parse_with_fallback(data, false);
});
