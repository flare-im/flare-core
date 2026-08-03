//! 对解压路径做覆盖率引导的模糊测试。
//!
//! 解压是解析链里唯一能「小输入 → 大输出」的一环，也就是解压炸弹的入口。
//! gzip 实现里有 16MB 输出上限（`MAX_GZIP_DECOMPRESSED_LEN`），这里断言无论
//! 喂什么字节都绕不过它 —— 绕过就意味着一个几 KB 的恶意包能把服务端打到 OOM。
#![no_main]

use flare_core::common::compression::{CompressionAlgorithm, CompressionUtil};
use libfuzzer_sys::fuzz_target;

const MAX_DECOMPRESSED: usize = 16 * 1024 * 1024;

fuzz_target!(|data: &[u8]| {
    if let Ok(out) = CompressionUtil::decompress(data, CompressionAlgorithm::Gzip) {
        assert!(
            out.len() <= MAX_DECOMPRESSED,
            "解压输出 {} 字节，超过 {MAX_DECOMPRESSED} 上限 —— 解压炸弹防护被绕过",
            out.len()
        );
    }
});
