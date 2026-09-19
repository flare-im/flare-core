//! 证书转换器
//!
//! 在不同格式之间转换证书和私钥

use crate::common::error::FlareError;
use crate::common::error::Result;

// PEM 解析用 rustls-pki-types 而非 rustls-pemfile：后者已停止维护
// （RUSTSEC-2025-0134，且无修复版本），其能力已并入 pki-types 本身。
// pki-types 本来就在依赖树里（rustls 的公共类型层），换过来是减依赖不是加依赖。
use rustls_pki_types::pem::PemObject;

/// 将 PEM 格式的证书转换为 DER 格式
pub fn pem_cert_to_der(pem_data: &[u8]) -> Result<Vec<u8>> {
    use rustls_pki_types::CertificateDer;

    match CertificateDer::pem_slice_iter(pem_data).next() {
        Some(Ok(cert)) => Ok(cert.to_vec()),
        Some(Err(e)) => Err(FlareError::protocol_error(format!(
            "Failed to parse PEM certificate: {}",
            e
        ))),
        None => Err(FlareError::protocol_error(
            "No certificates found in PEM data".to_string(),
        )),
    }
}

/// 将 PEM 格式的私钥转换为 DER 格式
///
/// 只接受 PKCS#8 与 SEC1，**刻意不接受 PKCS#1**——与换库之前的行为一致。
/// `PrivateKeyDer::from_pem_slice` 本身是收 PKCS#1 的，这里显式挡掉：
/// 调用方拿到的是裸 DER 字节、没有类型标记，悄悄多支持一种编码会让下游
/// 按错误的格式去解析。要放开须是明确决定，不该由换库顺带发生。
pub fn pem_key_to_der(pem_data: &[u8]) -> Result<Vec<u8>> {
    use rustls_pki_types::PrivateKeyDer;

    match PrivateKeyDer::from_pem_slice(pem_data) {
        Ok(PrivateKeyDer::Pkcs8(key)) => Ok(key.secret_pkcs8_der().to_vec()),
        Ok(PrivateKeyDer::Sec1(key)) => Ok(key.secret_sec1_der().to_vec()),
        Ok(_) => Err(FlareError::protocol_error(
            "Unsupported private key encoding in PEM data (expected PKCS#8 or SEC1)".to_string(),
        )),
        Err(e) => Err(FlareError::protocol_error(format!(
            "No private key found in PEM data: {}",
            e
        ))),
    }
}

/// 将 DER 格式的证书转换为 PEM 格式
pub fn der_cert_to_pem(der_data: &[u8]) -> String {
    use base64::Engine;
    let base64_cert = base64::engine::general_purpose::STANDARD.encode(der_data);
    // 将 base64 字符串按 64 字符一行格式化
    let formatted = base64_cert
        .as_bytes()
        .chunks(64)
        .map(|chunk| String::from_utf8_lossy(chunk))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "-----BEGIN CERTIFICATE-----\n{}\n-----END CERTIFICATE-----\n",
        formatted
    )
}

/// 将 DER 格式的私钥转换为 PEM 格式
pub fn der_key_to_pem(der_data: &[u8]) -> String {
    use base64::Engine;
    let base64_key = base64::engine::general_purpose::STANDARD.encode(der_data);
    // 将 base64 字符串按 64 字符一行格式化
    let formatted = base64_key
        .as_bytes()
        .chunks(64)
        .map(|chunk| String::from_utf8_lossy(chunk))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "-----BEGIN PRIVATE KEY-----\n{}\n-----END PRIVATE KEY-----\n",
        formatted
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // 固件由 openssl 生成：cert=自签 ed25519 证书，k8=PKCS#8，
    // sec1=EC prime256v1（SEC1），pkcs1=RSA traditional（PKCS#1）。
    const CERT_PEM: &[u8] = include_bytes!("testdata/cert.pem");
    const PKCS8_PEM: &[u8] = include_bytes!("testdata/k8.pem");
    const SEC1_PEM: &[u8] = include_bytes!("testdata/sec1.pem");
    const PKCS1_PEM: &[u8] = include_bytes!("testdata/pkcs1.pem");

    #[test]
    fn parses_pem_certificate_to_der() {
        let der = pem_cert_to_der(CERT_PEM).expect("应能解析自签证书");
        // DER 的 SEQUENCE 标签，确认拿到的是结构本身而不是 PEM 文本残留
        assert_eq!(der[0], 0x30, "证书 DER 应以 SEQUENCE 开头");
        assert!(der.len() > 100);
    }

    #[test]
    fn parses_pkcs8_and_sec1_keys() {
        for (name, pem) in [("PKCS#8", PKCS8_PEM), ("SEC1", SEC1_PEM)] {
            let der = pem_key_to_der(pem).unwrap_or_else(|e| panic!("{name} 应能解析: {e}"));
            assert_eq!(der[0], 0x30, "{name} 私钥 DER 应以 SEQUENCE 开头");
        }
    }

    // 这条锁的是「换库不得顺带扩大接受范围」：pki-types 本身收 PKCS#1，
    // 而调用方拿到的是无类型标记的裸 DER，多认一种编码会让下游按错格式解析。
    #[test]
    fn rejects_pkcs1_key_as_before() {
        let err = pem_key_to_der(PKCS1_PEM).expect_err("PKCS#1 应被拒绝");
        assert!(
            err.to_string().contains("PKCS#8 or SEC1"),
            "错误应说明期望的编码，实际: {err}"
        );
    }

    #[test]
    fn rejects_input_without_pem_blocks() {
        assert!(pem_cert_to_der(b"not a pem at all").is_err());
        assert!(pem_key_to_der(b"not a pem at all").is_err());
    }
}
