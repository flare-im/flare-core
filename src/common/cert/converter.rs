//! 证书转换器
//! 
//! 在不同格式之间转换证书和私钥

use crate::common::error::Result;
use crate::common::error::FlareError;

/// 将 PEM 格式的证书转换为 DER 格式
pub fn pem_cert_to_der(pem_data: &[u8]) -> Result<Vec<u8>> {
    use rustls_pemfile::certs;
    
    let mut reader = std::io::BufReader::new(pem_data);
    let mut certs_iter = certs(&mut reader);
    
    match certs_iter.next() {
        Some(Ok(cert)) => Ok(cert.to_vec()),
        Some(Err(e)) => Err(FlareError::protocol_error(
            format!("Failed to parse PEM certificate: {}", e)
        )),
        None => Err(FlareError::protocol_error(
            "No certificates found in PEM data".to_string()
        )),
    }
}

/// 将 PEM 格式的私钥转换为 DER 格式
pub fn pem_key_to_der(pem_data: &[u8]) -> Result<Vec<u8>> {
    use rustls_pemfile::{read_one, Item};
    
    let mut reader = std::io::BufReader::new(pem_data);
    
    loop {
        match read_one(&mut reader)
            .map_err(|e| FlareError::protocol_error(
                format!("Failed to parse PEM key: {}", e)
            ))? {
            Some(Item::Pkcs8Key(key)) => {
                return Ok(key.secret_pkcs8_der().to_vec());
            }
            Some(Item::Sec1Key(key)) => {
                // SEC1 格式的私钥
                return Ok(key.secret_sec1_der().to_vec());
            }
            Some(_) => continue, // 跳过其他类型的项目
            None => break,
        }
    }
    
    Err(FlareError::protocol_error(
        "No private key found in PEM data".to_string()
    ))
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
    format!("-----BEGIN CERTIFICATE-----\n{}\n-----END CERTIFICATE-----\n", formatted)
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
    format!("-----BEGIN PRIVATE KEY-----\n{}\n-----END PRIVATE KEY-----\n", formatted)
}

