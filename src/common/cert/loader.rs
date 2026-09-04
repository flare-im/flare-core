//! 证书加载器
//!
//! 从文件或字符串加载证书和私钥

use crate::common::error::FlareError;
use crate::common::error::Result;
use std::fs;
use std::path::Path;

/// 从文件加载 DER 格式的证书
pub fn load_cert_der_from_file<P: AsRef<Path>>(path: P) -> Result<Vec<u8>> {
    // 文件既可以是 DER，也可以是 PEM（自动识别 `-----BEGIN`）：运维手里的证书几乎都是 PEM，
    // 此前只认 DER，挂上 PEM 会在握手时报一个看不出原因的证书错误。
    let bytes = fs::read(path.as_ref())
        .map_err(|e| FlareError::protocol_error(format!("Failed to read certificate file: {}", e)))?;
    cert_bytes_to_der(bytes)
}

/// 证书字节 → DER：PEM 文本自动转换，其余按 DER 原样返回。
pub fn cert_bytes_to_der(bytes: Vec<u8>) -> Result<Vec<u8>> {
    if looks_like_pem(&bytes) {
        return crate::common::cert::converter::pem_cert_to_der(&bytes);
    }
    Ok(bytes)
}

fn looks_like_pem(bytes: &[u8]) -> bool {
    let head = &bytes[..bytes.len().min(64)];
    String::from_utf8_lossy(head).trim_start().starts_with("-----BEGIN")
}

/// 从文件加载 DER 格式的私钥
pub fn load_key_der_from_file<P: AsRef<Path>>(path: P) -> Result<Vec<u8>> {
    let bytes = fs::read(path.as_ref())
        .map_err(|e| FlareError::protocol_error(format!("Failed to read private key file: {}", e)))?;
    if looks_like_pem(&bytes) {
        return crate::common::cert::converter::pem_key_to_der(&bytes);
    }
    Ok(bytes)
}

/// 从字符串加载 DER 格式的证书（Base64 编码）
pub fn load_cert_der_from_string(cert_str: &str) -> Result<Vec<u8>> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(cert_str.trim())
        .map_err(|e| {
            FlareError::protocol_error(format!("Failed to decode base64 certificate: {}", e))
        })
}

/// 从字符串加载 DER 格式的私钥（Base64 编码）
pub fn load_key_der_from_string(key_str: &str) -> Result<Vec<u8>> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(key_str.trim())
        .map_err(|e| {
            FlareError::protocol_error(format!("Failed to decode base64 private key: {}", e))
        })
}

/// 从文件加载 PEM 格式的证书
pub fn load_cert_pem_from_file<P: AsRef<Path>>(path: P) -> Result<Vec<u8>> {
    let pem_data = fs::read(path.as_ref()).map_err(|e| {
        FlareError::protocol_error(format!("Failed to read PEM certificate file: {}", e))
    })?;

    crate::common::cert::converter::pem_cert_to_der(&pem_data).map_err(|e| {
        FlareError::protocol_error(format!("Failed to convert PEM certificate to DER: {}", e))
    })
}

/// 从文件加载 PEM 格式的私钥
pub fn load_key_pem_from_file<P: AsRef<Path>>(path: P) -> Result<Vec<u8>> {
    let pem_data = fs::read(path.as_ref()).map_err(|e| {
        FlareError::protocol_error(format!("Failed to read PEM private key file: {}", e))
    })?;

    crate::common::cert::converter::pem_key_to_der(&pem_data).map_err(|e| {
        FlareError::protocol_error(format!("Failed to convert PEM private key to DER: {}", e))
    })
}

/// 从字符串加载 PEM 格式的证书
pub fn load_cert_pem_from_string(pem_str: &str) -> Result<Vec<u8>> {
    crate::common::cert::converter::pem_cert_to_der(pem_str.as_bytes()).map_err(|e| {
        FlareError::protocol_error(format!("Failed to convert PEM certificate to DER: {}", e))
    })
}

/// 从字符串加载 PEM 格式的私钥
pub fn load_key_pem_from_string(pem_str: &str) -> Result<Vec<u8>> {
    crate::common::cert::converter::pem_key_to_der(pem_str.as_bytes()).map_err(|e| {
        FlareError::protocol_error(format!("Failed to convert PEM private key to DER: {}", e))
    })
}

#[cfg(test)]
mod pem_autodetect_tests {
    use super::*;

    #[test]
    fn pem_certificate_and_key_files_are_converted_to_der() {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let der = cert.cert.der().to_vec();
        let dir = std::env::temp_dir().join(format!("flare-pem-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let pem_path = dir.join("cert.pem");
        let der_path = dir.join("cert.der");
        fs::write(&pem_path, cert.cert.pem().as_bytes()).unwrap();
        fs::write(&der_path, &der).unwrap();
        assert_eq!(load_cert_der_from_file(&pem_path).unwrap(), der, "PEM 文件必须解成同一份 DER");
        assert_eq!(load_cert_der_from_file(&der_path).unwrap(), der, "DER 文件原样返回");
        let key_path = dir.join("key.pem");
        fs::write(&key_path, cert.signing_key.serialize_pem().as_bytes()).unwrap();
        assert_eq!(load_key_der_from_file(&key_path).unwrap(), cert.signing_key.serialize_der());
        let _ = fs::remove_dir_all(&dir);
    }
}
