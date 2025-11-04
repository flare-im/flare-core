//! 证书加载器
//! 
//! 从文件或字符串加载证书和私钥

use std::path::Path;
use std::fs;
use crate::common::error::Result;
use crate::common::error::FlareError;

/// 从文件加载 DER 格式的证书
pub fn load_cert_der_from_file<P: AsRef<Path>>(path: P) -> Result<Vec<u8>> {
    fs::read(path.as_ref())
        .map_err(|e| FlareError::protocol_error(
            format!("Failed to read certificate file: {}", e)
        ))
}

/// 从文件加载 DER 格式的私钥
pub fn load_key_der_from_file<P: AsRef<Path>>(path: P) -> Result<Vec<u8>> {
    fs::read(path.as_ref())
        .map_err(|e| FlareError::protocol_error(
            format!("Failed to read private key file: {}", e)
        ))
}

/// 从字符串加载 DER 格式的证书（Base64 编码）
pub fn load_cert_der_from_string(cert_str: &str) -> Result<Vec<u8>> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(cert_str.trim())
        .map_err(|e| FlareError::protocol_error(
            format!("Failed to decode base64 certificate: {}", e)
        ))
}

/// 从字符串加载 DER 格式的私钥（Base64 编码）
pub fn load_key_der_from_string(key_str: &str) -> Result<Vec<u8>> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(key_str.trim())
        .map_err(|e| FlareError::protocol_error(
            format!("Failed to decode base64 private key: {}", e)
        ))
}

/// 从文件加载 PEM 格式的证书
pub fn load_cert_pem_from_file<P: AsRef<Path>>(path: P) -> Result<Vec<u8>> {
    let pem_data = fs::read(path.as_ref())
        .map_err(|e| FlareError::protocol_error(
            format!("Failed to read PEM certificate file: {}", e)
        ))?;
    
    crate::common::cert::converter::pem_cert_to_der(&pem_data)
        .map_err(|e| FlareError::protocol_error(
            format!("Failed to convert PEM certificate to DER: {}", e)
        ))
}

/// 从文件加载 PEM 格式的私钥
pub fn load_key_pem_from_file<P: AsRef<Path>>(path: P) -> Result<Vec<u8>> {
    let pem_data = fs::read(path.as_ref())
        .map_err(|e| FlareError::protocol_error(
            format!("Failed to read PEM private key file: {}", e)
        ))?;
    
    crate::common::cert::converter::pem_key_to_der(&pem_data)
        .map_err(|e| FlareError::protocol_error(
            format!("Failed to convert PEM private key to DER: {}", e)
        ))
}

/// 从字符串加载 PEM 格式的证书
pub fn load_cert_pem_from_string(pem_str: &str) -> Result<Vec<u8>> {
    crate::common::cert::converter::pem_cert_to_der(pem_str.as_bytes())
        .map_err(|e| FlareError::protocol_error(
            format!("Failed to convert PEM certificate to DER: {}", e)
        ))
}

/// 从字符串加载 PEM 格式的私钥
pub fn load_key_pem_from_string(pem_str: &str) -> Result<Vec<u8>> {
    crate::common::cert::converter::pem_key_to_der(pem_str.as_bytes())
        .map_err(|e| FlareError::protocol_error(
            format!("Failed to convert PEM private key to DER: {}", e)
        ))
}

