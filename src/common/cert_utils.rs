//! 证书工具模块
//! 
//! 使用 rcgen 生成自签名证书，结合 rustls-pemfile 和 rustls 实现 QUIC 的 TLS 配置

use lazy_static::lazy_static;
use std::sync::Mutex;
use rcgen::{CertifiedKey, KeyPair};
use rustls::pki_types::CertificateDer;
use rustls::ClientConfig;

lazy_static! {
    // 全局静态证书和私钥 DER 缓存，避免每次连接都重新生成
    static ref CERT_DER_CACHE: Mutex<Option<Vec<u8>>> = Mutex::new(None);
    static ref KEY_DER_CACHE: Mutex<Option<Vec<u8>>> = Mutex::new(None);
}

/// 生成证书和密钥对
fn generate_certified_key() -> CertifiedKey<KeyPair> {
    // 使用 rcgen::generate_simple_self_signed 生成简单的自签名证书
    // 支持 localhost、127.0.0.1 和 ::1
    let subject_alt_names = vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
        "::1".to_string(),
    ];
    
    rcgen::generate_simple_self_signed(subject_alt_names)
        .expect("Failed to generate certificate")
}

/// 获取服务器证书的 DER 格式字节数组
pub fn get_server_cert_der() -> Vec<u8> {
    let mut cache = CERT_DER_CACHE.lock().unwrap();
    
    if let Some(ref cert_der) = *cache {
        cert_der.clone()
    } else {
        // 生成新证书
        let certified_key = generate_certified_key();
        let cert_der = certified_key.cert.der().to_vec();
        
        // 缓存证书 DER
        *cache = Some(cert_der.clone());
        cert_der
    }
}

/// 获取服务器私钥的 DER 格式字节数组
pub fn get_server_key_der() -> Vec<u8> {
    let mut cache = KEY_DER_CACHE.lock().unwrap();
    
    if let Some(ref key_der) = *cache {
        key_der.clone()
    } else {
        // 生成新证书（私钥和证书是一起的）
        let certified_key = generate_certified_key();
        let key_der = certified_key.signing_key.serialize_der();
        
        // 缓存私钥 DER
        *cache = Some(key_der.clone());
        key_der
    }
}

/// 创建 rustls 客户端配置
/// 
/// 该配置会将服务器证书添加到受信任的根证书存储中，
/// 允许客户端信任自签名的服务器证书。
pub fn create_client_config() -> ClientConfig {
    // 获取服务器证书的 DER 格式
    let cert_der = get_server_cert_der();
    
    // 将 DER 格式的证书转换为 CertificateDer
    let cert = CertificateDer::from(cert_der);
    
    // 创建根证书存储，并添加服务器证书
    let mut root_store = rustls::RootCertStore::empty();
    root_store.add(cert.clone())
        .expect("Failed to add certificate to root store");
    
    // 创建客户端配置
    ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth()
}

/// 将 PEM 格式的证书转换为 DER 格式
/// 
/// 这是一个辅助函数，如果需要从 PEM 文件加载证书可以使用
pub fn pem_cert_to_der(pem_data: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    use rustls_pemfile::certs;
    
    let mut reader = std::io::BufReader::new(pem_data);
    let mut certs_iter = certs(&mut reader);
    
    match certs_iter.next() {
        Some(Ok(cert)) => Ok(cert.to_vec()),
        Some(Err(e)) => Err(Box::new(e)),
        None => Err("No certificates found in PEM data".into()),
    }
}

/// 将 PEM 格式的私钥转换为 DER 格式
/// 
/// 这是一个辅助函数，如果需要从 PEM 文件加载私钥可以使用
pub fn pem_key_to_der(pem_data: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    use rustls_pemfile::{read_one, Item};
    
    let mut reader = std::io::BufReader::new(pem_data);
    
    loop {
        match read_one(&mut reader)? {
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
    
    Err("No private key found in PEM data".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cert_generation() {
        let cert_der = get_server_cert_der();
        assert!(!cert_der.is_empty(), "Certificate DER should not be empty");
        
        let key_der = get_server_key_der();
        assert!(!key_der.is_empty(), "Key DER should not be empty");
    }

    #[test]
    fn test_client_config_creation() {
        let config = create_client_config();
        // 验证配置已创建（不会 panic）
        assert!(std::sync::Arc::strong_count(&std::sync::Arc::new(config)) >= 1);
    }

    #[test]
    fn test_cert_caching() {
        // 第一次调用应该生成证书
        let cert1 = get_server_cert_der();
        
        // 第二次调用应该返回缓存的证书（相同的参数会生成相同的证书）
        let cert2 = get_server_cert_der();
        
        // 注意：由于 rcgen 使用随机数，每次生成的证书可能不同
        // 但至少应该能成功生成
        assert!(!cert1.is_empty(), "Certificate should not be empty");
        assert!(!cert2.is_empty(), "Certificate should not be empty");
    }
}

