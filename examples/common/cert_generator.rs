//! 证书生成工具
//!
//! 用于生成QUIC通信所需的TLS证书

use std::fs;
use std::path::Path;
use rcgen::{generate_simple_self_signed, CertifiedKey};

/// 生成自签名证书
pub fn generate_certificates() -> Result<(), Box<dyn std::error::Error>> {
    // 创建证书目录
    let cert_dir = "certs";
    if !Path::new(cert_dir).exists() {
        fs::create_dir_all(cert_dir)?;
    }

    // 生成服务器证书
    let server_cert = generate_server_certificate()?;
    
    // 生成客户端证书
    let client_cert = generate_client_certificate()?;

    // 保存服务器证书文件
    fs::write(format!("{}/server.crt", cert_dir), server_cert.cert.pem())?;
    fs::write(format!("{}/server.key", cert_dir), server_cert.key_pair.serialize_pem())?;
    
    // 保存客户端证书文件
    fs::write(format!("{}/client.crt", cert_dir), client_cert.cert.pem())?;
    fs::write(format!("{}/client.key", cert_dir), client_cert.key_pair.serialize_pem())?;

    println!("证书已生成到 {} 目录", cert_dir);
    println!("- server.crt/server.key: 服务器证书和私钥");
    println!("- client.crt/client.key: 客户端证书和私钥");

    Ok(())
}

/// 生成服务器证书
fn generate_server_certificate() -> Result<CertifiedKey, Box<dyn std::error::Error>> {
    // 为服务器生成自签名证书，支持localhost和127.0.0.1
    let subject_alt_names = vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
    ];
    
    Ok(generate_simple_self_signed(subject_alt_names)?)
}

/// 生成客户端证书
fn generate_client_certificate() -> Result<CertifiedKey, Box<dyn std::error::Error>> {
    // 为客户端生成自签名证书
    let subject_alt_names = vec!["flare-core-client".to_string()];
    
    Ok(generate_simple_self_signed(subject_alt_names)?)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("开始生成QUIC通信证书...");
    generate_certificates()?;
    println!("证书生成完成！");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_certificates() {
        // 清理可能存在的旧证书
        let _ = fs::remove_dir_all("certs");
        
        // 生成证书
        assert!(generate_certificates().is_ok());
        
        // 验证证书文件是否存在
        assert!(Path::new("certs/server.crt").exists());
        assert!(Path::new("certs/server.key").exists());
        assert!(Path::new("certs/client.crt").exists());
        assert!(Path::new("certs/client.key").exists());
    }
}