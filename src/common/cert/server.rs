//! 服务器证书工具
//!
//! 提供便捷的服务器证书加载和客户端配置创建功能
//! 证书必须通过 scripts/generate_cert.rs 生成

use crate::common::cert::pinning::{PinnedServerCertVerifier, TlsPinningPolicy};
use crate::common::cert::{load_cert_der_from_file, load_key_der_from_file};
use crate::common::config_types::TlsConfig;
use crate::common::error::FlareError;
use crate::common::error::Result;
use lazy_static::lazy_static;
use rustls::ClientConfig;
use rustls::pki_types::CertificateDer;
use std::path::Path;
use std::sync::{Arc, Mutex};

lazy_static! {
    // 全局静态证书和私钥 DER 缓存，避免每次连接都重新加载
    static ref CERT_DER_CACHE: Mutex<Option<Vec<u8>>> = Mutex::new(None);
    static ref KEY_DER_CACHE: Mutex<Option<Vec<u8>>> = Mutex::new(None);
}

/// 默认证书文件路径
fn default_cert_path() -> &'static Path {
    Path::new("certs/server.crt")
}

/// 默认私钥文件路径
fn default_key_path() -> &'static Path {
    Path::new("certs/server.key")
}

/// 自动生成证书（如果不存在）
fn ensure_certificates_exist() -> Result<()> {
    use std::fs;

    let cert_path = default_cert_path();
    let key_path = default_key_path();

    // 如果证书文件不存在，自动生成
    if !cert_path.exists() || !key_path.exists() {
        // 确保证书目录存在
        if let Some(parent) = cert_path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                FlareError::protocol_error(format!("Failed to create certs directory: {}", e))
            })?;
        }

        // 生成证书
        let subject_alt_names = vec![
            "localhost".to_string(),
            "127.0.0.1".to_string(),
            "::1".to_string(),
        ];

        let certified_key = rcgen::generate_simple_self_signed(subject_alt_names).map_err(|e| {
            FlareError::protocol_error(format!("Failed to generate certificate: {}", e))
        })?;

        let cert_der = certified_key.cert.der().to_vec();
        let key_der = certified_key.signing_key.serialize_der();

        // 保存证书
        fs::write(cert_path, &cert_der).map_err(|e| {
            FlareError::protocol_error(format!("Failed to write certificate file: {}", e))
        })?;

        // 保存私钥
        fs::write(key_path, &key_der).map_err(|e| {
            FlareError::protocol_error(format!("Failed to write private key file: {}", e))
        })?;

        tracing::info!("✅ 自动生成证书: certs/server.crt 和 certs/server.key");
    }

    Ok(())
}

/// 获取服务器证书的 DER 格式字节数组
/// 从默认路径 certs/server.crt 加载证书（带缓存）
/// 如果证书不存在，会自动生成
pub fn get_server_cert_der() -> Result<Vec<u8>> {
    // 确保证书存在
    ensure_certificates_exist()?;

    let mut cache = CERT_DER_CACHE.lock().unwrap();

    if let Some(ref cert_der) = *cache {
        Ok(cert_der.clone())
    } else {
        // 从文件加载证书
        let cert_der = load_cert_der_from_file(default_cert_path())?;

        // 缓存证书 DER
        *cache = Some(cert_der.clone());
        Ok(cert_der)
    }
}

/// 获取服务器私钥的 DER 格式字节数组
/// 从默认路径 certs/server.key 加载私钥（带缓存）
/// 如果私钥不存在，会自动生成
pub fn get_server_key_der() -> Result<Vec<u8>> {
    // 确保证书存在
    ensure_certificates_exist()?;

    let mut cache = KEY_DER_CACHE.lock().unwrap();

    if let Some(ref key_der) = *cache {
        Ok(key_der.clone())
    } else {
        // 从文件加载私钥
        let key_der = load_key_der_from_file(default_key_path())?;

        // 缓存私钥 DER
        *cache = Some(key_der.clone());
        Ok(key_der)
    }
}

/// 初始化 rustls CryptoProvider（如果尚未初始化）
/// 使用 Once 确保只初始化一次
fn ensure_crypto_provider() {
    use std::sync::Once;
    static INIT: Once = Once::new();

    INIT.call_once(|| {
        // 使用 ring 作为默认的 CryptoProvider（因为 Cargo.toml 中启用了 ring 特性）
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// 创建 rustls 客户端配置
///
/// 该配置会加载系统 WebPKI 根证书；如果当前工作目录下已经存在
/// `certs/server.crt`，也会将其作为本地开发证书加入信任根。
///
/// 客户端侧不会自动生成默认证书，避免 native app 在自己的工作目录里
/// 生成一份与服务端不匹配的证书并误用为信任根。
pub fn create_client_config() -> Result<ClientConfig> {
    create_client_config_with_tls(&TlsConfig::none())
}

/// 从指定证书路径创建 rustls 客户端配置
pub fn create_client_config_with_cert<P: AsRef<Path>>(cert_path: P) -> Result<ClientConfig> {
    // 确保 CryptoProvider 已初始化
    ensure_crypto_provider();

    let cert_der = load_cert_der_from_file(cert_path)?;
    let cert = CertificateDer::from(cert_der);

    let mut root_store = rustls::RootCertStore::empty();
    root_store.add(cert.clone()).map_err(|e| {
        FlareError::protocol_error(format!("Failed to add certificate to root store: {}", e))
    })?;

    Ok(ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth())
}

/// 从 [`TlsConfig`] 创建 rustls 客户端配置，并在配置 pin 时安装 SPKI-aware verifier。
pub fn create_client_config_with_tls(tls: &TlsConfig) -> Result<ClientConfig> {
    ensure_crypto_provider();

    let root_store = build_client_root_store(tls)?;
    if tls.has_certificate_pins() {
        let verifier = rustls::client::WebPkiServerVerifier::builder(Arc::new(root_store))
            .build()
            .map_err(|e| {
                FlareError::protocol_error(format!("Failed to build webpki server verifier: {e}"))
            })?;
        let policy = TlsPinningPolicy::from_tls_config(tls)?;
        let verifier = Arc::new(PinnedServerCertVerifier::new(verifier, policy));
        return Ok(ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_no_client_auth());
    }

    Ok(ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth())
}

fn build_client_root_store(tls: &TlsConfig) -> Result<rustls::RootCertStore> {
    let mut root_store = rustls::RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    if let Some(cert_data) = tls.ca_cert_data.clone() {
        add_root_certificate(&mut root_store, cert_data)?;
    } else if let Some(path) = tls.ca_cert_path.as_ref() {
        add_root_certificate(&mut root_store, load_cert_der_from_file(path)?)?;
    } else if let Some(cert_der) = load_existing_default_server_cert_der()? {
        add_root_certificate(&mut root_store, cert_der)?;
    }

    Ok(root_store)
}

fn load_existing_default_server_cert_der() -> Result<Option<Vec<u8>>> {
    let cert_path = default_cert_path();
    if !cert_path.exists() {
        return Ok(None);
    }
    load_cert_der_from_file(cert_path).map(Some)
}

fn add_root_certificate(root_store: &mut rustls::RootCertStore, cert_der: Vec<u8>) -> Result<()> {
    let cert = CertificateDer::from(cert_der);
    root_store.add(cert).map_err(|e| {
        FlareError::protocol_error(format!("Failed to add certificate to root store: {}", e))
    })?;
    Ok(())
}
