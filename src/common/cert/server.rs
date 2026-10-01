//! 服务器证书工具
//!
//! 提供便捷的服务器证书加载和客户端配置创建功能
//! 证书必须通过 scripts/generate_cert.rs 生成

use crate::common::cert::pinning::{PinnedServerCertVerifier, TlsPinningPolicy};
use crate::common::cert::{
    load_cert_der_from_file, load_key_der_from_file, pem_cert_to_der, pem_key_to_der,
};
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

/// Deployment-provided server certificate and private key (PEM, or base64 of
/// PEM like the other injected keys). When both are set they replace the files
/// under `certs/`: a QUIC listener behind UDP passthrough must present the
/// installation's certificate, not the auto-generated localhost one.
pub const SERVER_CERT_ENV: &str = "FLARE_QUIC_TLS_CERT";
pub const SERVER_KEY_ENV: &str = "FLARE_QUIC_TLS_KEY";

fn injected_pem(name: &str, value: Option<&str>) -> Result<Option<Vec<u8>>> {
    use base64::Engine;
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    if value.starts_with("-----BEGIN ") {
        return Ok(Some(value.as_bytes().to_vec()));
    }
    base64::engine::general_purpose::STANDARD
        .decode(value)
        .map(Some)
        .map_err(|_| FlareError::protocol_error(format!("{name} is neither PEM nor base64 PEM")))
}

/// DER certificate and key from the injected values; `None` when neither is set.
fn injected_server_identity(
    cert: Option<&str>,
    key: Option<&str>,
) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
    match (
        injected_pem(SERVER_CERT_ENV, cert)?,
        injected_pem(SERVER_KEY_ENV, key)?,
    ) {
        (None, None) => Ok(None),
        (Some(cert), Some(key)) => Ok(Some((pem_cert_to_der(&cert)?, pem_key_to_der(&key)?))),
        _ => Err(FlareError::protocol_error(format!(
            "{SERVER_CERT_ENV} and {SERVER_KEY_ENV} must be set together"
        ))),
    }
}

/// Load the server identity once: injected values first, then `certs/` files
/// (generated for local development when absent).
fn load_server_identity() -> Result<()> {
    let mut cert_cache = CERT_DER_CACHE.lock().unwrap();
    let mut key_cache = KEY_DER_CACHE.lock().unwrap();
    if cert_cache.is_some() && key_cache.is_some() {
        return Ok(());
    }
    let injected = injected_server_identity(
        std::env::var(SERVER_CERT_ENV).ok().as_deref(),
        std::env::var(SERVER_KEY_ENV).ok().as_deref(),
    )?;
    let (cert_der, key_der) = match injected {
        Some(identity) => {
            tracing::info!("server certificate loaded from {SERVER_CERT_ENV}/{SERVER_KEY_ENV}");
            identity
        }
        None => {
            ensure_certificates_exist()?;
            (
                load_cert_der_from_file(default_cert_path())?,
                load_key_der_from_file(default_key_path())?,
            )
        }
    };
    *cert_cache = Some(cert_der);
    *key_cache = Some(key_der);
    Ok(())
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

        tracing::info!("自动生成证书: certs/server.crt 和 certs/server.key");
    }

    Ok(())
}

/// 获取服务器证书的 DER 格式字节数组
/// 从默认路径 certs/server.crt 加载证书（带缓存）
/// 如果证书不存在，会自动生成
pub fn get_server_cert_der() -> Result<Vec<u8>> {
    load_server_identity()?;
    Ok(CERT_DER_CACHE.lock().unwrap().clone().unwrap_or_default())
}

/// 获取服务器私钥的 DER 格式字节数组
/// 从默认路径 certs/server.key 加载私钥（带缓存）
/// 如果私钥不存在，会自动生成
pub fn get_server_key_der() -> Result<Vec<u8>> {
    load_server_identity()?;
    Ok(KEY_DER_CACHE.lock().unwrap().clone().unwrap_or_default())
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

#[cfg(test)]
mod injected_identity_tests {
    use super::*;
    use base64::Engine;

    const CERT_PEM: &str = include_str!("testdata/cert.pem");
    const KEY_PEM: &str = include_str!("testdata/k8.pem");

    #[test]
    fn injected_pem_or_base64_pem_replaces_the_files() {
        let from_pem = injected_server_identity(Some(CERT_PEM), Some(KEY_PEM))
            .unwrap()
            .expect("identity");
        assert_eq!(from_pem.0, pem_cert_to_der(CERT_PEM.as_bytes()).unwrap());
        assert_eq!(from_pem.1, pem_key_to_der(KEY_PEM.as_bytes()).unwrap());

        let b64 = base64::engine::general_purpose::STANDARD;
        let from_b64 =
            injected_server_identity(Some(&b64.encode(CERT_PEM)), Some(&b64.encode(KEY_PEM)))
                .unwrap()
                .expect("identity");
        assert_eq!(from_b64, from_pem);
    }

    #[test]
    fn absent_values_fall_back_and_half_or_bad_values_fail() {
        assert!(injected_server_identity(None, Some(" ")).unwrap().is_none());
        assert!(injected_server_identity(Some(CERT_PEM), None).is_err());
        assert!(injected_server_identity(None, Some(KEY_PEM)).is_err());
        assert!(injected_server_identity(Some("not base64!"), Some(KEY_PEM)).is_err());
        assert!(injected_server_identity(Some(KEY_PEM), Some(KEY_PEM)).is_err());
    }
}
