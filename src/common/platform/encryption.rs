//! AES-256-GCM encryption key resolution for Native / WASM.

use std::sync::{Arc, Mutex, OnceLock};

use crate::common::encryption::{Aes256GcmEncryptor, EncryptionUtil};
use crate::common::error::{FlareError, Result};

use super::env::optional_env;

/// Required length for AES-256-GCM keys.
pub const AES256_KEY_LEN: usize = 32;

static RUNTIME_ENCRYPTION_KEY: OnceLock<Mutex<Option<Vec<u8>>>> = OnceLock::new();

fn encryption_key_slot() -> &'static Mutex<Option<Vec<u8>>> {
    RUNTIME_ENCRYPTION_KEY.get_or_init(|| Mutex::new(None))
}

/// Set encryption key at runtime (WASM/JS 注入；Native 测试亦可用).
pub fn set_runtime_encryption_key(key: Vec<u8>) -> Result<()> {
    if key.len() != AES256_KEY_LEN {
        return Err(FlareError::protocol_error(format!(
            "encryption key must be exactly {AES256_KEY_LEN} bytes, got {}",
            key.len()
        )));
    }
    *encryption_key_slot()
        .lock()
        .map_err(|_| FlareError::general_error("encryption key lock poisoned"))? = Some(key);
    Ok(())
}

/// Clear a previously injected runtime key.
pub fn clear_runtime_encryption_key() {
    if let Ok(mut slot) = encryption_key_slot().lock() {
        *slot = None;
    }
}

/// Whether a runtime key was injected via [`set_runtime_encryption_key`].
pub fn has_runtime_encryption_key() -> bool {
    encryption_key_slot()
        .lock()
        .ok()
        .and_then(|slot| slot.as_ref().map(|key| !key.is_empty()))
        .unwrap_or(false)
}

/// Parse UTF-8 key string (must be exactly 32 bytes).
pub fn parse_encryption_key_utf8(key: &str) -> Result<Vec<u8>> {
    let bytes = key.as_bytes();
    if bytes.len() != AES256_KEY_LEN {
        return Err(FlareError::protocol_error(format!(
            "encryption key string must be exactly {AES256_KEY_LEN} UTF-8 bytes, got {}",
            bytes.len()
        )));
    }
    Ok(bytes.to_vec())
}

/// Parse hex-encoded 32-byte key (64 hex chars).
pub fn parse_encryption_key_hex(hex: &str) -> Result<Vec<u8>> {
    let bytes = crate::common::utils::hex_to_bytes(hex).map_err(|error| {
        FlareError::protocol_error(format!("invalid encryption key hex: {error}"))
    })?;
    if bytes.len() != AES256_KEY_LEN {
        return Err(FlareError::protocol_error(format!(
            "encryption key hex must decode to {AES256_KEY_LEN} bytes, got {}",
            bytes.len()
        )));
    }
    Ok(bytes)
}

/// Resolve key bytes: runtime injection > `ENCRYPTION_KEY` env (Native) > demo default.
pub fn resolve_encryption_key_bytes(default_demo_key: Option<&[u8; AES256_KEY_LEN]>) -> Vec<u8> {
    if let Ok(slot) = encryption_key_slot().lock()
        && let Some(key) = slot.as_ref()
    {
        return key.clone();
    }
    if let Some(value) = optional_env("ENCRYPTION_KEY") {
        return value.into_bytes();
    }
    default_demo_key.map(|key| key.to_vec()).unwrap_or_default()
}

/// Register AES-256-GCM encryptor using [`resolve_encryption_key_bytes`].
pub fn register_aes256_encryption(default_demo_key: Option<&[u8; AES256_KEY_LEN]>) -> Result<()> {
    let key = resolve_encryption_key_bytes(default_demo_key);
    if key.len() != AES256_KEY_LEN {
        return Err(FlareError::protocol_error(format!(
            "encryption key must be exactly {AES256_KEY_LEN} bytes, got {} (set flare_set_encryption_key or ENCRYPTION_KEY)",
            key.len()
        )));
    }
    let encryptor = Aes256GcmEncryptor::new(&key)?;
    EncryptionUtil::register_custom(Arc::new(encryptor));
    Ok(())
}
