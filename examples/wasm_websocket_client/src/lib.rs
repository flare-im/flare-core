//! Browser WebAssembly demo: flare-core `FlareClientBuilder` + WebSocket transport.
//!
//! Designed to work with Native `flare_chat_server` (WebSocket @ :8080).

mod tokio_runtime;
mod wasm_utils;

use std::sync::{Arc, OnceLock};

use flare_core::client::builder::flare::FlareClient;
use flare_core::common::platform::{
    clear_runtime_encryption_key, format_now_rfc3339, has_runtime_encryption_key,
    parse_encryption_key_hex, parse_encryption_key_utf8, register_aes256_encryption,
    runtime_instance_id, set_runtime_encryption_key, wall_clock_ms, AES256_KEY_LEN,
};
use flare_core::common::protocol::{
    frame_with_payload_command, generate_message_id, send_message, Reliability,
};
use tokio::sync::Mutex;
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

use wasm_utils::{
    flare_chat_flare_builder, message_parser_slot, register_flare_chat_encryption,
    WasmChatListener, DEMO_ENCRYPTION_KEY,
};

fn client_slot() -> &'static Arc<Mutex<Option<FlareClient>>> {
    static SLOT: OnceLock<Arc<Mutex<Option<FlareClient>>>> = OnceLock::new();
    SLOT.get_or_init(|| Arc::new(Mutex::new(None)))
}

fn log(msg: &str) {
    web_sys::console::log_1(&msg.into());
    let callback = js_sys::Reflect::get(&js_sys::global(), &"__flareAppendLog".into())
        .ok()
        .and_then(|value| value.dyn_into::<js_sys::Function>().ok());
    if let Some(callback) = callback {
        let _ = callback.call1(&JsValue::NULL, &msg.into());
    }
}

fn map_platform_err(err: flare_core::common::error::FlareError) -> JsValue {
    JsValue::from_str(&err.to_string())
}

fn ensure_encryption_registered() -> Result<(), JsValue> {
    register_flare_chat_encryption().map_err(map_platform_err)
}

/// Run work on the shared WASM Tokio `LocalSet` and expose it to JS as a Promise.
///
/// Do **not** use `#[wasm_bindgen] async fn` here: wasm-bindgen's async transform nests a
/// second executor that can starve `run_async`, so WebSocket never opens and DevTools shows
/// no network activity.
fn promise_run<F, T>(work: F) -> js_sys::Promise
where
    F: std::future::Future<Output = Result<T, JsValue>> + 'static,
    T: Into<JsValue> + 'static,
{
    future_to_promise(async move {
        tokio_runtime::run_async(work)
            .await
            .map(|value| value.into())
    })
}

async fn connect_impl(
    server_url: String,
    username: String,
    encryption_key: Option<String>,
) -> Result<JsValue, JsValue> {
    if let Some(key) = encryption_key.filter(|value| !value.is_empty()) {
        let bytes = parse_encryption_key_utf8(&key).map_err(map_platform_err)?;
        set_runtime_encryption_key(bytes).map_err(map_platform_err)?;
    }
    ensure_encryption_registered()?;

    let log_fn: Arc<dyn Fn(&str) + Send + Sync> = Arc::new(|line| log(line));
    let listener = Arc::new(WasmChatListener::new(Arc::clone(&log_fn)));

    log(&format!("[connect] opening WebSocket to {server_url} ..."));

    let client = flare_chat_flare_builder(server_url.clone(), username.clone(), listener)
        .build_with_race()
        .await
        .map_err(|e| JsValue::from_str(&format!("build/connect failed: {e}")))?;

    let parser = client.parser_snapshot().await;
    *message_parser_slot().lock().await = parser;

    log(&format!(
        "[connect] user={username} negotiated via FlareClientBuilder at {} (runtime={})",
        format_now_rfc3339(),
        runtime_instance_id()
    ));

    *client_slot().lock().await = Some(client);
    Ok(JsValue::UNDEFINED)
}

#[wasm_bindgen(start)]
pub fn wasm_start() {
    console_error_panic_hook::set_once();
    tokio_runtime::ensure_initialized();
}

/// Required AES-256 key length in bytes.
#[wasm_bindgen]
pub fn flare_encryption_key_len() -> u32 {
    AES256_KEY_LEN as u32
}

/// Inject encryption key from JS (exactly 32 UTF-8 bytes). Call before `flare_connect`.
#[wasm_bindgen]
pub fn flare_set_encryption_key(key: String) -> Result<(), JsValue> {
    let bytes = parse_encryption_key_utf8(&key).map_err(map_platform_err)?;
    set_runtime_encryption_key(bytes).map_err(map_platform_err)?;
    register_aes256_encryption(Some(DEMO_ENCRYPTION_KEY)).map_err(map_platform_err)?;
    log("[crypto] encryption key set from JS (utf8)");
    Ok(())
}

/// Inject encryption key from 64-char hex string. Call before `flare_connect`.
#[wasm_bindgen]
pub fn flare_set_encryption_key_hex(hex: String) -> Result<(), JsValue> {
    let bytes = parse_encryption_key_hex(&hex).map_err(map_platform_err)?;
    set_runtime_encryption_key(bytes).map_err(map_platform_err)?;
    register_aes256_encryption(Some(DEMO_ENCRYPTION_KEY)).map_err(map_platform_err)?;
    log("[crypto] encryption key set from JS (hex)");
    Ok(())
}

#[wasm_bindgen]
pub fn flare_clear_encryption_key() {
    clear_runtime_encryption_key();
    log("[crypto] runtime encryption key cleared (demo key used on next register)");
}

#[wasm_bindgen]
pub fn flare_has_encryption_key() -> bool {
    has_runtime_encryption_key()
}

#[wasm_bindgen]
pub fn flare_wall_clock_ms() -> u64 {
    wall_clock_ms()
}

#[wasm_bindgen]
pub fn flare_now_rfc3339() -> String {
    format_now_rfc3339()
}

#[wasm_bindgen]
pub fn flare_runtime_id() -> String {
    runtime_instance_id()
}

/// Connect via `FlareClientBuilder` (WebSocket + ClientCore 协商).
#[wasm_bindgen]
pub fn flare_connect(
    server_url: String,
    username: String,
    encryption_key: Option<String>,
) -> js_sys::Promise {
    promise_run(connect_impl(server_url, username, encryption_key))
}

#[wasm_bindgen]
pub fn flare_disconnect() -> js_sys::Promise {
    promise_run(async {
        let mut guard = client_slot().lock().await;
        if let Some(client) = guard.take() {
            client
                .disconnect()
                .await
                .map_err(|e| JsValue::from_str(&format!("disconnect failed: {e}")))?;
        }
        Ok(JsValue::UNDEFINED)
    })
}

#[wasm_bindgen]
pub fn flare_send(text: String) -> js_sys::Promise {
    promise_run(async move {
        let guard = client_slot().lock().await;
        let client = guard
            .as_ref()
            .ok_or_else(|| JsValue::from_str("not connected; call flare_connect first"))?;

        let msg = send_message(generate_message_id(), text.into_bytes(), None, None);
        let frame = frame_with_payload_command(msg, Reliability::AtLeastOnce);

        client
            .send_frame_and_wait(&frame, std::time::Duration::from_secs(5))
            .await
            .map_err(|e| JsValue::from_str(&format!("send failed: {e}")))?;
        Ok(JsValue::UNDEFINED)
    })
}

#[wasm_bindgen]
pub fn flare_is_connected() -> js_sys::Promise {
    promise_run(async {
        let guard = client_slot().lock().await;
        let connected = match guard.as_ref() {
            Some(client) => client.is_connected_async().await,
            None => false,
        };
        Ok(JsValue::from(connected))
    })
}
