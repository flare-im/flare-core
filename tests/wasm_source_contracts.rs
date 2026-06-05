use std::fs;
use std::path::Path;

fn manifest_dir() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn wasm_websocket_yields_with_js_microtask_before_opening_socket() {
    let source = fs::read_to_string(manifest_dir().join("src/transport/websocket_wasm.rs"))
        .expect("wasm websocket transport should be readable");

    let connect_start = source
        .find("pub async fn connect")
        .expect("connect function should exist");
    let websocket_new = source[connect_start..]
        .find("WebSocket::new")
        .expect("connect should create a WebSocket");
    let before_open = &source[connect_start..connect_start + websocket_new];

    assert!(
        before_open.contains("yield_to_event_loop().await"),
        "WASM WebSocket connect should yield through a JS microtask before WebSocket::new"
    );
    assert!(
        !before_open.contains("sleep(std::time::Duration::from_millis(0)).await"),
        "0ms wasmtimer sleep can starve before WebSocket::new; use yield_to_event_loop instead"
    );
}

#[test]
fn wasm_websocket_decodes_arraybuffer_message_events() {
    let source = fs::read_to_string(manifest_dir().join("src/transport/websocket_wasm.rs"))
        .expect("wasm websocket transport should be readable");

    assert!(
        source.contains("ArrayBuffer"),
        "WASM WebSocket onmessage should decode ArrayBuffer events when binary_type is Arraybuffer"
    );
    assert!(
        source.contains("Uint8Array::new"),
        "ArrayBuffer events should be wrapped in Uint8Array before copying bytes"
    );
}

#[test]
fn wasm_event_loop_yield_uses_macrotask_not_microtask_spin() {
    let source = fs::read_to_string(manifest_dir().join("src/common/platform/async_time.rs"))
        .expect("async time platform module should be readable");

    assert!(
        source.contains("setTimeout"),
        "WASM event loop yield should use setTimeout(0) so browser macrotasks can run"
    );
    assert!(
        !source.contains("Promise::resolve"),
        "Promise.resolve microtask yielding can starve WebSocket open/message events"
    );
}

#[test]
fn wasm_runtime_logs_are_forwarded_to_page_log_area() {
    let lib_source =
        fs::read_to_string(manifest_dir().join("examples/wasm_websocket_client/src/lib.rs"))
            .expect("wasm example lib should be readable");
    let page_source =
        fs::read_to_string(manifest_dir().join("examples/wasm_websocket_client/index.html"))
            .expect("wasm example page should be readable");

    assert!(
        page_source.contains("__flareAppendLog"),
        "WASM page should expose appendLog through a global callback"
    );
    assert!(
        lib_source.contains("__flareAppendLog"),
        "Rust-side WASM log() should forward listener logs into the page log area"
    );
}

#[test]
fn wasm_heartbeat_runs_as_detached_background_task() {
    let source = fs::read_to_string(manifest_dir().join("src/client/heartbeat/manager.rs"))
        .expect("heartbeat manager should be readable");

    assert!(
        source.contains("target_arch = \"wasm32\""),
        "heartbeat manager should have WASM-specific task spawning"
    );
    assert!(
        source.contains("wasm_tokio::spawn_detached"),
        "WASM heartbeat must use the persistent detached runtime driver after connect returns"
    );
}

#[test]
fn heartbeat_stop_signals_are_sent_without_awaiting() {
    let client_source = fs::read_to_string(manifest_dir().join("src/client/heartbeat/manager.rs"))
        .expect("client heartbeat manager should be readable");
    let server_source = fs::read_to_string(manifest_dir().join("src/server/heartbeat/detector.rs"))
        .expect("server heartbeat detector should be readable");

    assert!(
        client_source.contains("try_send(())"),
        "client heartbeat stop should synchronously enqueue its stop signal"
    );
    assert!(
        server_source.contains("try_send(())"),
        "server heartbeat stop should synchronously enqueue its stop signal"
    );
    assert!(
        !client_source.contains("drop(tx.send(()))"),
        "dropping an async send future does not send the client heartbeat stop signal"
    );
    assert!(
        !server_source.contains("drop(tx.send(()))"),
        "dropping an async send future does not send the server heartbeat stop signal"
    );
}

#[test]
fn wasm_background_tasks_do_not_enter_tokio_handle() {
    let runtime_source = fs::read_to_string(manifest_dir().join("src/client/runtime.rs"))
        .expect("client runtime should be readable");
    let wasm_tokio_source = fs::read_to_string(manifest_dir().join("src/client/wasm_tokio.rs"))
        .expect("wasm tokio driver should be readable");

    assert!(
        !runtime_source.contains("runtime_handle()"),
        "WASM spawn_client_task should be polled by JS without entering the Tokio handle"
    );
    let spawn_detached = wasm_tokio_source
        .find("pub fn spawn_detached")
        .expect("spawn_detached should exist");
    let run_async = wasm_tokio_source[spawn_detached..]
        .find("pub async fn run_async")
        .expect("run_async should follow spawn_detached");
    let spawn_detached_body = &wasm_tokio_source[spawn_detached..spawn_detached + run_async];
    assert!(
        !spawn_detached_body.contains(".enter()"),
        "detached WASM tasks can outlive run_async and must not create nested EnterGuards"
    );
}
