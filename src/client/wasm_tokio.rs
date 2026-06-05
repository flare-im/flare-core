//! Browser WASM async driver (Tokio `current_thread` + persistent `LocalSet`).
//!
//! `Runtime::block_on` is unsupported on `wasm32-unknown-unknown`. This module:
//! - keeps one runtime + thread-local `LocalSet`
//! - uses a single `run_until` per `run_async` invoke (no nested drivers)
//! - serializes `run_async` invokes so nested JS/IDB callbacks do not overlap
//!
//! See [`crate::transport::connection`] for why browser WebSocket observers must not spawn
//! unbounded async work directly from sync callbacks — use `ClientCore::push_wasm_inbound`.

use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;
use std::sync::OnceLock;

use futures_channel::oneshot;
use futures_util::FutureExt;
use tokio::runtime::{Builder, Handle, Runtime};
use tokio::task::LocalSet;
use wasm_bindgen_futures::spawn_local;

use crate::common::platform::{sleep, yield_to_event_loop};
use std::time::Duration;

thread_local! {
    static LOCAL_SET: RefCell<Option<Rc<LocalSet>>> = const { RefCell::new(None) };
    static INVOKE_BUSY: RefCell<bool> = const { RefCell::new(false) };
    static INVOKE_WAITERS: RefCell<Vec<oneshot::Sender<()>>> = const { RefCell::new(Vec::new()) };
}

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        Builder::new_current_thread()
            .build()
            .expect("failed to build wasm tokio runtime")
    })
}

pub(crate) fn runtime_handle() -> Handle {
    runtime().handle().clone()
}

fn local_set() -> Rc<LocalSet> {
    LOCAL_SET.with(|slot| {
        if slot.borrow().is_none() {
            *slot.borrow_mut() = Some(Rc::new(LocalSet::new()));
        }
        slot.borrow().as_ref().expect("local set").clone()
    })
}

async fn acquire_invoke_slot() {
    loop {
        let wait_rx = {
            let mut rx_slot = None;
            INVOKE_BUSY.with(|busy| {
                if !*busy.borrow() {
                    *busy.borrow_mut() = true;
                    return;
                }
                let (tx, rx) = oneshot::channel();
                INVOKE_WAITERS.with(|waiters| waiters.borrow_mut().push(tx));
                rx_slot = Some(rx);
            });
            rx_slot
        };
        match wait_rx {
            Some(mut rx) => loop {
                match rx.try_recv() {
                    Ok(Some(())) => return,
                    Ok(None) => sleep(Duration::from_millis(1)).await,
                    Err(_) => return,
                }
            },
            None => return,
        }
    }
}

fn release_invoke_slot() {
    let next = INVOKE_WAITERS.with(|waiters| waiters.borrow_mut().pop());
    if let Some(tx) = next {
        let _ = tx.send(());
        return;
    }
    INVOKE_BUSY.with(|busy| {
        *busy.borrow_mut() = false;
    });
}

/// Initialize the WASM Tokio driver (idempotent).
pub fn ensure_initialized() {
    let _ = runtime();
    let _ = local_set();
}

/// Spawn a long-lived task (reconnect, event forward, heartbeat, etc.).
pub fn spawn_detached<F>(future: F)
where
    F: Future<Output = ()> + 'static,
{
    ensure_initialized();
    spawn_local(future);
}

/// Run one future on the WASM runtime without `block_on`.
pub async fn run_async<F, T>(future: F) -> T
where
    F: Future<Output = T> + 'static,
    T: 'static,
{
    ensure_initialized();
    acquire_invoke_slot().await;

    struct ReleaseInvokeSlot;
    impl Drop for ReleaseInvokeSlot {
        fn drop(&mut self) {
            release_invoke_slot();
        }
    }
    let _release = ReleaseInvokeSlot;

    let local = local_set();
    let handle = runtime_handle();
    let _guard = handle.enter();
    let local_for_run = Rc::clone(&local);
    let output = local
        .run_until(async move {
            let mut join = local_for_run.spawn_local(future);
            loop {
                futures_util::select! {
                    out = (&mut join).fuse() => {
                        return out.expect("wasm invoke task join failed");
                    }
                    _ = yield_to_event_loop().fuse() => {}
                }
            }
        })
        .await;

    output
}
