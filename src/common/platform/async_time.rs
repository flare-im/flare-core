//! Async timers: Native `tokio::time`, browser WASM `wasmtimer::tokio`.

#[cfg(not(target_arch = "wasm32"))]
pub use tokio::time::{interval, sleep, timeout};

#[cfg(target_arch = "wasm32")]
pub use wasmtimer::tokio::{interval, sleep, timeout};

#[cfg(target_arch = "wasm32")]
pub async fn yield_to_event_loop() {
    use js_sys::Promise;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use wasm_bindgen::{JsCast, JsValue};
    use wasm_bindgen_futures::JsFuture;

    struct SendJsFuture(JsFuture);

    // Browser WASM runs this future on the JS main thread. The crate keeps async trait futures
    // Send-compatible so public transport traits stay uniform across Native and WASM targets.
    unsafe impl Send for SendJsFuture {}

    impl Future for SendJsFuture {
        type Output = Result<JsValue, JsValue>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let inner = unsafe { self.map_unchecked_mut(|this| &mut this.0) };
            Future::poll(inner, cx)
        }
    }

    let promise = Promise::new(&mut |resolve, reject| {
        let global = js_sys::global();
        let set_timeout = js_sys::Reflect::get(&global, &JsValue::from_str("setTimeout"))
            .ok()
            .and_then(|value| value.dyn_into::<js_sys::Function>().ok());

        if let Some(set_timeout) = set_timeout {
            let _ = set_timeout.call2(&global, &resolve, &JsValue::from_f64(0.0));
        } else {
            let _ = reject.call1(
                &JsValue::NULL,
                &JsValue::from_str("setTimeout is unavailable"),
            );
        }
    });
    let _ = SendJsFuture(JsFuture::from(promise)).await;
}
