//! Platform runtime hooks for client background tasks.

#[cfg(not(target_arch = "wasm32"))]
pub fn spawn_client_task<F>(future: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(future);
        return;
    }
    if let Ok(runtime) = tokio::runtime::Runtime::new() {
        runtime.spawn(future);
    }
}

#[cfg(target_arch = "wasm32")]
pub fn spawn_client_task<F>(future: F)
where
    F: std::future::Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(future);
}

#[cfg(not(target_arch = "wasm32"))]
pub fn run_client_async<F, T>(future: F) -> T
where
    F: std::future::Future<Output = T>,
{
    tokio::task::block_in_place(|| {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            return handle.block_on(future);
        }
        let rt = tokio::runtime::Runtime::new().expect("failed to build tokio runtime");
        rt.block_on(future)
    })
}
