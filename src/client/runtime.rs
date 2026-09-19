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
    // 不在 runtime 里：必须把 runtime 的生命周期交给一个独立线程。
    //
    // 原来是就地 `Runtime::new()` 然后 `runtime.spawn(future)` —— 函数一返回
    // runtime 就被丢弃，连带把还没跑起来的任务一起丢掉，是个**静默的空操作**：
    // 不报错、不 panic，只是什么都没发生。
    std::thread::spawn(move || {
        if let Ok(runtime) = tokio::runtime::Runtime::new() {
            runtime.block_on(future);
        }
    });
}

#[cfg(target_arch = "wasm32")]
pub fn spawn_client_task<F>(future: F)
where
    F: std::future::Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(future);
}

/// 从同步上下文里等一个 future 出结果。
///
/// ⚠️ **这是不得已的桥，不要放进热路径**：即使在多线程 runtime 上，
/// `block_in_place` 也要把本 worker 的整个任务队列交接给另一个线程。
/// 凡是调用方本身就在 async 上下文里的，一律直接 `.await` 对应的 `*_async` 方法。
#[cfg(not(target_arch = "wasm32"))]
pub fn run_client_async<F, T>(future: F) -> T
where
    F: std::future::Future<Output = T>,
{
    use tokio::runtime::RuntimeFlavor;

    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| handle.block_on(future))
        }
        // current-thread runtime 上没有正确答案：`block_in_place` 直接 panic，
        // 而 `block_on` 会自锁——只有一个线程的 runtime 无法在阻塞自己的同时
        // 推进这个 future。与其抛出 tokio 那句难以定位的
        // "can call blocking only when running on the multi-threaded runtime"，
        // 不如直接说清楚出路。
        Ok(_) => panic!(
            "run_client_async 不能用在 current-thread runtime 上：它要阻塞当前线程，             而该 runtime 只有这一个线程来推进 future，必然死锁。             请改调对应的 `*_async` 方法（同步版都有异步孪生），             或者把 runtime 建成 multi_thread。"
        ),
        // 完全不在 runtime 里：自建一个临时 runtime 即可。
        Err(_) => {
            let rt = tokio::runtime::Runtime::new().expect("failed to build tokio runtime");
            rt.block_on(future)
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn run_client_async_works_without_any_runtime() {
        assert_eq!(super::run_client_async(async { 7 }), 7);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_client_async_works_on_multi_thread_runtime() {
        assert_eq!(super::run_client_async(async { 7 }), 7);
    }

    /// 单线程 runtime 上必须给出能照着改的报错，而不是 tokio 那句
    /// "can call blocking only when running on the multi-threaded runtime"。
    #[tokio::test]
    #[should_panic(expected = "请改调对应的 `*_async` 方法")]
    async fn run_client_async_explains_itself_on_current_thread_runtime() {
        super::run_client_async(async { 7 });
    }

    /// 不在 runtime 里 spawn 出去的任务必须真的跑起来。
    /// 原实现把 runtime 建在栈上、spawn 完就返回，任务随 runtime 一起被丢掉——
    /// 不报错也不 panic，纯静默失效。
    #[test]
    fn spawn_client_task_actually_runs_without_a_runtime() {
        let ran = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&ran);
        super::spawn_client_task(async move {
            flag.store(true, Ordering::SeqCst);
        });
        for _ in 0..200 {
            if ran.load(Ordering::SeqCst) {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        panic!("没有 runtime 时 spawn 出去的任务必须真的执行");
    }
}
