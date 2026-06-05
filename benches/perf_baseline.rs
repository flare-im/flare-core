use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use flare_core::common::platform::{MonotonicInstant, monotonic_now};
use flare_core::common::protocol::{frame_with_payload_command, send_message};
use flare_core::common::{
    CompressionAlgorithm, EncryptionAlgorithm, MessageParser, MessagePipeline, Reliability,
    SerializationFormat, ValidationMiddleware,
};
use flare_core::server::{ConnectionManager, ConnectionManagerTrait};
use flare_core::transport::connection::Connection;
use flare_core::transport::events::ArcObserver;
use serde::Serialize;

const SMALL_PAYLOAD: usize = 256;
const LARGE_PAYLOAD: usize = 1024;
const CODEC_ITERS: u64 = 100_000;
const JSON_ITERS: u64 = 50_000;
const GZIP_ITERS: u64 = 10_000;
const PIPELINE_ITERS: u64 = 50_000;
const REGISTRATION_ITERS: u64 = 50_000;
const FANOUT_CONNECTIONS: usize = 1_000;
const FANOUT_ITERS: u64 = 200;

#[derive(Serialize)]
struct BenchResult {
    name: &'static str,
    iterations: u64,
    elapsed_ms: f64,
    ns_per_op: f64,
    ops_per_sec: f64,
    notes: &'static str,
}

struct CountingConnection {
    sends: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
    last_active: MonotonicInstant,
}

impl CountingConnection {
    fn new(sends: Arc<AtomicU64>, bytes: Arc<AtomicU64>) -> Self {
        Self {
            sends,
            bytes,
            last_active: monotonic_now(),
        }
    }
}

#[async_trait]
impl Connection for CountingConnection {
    fn add_observer(&mut self, _observer: ArcObserver) {}

    fn remove_observer(&mut self, _observer: ArcObserver) {}

    async fn send(&mut self, data: &[u8]) -> flare_core::common::Result<()> {
        self.sends.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(data.len() as u64, Ordering::Relaxed);
        self.last_active = monotonic_now();
        Ok(())
    }

    async fn close(&mut self) -> flare_core::common::Result<()> {
        Ok(())
    }

    fn last_active_time(&self) -> MonotonicInstant {
        self.last_active
    }

    fn update_active_time(&mut self) {
        self.last_active = monotonic_now();
    }
}

fn payload_frame(size: usize) -> flare_core::common::Frame {
    frame_with_payload_command(
        send_message("bench-message".to_string(), vec![b'x'; size], None, None),
        Reliability::AtLeastOnce,
    )
}

fn run_sync<F>(name: &'static str, iterations: u64, notes: &'static str, mut f: F) -> BenchResult
where
    F: FnMut(u64),
{
    let started = Instant::now();
    for i in 0..iterations {
        f(black_box(i));
    }
    result(name, iterations, notes, started.elapsed())
}

fn run_async<F, Fut>(
    rt: &tokio::runtime::Runtime,
    name: &'static str,
    iterations: u64,
    notes: &'static str,
    mut f: F,
) -> BenchResult
where
    F: FnMut(u64) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let started = Instant::now();
    rt.block_on(async {
        for i in 0..iterations {
            f(black_box(i)).await;
        }
    });
    result(name, iterations, notes, started.elapsed())
}

fn result(
    name: &'static str,
    iterations: u64,
    notes: &'static str,
    elapsed: Duration,
) -> BenchResult {
    let elapsed_ns = elapsed.as_nanos() as f64;
    let iterations_f = iterations as f64;
    BenchResult {
        name,
        iterations,
        elapsed_ms: elapsed.as_secs_f64() * 1_000.0,
        ns_per_op: elapsed_ns / iterations_f,
        ops_per_sec: iterations_f / elapsed.as_secs_f64(),
        notes,
    }
}

fn bench_codec(results: &mut Vec<BenchResult>) {
    let protobuf = MessageParser::new(
        SerializationFormat::Protobuf,
        CompressionAlgorithm::None,
        EncryptionAlgorithm::None,
    );
    let json = MessageParser::new(
        SerializationFormat::Json,
        CompressionAlgorithm::None,
        EncryptionAlgorithm::None,
    );
    let gzip = MessageParser::new(
        SerializationFormat::Protobuf,
        CompressionAlgorithm::Gzip,
        EncryptionAlgorithm::None,
    );
    let small_frame = payload_frame(SMALL_PAYLOAD);
    let large_frame = payload_frame(LARGE_PAYLOAD);

    results.push(run_sync(
        "codec.protobuf.round_trip.256b",
        CODEC_ITERS,
        "serialize + parse one payload Frame",
        |_| {
            let data = protobuf.serialize(black_box(&small_frame)).unwrap();
            let parsed = protobuf.parse(black_box(&data)).unwrap();
            black_box(parsed.message_id);
        },
    ));

    results.push(run_sync(
        "codec.json.round_trip.256b",
        JSON_ITERS,
        "serialize + parse one payload Frame",
        |_| {
            let data = json.serialize(black_box(&small_frame)).unwrap();
            let parsed = json.parse(black_box(&data)).unwrap();
            black_box(parsed.message_id);
        },
    ));

    results.push(run_sync(
        "codec.protobuf_gzip.round_trip.1kb",
        GZIP_ITERS,
        "serialize + gzip + gunzip + parse one payload Frame",
        |_| {
            let data = gzip.serialize(black_box(&large_frame)).unwrap();
            let parsed = gzip.parse(black_box(&data)).unwrap();
            black_box(parsed.message_id);
        },
    ));
}

fn bench_pipeline(rt: &tokio::runtime::Runtime, results: &mut Vec<BenchResult>) {
    let parser = MessageParser::new(
        SerializationFormat::Protobuf,
        CompressionAlgorithm::None,
        EncryptionAlgorithm::None,
    );
    let frame = payload_frame(SMALL_PAYLOAD);
    let data = parser.serialize(&frame).unwrap();
    let pipeline = MessagePipeline::new(parser);
    rt.block_on(async {
        pipeline
            .add_middleware(Arc::new(ValidationMiddleware::new("noop", |_| Ok(()))))
            .await;
    });

    results.push(run_async(
        rt,
        "pipeline.process_raw.validate_no_response.256b",
        PIPELINE_ITERS,
        "parse + validation middleware + no response",
        |_| {
            let pipeline_ref = &pipeline;
            let data_ref = &data;
            async move {
                let response = pipeline_ref
                    .process_raw(black_box(data_ref), Some("conn-1"))
                    .await;
                black_box(response.unwrap());
            }
        },
    ));
}

fn bench_connection_manager(rt: &tokio::runtime::Runtime, results: &mut Vec<BenchResult>) {
    let manager = ConnectionManager::new();
    let sends = Arc::new(AtomicU64::new(0));
    let bytes = Arc::new(AtomicU64::new(0));
    results.push(run_sync(
        "connection_manager.add_update_remove",
        REGISTRATION_ITERS,
        "single connection lifecycle with sharded manager",
        |i| {
            let id = format!("bench-conn-{i}");
            manager
                .add_connection(
                    id.clone(),
                    Box::new(CountingConnection::new(
                        Arc::clone(&sends),
                        Arc::clone(&bytes),
                    )),
                    Some(format!("user-{}", i % 1024)),
                    false,
                )
                .unwrap();
            manager.update_connection_active(&id).unwrap();
            manager.remove_connection(&id).unwrap();
            black_box(manager.connection_count());
        },
    ));

    let manager = ConnectionManager::with_limits(Duration::from_secs(1), 256);
    let sends = Arc::new(AtomicU64::new(0));
    let bytes = Arc::new(AtomicU64::new(0));
    for i in 0..FANOUT_CONNECTIONS {
        manager
            .add_connection(
                format!("fanout-{i}"),
                Box::new(CountingConnection::new(
                    Arc::clone(&sends),
                    Arc::clone(&bytes),
                )),
                Some(format!("user-{i}")),
                false,
            )
            .unwrap();
    }
    let data = vec![b'x'; SMALL_PAYLOAD];
    results.push(run_async(
        rt,
        "connection_manager.broadcast.1000x256b",
        FANOUT_ITERS,
        "broadcast to 1,000 in-memory connections; one op = one broadcast",
        |_| {
            let manager_ref = &manager;
            let data_ref = &data;
            async move {
                manager_ref.broadcast(black_box(data_ref)).await.unwrap();
            }
        },
    ));

    let parser = MessageParser::new(
        SerializationFormat::Protobuf,
        CompressionAlgorithm::None,
        EncryptionAlgorithm::None,
    );
    let frame = payload_frame(SMALL_PAYLOAD);
    results.push(run_async(
        rt,
        "connection_manager.broadcast_frame_explicit_parser.1000x256b",
        FANOUT_ITERS,
        "serialize one Frame once, then broadcast bytes to 1,000 in-memory connections",
        |_| {
            let manager_ref = &manager;
            let frame_ref = &frame;
            let parser_ref = &parser;
            async move {
                manager_ref
                    .broadcast_frame(black_box(frame_ref), Some(black_box(parser_ref)))
                    .await
                    .unwrap();
            }
        },
    ));
    black_box(sends.load(Ordering::Relaxed));
    black_box(bytes.load(Ordering::Relaxed));

    let cleanup_manager = ConnectionManager::with_limits(Duration::from_secs(1), 256);
    let sends = Arc::new(AtomicU64::new(0));
    let bytes = Arc::new(AtomicU64::new(0));
    for i in 0..FANOUT_CONNECTIONS {
        cleanup_manager
            .add_connection(
                format!("timeout-{i}"),
                Box::new(CountingConnection::new(
                    Arc::clone(&sends),
                    Arc::clone(&bytes),
                )),
                Some(format!("timeout-user-{i}")),
                false,
            )
            .unwrap();
    }
    std::thread::sleep(Duration::from_millis(2));
    results.push(run_async(
        rt,
        "connection_manager.cleanup_timeout_trait.1000",
        1,
        "snapshot, close, and batch-remove 1,000 timed-out connections",
        |_| {
            let manager_ref = &cleanup_manager;
            async move {
                let removed = ConnectionManagerTrait::cleanup_timeout_connections(
                    manager_ref,
                    Duration::ZERO,
                )
                .await;
                black_box(removed.len());
            }
        },
    ));
    black_box(cleanup_manager.connection_count());
}

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("benchmark runtime should build");
    let mut results = Vec::new();

    bench_codec(&mut results);
    bench_pipeline(&rt, &mut results);
    bench_connection_manager(&rt, &mut results);

    println!("{}", serde_json::to_string_pretty(&results).unwrap());
}
