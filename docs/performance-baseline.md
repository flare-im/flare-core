# flare-core Performance Baseline

Date: 2026-06-05

## Scope

This report covers the `flare-core` transport foundation only:

- Frame serialization/parsing
- Message pipeline dispatch
- Connection manager registration and active-time update
- In-memory fanout through the `ConnectionManagerTrait`

It intentionally excludes IM-domain semantics such as `seq`, sync cursors, offline storage,
Social permission hooks, push workers, and read models. Those belong in `flare-im-core`,
`flare-server-core`, and `flare-social` benchmarks.

## Environment

- Host: Darwin 25.3.0 arm64
- CPU: Apple M1 Pro
- Cores: 10
- Memory: 16 GiB
- Rust: `rustc 1.94.1 (e408947bf 2026-03-25)`
- Cargo: `cargo 1.94.1 (29ea6fb6a 2026-03-24)`

## Command

```bash
cargo bench --bench perf_baseline
```

The benchmark is implemented in `benches/perf_baseline.rs` and uses no external benchmark
framework. It prints JSON so CI or release scripts can archive and compare results.

## Results

| Benchmark | Iterations | ns/op | Throughput / Latency | Notes |
| --- | ---: | ---: | ---: | --- |
| `codec.protobuf.round_trip.256b` | 100,000 | 982 | 1,017,824 | serialize + parse one payload Frame |
| `codec.json.round_trip.256b` | 50,000 | 5,052 | 197,954 | serialize + parse one payload Frame |
| `codec.protobuf_gzip.round_trip.1kb` | 10,000 | 19,602 | 51,015 | serialize + gzip + gunzip + parse one payload Frame |
| `pipeline.process_raw.validate_no_response.256b` | 50,000 | 712 | 1,405,371 | parse + validation middleware + no response |
| `connection_manager.add_update_remove` | 50,000 | 686 | 1,457,953 | single connection lifecycle with sharded manager |
| `connection_manager.broadcast.1000x256b` | 200 | 238,794 | 4,188 broadcasts/sec | one op sends to 1,000 in-memory connections |
| `connection_manager.broadcast_frame_explicit_parser.1000x256b` | 200 | 358,545 | 2,789 broadcasts/sec | one op serializes one Frame once and sends to 1,000 connections |
| `connection_manager.cleanup_timeout_trait.1000` | 1 | 727,209 | 0.727 ms/op | snapshot, close, and batch-remove 1,000 timed-out connections |

`connection_manager.broadcast.1000x256b` translates to roughly 4.19M mock per-connection sends/sec
on this machine. This is not network throughput; it measures manager fanout overhead plus async
locking against in-memory `Connection` implementations.

`connection_manager.broadcast_frame_explicit_parser.1000x256b` translates to roughly 2.79M mock
per-connection Frame sends/sec when all recipients share the explicit parser.

## Architectural Reading

### Transport Layer

`flare-core` is correctly positioned as the long-connection and frame layer. The measured hot paths
do not introduce IM message ordering, Social rules, or sync semantics, so the base remains compatible
with the intended split:

- `flare-core`: transport frame, negotiation, heartbeat, connection lifecycle
- `flare-server-core`: runtime, context propagation, service discovery, event bus, observability
- `flare-im-core`: seq, delivery, sync, storage, push authority
- `flare-social`: user/relation/group authority through hooks and bridge projections

### Feishu / WeChat / Telegram Alignment

- Feishu/Lark: transport and business gates remain separated; future PreSend checks belong in
  `flare-social-hook`, not in `flare-core`.
- WeChat: heartbeat and connection lifecycle now behave like a reliable long-connection substrate;
  send FSM and offline retry still belong in the SDK/core layers.
- Telegram: codec and frame path stays sync-cursor neutral; cloud sync benchmarks should be added in
  `flare-im-core`, not here.

## Findings

1. Protobuf is the right default for the transport frame path.
   JSON round-trip is about 7.0x slower than Protobuf for a 256-byte payload on this machine.

2. Gzip should be thresholded.
   Protobuf+Gzip for a 1 KiB payload is about 25.3x slower than plain Protobuf for a 256-byte payload.
   Small chat/control frames should avoid compression unless payload size or transport policy justifies it.

3. In-memory connection registration and active update are not the current bottleneck.
   `add_connection + update_active + remove_connection` is below 1 microsecond/op in this benchmark.

4. Fanout manager overhead is acceptable as a base, but production fanout bottlenecks will move to
   per-connection write queues, socket backpressure, slow consumers, and gateway routing.

5. `MetricsMiddleware` previously recorded near-zero durations because it stored `Instant::elapsed()`
   immediately after creating the `Instant`. It now stores an absolute wall-clock millisecond value
   and reports a real elapsed duration.

6. `MessagePipeline` now uses copy-on-write snapshots for middleware and processor lists.
   In-flight middleware no longer holds registry locks across `await`, and one raw message uses a
   single parser snapshot for request handling and response serialization.

7. `ConnectionManager` fanout now snapshots connection handles before async writes.
   Byte-level broadcast avoids the previous ID-list + per-send shard lookup pattern, and Frame
   fanout reuses cached per-connection parsers when available.

8. Frame fanout with an explicit parser now serializes the Frame once, uses lightweight auth
   snapshots, and batches successful `last_active` updates by shard after fanout.

9. Timeout cleanup now snapshots timed-out connection handles, closes them without re-reading the
   connection map, then batch-removes connection and user-index entries by shard.

10. Heartbeat cleanup logs now emit structured counts plus a small connection-id sample instead of
    dumping every timed-out connection ID during timeout storms.

11. `ServerCore` now reuses shared `MessagePipeline` instances by negotiated parser profile
    (`format + compression + encryption`). When middleware or processors are configured, thousands
    of connections with the same profile no longer allocate duplicate pipelines.

## Recommended Next Optimizations

1. Add compression thresholds at the caller/config layer.
   Keep Protobuf/no-compression as the default for small control and chat frames; use Gzip only above
   a measured payload threshold.

2. Move gateway-scale fanout pressure tests to `flare-server-core` or `flare-im-core`.
   Real fanout must include bounded per-connection queues, slow consumer eviction, and online route
   distribution.

3. Add an end-to-end transport benchmark with real WebSocket and TCP sockets.
   This should measure connect negotiation latency, send/ack latency, idle heartbeat stability, and
   reconnect behavior with concurrent clients.

4. Keep `flare-core` free of IM semantics.
   Seq allocation, message dedupe, offline pull, and Social access checks should remain outside this
   crate.

## Reproducibility Notes

Benchmarks are microbenchmarks and should be compared on the same host under similar load. Treat
single-run deltas below 5-10% as noise unless repeated runs confirm the trend.
