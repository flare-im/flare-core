use super::*;
use crate::common::serializer::{SerializationUtil, Serializer};
use crate::transport::connection::Connection;
use crate::transport::events::ArcObserver;
use async_trait::async_trait;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

struct MockConnection {
    last_active: Mutex<Instant>,
    closed: Arc<AtomicBool>,
    send_delay: Option<Duration>,
    send_count: Arc<AtomicUsize>,
}

impl MockConnection {
    fn new() -> Self {
        Self {
            last_active: Mutex::new(Instant::now()),
            closed: Arc::new(AtomicBool::new(false)),
            send_delay: None,
            send_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn with_closed_flag(closed: Arc<AtomicBool>) -> Self {
        Self {
            last_active: Mutex::new(Instant::now()),
            closed,
            send_delay: None,
            send_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn with_send_probe(send_delay: Duration, send_count: Arc<AtomicUsize>) -> Self {
        Self {
            last_active: Mutex::new(Instant::now()),
            closed: Arc::new(AtomicBool::new(false)),
            send_delay: Some(send_delay),
            send_count,
        }
    }
}

struct CountingSerializer {
    serialize_count: Arc<AtomicUsize>,
}

impl Serializer for CountingSerializer {
    fn serialize(&self, _frame: &crate::common::protocol::Frame) -> Result<Vec<u8>> {
        self.serialize_count.fetch_add(1, Ordering::SeqCst);
        Ok(b"counting-serializer-frame".to_vec())
    }

    fn deserialize(&self, _data: &[u8]) -> Result<crate::common::protocol::Frame> {
        Ok(crate::common::protocol::Frame::default())
    }

    fn format(&self) -> crate::common::protocol::SerializationFormat {
        crate::common::protocol::SerializationFormat::Protobuf
    }

    fn name(&self) -> &'static str {
        "connection_manager_broadcast_frame_counting_serializer"
    }
}

#[async_trait]
impl Connection for MockConnection {
    fn add_observer(&mut self, _observer: ArcObserver) {}
    fn remove_observer(&mut self, _observer: ArcObserver) {}
    async fn send(&mut self, _data: &[u8]) -> Result<()> {
        self.send_count.fetch_add(1, Ordering::SeqCst);
        if let Some(delay) = self.send_delay {
            tokio::time::sleep(delay).await;
        }
        Ok(())
    }
    async fn close(&mut self) -> Result<()> {
        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }
    fn last_active_time(&self) -> Instant {
        *self.last_active.lock().unwrap()
    }
    fn update_active_time(&mut self) {
        *self.last_active.lock().unwrap() = Instant::now();
    }
}

async fn wait_for_send_count(send_count: &AtomicUsize, expected: usize) {
    tokio::time::timeout(Duration::from_millis(500), async {
        while send_count.load(Ordering::SeqCst) < expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "timed out waiting for send_count to reach {expected}; current={}",
            send_count.load(Ordering::SeqCst)
        )
    });
}

#[test]
fn test_add_and_get_connection() {
    let manager = ConnectionManager::new();
    let connection = Box::new(MockConnection::new());

    manager
        .add_connection("conn1".to_string(), connection, None, false)
        .unwrap();

    let (_, info) = manager.get_connection("conn1").unwrap();
    assert_eq!(info.connection_id, "conn1");
}

#[test]
fn test_remove_connection() {
    let manager = ConnectionManager::new();
    let connection = Box::new(MockConnection::new());

    manager
        .add_connection("conn1".to_string(), connection, None, false)
        .unwrap();
    assert_eq!(manager.connection_count(), 1);

    manager.remove_connection("conn1").unwrap();
    assert_eq!(manager.connection_count(), 0);
}

#[test]
fn test_user_binding() {
    let manager = ConnectionManager::new();
    let connection = Box::new(MockConnection::new());

    manager
        .add_connection("conn1".to_string(), connection, None, false)
        .unwrap();
    manager.bind_user("conn1", "user1".to_string()).unwrap();

    let connections = manager.get_user_connections("user1");
    assert_eq!(connections, vec!["conn1"]);
    assert_eq!(manager.user_count(), 1);

    manager.remove_connection("conn1").unwrap();
    assert_eq!(manager.user_count(), 0);
}

#[test]
fn test_same_user_multiple_connections_count_as_one_user() {
    let manager = ConnectionManager::new();

    manager
        .add_connection(
            "conn1".to_string(),
            Box::new(MockConnection::new()),
            Some("user1".to_string()),
            false,
        )
        .unwrap();
    manager
        .add_connection(
            "conn2".to_string(),
            Box::new(MockConnection::new()),
            Some("user1".to_string()),
            false,
        )
        .unwrap();

    assert_eq!(manager.user_count(), 1);

    manager.remove_connection("conn1").unwrap();
    assert_eq!(manager.user_count(), 1);

    manager.remove_connection("conn2").unwrap();
    assert_eq!(manager.user_count(), 0);
}

#[test]
fn test_add_connection_with_limit_rejects_when_capacity_full() {
    let manager = ConnectionManager::new();

    manager
        .add_connection_with_limit(
            "conn1".to_string(),
            Box::new(MockConnection::new()),
            None,
            false,
            1,
        )
        .unwrap();

    let result = manager.add_connection_with_limit(
        "conn2".to_string(),
        Box::new(MockConnection::new()),
        None,
        false,
        1,
    );

    assert!(result.is_err());
    assert_eq!(manager.connection_count(), 1);
    assert!(manager.get_connection("conn2").is_none());
}

#[test]
fn test_cleanup_timeout() {
    let manager = ConnectionManager::new();
    let connection = Box::new(MockConnection::new());

    manager
        .add_connection("conn1".to_string(), connection, None, false)
        .unwrap();

    // 等待一段时间，让连接超时
    std::thread::sleep(Duration::from_millis(10));

    let cleaned = manager.cleanup_timeout_connections(Duration::from_millis(5));
    assert!(cleaned.contains(&"conn1".to_string()));
    assert_eq!(manager.connection_count(), 0);
}

#[test]
fn timeout_connection_snapshots_keep_handles_after_connection_shards_are_locked() {
    let manager = ConnectionManager::new();

    for idx in 0..2 {
        manager
            .add_connection(
                format!("conn{idx}"),
                Box::new(MockConnection::new()),
                Some(format!("user{idx}")),
                false,
            )
            .unwrap();
    }

    std::thread::sleep(Duration::from_millis(10));

    let snapshots = manager.timeout_connection_snapshots(Duration::from_millis(5));
    assert_eq!(snapshots.len(), 2);

    let _shard_guards: Vec<_> = manager
        .connection_shards
        .iter()
        .map(|shard| shard.write().unwrap())
        .collect();

    let mut snapshot_ids: Vec<_> = snapshots
        .into_iter()
        .map(|(connection_id, connection, user_id)| {
            assert!(connection.try_lock().is_ok());
            assert!(user_id.is_some());
            connection_id
        })
        .collect();
    snapshot_ids.sort();

    assert_eq!(snapshot_ids, vec!["conn0", "conn1"]);
}

#[tokio::test]
async fn trait_cleanup_timeout_connections_closes_underlying_connection() {
    let manager = ConnectionManager::new();
    let closed = Arc::new(AtomicBool::new(false));

    manager
        .add_connection(
            "conn1".to_string(),
            Box::new(MockConnection::with_closed_flag(Arc::clone(&closed))),
            None,
            false,
        )
        .unwrap();

    std::thread::sleep(Duration::from_millis(10));

    let cleaned =
        ConnectionManagerTrait::cleanup_timeout_connections(&manager, Duration::from_millis(5))
            .await;

    assert_eq!(cleaned, vec!["conn1".to_string()]);
    assert!(closed.load(Ordering::SeqCst));
}

#[tokio::test]
async fn trait_broadcast_sends_to_connections_concurrently() {
    let manager = ConnectionManager::new();
    let send_count = Arc::new(AtomicUsize::new(0));

    for idx in 0..3 {
        manager
            .add_connection(
                format!("conn{idx}"),
                Box::new(MockConnection::with_send_probe(
                    Duration::from_millis(100),
                    Arc::clone(&send_count),
                )),
                None,
                false,
            )
            .unwrap();
    }

    let started = Instant::now();
    ConnectionManagerTrait::broadcast(&manager, b"payload")
        .await
        .unwrap();
    let elapsed = started.elapsed();

    wait_for_send_count(&send_count, 3).await;
    assert_eq!(send_count.load(Ordering::SeqCst), 3);
    assert!(
        elapsed < Duration::from_millis(220),
        "broadcast should fan out concurrently; elapsed={elapsed:?}"
    );
}

#[tokio::test]
async fn trait_broadcast_frame_sends_to_connections_concurrently() {
    let manager = ConnectionManager::new();
    let send_count = Arc::new(AtomicUsize::new(0));

    for idx in 0..3 {
        manager
            .add_connection(
                format!("conn{idx}"),
                Box::new(MockConnection::with_send_probe(
                    Duration::from_millis(100),
                    Arc::clone(&send_count),
                )),
                None,
                false,
            )
            .unwrap();
    }

    let frame = crate::common::protocol::frame_with_system_command(
        crate::common::protocol::ping(),
        crate::common::protocol::Reliability::AtLeastOnce,
    );

    let started = Instant::now();
    ConnectionManagerTrait::broadcast_frame(&manager, &frame, None)
        .await
        .unwrap();
    let elapsed = started.elapsed();

    wait_for_send_count(&send_count, 3).await;
    assert_eq!(send_count.load(Ordering::SeqCst), 3);
    assert!(
        elapsed < Duration::from_millis(220),
        "broadcast_frame should fan out concurrently; elapsed={elapsed:?}"
    );
}

#[tokio::test]
async fn broadcast_frame_with_explicit_parser_serializes_once() {
    let manager = ConnectionManager::new();
    let send_count = Arc::new(AtomicUsize::new(0));
    let serialize_count = Arc::new(AtomicUsize::new(0));

    SerializationUtil::register_custom(Arc::new(CountingSerializer {
        serialize_count: Arc::clone(&serialize_count),
    }));

    for idx in 0..3 {
        manager
            .add_connection(
                format!("conn{idx}"),
                Box::new(MockConnection::with_send_probe(
                    Duration::ZERO,
                    Arc::clone(&send_count),
                )),
                None,
                false,
            )
            .unwrap();
    }

    let parser = crate::common::MessageParser::with_custom_format(
        "connection_manager_broadcast_frame_counting_serializer",
        crate::common::compression::CompressionAlgorithm::None,
        crate::common::encryption::EncryptionAlgorithm::None,
    );
    let frame = crate::common::protocol::frame_with_system_command(
        crate::common::protocol::ping(),
        crate::common::protocol::Reliability::AtLeastOnce,
    );

    ConnectionManagerTrait::broadcast_frame(&manager, &frame, Some(&parser))
        .await
        .unwrap();

    wait_for_send_count(&send_count, 3).await;
    assert_eq!(send_count.load(Ordering::SeqCst), 3);
    assert_eq!(
        serialize_count.load(Ordering::SeqCst),
        1,
        "broadcast_frame should serialize once when every recipient uses the explicit parser"
    );
}

#[tokio::test]
async fn write_worker_times_out_and_removes_slow_connection() {
    let manager = ConnectionManager::with_send_timeout(Duration::from_millis(50));
    let send_count = Arc::new(AtomicUsize::new(0));

    manager
        .add_connection(
            "slow".to_string(),
            Box::new(MockConnection::with_send_probe(
                Duration::from_millis(250),
                Arc::clone(&send_count),
            )),
            None,
            false,
        )
        .unwrap();

    let started = Instant::now();
    let result = ConnectionManagerTrait::send_to_connection(&manager, "slow", b"payload").await;
    let elapsed = started.elapsed();

    assert!(result.is_ok());
    assert!(
        elapsed < Duration::from_millis(50),
        "send should enqueue without waiting for the slow socket write; elapsed={elapsed:?}"
    );
    wait_for_send_count(&send_count, 1).await;
    tokio::time::timeout(Duration::from_millis(500), async {
        while manager.get_connection("slow").is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("slow connection should be removed after writer timeout");
    assert!(manager.get_connection("slow").is_none());
    assert_eq!(manager.connection_count(), 0);
}

#[tokio::test]
async fn bounded_write_queue_isolates_slow_consumer_without_waiting_for_socket_write() {
    let manager = Arc::new(ConnectionManager::with_write_queue_limits(
        Duration::from_secs(5),
        256,
        1,
    ));
    let send_count = Arc::new(AtomicUsize::new(0));

    manager
        .add_connection(
            "slow".to_string(),
            Box::new(MockConnection::with_send_probe(
                Duration::from_millis(250),
                Arc::clone(&send_count),
            )),
            None,
            false,
        )
        .unwrap();

    let first_manager = Arc::clone(&manager);
    let first_send = tokio::spawn(async move {
        ConnectionManagerTrait::send_to_connection(&*first_manager, "slow", b"first").await
    });

    let wait_started = tokio::time::timeout(Duration::from_millis(100), async {
        while send_count.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        wait_started.is_ok(),
        "first write should reach the underlying connection"
    );

    let second_started = Instant::now();
    let second = ConnectionManagerTrait::send_to_connection(&*manager, "slow", b"second").await;
    let second_elapsed = second_started.elapsed();

    let third_started = Instant::now();
    let third = ConnectionManagerTrait::send_to_connection(&*manager, "slow", b"third").await;
    let third_elapsed = third_started.elapsed();

    assert!(
        second.is_ok(),
        "one queued write should be accepted while the socket write is in flight"
    );
    assert!(
        second_elapsed < Duration::from_millis(50),
        "enqueue should not wait for the slow socket write; elapsed={second_elapsed:?}"
    );
    assert!(
        third.is_err(),
        "full queue should isolate the slow consumer"
    );
    assert!(
        third_elapsed < Duration::from_millis(50),
        "full queue detection should be immediate; elapsed={third_elapsed:?}"
    );
    assert!(manager.get_connection("slow").is_none());
    assert_eq!(manager.connection_count(), 0);

    let _ = first_send.await;
}

#[test]
fn connection_snapshots_can_be_used_after_connection_shards_are_locked() {
    let manager = ConnectionManager::new();

    for idx in 0..2 {
        manager
            .add_connection(
                format!("conn{idx}"),
                Box::new(MockConnection::new()),
                None,
                false,
            )
            .unwrap();
    }

    let snapshots = manager.connection_snapshots();
    assert_eq!(snapshots.len(), 2);

    let _shard_guards: Vec<_> = manager
        .connection_shards
        .iter()
        .map(|shard| shard.write().unwrap())
        .collect();

    let mut snapshot_ids: Vec<_> = snapshots
        .into_iter()
        .map(|(connection_id, _writer, info)| {
            assert_eq!(info.connection_id, connection_id);
            connection_id
        })
        .collect();
    snapshot_ids.sort();

    assert_eq!(snapshot_ids, vec!["conn0", "conn1"]);
}

#[test]
fn update_connections_active_batches_successful_connection_ids() {
    let manager = ConnectionManager::new();
    let old_active = Instant::now() - Duration::from_secs(60);

    for idx in 0..3 {
        let connection_id = format!("conn{idx}");
        manager
            .add_connection(
                connection_id.clone(),
                Box::new(MockConnection::new()),
                None,
                false,
            )
            .unwrap();

        let mut shard = manager.connection_shard(&connection_id).write().unwrap();
        let (_, _, info) = shard.get_mut(&connection_id).unwrap();
        info.last_active = old_active;
    }

    let updated = manager.update_connections_active([
        "conn0".to_string(),
        "conn1".to_string(),
        "missing".to_string(),
    ]);

    assert_eq!(updated, 2);
    assert!(manager.get_connection("conn0").unwrap().1.last_active > old_active);
    assert!(manager.get_connection("conn1").unwrap().1.last_active > old_active);
    assert_eq!(
        manager.get_connection("conn2").unwrap().1.last_active,
        old_active
    );
}

#[tokio::test]
async fn configured_fanout_concurrency_limits_broadcast_parallelism() {
    let manager = ConnectionManager::with_limits(Duration::from_secs(10), 1);
    let send_count = Arc::new(AtomicUsize::new(0));

    for idx in 0..3 {
        manager
            .add_connection(
                format!("conn{idx}"),
                Box::new(MockConnection::with_send_probe(
                    Duration::from_millis(80),
                    Arc::clone(&send_count),
                )),
                None,
                false,
            )
            .unwrap();
    }

    let started = Instant::now();
    ConnectionManagerTrait::broadcast(&manager, b"payload")
        .await
        .unwrap();
    let elapsed = started.elapsed();

    wait_for_send_count(&send_count, 3).await;
    assert_eq!(send_count.load(Ordering::SeqCst), 3);
    assert!(
        elapsed < Duration::from_millis(50),
        "broadcast should enqueue without waiting for slow socket writes; elapsed={elapsed:?}"
    );
}

#[test]
fn connection_count_does_not_depend_on_connections_map_lock() {
    let manager = ConnectionManager::new();

    manager
        .add_connection(
            "conn1".to_string(),
            Box::new(MockConnection::new()),
            None,
            false,
        )
        .unwrap();

    let poison_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let shard = manager.connection_shard_index("conn1");
        let _guard = manager.connection_shards[shard].write().unwrap();
        panic!("poison connection shard lock");
    }));
    assert!(poison_result.is_err());

    assert_eq!(manager.connection_count(), 1);
}

#[test]
fn stats_total_users_does_not_depend_on_user_connections_map_lock() {
    let manager = ConnectionManager::new();

    manager
        .add_connection(
            "conn1".to_string(),
            Box::new(MockConnection::new()),
            Some("user1".to_string()),
            false,
        )
        .unwrap();

    let poison_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = manager.user_connection_shard("user1").write().unwrap();
        panic!("poison user_connection shard lock");
    }));
    assert!(poison_result.is_err());

    assert_eq!(manager.stats().total_users, 1);
}

#[test]
fn add_connection_releases_reserved_slot_when_shard_lock_fails() {
    let manager = ConnectionManager::new();
    let connection_id = "conn1";

    let poison_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = manager.connection_shard(connection_id).write().unwrap();
        panic!("poison connection shard lock");
    }));
    assert!(poison_result.is_err());

    let result = manager.add_connection(
        connection_id.to_string(),
        Box::new(MockConnection::new()),
        None,
        false,
    );

    assert!(result.is_err());
    assert_eq!(manager.connection_count(), 0);
}

#[test]
fn add_connection_rolls_back_insert_when_user_index_lock_fails() {
    let manager = ConnectionManager::new();

    let poison_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = manager.user_connection_shard("user1").write().unwrap();
        panic!("poison user_connection shard lock");
    }));
    assert!(poison_result.is_err());

    let result = manager.add_connection(
        "conn1".to_string(),
        Box::new(MockConnection::new()),
        Some("user1".to_string()),
        false,
    );

    assert!(result.is_err());
    assert_eq!(manager.connection_count(), 0);
    assert!(manager.get_connection("conn1").is_none());
    assert_eq!(manager.user_count(), 0);
}

#[test]
fn locked_connection_shard_does_not_block_other_shard_registration() {
    let manager = Arc::new(ConnectionManager::new());
    let locked_id = "locked-shard-connection";
    let locked_shard = manager.connection_shard_index(locked_id);
    let other_id = (0..10_000)
        .map(|idx| format!("other-shard-{idx}"))
        .find(|id| manager.connection_shard_index(id) != locked_shard)
        .expect("should find an id in a different shard");

    let _held_read_lock = manager.connection_shards[locked_shard].read().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let manager_clone = Arc::clone(&manager);

    std::thread::spawn(move || {
        let result =
            manager_clone.add_connection(other_id, Box::new(MockConnection::new()), None, false);
        tx.send(result.is_ok()).unwrap();
    });

    assert!(rx.recv_timeout(Duration::from_millis(150)).unwrap());
}

#[test]
fn locked_user_connection_shard_does_not_block_other_user_binding() {
    let manager = Arc::new(ConnectionManager::new());
    let locked_user_id = "locked-user";
    let locked_shard = manager.user_connection_shard_index(locked_user_id);
    let other_user_id = (0..10_000)
        .map(|idx| format!("other-user-{idx}"))
        .find(|id| manager.user_connection_shard_index(id) != locked_shard)
        .expect("should find a user id in a different shard");

    manager
        .add_connection(
            "conn1".to_string(),
            Box::new(MockConnection::new()),
            None,
            false,
        )
        .unwrap();

    let _held_read_lock = manager.user_connection_shards[locked_shard].read().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let manager_clone = Arc::clone(&manager);

    std::thread::spawn(move || {
        let result = manager_clone.bind_user("conn1", other_user_id);
        tx.send(result.is_ok()).unwrap();
    });

    assert!(rx.recv_timeout(Duration::from_millis(150)).unwrap());
}
