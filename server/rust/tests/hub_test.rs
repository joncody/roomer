use bytes::Bytes;
use roomer::{
    Adapter, AdapterError, BackpressureStrategy, Conn, HandlerError, Hub, InMemoryMetrics, Message,
    OutboundMessage,
};
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Default)]
struct MockClusterAdapter {
    sub_cb: std::sync::Mutex<Option<roomer::adapter::SubscribeCallback>>,
}

#[async_trait::async_trait]
impl Adapter for MockClusterAdapter {
    async fn publish_raw(&self, _room: &str, _raw_msg: &[u8]) -> Result<(), AdapterError> {
        Ok(())
    }

    async fn publish_direct_raw(
        &self,
        _target_node_id: &str,
        _raw_msg: &[u8],
    ) -> Result<(), AdapterError> {
        Ok(())
    }

    async fn subscribe(
        &self,
        callback: roomer::adapter::SubscribeCallback,
    ) -> Result<(), AdapterError> {
        *self.sub_cb.lock().unwrap() = Some(callback);
        Ok(())
    }

    async fn add_presence(&self, _room: &str, _conn_id: &str) -> Result<(), AdapterError> {
        Ok(())
    }

    async fn remove_presence(&self, _room: &str, _conn_id: &str) -> Result<(), AdapterError> {
        Ok(())
    }

    async fn get_presence(&self, _room: &str) -> Result<Vec<String>, AdapterError> {
        Ok(Vec::new())
    }

    async fn register_node(&self, _conn_id: &str) -> Result<(), AdapterError> {
        Ok(())
    }

    async fn unregister_node(&self, _conn_id: &str) -> Result<(), AdapterError> {
        Ok(())
    }

    async fn get_node_for_conn(&self, _conn_id: &str) -> Result<Option<String>, AdapterError> {
        Ok(None)
    }

    fn node_id(&self) -> &str {
        "mock-node"
    }

    async fn close(&self) -> Result<(), AdapterError> {
        Ok(())
    }
}

#[tokio::test]
async fn test_hub_concurrent_join_leave_and_direct_routing() {
    let metrics = Arc::new(InMemoryMetrics::new());
    let hub = Hub::new();
    hub.configure(Arc::new(roomer::LocalAdapter::default()), metrics.clone())
        .await;

    let (tx1, _rx1) = mpsc::channel(100);
    let (tx2, mut rx2) = mpsc::channel(100);

    let c1 = Conn::new("user_1".into(), Default::default(), tx1, metrics.clone());
    let c2 = Conn::new("user_2".into(), Default::default(), tx2, metrics.clone());

    hub.add_conn(c1.clone());
    hub.add_conn(c2.clone());

    assert_eq!(metrics.active_connections(), 2);

    hub.join_room("lobby", c1.clone());
    hub.join_room("lobby", c2.clone());

    assert_eq!(metrics.active_rooms(), 1);

    // Verify cluster presence snapshot returns both users
    let presence = hub.get_cluster_presence("lobby").await;
    assert_eq!(presence.len(), 2);

    // Direct message: user_1 -> user_2 using 12-byte wire format
    let dm = Message::new(
        "root",
        "dm",
        "user_2",
        "user_1",
        Bytes::from_static(b"secret"),
    );
    hub.dispatch(c1.clone(), dm).await;

    let received = rx2
        .recv()
        .await
        .expect("user_2 should receive direct message");
    match received {
        OutboundMessage::Binary(bin) => {
            let parsed = Message::decode(bin).expect("valid frame");
            assert_eq!(parsed.version, 1);
            assert_eq!(parsed.flags, 0);
            assert_eq!(parsed.event, "dm");
            assert_eq!(parsed.payload, Bytes::from_static(b"secret"));
        }
        _ => panic!("Expected binary frame"),
    }

    // Leave room
    hub.leave_room("lobby", &c1);
    let room = hub.get_room("lobby").expect("room still has c2");
    assert_eq!(room.snapshot().len(), 1);

    hub.leave_room("lobby", &c2);
    assert!(
        hub.get_room("lobby").is_none(),
        "empty room should be garbage collected"
    );
    assert_eq!(metrics.active_rooms(), 0);
}

#[tokio::test]
async fn test_hub_cluster_metrics_tracking() {
    let metrics = Arc::new(InMemoryMetrics::new());
    let hub = Hub::new();
    let adapter = Arc::new(MockClusterAdapter::default());
    hub.configure(adapter.clone(), metrics.clone()).await;

    // 1. Broadcast triggers on_cluster_publish
    let msg = Message::new("room1", "chat", "", "u1", Bytes::from_static(b"test"));
    hub.broadcast_room(None, msg);

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(metrics.cluster_published(), 1);

    // 2. Incoming cluster message triggers on_cluster_received
    let sub_cb = adapter
        .sub_cb
        .lock()
        .unwrap()
        .clone()
        .expect("subscriber registered");
    sub_cb(
        "room:room1",
        "remote_node",
        Bytes::from_static(b"cluster_payload"),
    );

    assert_eq!(metrics.cluster_received(), 1);
}

#[tokio::test]
async fn test_conn_control_plane_rate_limiter() {
    let (tx, _rx) = mpsc::channel(100);
    let metrics = Arc::new(InMemoryMetrics::new());
    let conn = Conn::with_rate_limit(
        "limited_conn".into(),
        Default::default(),
        tx,
        metrics,
        BackpressureStrategy::DropSlowClient,
        5.0, // 5 tokens/sec
        3.0, // burst of 3
    );

    // 3 immediate events consume burst
    assert!(conn.allow_control_event());
    assert!(conn.allow_control_event());
    assert!(conn.allow_control_event());

    // 4th immediate event must be rate limited
    assert!(!conn.allow_control_event());
}

#[tokio::test]
async fn test_conn_close_with_and_hub_disconnect() {
    let hub = Hub::new();
    let (tx, mut rx) = mpsc::channel(100);
    let conn = Conn::new(
        "disconnect_user".into(),
        Default::default(),
        tx,
        hub.metrics(),
    );
    hub.add_conn(conn.clone());

    // Hub.disconnect sends close frame and initiates teardown
    let disconnected = hub.disconnect("disconnect_user", 4001, "Invalid authentication key");
    assert!(disconnected);

    let received = rx.recv().await.expect("should receive close frame");
    match received {
        OutboundMessage::Close(code, reason) => {
            assert_eq!(code, 4001);
            assert_eq!(reason, "Invalid authentication key");
        }
        _ => panic!("Expected OutboundMessage::Close"),
    }

    // Verify duplicate close_with calls are suppressed and do not queue multiple close frames
    let duplicate = conn.close_with(4002, "Duplicate disconnect");
    assert!(!duplicate);
    assert!(
        rx.try_recv().is_err(),
        "no duplicate close message should be queued"
    );
}

#[tokio::test]
async fn test_handler_registration_guards() {
    let hub = Hub::new();

    // Reserved event should fail
    let res = hub.register_handler("join", Arc::new(|_, _| Box::pin(async { Ok(()) })));
    assert!(matches!(res, Err(HandlerError::ReservedEvent(_))));

    // Custom event succeeds
    let res = hub.register_handler("custom", Arc::new(|_, _| Box::pin(async { Ok(()) })));
    assert!(res.is_ok());

    // Duplicate event fails
    let res2 = hub.register_handler("custom", Arc::new(|_, _| Box::pin(async { Ok(()) })));
    assert!(matches!(res2, Err(HandlerError::DuplicateHandler(_))));
}

#[tokio::test]
async fn test_hub_shutdown_close_frames() {
    let hub = Hub::new();
    let (tx1, mut rx1) = mpsc::channel(100);
    let c1 = Conn::new("user_1".into(), Default::default(), tx1, hub.metrics());
    hub.add_conn(c1);

    hub.shutdown().await.expect("shutdown succeeded");

    let received = rx1.recv().await.expect("should receive close frame");
    match received {
        OutboundMessage::Close(code, reason) => {
            assert_eq!(code, 1001);
            assert_eq!(reason, "Server shutting down");
        }
        _ => panic!("Expected close frame"),
    }
}
