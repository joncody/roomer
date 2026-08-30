use bytes::Bytes;
use roomer::{Conn, HandlerError, Hub, InMemoryMetrics, Message, OutboundMessage};
use std::sync::Arc;
use tokio::sync::mpsc;

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

    // Direct message: user_1 -> user_2
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
