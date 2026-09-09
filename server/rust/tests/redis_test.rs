#[cfg(feature = "redis-adapter")]
mod redis_tests {
    use bytes::Bytes;
    use roomer::{Adapter, Message, RedisAdapter};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[test]
    fn test_redis_envelope_encoding_decoding() {
        let node_id = "node-alpha-123";
        let original_msg = Message::new(
            "lobby",
            "chat",
            "",
            "user_1",
            Bytes::from_static(b"cluster test"),
        );
        let raw_bytes = original_msg.encode();

        let envelope = RedisAdapter::encode_envelope(node_id, &raw_bytes);
        let (sender_node, payload_data) =
            RedisAdapter::decode_envelope(envelope).expect("envelope should decode");

        assert_eq!(sender_node, node_id);
        let decoded_msg = Message::decode(payload_data).expect("message should decode");
        assert_eq!(decoded_msg, original_msg);
    }

    #[tokio::test]
    async fn test_live_redis_two_node_sync_and_presence() {
        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());

        let client = match ::redis::Client::open(redis_url.clone()) {
            Ok(c) => c,
            Err(_) => return,
        };
        if client.get_multiplexed_async_connection().await.is_err() {
            eprintln!("Skipping live Redis test: Redis not reachable at {redis_url}");
            return;
        }

        let prefix = format!("roomer:test:{}:", uuid::Uuid::new_v4());

        let node_a = RedisAdapter::builder(&redis_url)
            .node_id("node_A")
            .prefix(&prefix)
            .build()
            .unwrap();

        let node_b = RedisAdapter::builder(&redis_url)
            .node_id("node_B")
            .prefix(&prefix)
            .build()
            .unwrap();

        let node_b_received = Arc::new(AtomicUsize::new(0));
        let node_a_received = Arc::new(AtomicUsize::new(0));

        let b_counter = node_b_received.clone();
        node_b
            .subscribe(Arc::new(move |_room, _sender, _raw| {
                b_counter.fetch_add(1, Ordering::SeqCst);
            }))
            .await
            .unwrap();

        let a_counter = node_a_received.clone();
        node_a
            .subscribe(Arc::new(move |_room, _sender, _raw| {
                a_counter.fetch_add(1, Ordering::SeqCst);
            }))
            .await
            .unwrap();

        // 1. Verify Cluster Presence Sync & Pipelined Heartbeat Touch
        node_a.add_presence("lobby", "client_on_A").await.unwrap();
        node_b.add_presence("lobby", "client_on_B").await.unwrap();

        let presence = node_a.get_presence("lobby").await.unwrap();
        assert_eq!(
            presence.len(),
            2,
            "Presence set must return members across all cluster nodes"
        );
        assert!(presence.contains(&"client_on_A".to_string()));
        assert!(presence.contains(&"client_on_B".to_string()));

        // Pipelined presence touch across rooms
        node_a
            .touch_presence("client_on_A", &["lobby".into(), "room_touch".into()])
            .await
            .unwrap();
        let touch_presence = node_a.get_presence("room_touch").await.unwrap();
        assert_eq!(touch_presence, vec!["client_on_A".to_string()]);

        // 2. Verify Node Registry
        node_b.register_node("client_on_B").await.unwrap();
        let target_node = node_a.get_node_for_conn("client_on_B").await.unwrap();
        assert_eq!(target_node, Some("node_B".to_string()));

        // Allow Redis subscription to register
        tokio::time::sleep(Duration::from_millis(100)).await;

        // 3. Node A publishes 100 broadcast messages
        let total = 100;
        for i in 0..total {
            let msg = Message::new("bench_room", "chat", "", "client_1", format!("msg_{i}"));
            node_a.publish("bench_room", &msg).await.unwrap();
        }

        // Wait for delivery
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while tokio::time::Instant::now() < deadline {
            if node_b_received.load(Ordering::SeqCst) >= total {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert!(
            node_b_received.load(Ordering::SeqCst) >= total,
            "Node B must receive messages from Node A"
        );
        assert_eq!(
            node_a_received.load(Ordering::SeqCst),
            0,
            "Node A must receive 0 (loopback suppressed)"
        );

        // Cleanup presence
        node_a
            .remove_presence("lobby", "client_on_A")
            .await
            .unwrap();
        node_b
            .remove_presence("lobby", "client_on_B")
            .await
            .unwrap();
        node_b.unregister_node("client_on_B").await.unwrap();

        node_a.close().await.unwrap();
        node_b.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_live_redis_true_wire_level_unicast() {
        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());

        let client = match ::redis::Client::open(redis_url.clone()) {
            Ok(c) => c,
            Err(_) => return,
        };
        if client.get_multiplexed_async_connection().await.is_err() {
            eprintln!("Skipping live Redis test: Redis not reachable at {redis_url}");
            return;
        }

        let prefix = format!("roomer:unicast:{}:", uuid::Uuid::new_v4());

        // Sender, Target, and Bystander nodes
        let node_sender = RedisAdapter::builder(&redis_url)
            .node_id("node_sender")
            .prefix(&prefix)
            .build()
            .unwrap();

        let node_target = RedisAdapter::builder(&redis_url)
            .node_id("node_target")
            .prefix(&prefix)
            .build()
            .unwrap();

        let node_bystander = RedisAdapter::builder(&redis_url)
            .node_id("node_bystander")
            .prefix(&prefix)
            .build()
            .unwrap();

        let target_received = Arc::new(AtomicUsize::new(0));
        let bystander_received = Arc::new(AtomicUsize::new(0));

        let t_counter = target_received.clone();
        node_target
            .subscribe(Arc::new(move |_room, _sender, _raw| {
                t_counter.fetch_add(1, Ordering::SeqCst);
            }))
            .await
            .unwrap();

        let b_counter = bystander_received.clone();
        node_bystander
            .subscribe(Arc::new(move |_room, _sender, _raw| {
                b_counter.fetch_add(1, Ordering::SeqCst);
            }))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        let dm = Message::new(
            "root",
            "dm",
            "client_target",
            "client_sender",
            Bytes::from_static(b"wire_level_secret"),
        );

        // Send unicast message targeted exclusively to "node_target"
        node_sender.publish_direct("node_target", &dm).await.unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while tokio::time::Instant::now() < deadline {
            if target_received.load(Ordering::SeqCst) >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert_eq!(
            target_received.load(Ordering::SeqCst),
            1,
            "Target node must receive the direct message"
        );
        assert_eq!(
            bystander_received.load(Ordering::SeqCst),
            0,
            "Bystander node must NOT receive unicast traffic over the wire"
        );

        node_sender.close().await.unwrap();
        node_target.close().await.unwrap();
        node_bystander.close().await.unwrap();
    }
}
