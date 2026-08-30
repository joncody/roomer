//! Multi-node distributed adapters (Local in-memory, Redis Pub/Sub) with presence sets and unicast routing.

use crate::error::AdapterError;
use crate::message::Message;
use async_trait::async_trait;
use dashmap::{DashMap, DashSet};
use std::sync::Arc;

/// Callback type signature for cluster subscription listeners.
pub type SubscribeCallback = Arc<dyn Fn(&str, Message) + Send + Sync + 'static>;

/// Trait defining horizontal scaling message broadcast and cluster presence adapters.
#[async_trait]
pub trait Adapter: Send + Sync + 'static {
    /// Publishes a message to all cluster nodes for a specified room.
    async fn publish(&self, room: &str, msg: &Message) -> Result<(), AdapterError>;

    /// Publishes a message directly to a specific target cluster node (unicast).
    async fn publish_direct(&self, target_node_id: &str, msg: &Message) -> Result<(), AdapterError>;

    /// Subscribes to cluster messages and invokes the given callback.
    async fn subscribe(&self, callback: SubscribeCallback) -> Result<(), AdapterError>;

    /// Adds a connection ID to a room's cluster-wide presence set.
    async fn add_presence(&self, room: &str, conn_id: &str) -> Result<(), AdapterError>;

    /// Removes a connection ID from a room's cluster-wide presence set.
    async fn remove_presence(&self, room: &str, conn_id: &str) -> Result<(), AdapterError>;

    /// Retrieves all connection IDs in a room across the entire cluster.
    async fn get_presence(&self, room: &str) -> Result<Vec<String>, AdapterError>;

    /// Maps a connection ID to this node ID in the cluster registry.
    async fn register_node(&self, conn_id: &str) -> Result<(), AdapterError>;

    /// Removes a connection ID mapping from the cluster registry.
    async fn unregister_node(&self, conn_id: &str) -> Result<(), AdapterError>;

    /// Retrieves the node ID hosting a given connection ID.
    async fn get_node_for_conn(&self, conn_id: &str) -> Result<Option<String>, AdapterError>;

    /// Returns the unique cluster identifier of this server instance.
    fn node_id(&self) -> &str;

    /// Closes and drains all cluster connections.
    async fn close(&self) -> Result<(), AdapterError>;
}

/// Default in-memory adapter for single-instance deployments.
#[derive(Default, Clone, Debug)]
pub struct LocalAdapter {
    presence: Arc<DashMap<String, DashSet<String>>>,
    node_map: Arc<DashMap<String, String>>,
}

#[async_trait]
impl Adapter for LocalAdapter {
    async fn publish(&self, _room: &str, _msg: &Message) -> Result<(), AdapterError> {
        Ok(())
    }
    async fn publish_direct(&self, _target_node_id: &str, _msg: &Message) -> Result<(), AdapterError> {
        Ok(())
    }
    async fn subscribe(&self, _callback: SubscribeCallback) -> Result<(), AdapterError> {
        Ok(())
    }
    async fn add_presence(&self, room: &str, conn_id: &str) -> Result<(), AdapterError> {
        self.presence
            .entry(room.to_string())
            .or_default()
            .insert(conn_id.to_string());
        Ok(())
    }
    async fn remove_presence(&self, room: &str, conn_id: &str) -> Result<(), AdapterError> {
        if let Some(set) = self.presence.get(room) {
            set.remove(conn_id);
        }
        Ok(())
    }
    async fn get_presence(&self, room: &str) -> Result<Vec<String>, AdapterError> {
        Ok(self
            .presence
            .get(room)
            .map(|set| set.iter().map(|k| k.clone()).collect())
            .unwrap_or_default())
    }
    async fn register_node(&self, conn_id: &str) -> Result<(), AdapterError> {
        self.node_map.insert(conn_id.to_string(), "local-node".into());
        Ok(())
    }
    async fn unregister_node(&self, conn_id: &str) -> Result<(), AdapterError> {
        self.node_map.remove(conn_id);
        Ok(())
    }
    async fn get_node_for_conn(&self, conn_id: &str) -> Result<Option<String>, AdapterError> {
        Ok(self.node_map.get(conn_id).map(|v| v.value().clone()))
    }
    fn node_id(&self) -> &str {
        "local-node"
    }
    async fn close(&self) -> Result<(), AdapterError> {
        Ok(())
    }
}

/// Dynamic trait object alias for distributed adapters.
pub type DynAdapter = Arc<dyn Adapter>;

#[cfg(feature = "redis-adapter")]
/// Redis pub/sub clustering adapter with presence tracking, loopback suppression, and unicast routing.
pub mod redis {
    use super::{Adapter, AdapterError, SubscribeCallback};
    use crate::message::Message;
    use async_trait::async_trait;
    use bytes::{Buf, BufMut, Bytes, BytesMut};
    use futures_util::StreamExt;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::{oneshot, Mutex};
    use tracing::{error, info, warn};
    use uuid::Uuid;

    /// Builder for configuring and initializing a `RedisAdapter`.
    pub struct RedisAdapterBuilder {
        redis_url: String,
        prefix: String,
        publish_timeout: Duration,
        node_id: Option<String>,
    }

    impl RedisAdapterBuilder {
        /// Creates a new builder with the given Redis URL.
        pub fn new(redis_url: impl Into<String>) -> Self {
            Self {
                redis_url: redis_url.into(),
                prefix: "roomer:demo:".into(),
                publish_timeout: Duration::from_secs(5),
                node_id: None,
            }
        }

        /// Sets the key prefix for Redis pub/sub channels and presence keys.
        pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
            let mut p = prefix.into();
            if !p.ends_with(':') {
                p.push(':');
            }
            self.prefix = p;
            self
        }

        /// Sets timeout duration for PUBLISH commands.
        pub fn publish_timeout(mut self, timeout: Duration) -> Self {
            self.publish_timeout = timeout;
            self
        }

        /// Overrides the unique cluster node ID (defaults to UUID v4).
        pub fn node_id(mut self, node_id: impl Into<String>) -> Self {
            self.node_id = Some(node_id.into());
            self
        }

        /// Builds and returns the `RedisAdapter`.
        ///
        /// # Errors
        /// Returns `AdapterError` if the Redis client cannot be opened.
        pub fn build(self) -> Result<RedisAdapter, AdapterError> {
            let mut url = self.redis_url;
            if !url.starts_with("redis://") && !url.starts_with("rediss://") {
                url = format!("redis://{}", url);
            }

            let client = ::redis::Client::open(url)
                .map_err(|e| AdapterError::ConnectionFailed(e.to_string()))?;
            let node_id = self.node_id.unwrap_or_else(|| Uuid::new_v4().to_string());

            Ok(RedisAdapter {
                client,
                node_id,
                prefix: self.prefix,
                publish_timeout: self.publish_timeout,
                multiplexed_conn: Arc::new(Mutex::new(None)),
                shutdown_tx: Mutex::new(None),
            })
        }
    }

    /// Redis pub/sub cluster adapter with automatic presence sync, loopback suppression, and unicast routing.
    pub struct RedisAdapter {
        client: ::redis::Client,
        node_id: String,
        prefix: String,
        publish_timeout: Duration,
        multiplexed_conn: Arc<Mutex<Option<::redis::aio::MultiplexedConnection>>>,
        shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
    }

    impl RedisAdapter {
        /// Creates a new `RedisAdapter` with default configuration.
        ///
        /// # Errors
        /// Returns `AdapterError` if connection parsing fails.
        pub fn new(redis_url: &str, prefix: Option<&str>) -> Result<Self, AdapterError> {
            let mut builder = RedisAdapterBuilder::new(redis_url);
            if let Some(p) = prefix {
                builder = builder.prefix(p);
            }
            builder.build()
        }

        /// Returns a builder instance for customizing settings.
        pub fn builder(redis_url: &str) -> RedisAdapterBuilder {
            RedisAdapterBuilder::new(redis_url)
        }

        async fn get_publish_conn(&self) -> Result<::redis::aio::MultiplexedConnection, AdapterError> {
            let mut guard = self.multiplexed_conn.lock().await;
            if let Some(ref conn) = *guard {
                Ok(conn.clone())
            } else {
                let conn = self
                    .client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|e| AdapterError::ConnectionFailed(e.to_string()))?;
                *guard = Some(conn.clone());
                Ok(conn)
            }
        }

        /// Encodes a message with a 4-byte length-prefixed node ID envelope.
        pub fn encode_envelope(node_id: &str, raw_msg: &[u8]) -> Bytes {
            let node_bytes = node_id.as_bytes();
            let mut env = BytesMut::with_capacity(4 + node_bytes.len() + raw_msg.len());
            env.put_u32(node_bytes.len() as u32);
            env.put_slice(node_bytes);
            env.put_slice(raw_msg);
            env.freeze()
        }

        /// Decodes a node envelope, extracting sender node ID and raw message payload.
        pub fn decode_envelope(mut data: Bytes) -> Option<(String, Bytes)> {
            if data.len() < 4 {
                return None;
            }
            let sender_node_len = data.get_u32() as usize;
            if data.remaining() < sender_node_len {
                return None;
            }
            let sender_bytes = data.split_to(sender_node_len);
            let sender_node = String::from_utf8(sender_bytes.to_vec()).ok()?;
            Some((sender_node, data))
        }
    }

    #[async_trait]
    impl Adapter for RedisAdapter {
        fn node_id(&self) -> &str {
            &self.node_id
        }

        async fn publish(&self, room: &str, msg: &Message) -> Result<(), AdapterError> {
            let mut conn = self.get_publish_conn().await?;
            let channel = format!("{}{}", self.prefix, room);
            let raw_msg = msg.encode();
            let payload = Self::encode_envelope(&self.node_id, &raw_msg);

            let _receivers: i64 = tokio::time::timeout(self.publish_timeout, async {
                ::redis::cmd("PUBLISH")
                    .arg(&channel)
                    .arg(payload.as_ref())
                    .query_async(&mut conn)
                    .await
            })
            .await
            .map_err(|_| AdapterError::PublishTimeout)?
            .map_err(|e| {
                error!(error = %e, channel = %channel, "Redis PUBLISH command failed");
                AdapterError::PublishFailed(e.to_string())
            })?;

            Ok(())
        }

        async fn publish_direct(&self, target_node_id: &str, msg: &Message) -> Result<(), AdapterError> {
            let mut conn = self.get_publish_conn().await?;
            let channel = format!("{}node:{}", self.prefix, target_node_id);
            let raw_msg = msg.encode();
            let payload = Self::encode_envelope(&self.node_id, &raw_msg);

            let _receivers: i64 = tokio::time::timeout(self.publish_timeout, async {
                ::redis::cmd("PUBLISH")
                    .arg(&channel)
                    .arg(payload.as_ref())
                    .query_async(&mut conn)
                    .await
            })
            .await
            .map_err(|_| AdapterError::PublishTimeout)?
            .map_err(|e| {
                error!(error = %e, channel = %channel, "Redis unicast PUBLISH failed");
                AdapterError::PublishFailed(e.to_string())
            })?;

            Ok(())
        }

        async fn add_presence(&self, room: &str, conn_id: &str) -> Result<(), AdapterError> {
            let mut conn = self.get_publish_conn().await?;
            let key = format!("{}presence:{}", self.prefix, room);
            let _res: i64 = ::redis::cmd("SADD")
                .arg(&key)
                .arg(conn_id)
                .query_async(&mut conn)
                .await
                .map_err(|e| AdapterError::PublishFailed(e.to_string()))?;
            Ok(())
        }

        async fn remove_presence(&self, room: &str, conn_id: &str) -> Result<(), AdapterError> {
            let mut conn = self.get_publish_conn().await?;
            let key = format!("{}presence:{}", self.prefix, room);
            let _res: i64 = ::redis::cmd("SREM")
                .arg(&key)
                .arg(conn_id)
                .query_async(&mut conn)
                .await
                .map_err(|e| AdapterError::PublishFailed(e.to_string()))?;
            Ok(())
        }

        async fn get_presence(&self, room: &str) -> Result<Vec<String>, AdapterError> {
            let mut conn = self.get_publish_conn().await?;
            let key = format!("{}presence:{}", self.prefix, room);
            let members: Vec<String> = ::redis::cmd("SMEMBERS")
                .arg(&key)
                .query_async(&mut conn)
                .await
                .map_err(|e| AdapterError::PublishFailed(e.to_string()))?;
            Ok(members)
        }

        async fn register_node(&self, conn_id: &str) -> Result<(), AdapterError> {
            let mut conn = self.get_publish_conn().await?;
            let key = format!("{}conn_node:{}", self.prefix, conn_id);
            let _res: String = ::redis::cmd("SET")
                .arg(&key)
                .arg(&self.node_id)
                .arg("EX")
                .arg(86400)
                .query_async(&mut conn)
                .await
                .map_err(|e| AdapterError::PublishFailed(e.to_string()))?;
            Ok(())
        }

        async fn unregister_node(&self, conn_id: &str) -> Result<(), AdapterError> {
            let mut conn = self.get_publish_conn().await?;
            let key = format!("{}conn_node:{}", self.prefix, conn_id);
            let _res: i64 = ::redis::cmd("DEL")
                .arg(&key)
                .query_async(&mut conn)
                .await
                .map_err(|e| AdapterError::PublishFailed(e.to_string()))?;
            Ok(())
        }

        async fn get_node_for_conn(&self, conn_id: &str) -> Result<Option<String>, AdapterError> {
            let mut conn = self.get_publish_conn().await?;
            let key = format!("{}conn_node:{}", self.prefix, conn_id);
            let node: Option<String> = ::redis::cmd("GET")
                .arg(&key)
                .query_async(&mut conn)
                .await
                .map_err(|e| AdapterError::PublishFailed(e.to_string()))?;
            Ok(node)
        }

        async fn subscribe(&self, callback: SubscribeCallback) -> Result<(), AdapterError> {
            let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
            *self.shutdown_tx.lock().await = Some(shutdown_tx);

            let client = self.client.clone();
            let prefix = self.prefix.clone();
            let node_id = self.node_id.clone();
            let pattern = format!("{}*", prefix);

            let mut pubsub_conn = client
                .get_async_pubsub()
                .await
                .map_err(|e| AdapterError::ConnectionFailed(e.to_string()))?;

            pubsub_conn
                .psubscribe(&pattern)
                .await
                .map_err(|e| AdapterError::SubscribeFailed(e.to_string()))?;

            info!(node_id = %node_id, pattern = %pattern, "Successfully subscribed to Redis cluster pattern");

            tokio::spawn(async move {
                let mut stream = pubsub_conn.into_on_message();

                'outer: loop {
                    loop {
                        tokio::select! {
                            _ = &mut shutdown_rx => return,
                            redis_msg = stream.next() => {
                                match redis_msg {
                                    Some(msg) => {
                                        let payload_bytes = msg.get_payload_bytes();
                                        let data = Bytes::copy_from_slice(payload_bytes);

                                        if let Some((sender_node, raw_data)) = RedisAdapter::decode_envelope(data) {
                                            // Loopback Suppression: Drop messages originating from our own node
                                            if sender_node == node_id {
                                                continue;
                                            }

                                            if let Some(packet) = Message::decode(raw_data) {
                                                let channel_name = msg.get_channel_name();
                                                let channel_suffix = channel_name.strip_prefix(&prefix).unwrap_or(channel_name);
                                                callback(channel_suffix, packet);
                                            }
                                        }
                                    }
                                    None => {
                                        warn!("Redis pub/sub stream terminated unexpectedly. Attempting reconnect...");
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    // Reconnection loop with exponential backoff and jitter
                    let mut backoff = Duration::from_millis(200);
                    let max_backoff = Duration::from_secs(5);

                    loop {
                        tokio::select! {
                            _ = &mut shutdown_rx => return,
                            _ = tokio::time::sleep(backoff) => {
                                match client.get_async_pubsub().await {
                                    Ok(mut new_pubsub) => {
                                        match new_pubsub.psubscribe(&pattern).await {
                                            Ok(()) => {
                                                info!(node_id = %node_id, pattern = %pattern, "Successfully reconnected and re-subscribed to Redis cluster");
                                                stream = new_pubsub.into_on_message();
                                                continue 'outer;
                                            }
                                            Err(err) => {
                                                warn!(error = %err, "Failed to re-subscribe to Redis pattern, retrying...");
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        warn!(error = %err, "Failed to re-establish Redis pub/sub connection, retrying...");
                                    }
                                }

                                let jitter = Duration::from_millis(
                                    (tokio::time::Instant::now().elapsed().as_nanos() % 150) as u64
                                );
                                backoff = (backoff * 2).min(max_backoff) + jitter;
                            }
                        }
                    }
                }
            });

            Ok(())
        }

        async fn close(&self) -> Result<(), AdapterError> {
            if let Some(tx) = self.shutdown_tx.lock().await.take() {
                let _ = tx.send(());
            }
            Ok(())
        }
    }
}
