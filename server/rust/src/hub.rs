use crate::adapter::{DynAdapter, LocalAdapter};
use crate::conn::Conn;
use crate::error::HandlerError;
use crate::message::Message;
use crate::metrics::{DynMetrics, NopMetrics};
use crate::room::Room;
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::error;

/// Reserved protocol event names.
pub const RESERVED_EVENTS: &[&str] = &[
    "join",
    "leave",
    "join_ack",
    "leave_ack",
    "new_member",
    "member_left",
    "open",
    "close",
];

/// Type signature for custom registered asynchronous message handlers.
pub type MessageHandler = Arc<
    dyn Fn(
            Arc<Conn>,
            Message,
        ) -> futures_util::future::BoxFuture<
            'static,
            Result<(), Box<dyn std::error::Error + Send + Sync>>,
        > + Send
        + Sync
        + 'static,
>;

/// Central coordinator managing active connections, room sharding, and cluster routing.
pub struct Hub {
    conns: Arc<DashMap<String, Arc<Conn>>>,
    rooms: Arc<DashMap<String, Arc<Room>>>,
    handlers: DashMap<String, MessageHandler>,
    adapter: Arc<RwLock<DynAdapter>>,
    metrics: std::sync::RwLock<DynMetrics>,
}

impl Default for Hub {
    fn default() -> Self {
        Self {
            conns: Arc::new(DashMap::new()),
            rooms: Arc::new(DashMap::new()),
            handlers: DashMap::new(),
            adapter: Arc::new(RwLock::new(Arc::new(LocalAdapter::default()))),
            metrics: std::sync::RwLock::new(Arc::new(NopMetrics)),
        }
    }
}

impl Hub {
    /// Creates a new `Hub` instance wrapped in an `Arc`.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Configures the distributed cluster adapter and telemetry metrics collector.
    pub async fn configure(&self, adapter: DynAdapter, metrics: DynMetrics) {
        {
            let mut ad = self.adapter.write().await;
            *ad = adapter.clone();
        }
        {
            let mut m = self.metrics.write().unwrap();
            *m = metrics;
        }

        let rooms = Arc::clone(&self.rooms);
        let conns = Arc::clone(&self.conns);
        let _ = adapter
            .subscribe(Arc::new(move |channel_suffix, _sender_node, raw_frame| {
                // Targeted unicast direct messaging: "node:node_UUID" or "root"
                if channel_suffix.starts_with("node:") || channel_suffix == "root" {
                    if let Some(packet) = Message::decode(raw_frame.clone()) {
                        if !packet.dst.is_empty() {
                            if let Some(dst_conn) = conns.get(&packet.dst) {
                                dst_conn.try_send(raw_frame);
                            }
                            return;
                        }
                    }
                }

                // Zero-copy local room fanout directly using raw wire Bytes
                if let Some(room) = rooms.get(channel_suffix) {
                    room.emit_local(None, raw_frame);
                }
            }))
            .await;
    }

    /// Returns a reference to the active metrics collector.
    pub fn metrics(&self) -> DynMetrics {
        self.metrics.read().unwrap().clone()
    }

    /// Registers a custom message handler for a specific event name.
    ///
    /// # Errors
    /// Returns `HandlerError::ReservedEvent` if the name is reserved,
    /// or `HandlerError::DuplicateHandler` if already registered.
    pub fn register_handler(
        &self,
        event: &str,
        handler: MessageHandler,
    ) -> Result<(), HandlerError> {
        if RESERVED_EVENTS.contains(&event) {
            return Err(HandlerError::ReservedEvent(event.to_string()));
        }
        if self.handlers.contains_key(event) {
            return Err(HandlerError::DuplicateHandler(event.to_string()));
        }
        self.handlers.insert(event.to_string(), handler);
        Ok(())
    }

    /// Retrieves an active connection by client ID.
    pub fn get_conn(&self, id: &str) -> Option<Arc<Conn>> {
        self.conns.get(id).map(|r| r.value().clone())
    }

    /// Adds a connection to the active hub registry and tracks node registration.
    pub fn add_conn(&self, conn: Arc<Conn>) {
        self.conns.insert(conn.id.clone(), conn.clone());
        let adapter_lock = self.adapter.clone();
        let conn_id = conn.id.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let adapter = adapter_lock.read().await;
                let _ = adapter.register_node(&conn_id).await;
            });
        }
        self.metrics().on_connect();
    }

    /// Removes a connection from the active hub registry and clears node mapping.
    pub fn remove_conn(&self, id: &str) {
        if self.conns.remove(id).is_some() {
            let adapter_lock = self.adapter.clone();
            let conn_id = id.to_string();
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let adapter = adapter_lock.read().await;
                    let _ = adapter.unregister_node(&conn_id).await;
                });
            }
            self.metrics().on_disconnect();
        }
    }

    /// Retrieves a room by name.
    pub fn get_room(&self, name: &str) -> Option<Arc<Room>> {
        self.rooms.get(name).map(|r| r.value().clone())
    }

    /// Atomically joins a connection into a room, updating cluster presence.
    pub fn join_room(&self, name: &str, conn: Arc<Conn>) {
        let room = self
            .rooms
            .entry(name.to_string())
            .or_insert_with(|| {
                self.metrics().on_room_created(name);
                Room::new(name)
            })
            .value()
            .clone();

        conn.track_room(name);
        room.add_member(conn.clone());

        let adapter_lock = self.adapter.clone();
        let name_str = name.to_string();
        let conn_id = conn.id.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let adapter = adapter_lock.read().await;
                let _ = adapter.add_presence(&name_str, &conn_id).await;
            });
        }

        let new_member_msg = Message::new(
            name,
            "new_member",
            "",
            "",
            Bytes::copy_from_slice(conn.id.as_bytes()),
        );
        self.broadcast_room(Some(&conn.id), new_member_msg);
    }

    /// Removes a connection from a specific room and cleans up empty rooms and presence.
    pub fn leave_room(&self, name: &str, conn: &Arc<Conn>) {
        conn.untrack_room(name);
        if let Some(room) = self.get_room(name) {
            room.remove_member(&conn.id);
            if self.rooms.remove_if(name, |_, r| r.is_empty()).is_some() {
                self.metrics().on_room_deleted(name);
            }

            let adapter_lock = self.adapter.clone();
            let name_str = name.to_string();
            let conn_id = conn.id.clone();
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let adapter = adapter_lock.read().await;
                    let _ = adapter.remove_presence(&name_str, &conn_id).await;
                });
            }

            let left_msg = Message::new(
                name,
                "member_left",
                "",
                "",
                Bytes::copy_from_slice(conn.id.as_bytes()),
            );
            self.broadcast_room(Some(&conn.id), left_msg);
        }
    }

    /// Removes a connection from every room it currently belongs to.
    pub fn leave_all_rooms(&self, conn: &Arc<Conn>) {
        for room_name in conn.joined_rooms() {
            self.leave_room(&room_name, conn);
        }
    }

    /// Retrieves all member connection IDs in a room across the entire cluster.
    pub async fn get_cluster_presence(&self, room_name: &str) -> Vec<String> {
        let mut member_set = HashSet::new();

        if let Some(r) = self.get_room(room_name) {
            for id in r.snapshot() {
                member_set.insert(id);
            }
        }

        let adapter = self.adapter.read().await;
        if let Ok(members) = adapter.get_presence(room_name).await {
            for id in members {
                member_set.insert(id);
            }
        }

        member_set.into_iter().collect()
    }

    /// Sends a direct message to a recipient using targeted node unicast.
    pub fn send_direct_to_cluster(&self, msg: Message) {
        let adapter_lock = self.adapter.clone();
        let dst_id_str = msg.dst.clone();
        let encoded = msg.encode();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let adapter = adapter_lock.read().await;
                if let Ok(Some(target_node)) = adapter.get_node_for_conn(&dst_id_str).await {
                    if adapter
                        .publish_direct_raw(&target_node, &encoded)
                        .await
                        .is_ok()
                    {
                        return;
                    }
                }
                // Fallback to cluster broadcast on root channel
                let _ = adapter.publish_raw("root", &encoded).await;
            });
        }
    }

    /// Broadcasts a message to local room members and publishes to the cluster adapter.
    pub fn broadcast_room(&self, exclude_id: Option<&str>, msg: Message) {
        let encoded = msg.encode();
        if let Some(room) = self.get_room(&msg.room) {
            room.emit_local(exclude_id, encoded.clone());
        }

        let adapter_lock = self.adapter.clone();
        let room_str = msg.room;

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let adapter = adapter_lock.read().await;
                if let Err(err) = adapter.publish_raw(&room_str, &encoded).await {
                    error!(room = %room_str, error = %err, "Failed to publish message to cluster adapter");
                }
            });
        }
    }

    /// Routes and dispatches an incoming packet.
    pub async fn dispatch(&self, conn: Arc<Conn>, mut msg: Message) {
        msg.src = conn.id.clone();

        match msg.event.as_str() {
            "join" => {
                self.join_room(&msg.room, conn.clone());
                let snap = self.get_cluster_presence(&msg.room).await;
                let members_json = serde_json::to_vec(&snap).unwrap_or_else(|_| b"[]".to_vec());
                let ack = Message::new(
                    &msg.room,
                    "join_ack",
                    "",
                    &conn.id,
                    Bytes::from(members_json),
                );
                conn.try_send(ack.encode());
            }
            "leave" => {
                self.leave_room(&msg.room, &conn);
                let ack = Message::new(
                    &msg.room,
                    "leave_ack",
                    "",
                    &conn.id,
                    Bytes::copy_from_slice(conn.id.as_bytes()),
                );
                conn.try_send(ack.encode());
            }
            _ => {
                if !msg.dst.is_empty() {
                    if let Some(dst_conn) = self.get_conn(&msg.dst) {
                        dst_conn.try_send(msg.encode());
                    } else {
                        self.send_direct_to_cluster(msg);
                    }
                    return;
                }

                if let Some(handler) = self.handlers.get(&msg.event) {
                    let h = handler.value().clone();
                    if let Ok(handle) = tokio::runtime::Handle::try_current() {
                        handle.spawn(async move {
                            if let Err(err) = h(conn, msg).await {
                                error!("Handler error: {:?}", err);
                            }
                        });
                    }
                    return;
                }

                self.broadcast_room(Some(&conn.id), msg);
            }
        }
    }

    /// Gracefully broadcasts a `1001 Going Away` close frame to all connections and closes adapters.
    pub async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for entry in self.conns.iter() {
            entry.value().try_send_close(1001, "Server shutting down");
        }
        self.conns.clear();
        self.rooms.clear();
        let adapter = self.adapter.read().await;
        adapter.close().await?;
        Ok(())
    }
}
