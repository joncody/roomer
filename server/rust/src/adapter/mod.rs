//! Multi-node distributed adapters (Local in-memory, Redis Pub/Sub) with presence sets and unicast routing.

use crate::error::AdapterError;
use crate::message::Message;
use async_trait::async_trait;
use bytes::Bytes;
use dashmap::{DashMap, DashSet};
use std::sync::Arc;

/// Callback type signature for cluster subscription listeners: `(channel_suffix, sender_node_id, raw_frame)`.
pub type SubscribeCallback = Arc<dyn Fn(&str, &str, Bytes) + Send + Sync + 'static>;

/// Trait defining horizontal scaling message broadcast and cluster presence adapters.
#[async_trait]
pub trait Adapter: Send + Sync + 'static {
    /// Publishes a message to all cluster nodes for a specified room.
    async fn publish(&self, room: &str, msg: &Message) -> Result<(), AdapterError> {
        self.publish_raw(room, &msg.encode()).await
    }

    /// Publishes a raw binary message frame directly to all cluster nodes for a specified room.
    async fn publish_raw(&self, room: &str, raw_msg: &[u8]) -> Result<(), AdapterError>;

    /// Publishes a message directly to a specific target cluster node (unicast).
    async fn publish_direct(
        &self,
        target_node_id: &str,
        msg: &Message,
    ) -> Result<(), AdapterError> {
        self.publish_direct_raw(target_node_id, &msg.encode()).await
    }

    /// Publishes a raw binary message frame directly to a specific target cluster node (unicast).
    async fn publish_direct_raw(
        &self,
        target_node_id: &str,
        raw_msg: &[u8],
    ) -> Result<(), AdapterError>;

    /// Subscribes to cluster messages and invokes the given callback.
    async fn subscribe(&self, callback: SubscribeCallback) -> Result<(), AdapterError>;

    /// Adds a connection ID to a room's cluster-wide presence set.
    async fn add_presence(&self, room: &str, conn_id: &str) -> Result<(), AdapterError>;

    /// Removes a connection ID from a room's cluster-wide presence set.
    async fn remove_presence(&self, room: &str, conn_id: &str) -> Result<(), AdapterError>;

    /// Retrieves all connection IDs in a room across the entire cluster.
    async fn get_presence(&self, room: &str) -> Result<Vec<String>, AdapterError>;

    /// Updates the last-seen heartbeat timestamp for a connection across the specified rooms.
    async fn touch_presence(&self, conn_id: &str, rooms: &[String]) -> Result<(), AdapterError> {
        let _ = (conn_id, rooms);
        Ok(())
    }

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
    async fn publish_raw(&self, _room: &str, _raw_msg: &[u8]) -> Result<(), AdapterError> {
        Ok(())
    }
    async fn publish_direct(
        &self,
        _target_node_id: &str,
        _msg: &Message,
    ) -> Result<(), AdapterError> {
        Ok(())
    }
    async fn publish_direct_raw(
        &self,
        _target_node_id: &str,
        _raw_msg: &[u8],
    ) -> Result<(), AdapterError> {
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
    async fn touch_presence(&self, _conn_id: &str, _rooms: &[String]) -> Result<(), AdapterError> {
        Ok(())
    }
    async fn register_node(&self, conn_id: &str) -> Result<(), AdapterError> {
        self.node_map
            .insert(conn_id.to_string(), "local-node".into());
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
pub mod redis;
