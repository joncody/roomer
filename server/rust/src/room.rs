//! Lock-striped concurrent room member management.

use crate::conn::Conn;
use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;

/// A lock-striped concurrent room managing member connections.
pub struct Room {
    /// Room channel name.
    pub name: String,
    /// Partitioned lock-striped map of connected members keyed by connection ID.
    pub members: DashMap<String, Arc<Conn>>,
}

impl Room {
    /// Creates a new `Room` wrapped in an `Arc`.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            name: name.into(),
            members: DashMap::new(),
        })
    }

    /// Inserts a connection into the room.
    pub fn add_member(&self, conn: Arc<Conn>) {
        self.members.insert(conn.id.clone(), conn);
    }

    /// Removes a member from the room by ID. Returns `true` if the room is now empty.
    pub fn remove_member(&self, conn_id: &str) -> bool {
        self.members.remove(conn_id);
        self.members.is_empty()
    }

    /// Checks if a member is currently present in the room.
    #[must_use]
    pub fn has_member(&self, conn_id: &str) -> bool {
        self.members.contains_key(conn_id)
    }

    /// Broadcasts binary data to local room members, optionally excluding a sender ID.
    pub fn emit_local(&self, exclude_id: Option<&str>, data: Bytes) {
        for entry in self.members.iter() {
            let id = entry.key();
            let member = entry.value();
            if let Some(excluded) = exclude_id {
                if id == excluded {
                    continue;
                }
            }
            member.try_send(data.clone());
        }
    }

    /// Returns a snapshot vector of current member connection IDs.
    #[must_use]
    pub fn snapshot(&self) -> Vec<String> {
        self.members.iter().map(|kv| kv.key().clone()).collect()
    }

    /// Returns `true` if the room has no members.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Returns the number of members currently in the room.
    #[must_use]
    pub fn len(&self) -> usize {
        self.members.len()
    }
}
