//! Persistent peer address store backed by an embedded redb database.
//!
//! Phase 3 — Resilient Discovery.
//!
//! [`PeerRecord`] captures everything we know about a remote peer so that
//! we can reconnect after an app restart without re-discovering from
//! scratch.  [`PeerCache`] wraps a redb database file on disk.

use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::path::Path;

// ── Record ──────────────────────────────────────────────────────────────────

/// All the information we persist about a single remote peer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerRecord {
    /// Base58-encoded [`libp2p::PeerId`].
    pub peer_id: String,
    /// Every known multiaddress for this peer (as strings).
    pub multiaddrs: Vec<String>,
    /// I2P destination string — reserved for Phase 4.
    #[serde(default)]
    pub i2p_destination: Option<String>,
    /// Unix timestamp (seconds) of the last successful connection.
    pub last_seen: u64,
    /// How many times we have connected to this peer.
    pub connection_count: u32,
    /// Measured round-trip time in milliseconds, if known.
    #[serde(default)]
    pub rtt_ms: Option<u32>,
    /// Does this peer offer Circuit Relay v2 service?
    #[serde(default)]
    pub is_relay: bool,
    /// Is this peer publicly reachable (not behind NAT)?
    #[serde(default)]
    pub is_public: bool,
}

// ── Cache ───────────────────────────────────────────────────────────────────

/// redb table definition: peer-id-string → JSON-encoded `PeerRecord`.
const PEER_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("peers");

/// Persistent peer address store.
///
/// All methods are `&self` — redb uses internal locking, so concurrent
/// reads and writes from multiple threads are safe.
pub struct PeerCache {
    db: Database,
}

impl PeerCache {
    /// Open (or create) a peer cache inside the given *directory*.
    ///
    /// The directory is created if it does not exist.  The database file
    /// is placed at `<dir>/peer_cache.redb`.
    pub fn open(dir: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        std::fs::create_dir_all(dir).ok();
        let db_path = dir.join("peer_cache.redb");
        let db = Database::create(&db_path)?;

        // Ensure the table exists.
        let txn = db.begin_write()?;
        let _ = txn.open_table(PEER_TABLE)?;
        txn.commit()?;

        Ok(Self { db })
    }

    /// Insert or update a peer record.
    pub fn save_peer(&self, record: &PeerRecord) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(PEER_TABLE)?;
            let value = serde_json::to_vec(record)?;
            table.insert(record.peer_id.as_str(), value.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Look up a single peer by base58 PeerId string.
    pub fn get_peer(
        &self,
        peer_id: &str,
    ) -> Result<Option<PeerRecord>, Box<dyn std::error::Error + Send + Sync>> {
        let txn = self.db.begin_read()?;
        let table = match txn.open_table(PEER_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        match table.get(peer_id)? {
            Some(v) => Ok(Some(serde_json::from_slice(v.value())?)),
            None => Ok(None),
        }
    }

    /// Return every cached peer record.
    pub fn all_peers(&self) -> Result<Vec<PeerRecord>, Box<dyn std::error::Error + Send + Sync>> {
        let txn = self.db.begin_read()?;
        let table = match txn.open_table(PEER_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let mut peers = Vec::new();
        for item in table.iter()? {
            let (_, v) = item?;
            let record: PeerRecord = serde_json::from_slice(v.value())?;
            peers.push(record);
        }
        Ok(peers)
    }

    /// Remove peers whose `last_seen` is older than `max_age_secs` ago.
    pub fn prune_stale(
        &self,
        max_age_secs: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let now = current_timestamp();
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(PEER_TABLE)?;
            let mut to_remove = Vec::new();
            for item in table.iter()? {
                let (k, v) = item?;
                let record: PeerRecord = serde_json::from_slice(v.value())?;
                if now.saturating_sub(record.last_seen) > max_age_secs {
                    to_remove.push(k.value().to_string());
                }
            }
            for k in to_remove {
                table.remove(k.as_str())?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// Number of cached peers.
    #[allow(dead_code)]
    pub fn len(&self) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.all_peers()?.len())
    }

    /// Whether the cache is empty.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.all_peers()?.is_empty())
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Current Unix timestamp in seconds.
pub fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}