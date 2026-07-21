//! Parallel discovery orchestrator and strategies.
//!
//! Phase 3 — Resilient Discovery.
//!
//! Each discovery strategy implements [`DiscoveryStrategy`].  The
//! [`DiscoveryOrchestrator`] runs all registered strategies concurrently,
//! collects the results, and deduplicates by PeerId.

use crate::peer_cache::{PeerCache, PeerRecord};
use async_trait::async_trait;
use futures::stream::{FuturesUnordered, StreamExt};
use std::sync::Arc;

// ── Bootstrap configuration ─────────────────────────────────────────────────

/// Configuration for the bootstrap-node fallback strategy.
#[derive(Debug, Clone)]
pub struct BootstrapConfig {
    /// Seed node multiaddrs, e.g.
    /// `["/ip4/seed1.hearty.io/tcp/4001/p2p/QmSeed1..."]`
    pub nodes: Vec<String>,
    /// How many bootstrap nodes to dial in parallel.
    pub parallel_dials: usize,
    /// Timeout for bootstrap dial attempts (seconds).
    #[allow(dead_code)]
    pub timeout_secs: u64,
    /// Stop bootstrapping after the first successful connection.
    #[allow(dead_code)]
    pub stop_after_first: bool,
}

impl Default for BootstrapConfig {
    fn default() -> Self {
        Self {
            nodes: Vec::new(),
            parallel_dials: 3,
            timeout_secs: 30,
            stop_after_first: true,
        }
    }
}

// ── Strategy trait ──────────────────────────────────────────────────────────

/// A single discovery strategy that can be run concurrently with others.
#[async_trait]
pub trait DiscoveryStrategy: Send + Sync {
    /// Execute the strategy, returning a list of discovered peer records.
    async fn execute(&self) -> Result<Vec<PeerRecord>, Box<dyn std::error::Error + Send + Sync>>;
    /// Human-readable name for logging.
    fn name(&self) -> &'static str;
}

// ── Orchestrator ────────────────────────────────────────────────────────────

/// Runs all registered discovery strategies concurrently and merges the
/// results, deduplicating by PeerId.
pub struct DiscoveryOrchestrator {
    strategies: Vec<Box<dyn DiscoveryStrategy>>,
}

impl DiscoveryOrchestrator {
    /// Create an empty orchestrator.
    pub fn new() -> Self {
        Self {
            strategies: Vec::new(),
        }
    }

    /// Register a discovery strategy.
    pub fn add_strategy(&mut self, strategy: Box<dyn DiscoveryStrategy>) {
        self.strategies.push(strategy);
    }

    /// Run all strategies in parallel, merge and deduplicate results.
    pub async fn discover(&self) -> Vec<PeerRecord> {
        let mut tasks = FuturesUnordered::new();

        for strategy in &self.strategies {
            tasks.push(async move {
                match strategy.execute().await {
                    Ok(peers) => {
                        if !peers.is_empty() {
                            tracing::info!(
                                "Discovery strategy '{}' found {} peers",
                                strategy.name(),
                                peers.len()
                            );
                        }
                        peers
                    }
                    Err(e) => {
                        tracing::warn!("Discovery strategy '{}' failed: {e}", strategy.name());
                        Vec::new()
                    }
                }
            });
        }

        let mut all_peers = Vec::new();
        while let Some(result) = tasks.next().await {
            all_peers.extend(result);
        }

        // Deduplicate by PeerId.
        all_peers.sort_by(|a, b| a.peer_id.cmp(&b.peer_id));
        all_peers.dedup_by(|a, b| a.peer_id == b.peer_id);
        all_peers
    }
}

impl Default for DiscoveryOrchestrator {
    fn default() -> Self {
        Self::new()
    }
}

// ── Concrete strategies ─────────────────────────────────────────────────────

/// Strategy that loads peers from the persistent [`PeerCache`].
pub struct PeerCacheStrategy {
    cache: Arc<PeerCache>,
}

impl PeerCacheStrategy {
    pub fn new(cache: Arc<PeerCache>) -> Self {
        Self { cache }
    }
}

#[async_trait]
impl DiscoveryStrategy for PeerCacheStrategy {
    async fn execute(&self) -> Result<Vec<PeerRecord>, Box<dyn std::error::Error + Send + Sync>> {
        let peers = self.cache.all_peers()?;
        if !peers.is_empty() {
            tracing::info!("PeerCache: loaded {} cached peers", peers.len());
        }
        Ok(peers)
    }

    fn name(&self) -> &'static str {
        "peer-cache"
    }
}

/// Strategy that parses configured bootstrap node multiaddrs and returns
/// them as [`PeerRecord`]s.  The actual dialing is performed by the caller
/// (the main event loop) after the orchestrator returns results.
pub struct BootstrapStrategy {
    config: BootstrapConfig,
}

impl BootstrapStrategy {
    pub fn new(config: BootstrapConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl DiscoveryStrategy for BootstrapStrategy {
    async fn execute(&self) -> Result<Vec<PeerRecord>, Box<dyn std::error::Error + Send + Sync>> {
        let now = crate::peer_cache::current_timestamp();
        let mut peers = Vec::new();
        for node_addr in self.config.nodes.iter().take(self.config.parallel_dials) {
            // Parse the multiaddr to extract PeerId and full address.
            if let Ok(ma) = node_addr.parse::<libp2p::Multiaddr>() {
                let peer_id = extract_peer_id_from_multiaddr(&ma);
                if let Some(pid) = peer_id {
                    peers.push(PeerRecord {
                        peer_id: pid.to_string(),
                        multiaddrs: vec![node_addr.clone()],
                        i2p_destination: None,
                        last_seen: now,
                        connection_count: 0,
                        rtt_ms: None,
                        is_relay: false,
                        is_public: true,
                    });
                }
            }
        }
        Ok(peers)
    }

    fn name(&self) -> &'static str {
        "bootstrap"
    }
}

/// Extract the [`libp2p::PeerId`] from a `Multiaddr` ending in `/p2p/<PeerId>`.
fn extract_peer_id_from_multiaddr(addr: &libp2p::Multiaddr) -> Option<libp2p::PeerId> {
    for protocol in addr.iter() {
        if let libp2p::core::multiaddr::Protocol::P2p(peer_id) = protocol {
            return Some(peer_id);
        }
    }
    None
}