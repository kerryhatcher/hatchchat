// Tests for the discovery orchestrator and strategies (Phase 3).

#[path = "../src/peer_cache.rs"]
mod peer_cache;

#[path = "../src/discovery.rs"]
mod discovery;

use discovery::{BootstrapConfig, BootstrapStrategy, DiscoveryOrchestrator, DiscoveryStrategy};
use peer_cache::{current_timestamp, PeerCache, PeerRecord};
use std::path::PathBuf;
use std::sync::Arc;

/// Unique temp directory for this test.
fn temp_dir(name: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "hatch-chat-disco-{}-{}-{}",
        std::process::id(),
        name,
        current_timestamp()
    ));
    dir
}

/// A mock strategy that returns a fixed set of peers.
struct MockStrategy {
    name: &'static str,
    peers: Vec<PeerRecord>,
}

#[async_trait::async_trait]
impl DiscoveryStrategy for MockStrategy {
    async fn execute(&self) -> Result<Vec<PeerRecord>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.peers.clone())
    }

    fn name(&self) -> &'static str {
        self.name
    }
}

/// A mock strategy that always fails.
struct FailingStrategy;

#[async_trait::async_trait]
impl DiscoveryStrategy for FailingStrategy {
    async fn execute(&self) -> Result<Vec<PeerRecord>, Box<dyn std::error::Error + Send + Sync>> {
        Err("mock failure".into())
    }

    fn name(&self) -> &'static str {
        "failing"
    }
}

fn make_record(peer_id: &str) -> PeerRecord {
    PeerRecord {
        peer_id: peer_id.to_string(),
        multiaddrs: vec!["/ip4/1.2.3.4/tcp/4001".to_string()],
        i2p_destination: None,
        last_seen: current_timestamp(),
        connection_count: 1,
        rtt_ms: None,
        is_relay: false,
        is_public: true,
    }
}

#[tokio::test]
async fn test_orchestrator_empty() {
    let orchestrator = DiscoveryOrchestrator::new();
    let result = orchestrator.discover().await;
    assert!(result.is_empty(), "empty orchestrator should return no peers");
}

#[tokio::test]
async fn test_orchestrator_single_strategy() {
    let mut orchestrator = DiscoveryOrchestrator::new();
    orchestrator.add_strategy(Box::new(MockStrategy {
        name: "mock-a",
        peers: vec![make_record("peerA"), make_record("peerB")],
    }));

    let result = orchestrator.discover().await;
    assert_eq!(result.len(), 2, "should return 2 peers from one strategy");
}

#[tokio::test]
async fn test_orchestrator_dedup() {
    let mut orchestrator = DiscoveryOrchestrator::new();
    orchestrator.add_strategy(Box::new(MockStrategy {
        name: "mock-a",
        peers: vec![make_record("peerA"), make_record("peerB")],
    }));
    orchestrator.add_strategy(Box::new(MockStrategy {
        name: "mock-b",
        peers: vec![make_record("peerB"), make_record("peerC")],
    }));

    let result = orchestrator.discover().await;
    // peerB is returned by both strategies; should be deduplicated.
    assert_eq!(result.len(), 3, "should have 3 unique peers after dedup");
    let ids: Vec<&str> = result.iter().map(|r| r.peer_id.as_str()).collect();
    assert!(ids.contains(&"peerA"));
    assert!(ids.contains(&"peerB"));
    assert!(ids.contains(&"peerC"));
}

#[tokio::test]
async fn test_orchestrator_strategy_failure_doesnt_block() {
    let mut orchestrator = DiscoveryOrchestrator::new();
    orchestrator.add_strategy(Box::new(FailingStrategy));
    orchestrator.add_strategy(Box::new(MockStrategy {
        name: "mock-success",
        peers: vec![make_record("peerA")],
    }));

    let result = orchestrator.discover().await;
    // The failing strategy should not block the successful one.
    assert_eq!(result.len(), 1, "should still get results from the working strategy");
    assert_eq!(result[0].peer_id, "peerA");
}

#[tokio::test]
async fn test_peer_cache_strategy() {
    let dir = temp_dir("cache_strategy");
    let cache = Arc::new(PeerCache::open(&dir).expect("open"));

    // Save a couple of peers.
    cache.save_peer(&make_record("cached1")).expect("save");
    cache.save_peer(&make_record("cached2")).expect("save");

    let strategy = discovery::PeerCacheStrategy::new(cache);
    let result = strategy.execute().await.expect("execute");
    assert_eq!(result.len(), 2, "should load 2 cached peers");
}

#[tokio::test]
async fn test_bootstrap_strategy() {
    // Generate a real PeerId for the multiaddr.
    let keypair = libp2p::identity::Keypair::generate_ed25519();
    let peer_id = keypair.public().to_peer_id();
    let addr = format!("/ip4/1.2.3.4/tcp/4001/p2p/{peer_id}");

    let config = BootstrapConfig {
        nodes: vec![addr.clone()],
        parallel_dials: 3,
        timeout_secs: 30,
        stop_after_first: true,
    };
    let strategy = BootstrapStrategy::new(config);
    let result = strategy.execute().await.expect("execute");
    assert_eq!(result.len(), 1, "should parse 1 bootstrap node");
    assert_eq!(result[0].peer_id, peer_id.to_string());
    assert_eq!(result[0].multiaddrs, vec![addr]);
}

#[tokio::test]
async fn test_bootstrap_strategy_invalid_addr() {
    let config = BootstrapConfig {
        nodes: vec!["not-a-multiaddr".to_string()],
        ..Default::default()
    };
    let strategy = BootstrapStrategy::new(config);
    let result = strategy.execute().await.expect("execute");
    assert!(result.is_empty(), "invalid multiaddr should yield no peers");
}

#[tokio::test]
async fn test_orchestrator_with_cache_and_bootstrap() {
    let dir = temp_dir("cache_and_bootstrap");
    let cache = Arc::new(PeerCache::open(&dir).expect("open"));
    cache.save_peer(&make_record("cached_peer")).expect("save");

    let keypair = libp2p::identity::Keypair::generate_ed25519();
    let bootstrap_peer_id = keypair.public().to_peer_id();
    let bootstrap_addr = format!("/ip4/10.0.0.1/tcp/4001/p2p/{bootstrap_peer_id}");

    let mut orchestrator = DiscoveryOrchestrator::new();
    orchestrator.add_strategy(Box::new(discovery::PeerCacheStrategy::new(cache)));
    orchestrator.add_strategy(Box::new(BootstrapStrategy::new(BootstrapConfig {
        nodes: vec![bootstrap_addr],
        ..Default::default()
    })));

    let result = orchestrator.discover().await;
    assert_eq!(result.len(), 2, "should have cached + bootstrap peers");
    let ids: Vec<&str> = result.iter().map(|r| r.peer_id.as_str()).collect();
    assert!(ids.contains(&"cached_peer"));
    assert!(ids.contains(&bootstrap_peer_id.to_string().as_str()));
}