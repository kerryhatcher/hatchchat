// Tests for the persistent peer cache (Phase 3).

#[path = "../src/peer_cache.rs"]
mod peer_cache;

use peer_cache::{current_timestamp, PeerCache, PeerRecord};
use std::path::PathBuf;

/// Create a unique temp directory for this test run.
fn temp_dir(name: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "hatch-chat-test-{}-{}-{}",
        std::process::id(),
        name,
        current_timestamp()
    ));
    dir
}

fn make_record(peer_id: &str, addrs: &[&str], last_seen: u64) -> PeerRecord {
    PeerRecord {
        peer_id: peer_id.to_string(),
        multiaddrs: addrs.iter().map(|s| s.to_string()).collect(),
        i2p_destination: None,
        last_seen,
        connection_count: 1,
        rtt_ms: Some(50),
        is_relay: false,
        is_public: true,
    }
}

#[test]
fn test_peer_cache_open() {
    let dir = temp_dir("open");
    let cache = PeerCache::open(&dir);
    assert!(cache.is_ok(), "PeerCache::open should succeed");
    // The database file should exist.
    assert!(dir.join("peer_cache.redb").exists());
}

#[test]
fn test_peer_cache_save_and_get() {
    let dir = temp_dir("save_get");
    let cache = PeerCache::open(&dir).expect("open");

    let record = make_record("12D3KooWTest", &["/ip4/1.2.3.4/tcp/4001"], current_timestamp());
    cache.save_peer(&record).expect("save");

    let got = cache.get_peer("12D3KooWTest").expect("get");
    assert!(got.is_some(), "peer should exist in cache");
    let got = got.unwrap();
    assert_eq!(got.peer_id, "12D3KooWTest");
    assert_eq!(got.multiaddrs, vec!["/ip4/1.2.3.4/tcp/4001"]);
    assert_eq!(got.connection_count, 1);
    assert_eq!(got.rtt_ms, Some(50));
    assert!(got.is_public);
}

#[test]
fn test_peer_cache_get_nonexistent() {
    let dir = temp_dir("get_nonexistent");
    let cache = PeerCache::open(&dir).expect("open");

    let got = cache.get_peer("nonexistent").expect("get");
    assert!(got.is_none(), "nonexistent peer should return None");
}

#[test]
fn test_peer_cache_all_peers() {
    let dir = temp_dir("all_peers");
    let cache = PeerCache::open(&dir).expect("open");

    // Start empty.
    let all = cache.all_peers().expect("all");
    assert!(all.is_empty(), "cache should start empty");

    // Save three peers.
    let now = current_timestamp();
    cache
        .save_peer(&make_record("peer1", &["/ip4/1.1.1.1/tcp/4001"], now))
        .expect("save");
    cache
        .save_peer(&make_record("peer2", &["/ip4/2.2.2.2/tcp/4001"], now))
        .expect("save");
    cache
        .save_peer(&make_record("peer3", &["/ip4/3.3.3.3/tcp/4001"], now))
        .expect("save");

    let all = cache.all_peers().expect("all");
    assert_eq!(all.len(), 3, "cache should have 3 peers");
}

#[test]
fn test_peer_cache_update_existing() {
    let dir = temp_dir("update");
    let cache = PeerCache::open(&dir).expect("open");

    let now = current_timestamp();
    let mut record = make_record("peer1", &["/ip4/1.1.1.1/tcp/4001"], now);
    cache.save_peer(&record).expect("save");

    // Update with new address and higher connection count.
    record.multiaddrs.push("/ip4/1.1.1.1/udp/4001/quic-v1".to_string());
    record.connection_count = 5;
    cache.save_peer(&record).expect("update");

    let got = cache.get_peer("peer1").expect("get").unwrap();
    assert_eq!(got.multiaddrs.len(), 2, "should have 2 addresses");
    assert_eq!(got.connection_count, 5, "connection count should be updated");
}

#[test]
fn test_peer_cache_prune_stale() {
    let dir = temp_dir("prune");
    let cache = PeerCache::open(&dir).expect("open");

    let now = current_timestamp();
    let old_time = now - 8 * 24 * 3600; // 8 days ago
    let recent_time = now - 3600; // 1 hour ago

    // Save a stale peer (8 days old).
    cache
        .save_peer(&make_record("stale_peer", &["/ip4/1.1.1.1/tcp/4001"], old_time))
        .expect("save");
    // Save a recent peer (1 hour old).
    cache
        .save_peer(&make_record("recent_peer", &["/ip4/2.2.2.2/tcp/4001"], recent_time))
        .expect("save");

    assert_eq!(cache.all_peers().expect("all").len(), 2);

    // Prune peers older than 7 days.
    cache.prune_stale(7 * 24 * 3600).expect("prune");

    let all = cache.all_peers().expect("all");
    assert_eq!(all.len(), 1, "only recent peer should remain");
    assert_eq!(all[0].peer_id, "recent_peer");
}

#[test]
fn test_peer_cache_survives_reopen() {
    let dir = temp_dir("reopen");
    let now = current_timestamp();

    // Save a peer.
    {
        let cache = PeerCache::open(&dir).expect("open");
        cache
            .save_peer(&make_record("persistent_peer", &["/ip4/9.9.9.9/tcp/4001"], now))
            .expect("save");
    }

    // Reopen and verify the peer is still there.
    {
        let cache = PeerCache::open(&dir).expect("reopen");
        let got = cache.get_peer("persistent_peer").expect("get");
        assert!(got.is_some(), "peer should survive reopen");
        assert_eq!(got.unwrap().peer_id, "persistent_peer");
    }
}

#[test]
fn test_peer_record_serialization() {
    let record = PeerRecord {
        peer_id: "12D3KooWTest".to_string(),
        multiaddrs: vec!["/ip4/1.2.3.4/tcp/4001".to_string()],
        i2p_destination: Some("i2p_dest_string".to_string()),
        last_seen: 1234567890,
        connection_count: 42,
        rtt_ms: None,
        is_relay: true,
        is_public: false,
    };

    let json = serde_json::to_string(&record).expect("serialize");
    let decoded: PeerRecord = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(decoded.peer_id, "12D3KooWTest");
    assert_eq!(decoded.multiaddrs, vec!["/ip4/1.2.3.4/tcp/4001"]);
    assert_eq!(decoded.i2p_destination, Some("i2p_dest_string".to_string()));
    assert_eq!(decoded.last_seen, 1234567890);
    assert_eq!(decoded.connection_count, 42);
    assert_eq!(decoded.rtt_ms, None);
    assert!(decoded.is_relay);
    assert!(!decoded.is_public);
}