// Tests for the libp2p-based networking module (Phases 1 & 2).

#[path = "../src/network.rs"]
mod network;

use libp2p::core::Multiaddr;
use libp2p::identity::Keypair;
use network::{
    build_behaviour, build_transport, close_relayed_connections, filter_addresses,
    is_direct_address, is_internet_address, ChatMessage, ChatResponse,
};

#[test]
fn test_keypair_and_peer_id() {
    let keypair = Keypair::generate_ed25519();
    let peer_id = keypair.public().to_peer_id();
    // A PeerId should always be derivable from a valid keypair.
    let s = peer_id.to_string();
    assert!(s.starts_with("12D3KooW") || s.len() > 10);
}

#[test]
fn test_build_transport() {
    // Building the transport should not panic and should return the
    // relay client behaviour alongside the boxed transport (Phase 2).
    let keypair = Keypair::generate_ed25519();
    let (transport, relay_client_behaviour) = build_transport(&keypair);
    // The transport is boxed; we just ensure it was created.
    let _ = transport;
    // The relay client behaviour should also have been created.
    let _ = relay_client_behaviour;
}

#[tokio::test]
async fn test_build_behaviour() {
    let keypair = Keypair::generate_ed25519();
    let peer_id = keypair.public().to_peer_id();
    let (_, relay_client_behaviour) = build_transport(&keypair);
    let behaviour = build_behaviour(&keypair, peer_id, relay_client_behaviour);
    // Verify all Phase 2 components are present by checking the NAT status
    // (AutoNAT starts in Unknown state).
    assert_eq!(
        behaviour.autonat.nat_status(),
        libp2p::autonat::NatStatus::Unknown
    );
}

#[test]
fn test_chat_message_serialization() {
    let msg = ChatMessage {
        from: "alice".into(),
        text: "hello world".into(),
        timestamp: 1234567890,
    };
    // Use serde_json since it's a direct dependency; the wire format is CBOR
    // but serde compatibility is what matters here.
    let json = serde_json::to_string(&msg).expect("serialize");
    let decoded: ChatMessage = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(decoded.from, "alice");
    assert_eq!(decoded.text, "hello world");
    assert_eq!(decoded.timestamp, 1234567890);
}

#[test]
fn test_chat_response_serialization() {
    let resp = ChatResponse { received: true };
    let json = serde_json::to_string(&resp).expect("serialize");
    let decoded: ChatResponse = serde_json::from_str(&json).expect("deserialize");
    assert!(decoded.received);
}

#[test]
fn test_is_internet_address_loopback() {
    let addr: Multiaddr = "/ip4/127.0.0.1/tcp/4001".parse().unwrap();
    assert!(!is_internet_address(&addr));
}

#[test]
fn test_is_internet_address_private() {
    let addr: Multiaddr = "/ip4/192.168.1.5/tcp/4001".parse().unwrap();
    assert!(!is_internet_address(&addr));
    let addr: Multiaddr = "/ip4/10.0.0.1/tcp/4001".parse().unwrap();
    assert!(!is_internet_address(&addr));
    let addr: Multiaddr = "/ip4/172.16.0.1/tcp/4001".parse().unwrap();
    assert!(!is_internet_address(&addr));
}

#[test]
fn test_is_internet_address_public() {
    let addr: Multiaddr = "/ip4/1.2.3.4/tcp/4001".parse().unwrap();
    assert!(is_internet_address(&addr));
}

#[test]
fn test_is_internet_address_ipv6_loopback() {
    let addr: Multiaddr = "/ip6/::1/tcp/4001".parse().unwrap();
    assert!(!is_internet_address(&addr));
}

#[test]
fn test_filter_addresses_no_local() {
    let addrs: Vec<Multiaddr> = vec![
        "/ip4/127.0.0.1/tcp/4001".parse().unwrap(),
        "/ip4/1.2.3.4/tcp/4001".parse().unwrap(),
        "/ip4/192.168.1.1/tcp/4001".parse().unwrap(),
    ];
    let filtered = filter_addresses(addrs, true);
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].to_string(), "/ip4/1.2.3.4/tcp/4001");
}

#[test]
fn test_filter_addresses_all_local() {
    let addrs: Vec<Multiaddr> = vec![
        "/ip4/127.0.0.1/tcp/4001".parse().unwrap(),
        "/ip4/192.168.1.1/tcp/4001".parse().unwrap(),
    ];
    let filtered = filter_addresses(addrs, true);
    assert!(filtered.is_empty());
}

#[test]
fn test_filter_addresses_disabled() {
    let addrs: Vec<Multiaddr> = vec![
        "/ip4/127.0.0.1/tcp/4001".parse().unwrap(),
        "/ip4/1.2.3.4/tcp/4001".parse().unwrap(),
    ];
    // When no_local is false, nothing should be filtered.
    let filtered = filter_addresses(addrs, false);
    assert_eq!(filtered.len(), 2);
}

// ── Phase 2 tests: is_direct_address & close_relayed_connections ───────────

#[test]
fn test_is_direct_address_tcp() {
    let addr: Multiaddr = "/ip4/1.2.3.4/tcp/4001".parse().unwrap();
    assert!(is_direct_address(&addr));
}

#[test]
fn test_is_direct_address_quic() {
    let addr: Multiaddr = "/ip4/1.2.3.4/udp/4001/quic-v1".parse().unwrap();
    assert!(is_direct_address(&addr));
}

#[test]
fn test_is_direct_address_relayed() {
    // A relayed address contains /p2p-circuit. Use real PeerIds.
    let keypair = Keypair::generate_ed25519();
    let relay_peer_id = keypair.public().to_peer_id();
    let dst_keypair = Keypair::generate_ed25519();
    let dst_peer_id = dst_keypair.public().to_peer_id();
    let addr: Multiaddr = format!(
        "/ip4/1.2.3.4/tcp/4001/p2p/{relay_peer_id}/p2p-circuit/p2p/{dst_peer_id}"
    )
    .parse()
    .unwrap();
    assert!(!is_direct_address(&addr));
}

#[test]
fn test_is_direct_address_p2p_circuit_only() {
    let addr: Multiaddr = "/p2p-circuit".parse().unwrap();
    assert!(!is_direct_address(&addr));
}

#[tokio::test]
async fn test_close_relayed_connections_no_op() {
    // When there are no tracked relayed connections, the function should
    // be a no-op (just removes the empty entry if present).
    let keypair = Keypair::generate_ed25519();
    let peer_id = keypair.public().to_peer_id();
    let (transport, relay_client_behaviour) = build_transport(&keypair);
    let behaviour = build_behaviour(&keypair, peer_id, relay_client_behaviour);
    let mut swarm = libp2p::Swarm::new(
        transport,
        behaviour,
        peer_id,
        libp2p::swarm::Config::with_tokio_executor(),
    );
    let mut relayed_conns: std::collections::HashMap<libp2p::PeerId, Vec<libp2p::swarm::ConnectionId>> =
        std::collections::HashMap::new();
    // No tracked relayed connections — should not panic.
    close_relayed_connections(&mut swarm, &mut relayed_conns, peer_id);
    assert!(relayed_conns.is_empty());
}