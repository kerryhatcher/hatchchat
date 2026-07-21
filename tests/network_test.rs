// Tests for the libp2p-based networking module (Phase 1).

#[path = "../src/network.rs"]
mod network;

use libp2p::core::Multiaddr;
use libp2p::identity::Keypair;
use network::{
    build_behaviour, build_transport, filter_addresses, is_internet_address, ChatMessage,
    ChatResponse,
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
    // Building the transport should not panic.
    let keypair = Keypair::generate_ed25519();
    let _transport = build_transport(&keypair);
}

#[tokio::test]
async fn test_build_behaviour() {
    let keypair = Keypair::generate_ed25519();
    let peer_id = keypair.public().to_peer_id();
    let _behaviour = build_behaviour(&keypair, peer_id);
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