use std::time::Duration;

// Include the network module
#[path = "../src/network.rs"]
mod network;

use network::{NetNode, WirePacket};

#[tokio::test]
async fn test_udp_broadcast_same_port() {
    env_logger::try_init().ok();

    // Two nodes on the SAME port with SO_REUSEADDR + SO_REUSEPORT
    let alice = NetNode::new("Alice".into(), 14242).await;
    let bob = NetNode::new("Bob".into(), 14242).await;

    println!("Alice addr: {}", alice.addr_hex);
    println!("Bob addr: {}", bob.addr_hex);

    // Alice sends an announce via UDP broadcast
    let announce = WirePacket::Announce {
        name: "Alice".into(),
        addr: alice.addr_hex.clone(),
    };
    alice.send(&announce).await;

    // Give the packet time to arrive
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Bob should receive it
    let mut received = false;
    for _ in 0..20 {
        if let Some(pkt) = bob.try_recv().await {
            println!("Bob received: {:?}", pkt);
            match pkt {
                WirePacket::Announce { name, addr } => {
                    assert_eq!(name, "Alice");
                    assert_eq!(addr, alice.addr_hex);
                    received = true;
                    break;
                }
                _ => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(received, "Bob should have received Alice's announce via UDP broadcast");
}

#[tokio::test]
async fn test_chat_request_flow() {
    env_logger::try_init().ok();

    let alice = NetNode::new("Alice".into(), 14243).await;
    let bob = NetNode::new("Bob".into(), 14243).await;

    // Alice sends a chat request to Bob
    let request = WirePacket::ChatRequest {
        from_name: "Alice".into(),
        from_addr: alice.addr_hex.clone(),
        to_addr: bob.addr_hex.clone(),
    };
    alice.send(&request).await;

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Bob should receive it
    let mut got_request = false;
    for _ in 0..20 {
        if let Some(pkt) = bob.try_recv().await {
            match pkt {
                WirePacket::ChatRequest { from_name, from_addr, to_addr } => {
                    assert_eq!(from_name, "Alice");
                    assert_eq!(from_addr, alice.addr_hex);
                    assert_eq!(to_addr, bob.addr_hex);
                    got_request = true;
                    break;
                }
                _ => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(got_request, "Bob should have received Alice's chat request");
}