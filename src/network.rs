//! libp2p-based networking module for hearty-p2p Phase 1.
//!
//! Provides the transport stack, network behaviour, and message types
//! used by the main binary.

use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::Boxed;
use libp2p::core::Multiaddr;
use libp2p::identity::Keypair;
use libp2p::kad::store::MemoryStore;
use libp2p::request_response;
use libp2p::swarm::NetworkBehaviour;
use libp2p::{gossipsub, identify, kad, mdns, PeerId, Transport};
use serde::{Deserialize, Serialize};
use std::time::Duration;

// ── Message types ───────────────────────────────────────────────────────────

/// A direct chat message sent via the request-response protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub from: String,
    pub text: String,
    pub timestamp: u64,
}

/// Acknowledgement returned by the recipient of a [`ChatMessage`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatResponse {
    pub received: bool,
}

/// GossipSub topic used for presence / discovery announcements.
pub const GOSSIPSUB_TOPIC: &str = "hearty-p2p/discovery/v1";

// ── Transport ───────────────────────────────────────────────────────────────

/// Build the composite transport: TCP (Noise + Yamux) **or** QUIC,
/// wrapped with DNS resolution.
///
/// QUIC handles its own security (TLS 1.3) and multiplexing internally,
/// so it is **not** chained through the upgrade/authenticate/multiplex
/// pipeline.
pub fn build_transport(keypair: &Keypair) -> Boxed<(PeerId, StreamMuxerBox)> {
    // TCP with Noise (security) + Yamux (multiplexing).
    let tcp = libp2p::tcp::tokio::Transport::new(libp2p::tcp::Config::default())
        .upgrade(libp2p::core::upgrade::Version::V1)
        .authenticate(
            libp2p::noise::Config::new(keypair).expect("noise config"),
        )
        .multiplex(libp2p::yamux::Config::default());

    // QUIC handles its own security (TLS 1.3) and multiplexing internally.
    // Do NOT chain upgrade/authenticate/multiplex on QUIC.
    let quic = libp2p::quic::tokio::Transport::new(libp2p::quic::Config::new(keypair));

    // TCP first, QUIC as fallback.  The two inner transports produce
    // different concrete muxer types, so we unify them to `StreamMuxerBox`
    // within the `map` closure.  We must NOT box the individual transports
    // before `or_transport`, because that wraps the
    // `MultiaddrNotSupported` error and prevents `OrTransport` from
    // falling through to the second transport.
    let or_transport = tcp.or_transport(quic).map(|either, _| match either {
        futures::future::Either::Left((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
        futures::future::Either::Right((peer_id, conn)) => (peer_id, StreamMuxerBox::new(conn)),
    });

    // Wrap with DNS resolution so /dnsaddr/ and /dns4/ multiaddrs work.
    libp2p::dns::tokio::Transport::system(or_transport)
        .expect("DNS transport initialization failed")
        .boxed()
}

// ── Network behaviour ───────────────────────────────────────────────────────

/// The combined network behaviour for the hearty-p2p node.
///
/// `mDNS` is always present; when `--no-local` is set the event loop
/// simply ignores discovered peers. The `NetworkBehaviour` derive macro
/// does not support `Option<T>` fields, so we cannot conditionally omit
/// mDNS at the type level.
#[derive(NetworkBehaviour)]
pub struct AppBehaviour {
    pub mdns: mdns::tokio::Behaviour,
    pub kademlia: kad::Behaviour<MemoryStore>,
    pub identify: identify::Behaviour,
    pub gossipsub: gossipsub::Behaviour,
    pub request_response: request_response::cbor::Behaviour<ChatMessage, ChatResponse>,
}

/// Build the [`AppBehaviour`] with sensible defaults.
pub fn build_behaviour(keypair: &Keypair, peer_id: PeerId) -> AppBehaviour {
    // mDNS — always created; ignored at the event-loop level when --no-local.
    let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id)
        .expect("mDNS initialization failed");

    // Kademlia DHT with an in-memory store.
    let store = MemoryStore::new(peer_id);
    let kademlia = kad::Behaviour::new(peer_id, store);

    // Identify — exchange listen addresses and observed addresses.
    let identify = identify::Behaviour::new(
        identify::Config::new(
            "/hatch-chat/0.1.0".to_string(),
            keypair.public(),
        )
        .with_agent_version("hatch-chat/0.1.0".to_string()),
    );

    // GossipSub — broadcast presence / discovery.
    let mut gossipsub = gossipsub::Behaviour::new(
        gossipsub::MessageAuthenticity::Signed(keypair.clone()),
        gossipsub::Config::default(),
    )
    .expect("gossipsub initialization failed");
    let topic = gossipsub::IdentTopic::new(GOSSIPSUB_TOPIC);
    let _ = gossipsub.subscribe(&topic);

    // Request-Response — direct, reliable messaging (CBOR).
    let request_response = request_response::cbor::Behaviour::new(
        [(
            libp2p::StreamProtocol::new("/hatch-chat/direct/1.0.0"),
            request_response::ProtocolSupport::Full,
        )],
        request_response::Config::default(),
    );

    AppBehaviour {
        mdns,
        kademlia,
        identify,
        gossipsub,
        request_response,
    }
}

// ── Address filtering (--no-local mode) ────────────────────────────────────

/// Returns `true` if *every* IP address in `addr` is a public/internet-routable
/// address. Loopback, private, link-local and unspecified addresses are
/// rejected.
pub fn is_internet_address(addr: &Multiaddr) -> bool {
    for protocol in addr.iter() {
        match protocol {
            libp2p::core::multiaddr::Protocol::Ip4(ip) => {
                if ip.is_loopback() || ip.is_private() || ip.is_link_local() {
                    return false;
                }
            }
            libp2p::core::multiaddr::Protocol::Ip6(ip) => {
                if ip.is_loopback() || ip.is_unspecified() || ip.is_unique_local() {
                    return false;
                }
                // Link-local IPv6 (fe80::/10)
                if ip.segments()[0] & 0xffc0 == 0xfe80 {
                    return false;
                }
            }
            libp2p::core::multiaddr::Protocol::Udp(_) => {}
            libp2p::core::multiaddr::Protocol::Tcp(_) => {}
            _ => {}
        }
    }
    true
}

/// Filter out local addresses when `no_local` is `true`.
pub fn filter_addresses(addrs: Vec<Multiaddr>, no_local: bool) -> Vec<Multiaddr> {
    if no_local {
        addrs
            .into_iter()
            .filter(is_internet_address)
            .collect()
    } else {
        addrs
    }
}

/// Convenience: the Kademlia bootstrap interval (10 minutes).
#[allow(dead_code)]
pub const KADEMLIA_BOOTSTRAP_INTERVAL: Duration = Duration::from_secs(600);