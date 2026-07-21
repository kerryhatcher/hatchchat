//! libp2p-based networking module for hearty-p2p.
//!
//! Provides the transport stack, network behaviour, and message types
//! used by the main binary. Phase 2 adds AutoNAT, Circuit Relay v2
//! (client + server), and DCUtR for NAT traversal.

use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::Boxed;
use libp2p::core::Multiaddr;
use libp2p::core::multiaddr::Protocol as MultiaddrProtocol;
use libp2p::identity::Keypair;
use libp2p::kad::store::MemoryStore;
use libp2p::request_response;
use libp2p::swarm::NetworkBehaviour;
use libp2p::{autonat, dcutr, gossipsub, identify, kad, mdns, relay, PeerId, Transport};
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

/// Build the composite transport and the corresponding relay **client**
/// behaviour.
///
/// The transport stack is:
///
/// ```text
/// Relay-client  ─┐
/// TCP (Noise+Yamux) ─┤── upgrade/authenticate/multiplex ──┐
///                   ────────────────────────────────────────┤── or_transport ── QUIC ── DNS ── Boxed
/// ```
///
/// Both the relay-client transport and the bare TCP transport produce raw
/// `AsyncRead + AsyncWrite` connections, so they are unified *before* the
/// upgrade pipeline and share the same Noise + Yamux layers. QUIC handles
/// its own security (TLS 1.3) and multiplexing internally and is therefore
/// added after the upgrade pipeline.
///
/// The relay client [`relay::client::Behaviour`] is returned alongside the
/// transport because the two are created as a pair by
/// [`relay::client::new`].
pub fn build_transport(
    keypair: &Keypair,
) -> (Boxed<(PeerId, StreamMuxerBox)>, relay::client::Behaviour) {
    // Create the relay client transport + behaviour pair.
    let local_peer_id = keypair.public().to_peer_id();
    let (relay_transport, relay_client_behaviour) = relay::client::new(local_peer_id);

    // Bare TCP — will be upgraded together with the relay transport below.
    let tcp = libp2p::tcp::tokio::Transport::new(libp2p::tcp::Config::default());

    // Relay-client transport first, TCP second. Both produce raw
    // `AsyncRead + AsyncWrite` connections, so they can share the same
    // upgrade → authenticate (Noise) → multiplex (Yamux) pipeline.
    let relay_or_tcp = relay_transport
        .or_transport(tcp)
        .upgrade(libp2p::core::upgrade::Version::V1)
        .authenticate(
            libp2p::noise::Config::new(keypair).expect("noise config"),
        )
        .multiplex(libp2p::yamux::Config::default());

    // QUIC handles its own security (TLS 1.3) and multiplexing internally.
    // Do NOT chain upgrade/authenticate/multiplex on QUIC.
    let quic = libp2p::quic::tokio::Transport::new(libp2p::quic::Config::new(keypair));

    // (Relay+TCP) first, QUIC as fallback.  The two inner transports
    // produce different concrete muxer types, so we unify them to
    // `StreamMuxerBox` within the `map` closure.
    let or_transport = relay_or_tcp.or_transport(quic).map(|either, _| match either {
        futures::future::Either::Left((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
        futures::future::Either::Right((peer_id, conn)) => (peer_id, StreamMuxerBox::new(conn)),
    });

    // Wrap with DNS resolution so /dnsaddr/ and /dns4/ multiaddrs work.
    let transport = libp2p::dns::tokio::Transport::system(or_transport)
        .expect("DNS transport initialization failed")
        .boxed();

    (transport, relay_client_behaviour)
}

// ── Network behaviour ───────────────────────────────────────────────────────

/// The combined network behaviour for the hearty-p2p node.
///
/// Phase 2 adds AutoNAT, Circuit Relay v2 (client + server), and DCUtR.
///
/// `mDNS` is always present; when `--no-local` is set the event loop
/// simply ignores discovered peers. The `NetworkBehaviour` derive macro
/// does not support `Option<T>` fields, so we cannot conditionally omit
/// mDNS at the type level. The same applies to the relay server: it is
/// always present but configured with `max_reservations: 0` when the node
/// is private (determined at runtime by AutoNAT).
#[derive(NetworkBehaviour)]
pub struct AppBehaviour {
    pub mdns: mdns::tokio::Behaviour,
    pub kademlia: kad::Behaviour<MemoryStore>,
    pub identify: identify::Behaviour,
    pub gossipsub: gossipsub::Behaviour,
    pub request_response: request_response::cbor::Behaviour<ChatMessage, ChatResponse>,
    /// AutoNAT — detects whether the local node is behind NAT.
    pub autonat: autonat::Behaviour,
    /// Circuit Relay v2 client — acquires reservations with public relays.
    pub relay_client: relay::client::Behaviour,
    /// Circuit Relay v2 server — relays traffic for other peers when public.
    /// Always present; configured with `max_reservations: 0` when private.
    pub relay_server: relay::Behaviour,
    /// DCUtR — coordinates hole punching to upgrade relayed connections to direct.
    pub dcutr: dcutr::Behaviour,
}

/// Build a relay server config.
///
/// When `is_public` is `false` the server starts with `max_reservations: 0`
/// so it never accepts relay requests. When AutoNAT later determines the
/// node is public, the event loop logs that the relay server is active
/// (the libp2p 0.54 relay `Behaviour` does not expose a runtime config
/// update, so reconfiguration would require recreating the swarm).
fn build_relay_config(is_public: bool) -> relay::Config {
    if is_public {
        tracing::info!("Public node — enabling relay server");
        relay::Config::default()
    } else {
        tracing::info!("Behind NAT — relay server reservations disabled");
        relay::Config {
            max_reservations: 0,
            ..Default::default()
        }
    }
}

/// Build the [`AppBehaviour`] with sensible defaults.
///
/// The `relay_client` behaviour is created as a pair with the relay client
/// transport in [`build_transport`] and must be passed in here.
pub fn build_behaviour(
    keypair: &Keypair,
    peer_id: PeerId,
    relay_client: relay::client::Behaviour,
) -> AppBehaviour {
    // mDNS — always created; ignored at the event-loop level when --no-local.
    let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id)
        .expect("mDNS initialization failed");

    // Kademlia DHT with an in-memory store.
    let store = MemoryStore::new(peer_id);
    let kademlia = kad::Behaviour::new(peer_id, store);

    // Identify — exchange listen addresses and observed addresses.
    let identify = identify::Behaviour::new(
        identify::Config::new(
            "/hatch-chat/0.2.0".to_string(),
            keypair.public(),
        )
        .with_agent_version("hatch-chat/0.2.0".to_string()),
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

    // AutoNAT — probes connected peers to determine NAT status.
    let autonat = autonat::Behaviour::new(
        peer_id,
        autonat::Config::default(),
    );

    // Relay server — start private (max_reservations = 0). AutoNAT will
    // tell us at runtime if we're public, at which point we log that the
    // relay server would be enabled (reconfiguring requires recreating the
    // swarm in libp2p 0.54).
    let relay_server = relay::Behaviour::new(peer_id, build_relay_config(false));

    // DCUtR — hole punching to upgrade relayed → direct connections.
    let dcutr = dcutr::Behaviour::new(peer_id);

    AppBehaviour {
        mdns,
        kademlia,
        identify,
        gossipsub,
        request_response,
        autonat,
        relay_client,
        relay_server,
        dcutr,
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

// ── Relay address helpers (Phase 2) ────────────────────────────────────────

/// Returns `true` if `addr` is a *direct* (non-relayed) multiaddr.
///
/// A relayed address contains the `/p2p-circuit` protocol component, e.g.
/// `/ip4/1.2.3.4/tcp/4001/p2p/QmRelay/p2p-circuit/p2p/QmDestination`.
/// Addresses without `/p2p-circuit` are considered direct.
pub fn is_direct_address(addr: &Multiaddr) -> bool {
    !addr.iter().any(|p| matches!(p, MultiaddrProtocol::P2pCircuit))
}

/// Close all *relayed* connections to `peer_id` on the given [`Swarm`].
///
/// This is called when a direct connection to a peer is established so
/// that the (higher-latency) relayed connection can be replaced by the
/// direct one. Direct connections to the same peer are left untouched.
///
/// Because the libp2p 0.54 `Swarm` does not expose per-connection address
/// enumeration, the caller must track relayed connection IDs (e.g. via
/// `SwarmEvent::ConnectionEstablished` where `endpoint.is_relayed()` is
/// `true`) and pass them in via `relayed_conns`.
pub fn close_relayed_connections<TBehaviour: NetworkBehaviour>(
    swarm: &mut libp2p::Swarm<TBehaviour>,
    relayed_conns: &mut std::collections::HashMap<PeerId, Vec<libp2p::swarm::ConnectionId>>,
    peer_id: PeerId,
) {
    if let Some(conn_ids) = relayed_conns.remove(&peer_id) {
        for conn_id in conn_ids {
            tracing::info!(
                "Closing relayed connection {conn_id} to {peer_id} \
                 (direct connection established)"
            );
            // `close_connection` is non-blocking; the actual close is observed
            // via `SwarmEvent::ConnectionClosed`.
            let _ = swarm.close_connection(conn_id);
        }
    }
}

/// Check the current AutoNAT status of the node and log it.
///
/// Returns `true` if the node is publicly reachable.
#[allow(dead_code)]
pub fn log_nat_status(behaviour: &AppBehaviour) -> bool {
    match behaviour.autonat.nat_status() {
        autonat::NatStatus::Public(ref addr) => {
            tracing::info!("AutoNAT: publicly reachable at {addr}");
            true
        }
        autonat::NatStatus::Private => {
            tracing::info!("AutoNAT: behind NAT — need relay + hole punching");
            false
        }
        autonat::NatStatus::Unknown => {
            tracing::info!("AutoNAT: NAT status unknown");
            false
        }
    }
}