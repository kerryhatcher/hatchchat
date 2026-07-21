//! hatch-chat — hearty-p2p Phase 1: direct P2P with libp2p.
//!
//! Creates a libp2p swarm with TCP + QUIC transports, mDNS LAN discovery,
//! Kademlia DHT, Identify, GossipSub, and a CBOR request-response chat
//! protocol. Supports `--no-local` to force internet-only connections.

mod network;

use clap::Parser;
use futures::StreamExt;
use libp2p::swarm::SwarmEvent;
use libp2p::{Multiaddr, PeerId, Swarm};
use network::{
    build_behaviour, build_transport, close_relayed_connections, filter_addresses,
    is_direct_address, is_internet_address, ChatResponse,
};
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// CLI arguments.
#[derive(Parser, Debug)]
#[command(name = "hatch-chat", about = "Hearty P2P chat — Phase 1")]
struct Args {
    /// Port to listen on (0 = random free port).
    #[arg(long, default_value = "0")]
    port: u16,

    /// Disable LAN discovery and local address connections.
    /// Forces internet-only communication — useful for testing
    /// NAT traversal with two instances on one machine.
    #[arg(long)]
    no_local: bool,

    /// Bootstrap node multiaddr to connect to on startup.
    #[arg(long)]
    bootstrap: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let no_local = args.no_local;

    // Generate an Ed25519 keypair → derive PeerId.
    let keypair = libp2p::identity::Keypair::generate_ed25519();
    let peer_id = keypair.public().to_peer_id();
    tracing::info!("Local PeerId: {peer_id}");

    // Build transport and behaviour.
    let (transport, relay_client_behaviour) = build_transport(&keypair);
    let behaviour = build_behaviour(&keypair, peer_id, relay_client_behaviour);

    let mut swarm: Swarm<network::AppBehaviour> = Swarm::new(
        transport,
        behaviour,
        peer_id,
        libp2p::swarm::Config::with_tokio_executor(),
    );

    tracing::info!("Subscribed to gossipsub topic: {}", network::GOSSIPSUB_TOPIC);

    // Listen on TCP and QUIC.
    let tcp_addr: Multiaddr = format!("/ip4/0.0.0.0/tcp/{}", args.port).parse()?;
    let quic_addr: Multiaddr = format!("/ip4/0.0.0.0/udp/{}/quic-v1", args.port).parse()?;
    swarm.listen_on(tcp_addr)?;
    swarm.listen_on(quic_addr)?;

    // Bootstrap if provided.
    if let Some(bs) = &args.bootstrap {
        match bs.parse::<Multiaddr>() {
            Ok(addr) => {
                tracing::info!("Dialing bootstrap node at {addr}");
                swarm.dial(addr.clone())?;

                // Try to extract the PeerId from the multiaddr and add it to Kademlia.
                if let Some(bs_peer_id) = extract_peer_id(&addr) {
                    swarm
                        .behaviour_mut()
                        .kademlia
                        .add_address(&bs_peer_id, addr);
                    tracing::info!("Added bootstrap peer {bs_peer_id} to Kademlia DHT");
                }
            }
            Err(e) => {
                tracing::error!("Invalid bootstrap multiaddr '{bs}': {e}");
            }
        }
    }

    // Periodic Kademlia bootstrap.
    let mut kad_bootstrap = tokio::time::interval(Duration::from_secs(10));
    kad_bootstrap.tick().await; // consume the immediate first tick

    // Ctrl+C → graceful shutdown.
    let (ctrl_c_tx, mut ctrl_c_rx) = tokio::sync::mpsc::channel::<()>(1);
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("ctrl_c handler");
        // Send shutdown signal; ignore the returned future.
        drop(ctrl_c_tx.send(()));
    });

    // Track relayed connections per peer so we can close them when a
    // direct connection is established (connection priority).
    let mut relayed_conns: HashMap<PeerId, Vec<libp2p::swarm::ConnectionId>> = HashMap::new();

    tracing::info!("Starting swarm event loop…  Press Ctrl+C to quit.");

    loop {
        tokio::select! {
            _ = ctrl_c_rx.recv() => {
                tracing::info!("Ctrl+C received — shutting down.");
                break;
            }
            _ = kad_bootstrap.tick() => {
                let _ = swarm.behaviour_mut().kademlia.bootstrap();
            }
            event = swarm.select_next_some() => {
                handle_swarm_event(event, &mut swarm, no_local, &mut relayed_conns);
            }
        }
    }

    Ok(())
}

/// Extract the [`PeerId`] from a `Multiaddr` ending in `/p2p/<PeerId>`.
fn extract_peer_id(addr: &Multiaddr) -> Option<PeerId> {
    for protocol in addr.iter() {
        if let libp2p::core::multiaddr::Protocol::P2p(peer_id) = protocol {
            return Some(peer_id);
        }
    }
    None
}

/// Dial a peer only if the address passes the `--no-local` filter.
fn safe_dial(swarm: &mut Swarm<network::AppBehaviour>, addr: Multiaddr, no_local: bool) {
    if no_local && !is_internet_address(&addr) {
        tracing::debug!("--no-local: skipping local address {addr}");
        return;
    }
    if let Err(e) = swarm.dial(addr.clone()) {
        tracing::warn!("Failed to dial {addr}: {e}");
    }
}

#[allow(clippy::too_many_lines)]
fn handle_swarm_event(
    event: SwarmEvent<network::AppBehaviourEvent>,
    swarm: &mut Swarm<network::AppBehaviour>,
    no_local: bool,
    relayed_conns: &mut HashMap<PeerId, Vec<libp2p::swarm::ConnectionId>>,
) {
    match event {
        SwarmEvent::NewListenAddr { address, .. } => {
            tracing::info!("Listening on: {address}");
        }

        SwarmEvent::ConnectionEstablished {
            peer_id,
            connection_id,
            endpoint,
            ..
        } => {
            let remote_addr = endpoint.get_remote_address();
            if is_direct_address(remote_addr) {
                tracing::info!(
                    "Direct connection to peer {peer_id} (conn {connection_id}) at {remote_addr}"
                );
                // Connection priority: close any relayed connections to this peer.
                close_relayed_connections(swarm, relayed_conns, peer_id);
            } else {
                tracing::info!(
                    "Relayed connection to peer {peer_id} (conn {connection_id}) via {remote_addr}"
                );
                relayed_conns
                    .entry(peer_id)
                    .or_default()
                    .push(connection_id);
            }
        }

        SwarmEvent::ConnectionClosed {
            peer_id,
            connection_id,
            cause,
            ..
        } => {
            tracing::info!("Connection closed with {peer_id} (conn {connection_id}): {cause:?}");
            // Remove the closed connection from our relayed tracking.
            if let Some(conns) = relayed_conns.get_mut(&peer_id) {
                conns.retain(|id| *id != connection_id);
                if conns.is_empty() {
                    relayed_conns.remove(&peer_id);
                }
            }
        }

        SwarmEvent::OutgoingConnectionError {
            peer_id, error, ..
        } => {
            tracing::warn!("Outgoing connection error (peer {peer_id:?}): {error}");
        }

        SwarmEvent::IncomingConnectionError {
            error,
            connection_id,
            ..
        } => {
            tracing::warn!("Incoming connection error (conn {connection_id}): {error}");
        }

        // ── mDNS ──────────────────────────────────────────────────────────
        SwarmEvent::Behaviour(network::AppBehaviourEvent::Mdns(
            libp2p::mdns::Event::Discovered(peers),
        )) => {
            if no_local {
                // Internet-only mode: ignore mDNS entirely.
                return;
            }
            for (peer_id, addr) in peers {
                tracing::info!("mDNS discovered: {peer_id} at {addr}");
                swarm
                    .behaviour_mut()
                    .kademlia
                    .add_address(&peer_id, addr.clone());
                safe_dial(swarm, addr, no_local);
            }
        }

        SwarmEvent::Behaviour(network::AppBehaviourEvent::Mdns(
            libp2p::mdns::Event::Expired(peers),
        )) => {
            for (peer_id, _addr) in peers {
                tracing::debug!("mDNS expired: {peer_id}");
            }
        }

        // ── Kademlia ──────────────────────────────────────────────────────
        SwarmEvent::Behaviour(network::AppBehaviourEvent::Kademlia(event)) => {
            match event {
                libp2p::kad::Event::RoutingUpdated {
                    peer,
                    addresses,
                    is_new_peer,
                    ..
                } => {
                    tracing::debug!(
                        "Kademlia routing updated: {peer} (new={is_new_peer}) — {addresses:?}"
                    );
                }
                libp2p::kad::Event::OutboundQueryProgressed {
                    id, result, ..
                } => {
                    tracing::debug!("Kademlia query {id} progressed: {result:?}");
                }
                _ => {
                    tracing::trace!("Kademlia event: {event:?}");
                }
            }
        }

        // ── Identify ──────────────────────────────────────────────────────
        SwarmEvent::Behaviour(network::AppBehaviourEvent::Identify(
            libp2p::identify::Event::Received {
                peer_id, info, ..
            },
        )) => {
            tracing::info!(
                "Identify received from {peer_id}: agent={} listen_addrs={:?}",
                info.agent_version,
                info.listen_addrs
            );
            let filtered = filter_addresses(info.listen_addrs, no_local);
            for addr in filtered {
                tracing::debug!("Adding identified address for {peer_id}: {addr}");
                swarm
                    .behaviour_mut()
                    .kademlia
                    .add_address(&peer_id, addr);
            }
        }
        SwarmEvent::Behaviour(network::AppBehaviourEvent::Identify(event)) => {
            tracing::trace!("Identify event: {event:?}");
        }

        // ── GossipSub ─────────────────────────────────────────────────────
        SwarmEvent::Behaviour(network::AppBehaviourEvent::Gossipsub(event)) => {
            match event {
                libp2p::gossipsub::Event::Message {
                    propagation_source: peer_id,
                    message_id,
                    message,
                } => {
                    tracing::info!(
                        "GossipSub message from {peer_id} (id {message_id}): {} bytes",
                        message.data.len()
                    );
                }
                libp2p::gossipsub::Event::Subscribed { peer_id, topic } => {
                    tracing::info!("Peer {peer_id} subscribed to {topic}");
                }
                libp2p::gossipsub::Event::Unsubscribed { peer_id, topic } => {
                    tracing::debug!("Peer {peer_id} unsubscribed from {topic}");
                }
                _ => {
                    tracing::trace!("GossipSub event: {event:?}");
                }
            }
        }

        // ── Request-Response ──────────────────────────────────────────────
        SwarmEvent::Behaviour(network::AppBehaviourEvent::RequestResponse(event)) => {
            match event {
                libp2p::request_response::Event::Message {
                    peer,
                    message,
                    ..
                } => {
                    match message {
                        libp2p::request_response::Message::Request {
                            request_id,
                            request,
                            channel,
                        } => {
                            tracing::info!(
                                "Direct message from {peer} (req {request_id}): from={}, text={}",
                                request.from,
                                request.text
                            );
                            // Acknowledge receipt.
                            let _ = swarm
                                .behaviour_mut()
                                .request_response
                                .send_response(channel, ChatResponse { received: true });
                        }
                        libp2p::request_response::Message::Response {
                            request_id,
                            response,
                        } => {
                            tracing::info!(
                                "Response for req {request_id} from {peer}: received={}",
                                response.received
                            );
                        }
                    }
                }
                libp2p::request_response::Event::OutboundFailure {
                    peer,
                    request_id,
                    error,
                } => {
                    tracing::warn!(
                        "Request-response outbound failure to {peer} (req {request_id}): {error}"
                    );
                }
                libp2p::request_response::Event::InboundFailure {
                    peer,
                    request_id,
                    error,
                } => {
                    tracing::warn!(
                        "Request-response inbound failure from {peer} (req {request_id:?}): {error}"
                    );
                }
                libp2p::request_response::Event::ResponseSent {
                    peer,
                    request_id,
                } => {
                    tracing::debug!("Response sent to {peer} (req {request_id})");
                }
            }
        }

        // ── AutoNAT ───────────────────────────────────────────────────────
        SwarmEvent::Behaviour(network::AppBehaviourEvent::Autonat(event)) => {
            match event {
                libp2p::autonat::Event::StatusChanged { old, new } => {
                    tracing::info!("AutoNAT status changed: {old:?} -> {new:?}");
                    match &new {
                        libp2p::autonat::NatStatus::Public(addr) => {
                            tracing::info!(
                                "AutoNAT: publicly reachable at {addr} \
                                 — relay server would be enabled (requires \
                                 swarm restart in libp2p 0.54)"
                            );
                        }
                        libp2p::autonat::NatStatus::Private => {
                            tracing::info!(
                                "AutoNAT: behind NAT — need relay + hole punching"
                            );
                        }
                        libp2p::autonat::NatStatus::Unknown => {
                            tracing::info!("AutoNAT: NAT status unknown");
                        }
                    }
                }
                libp2p::autonat::Event::OutboundProbe(probe) => {
                    tracing::debug!("AutoNAT outbound probe: {probe:?}");
                }
                libp2p::autonat::Event::InboundProbe(probe) => {
                    tracing::debug!("AutoNAT inbound probe: {probe:?}");
                }
            }
        }

        // ── DCUtR ────────────────────────────────────────────────────────
        SwarmEvent::Behaviour(network::AppBehaviourEvent::Dcutr(event)) => {
            match event.result {
                Ok(connection_id) => {
                    tracing::info!(
                        "DCUtR: hole punch succeeded with {} (conn {connection_id})",
                        event.remote_peer_id
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        "DCUtR: hole punch failed with {}: {error}",
                        event.remote_peer_id
                    );
                    // Connection stays on relay — still works, just higher latency.
                }
            }
        }

        // ── Relay Client ─────────────────────────────────────────────────
        SwarmEvent::Behaviour(network::AppBehaviourEvent::RelayClient(event)) => {
            match event {
                libp2p::relay::client::Event::ReservationReqAccepted {
                    relay_peer_id,
                    renewal,
                    limit,
                } => {
                    tracing::info!(
                        "Relay client: reservation accepted with relay {relay_peer_id} \
                         (renewal={renewal}, limit={limit:?})"
                    );
                }
                libp2p::relay::client::Event::OutboundCircuitEstablished {
                    relay_peer_id,
                    limit,
                } => {
                    tracing::info!(
                        "Relay client: outbound circuit established via {relay_peer_id} \
                         (limit={limit:?})"
                    );
                }
                libp2p::relay::client::Event::InboundCircuitEstablished {
                    src_peer_id,
                    limit,
                } => {
                    tracing::info!(
                        "Relay client: inbound circuit established from {src_peer_id} \
                         (limit={limit:?})"
                    );
                }
            }
        }

        // ── Relay Server ─────────────────────────────────────────────────
        SwarmEvent::Behaviour(network::AppBehaviourEvent::RelayServer(event)) => {
            match event {
                libp2p::relay::Event::ReservationReqAccepted {
                    src_peer_id,
                    renewed,
                } => {
                    tracing::info!(
                        "Relay server: reservation accepted from {src_peer_id} (renewed={renewed})"
                    );
                }
                libp2p::relay::Event::ReservationReqDenied { src_peer_id } => {
                    tracing::info!("Relay server: reservation denied for {src_peer_id}");
                }
                libp2p::relay::Event::ReservationTimedOut { src_peer_id } => {
                    tracing::info!("Relay server: reservation timed out for {src_peer_id}");
                }
                libp2p::relay::Event::CircuitReqAccepted {
                    src_peer_id,
                    dst_peer_id,
                } => {
                    tracing::info!(
                        "Relay server: circuit request accepted: {src_peer_id} -> {dst_peer_id}"
                    );
                }
                libp2p::relay::Event::CircuitReqDenied {
                    src_peer_id,
                    dst_peer_id,
                } => {
                    tracing::info!(
                        "Relay server: circuit request denied: {src_peer_id} -> {dst_peer_id}"
                    );
                }
                libp2p::relay::Event::CircuitClosed {
                    src_peer_id,
                    dst_peer_id,
                    error,
                } => {
                    tracing::info!(
                        "Relay server: circuit closed: {src_peer_id} -> {dst_peer_id} (error={error:?})"
                    );
                }
                // Deprecated variants are logged at trace level.
                _ => {
                    tracing::trace!("Relay server event: {event:?}");
                }
            }
        }

        SwarmEvent::NewExternalAddrCandidate { address } => {
            tracing::debug!("New external address candidate: {address}");
        }

        SwarmEvent::ExternalAddrConfirmed { address } => {
            tracing::info!("External address confirmed: {address}");
        }

        _ => {
            tracing::trace!("Swarm event: {event:?}");
        }
    }
}

/// Build a [`network::ChatMessage`] with the current timestamp — used by tests
/// and (in later phases) interactive input.
#[allow(dead_code)]
pub fn make_chat_message(from: &str, text: &str) -> network::ChatMessage {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    network::ChatMessage {
        from: from.to_string(),
        text: text.to_string(),
        timestamp,
    }
}