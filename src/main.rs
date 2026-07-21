//! hatch-chat — hearty-p2p Phase 3: Resilient Discovery.
//!
//! Builds on Phase 2 (NAT traversal) with persistent peer cache,
//! bootstrap node support, parallel discovery orchestration, Kademlia
//! DHT provider records, and peer exchange (PEX).
//!
//! The app now features a **TUI** (terminal user interface) that starts
//! immediately on launch and shows the P2P connection/discovery process
//! in real-time.  The swarm runs in the main tokio task while the TUI
//! runs in a separate OS thread; they communicate via channels.

mod discovery;
#[cfg(feature = "gui")]
mod gui;
mod network;
mod peer_cache;
mod tui;

use clap::Parser;
use futures::StreamExt;
use libp2p::gossipsub::IdentTopic;
use libp2p::swarm::SwarmEvent;
use libp2p::{Multiaddr, PeerId, Swarm};
use network::{
    advertise_in_dht, build_behaviour, build_transport, close_relayed_connections,
    filter_addresses, is_direct_address, is_internet_address, ChatResponse, PexResponse,
};
use peer_cache::{current_timestamp, PeerCache, PeerRecord};
use discovery::{BootstrapConfig, BootstrapStrategy, DiscoveryOrchestrator, PeerCacheStrategy};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tui::{UiEvent, UserAction};

#[derive(clap::Subcommand, Debug, Clone)]
enum Command {
    /// Launch the desktop GUI instead of the terminal UI.
    Gui,
}

/// CLI arguments.
#[derive(Parser, Debug)]
#[command(name = "hatch-chat", about = "Hearty P2P chat — Phase 3")]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,

    /// Port to listen on (0 = random free port).
    #[arg(long, default_value = "0")]
    port: u16,

    /// Disable LAN discovery and local address connections.
    /// Forces internet-only communication — useful for testing
    /// NAT traversal with two instances on one machine.
    #[arg(long)]
    no_local: bool,

    /// Bootstrap node multiaddr(s) to connect to on startup.
    /// Can be specified multiple times.
    #[arg(long)]
    bootstrap: Vec<String>,

    /// Act as a bootstrap seed node (does not dial other bootstrap nodes).
    #[arg(long)]
    bootstrap_seed: bool,

    /// Data directory for persistent state (peer cache).
    #[arg(long, default_value = ".hatch-chat")]
    data_dir: String,
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Redirect tracing to a file so the UI owns the terminal / stdout.
    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open("hatch-chat.log")?;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(Mutex::new(log_file))
        .init();

    let args = Args::parse();

    // Channels: UI ends held here, swarm ends handed to run_node.
    let (ui_tx, ui_rx) = mpsc::channel::<UiEvent>();
    let (action_tx, action_rx) = tokio::sync::mpsc::channel::<UserAction>(64);

    match args.command {
        Some(Command::Gui) => run_gui_path(args, ui_tx, ui_rx, action_tx, action_rx),
        None => run_tui_path(args, ui_tx, ui_rx, action_tx, action_rx),
    }
}

fn run_tui_path(
    args: Args,
    ui_tx: mpsc::Sender<UiEvent>,
    ui_rx: mpsc::Receiver<UiEvent>,
    action_tx: tokio::sync::mpsc::Sender<UserAction>,
    action_rx: tokio::sync::mpsc::Receiver<UserAction>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let tui_thread = std::thread::Builder::new()
        .name("hatch-chat-tui".into())
        .spawn(move || {
            if let Err(e) = tui::run_tui(ui_rx, action_tx, String::new(), 0) {
                eprintln!("TUI error: {e}");
            }
        })?;
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    let result = runtime.block_on(run_node(args, ui_tx, action_rx));
    let _ = tui_thread.join();
    result
}

#[cfg(feature = "gui")]
fn run_gui_path(
    args: Args,
    ui_tx: mpsc::Sender<UiEvent>,
    ui_rx: mpsc::Receiver<UiEvent>,
    action_tx: tokio::sync::mpsc::Sender<UserAction>,
    action_rx: tokio::sync::mpsc::Receiver<UserAction>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Swarm on a background thread with its own tokio runtime; GUI on main.
    let swarm_thread = std::thread::Builder::new()
        .name("hatch-chat-swarm".into())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => { eprintln!("runtime error: {e}"); return; }
            };
            if let Err(e) = rt.block_on(run_node(args, ui_tx, action_rx)) {
                eprintln!("swarm error: {e}");
            }
        })?;
    let _ = gui::run_gui(ui_rx, action_tx, String::new());
    let _ = swarm_thread.join();
    Ok(())
}

#[cfg(not(feature = "gui"))]
fn run_gui_path(
    _args: Args,
    _ui_tx: mpsc::Sender<UiEvent>,
    _ui_rx: mpsc::Receiver<UiEvent>,
    _action_tx: tokio::sync::mpsc::Sender<UserAction>,
    _action_rx: tokio::sync::mpsc::Receiver<UserAction>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    eprintln!("This binary was built without GUI support (rebuild with --features gui).");
    std::process::exit(2);
}

async fn run_node(
    args: Args,
    ui_tx: mpsc::Sender<UiEvent>,
    mut action_rx: tokio::sync::mpsc::Receiver<UserAction>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
        // libp2p 0.53+ defaults idle_connection_timeout to ZERO, which closes
        // any connection the moment it has no active stream — peers connect,
        // finish identify, then immediately drop with KeepAliveTimeout. Hold
        // idle connections open so discovered peers stay connected for chat.
        libp2p::swarm::Config::with_tokio_executor()
            .with_idle_connection_timeout(std::time::Duration::from_secs(60)),
    );

    tracing::info!("Subscribed to gossipsub topic: {}", network::GOSSIPSUB_TOPIC);

    // Listen on TCP and QUIC.
    let tcp_addr: Multiaddr = format!("/ip4/0.0.0.0/tcp/{}", args.port).parse()?;
    let quic_addr: Multiaddr = format!("/ip4/0.0.0.0/udp/{}/quic-v1", args.port).parse()?;
    swarm.listen_on(tcp_addr)?;
    swarm.listen_on(quic_addr)?;

    // ── Phase 3: Initialize PeerCache ───────────────────────────────────
    let data_dir = PathBuf::from(&args.data_dir);
    let peer_cache: Arc<PeerCache> = Arc::new(PeerCache::open(&data_dir)?);
    tracing::info!("Peer cache initialized at {}", data_dir.display());

    let cached_count = peer_cache.all_peers().map(|p| p.len()).unwrap_or(0);
    tracing::info!("Peer cache has {cached_count} cached peers");

    let _ = ui_tx.send(UiEvent::CacheCount(cached_count));

    // Send initial info to the TUI.
    let _ = ui_tx.send(UiEvent::Info(format!("Local PeerId: {peer_id}")));
    let _ = ui_tx.send(UiEvent::LocalPeerId(peer_id.to_string()));
    let _ = ui_tx.send(UiEvent::Info(format!(
        "Subscribed to gossipsub topic: {}",
        network::GOSSIPSUB_TOPIC
    )));
    let _ = ui_tx.send(UiEvent::Info(format!(
        "Peer cache initialized with {cached_count} peers"
    )));

    // ── Phase 3: Bootstrap & Discovery ──────────────────────────────────
    if !args.bootstrap_seed {
        let bootstrap_config = BootstrapConfig {
            nodes: args.bootstrap.clone(),
            ..Default::default()
        };

        // Run discovery orchestrator (cached peers + bootstrap nodes).
        let mut orchestrator = DiscoveryOrchestrator::new();
        orchestrator.add_strategy(Box::new(PeerCacheStrategy::new(peer_cache.clone())));
        orchestrator.add_strategy(Box::new(BootstrapStrategy::new(bootstrap_config)));

        let discovered = orchestrator.discover().await;
        let _ = ui_tx.send(UiEvent::Info(format!(
            "Discovery orchestrator found {} peers",
            discovered.len()
        )));
        tracing::info!("Discovery orchestrator found {} peers", discovered.len());

        for record in &discovered {
            for addr_str in &record.multiaddrs {
                if let Ok(ma) = addr_str.parse::<Multiaddr>() {
                    if no_local && !is_internet_address(&ma) {
                        tracing::debug!("--no-local: skipping local address {ma}");
                        continue;
                    }
                    let _ = ui_tx.send(UiEvent::PeerDiscovered {
                        peer_id: record.peer_id.clone(),
                        addr: addr_str.clone(),
                        source: "cache".to_string(),
                    });
                    tracing::info!("Dialing discovered peer {} at {}", record.peer_id, ma);
                    if let Err(e) = swarm.dial(ma.clone()) {
                        let _ = ui_tx.send(UiEvent::Warn(format!("Failed to dial {ma}: {e}")));
                        tracing::warn!("Failed to dial {ma}: {e}");
                    }
                    if let Some(pid) = extract_peer_id(&ma) {
                        swarm.behaviour_mut().kademlia.add_address(&pid, ma);
                    }
                }
            }
        }
    } else {
        let _ = ui_tx.send(UiEvent::Info(
            "Running as bootstrap seed node — skipping outbound discovery".to_string(),
        ));
        tracing::info!("Running as bootstrap seed node — skipping outbound discovery");
    }

    // Add bootstrap nodes to Kademlia DHT and dial them directly.
    // Bootstrap addresses bypass the --no-local filter because the user
    // explicitly provided them — they are trusted seeds.
    for bs in &args.bootstrap {
        if let Ok(addr) = bs.parse::<Multiaddr>() {
            if let Some(bs_peer_id) = extract_peer_id(&addr) {
                // Dial directly — don't let --no-local filter block explicitly
                // provided bootstrap addresses.
                if let Err(e) = swarm.dial(addr.clone()) {
                    let _ = ui_tx.send(UiEvent::Warn(format!("Failed to dial bootstrap {addr}: {e}")));
                    tracing::warn!("Failed to dial bootstrap {addr}: {e}");
                } else {
                    let _ = ui_tx.send(UiEvent::Info(format!(
                        "Dialing bootstrap node {bs_peer_id} at {addr}"
                    )));
                    tracing::info!("Dialing bootstrap node {bs_peer_id} at {addr}");
                }
                swarm
                    .behaviour_mut()
                    .kademlia
                    .add_address(&bs_peer_id, addr);
                let _ = ui_tx.send(UiEvent::Info(format!(
                    "Added bootstrap peer {bs_peer_id} to Kademlia DHT"
                )));
                tracing::info!("Added bootstrap peer {bs_peer_id} to Kademlia DHT");
            }
        }
    }

    // ── Periodic intervals ──────────────────────────────────────────────
    let mut kad_bootstrap = tokio::time::interval(Duration::from_secs(10));
    kad_bootstrap.tick().await; // consume the immediate first tick

    let mut prune_interval = tokio::time::interval(Duration::from_secs(3600)); // 1 hour
    prune_interval.tick().await; // consume immediate tick

    let mut dht_republish = tokio::time::interval(Duration::from_secs(1800)); // 30 minutes
    dht_republish.tick().await; // consume immediate tick

    // Ctrl+C → graceful shutdown (safety net; TUI normally handles quit).
    let (ctrl_c_tx, mut ctrl_c_rx) = tokio::sync::mpsc::channel::<()>(1);
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.expect("ctrl_c handler");
        let _ = ctrl_c_tx.send(()).await;
    });

    // Track relayed connections per peer so we can close them when a
    // direct connection is established (connection priority).
    let mut relayed_conns: HashMap<PeerId, Vec<libp2p::swarm::ConnectionId>> = HashMap::new();

    let _ = ui_tx.send(UiEvent::Info("Starting swarm event loop…".to_string()));
    tracing::info!("Starting swarm event loop…  Press Ctrl+C to quit.");

    loop {
        tokio::select! {
            // User actions from the TUI.
            action = action_rx.recv() => {
                match action {
                    Some(UserAction::Quit) => {
                        let _ = ui_tx.send(UiEvent::Info("Shutting down…".to_string()));
                        tracing::info!("Quit received from TUI — shutting down.");
                        break;
                    }
                    Some(UserAction::SendMessage { peer_id: target, text }) => {
                        let msg = network::ChatMessage {
                            from: peer_id.to_string(),
                            text: text.clone(),
                            timestamp: current_timestamp(),
                        };
                        let _ = swarm
                            .behaviour_mut()
                            .request_response
                            .send_request(&target, msg);
                        // Echo to our own event log.
                        let _ = ui_tx.send(UiEvent::ChatMessage {
                            from: "me".to_string(),
                            text,
                        });
                    }
                    Some(UserAction::Broadcast { text }) => {
                        let msg = network::ChatMessage {
                            from: peer_id.to_string(),
                            text: text.clone(),
                            timestamp: current_timestamp(),
                        };
                        let topic = IdentTopic::new(network::GOSSIPSUB_TOPIC);
                        match swarm.behaviour_mut().gossipsub.publish(topic, serde_json::to_vec(&msg).unwrap_or_default()) {
                            Ok(_) => {
                                let _ = ui_tx.send(UiEvent::ChatMessage {
                                    from: "me".to_string(),
                                    text,
                                });
                            }
                            Err(e) => {
                                let _ = ui_tx.send(UiEvent::Warn(format!("Broadcast failed: {e}")));
                            }
                        }
                    }
                    None => {
                        // TUI thread disconnected — shut down.
                        tracing::info!("TUI disconnected — shutting down.");
                        break;
                    }
                }
            }
            _ = ctrl_c_rx.recv() => {
                let _ = ui_tx.send(UiEvent::Info("Ctrl+C received — shutting down.".to_string()));
                tracing::info!("Ctrl+C received — shutting down.");
                break;
            }
            _ = kad_bootstrap.tick() => {
                let _ = swarm.behaviour_mut().kademlia.bootstrap();
            }
            _ = prune_interval.tick() => {
                // Prune peers not seen in 7 days.
                if let Err(e) = peer_cache.prune_stale(7 * 24 * 3600) {
                    let _ = ui_tx.send(UiEvent::Warn(format!("Failed to prune stale peers: {e}")));
                    tracing::warn!("Failed to prune stale peers: {e}");
                } else {
                    let _ = ui_tx.send(UiEvent::Info("Pruned stale peers (older than 7 days)".to_string()));
                    tracing::info!("Pruned stale peers (older than 7 days)");
                }
            }
            _ = dht_republish.tick() => {
                // Republish our DHT provider records.
                let addrs: Vec<String> = swarm.external_addresses()
                    .map(|a| a.to_string())
                    .collect();
                let our_record = PeerRecord {
                    peer_id: peer_id.to_string(),
                    multiaddrs: addrs,
                    i2p_destination: None,
                    last_seen: current_timestamp(),
                    connection_count: 0,
                    rtt_ms: None,
                    is_relay: false,
                    is_public: false,
                };
                advertise_in_dht(
                    &mut swarm.behaviour_mut().kademlia,
                    peer_id,
                    &our_record,
                );
                // Also query the DHT for other peers advertising the service.
                network::discover_via_dht(&mut swarm.behaviour_mut().kademlia);
                let _ = ui_tx.send(UiEvent::Info("Republished DHT provider records and started discovery query".to_string()));
                tracing::info!("Republished DHT provider records and started discovery query");
            }
            event = swarm.select_next_some() => {
                handle_swarm_event(
                    event,
                    &mut swarm,
                    no_local,
                    &mut relayed_conns,
                    &peer_cache,
                    peer_id,
                    &ui_tx,
                );
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

/// Save (or update) a peer record in the cache on connection.
fn save_peer_to_cache(peer_cache: &PeerCache, peer_id: PeerId, remote_addr: &Multiaddr) {
    let pid_str = peer_id.to_string();
    let existing = peer_cache.get_peer(&pid_str).ok().flatten();
    let record = PeerRecord {
        peer_id: pid_str,
        multiaddrs: {
            // Merge with existing addresses, deduplicated.
            let mut addrs = vec![remote_addr.to_string()];
            if let Some(ref ex) = existing {
                for a in &ex.multiaddrs {
                    if !addrs.contains(a) {
                        addrs.push(a.clone());
                    }
                }
            }
            addrs
        },
        i2p_destination: existing.as_ref().and_then(|r| r.i2p_destination.clone()),
        last_seen: current_timestamp(),
        connection_count: existing
            .as_ref()
            .map(|r| r.connection_count + 1)
            .unwrap_or(1),
        rtt_ms: existing.as_ref().and_then(|r| r.rtt_ms),
        is_relay: existing.as_ref().map(|r| r.is_relay).unwrap_or(false),
        is_public: existing.as_ref().map(|r| r.is_public).unwrap_or(false),
    };
    if let Err(e) = peer_cache.save_peer(&record) {
        tracing::warn!("Failed to save peer {peer_id} to cache: {e}");
    } else {
        tracing::debug!("Saved peer {peer_id} to cache (conns={})", record.connection_count);
    }
}

#[allow(clippy::too_many_lines)]
fn handle_swarm_event(
    event: SwarmEvent<network::AppBehaviourEvent>,
    swarm: &mut Swarm<network::AppBehaviour>,
    no_local: bool,
    relayed_conns: &mut HashMap<PeerId, Vec<libp2p::swarm::ConnectionId>>,
    peer_cache: &PeerCache,
    our_peer_id: PeerId,
    ui_tx: &mpsc::Sender<UiEvent>,
) {
    match event {
        SwarmEvent::NewListenAddr { address, .. } => {
            let _ = ui_tx.send(UiEvent::ListenAddr(address.to_string()));
            tracing::info!("Listening on: {address}");
        }

        SwarmEvent::ConnectionEstablished {
            peer_id,
            connection_id,
            endpoint,
            ..
        } => {
            let remote_addr = endpoint.get_remote_address();

            // ── Phase 3: Save peer to cache ────────────────────────────
            save_peer_to_cache(peer_cache, peer_id, remote_addr);

            // ── Phase 3: Send PEX message ───────────────────────────────
            let known_peers = peer_cache.all_peers().unwrap_or_default();
            let pex_msg = network::PeerExchangeMessage {
                peers: known_peers,
                timestamp: current_timestamp(),
            };
            swarm
                .behaviour_mut()
                .pex
                .send_request(&peer_id, pex_msg);
            tracing::debug!("Sent PEX message to {peer_id}");

            let direct = is_direct_address(remote_addr);
            let _ = ui_tx.send(UiEvent::PeerConnected {
                peer_id: peer_id.to_string(),
                addr: remote_addr.to_string(),
                direct,
            });

            if direct {
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

            // Add peer to Kademlia routing table.
            swarm
                .behaviour_mut()
                .kademlia
                .add_address(&peer_id, remote_addr.clone());
        }

        SwarmEvent::ConnectionClosed {
            peer_id,
            connection_id,
            cause,
            ..
        } => {
            let _ = ui_tx.send(UiEvent::PeerDisconnected {
                peer_id: peer_id.to_string(),
            });
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
            let _ = ui_tx.send(UiEvent::Warn(format!(
                "Outgoing connection error (peer {peer_id:?}): {error}"
            )));
            tracing::warn!("Outgoing connection error (peer {peer_id:?}): {error}");
        }

        SwarmEvent::IncomingConnectionError {
            error,
            connection_id,
            ..
        } => {
            let _ = ui_tx.send(UiEvent::Warn(format!(
                "Incoming connection error (conn {connection_id}): {error}"
            )));
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
                let _ = ui_tx.send(UiEvent::PeerDiscovered {
                    peer_id: peer_id.to_string(),
                    addr: addr.to_string(),
                    source: "mDNS".to_string(),
                });
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
                    if is_new_peer {
                        let _ = ui_tx.send(UiEvent::PeerDiscovered {
                            peer_id: peer.to_string(),
                            addr: addresses.iter().map(|a| a.to_string()).collect::<Vec<_>>().join(", "),
                            source: "DHT".to_string(),
                        });
                    }
                    tracing::debug!(
                        "Kademlia routing updated: {peer} (new={is_new_peer}) — {addresses:?}"
                    );
                }
                libp2p::kad::Event::OutboundQueryProgressed {
                    id, result, ..
                } => {
                    match &result {
                        libp2p::kad::QueryResult::GetRecord(Ok(ok)) => {
                            match ok {
                                libp2p::kad::GetRecordOk::FoundRecord(peer_record) => {
                                    let _ = ui_tx.send(UiEvent::DhtRecord(format!(
                                        "GetRecord query {id}: found record with {} bytes",
                                        peer_record.record.value.len()
                                    )));
                                    tracing::info!(
                                        "Kademlia GetRecord query {id}: found record with {} bytes",
                                        peer_record.record.value.len()
                                    );
                                    // Try to parse as a PeerRecord and save to cache.
                                    if let Ok(rec) =
                                        serde_json::from_slice::<peer_cache::PeerRecord>(&peer_record.record.value)
                                    {
                                        if !rec.peer_id.is_empty()
                                            && !rec.multiaddrs.is_empty()
                                        {
                                            if let Err(e) = peer_cache.save_peer(&rec) {
                                                let _ = ui_tx.send(UiEvent::Warn(format!(
                                                    "Failed to save DHT-discovered peer {}: {e}",
                                                    rec.peer_id
                                                )));
                                                tracing::warn!(
                                                    "Failed to save DHT-discovered peer {}: {e}",
                                                    rec.peer_id
                                                );
                                            } else {
                                                let _ = ui_tx.send(UiEvent::Info(format!(
                                                    "Saved DHT-discovered peer {} to cache",
                                                    rec.peer_id
                                                )));
                                                tracing::info!(
                                                    "Saved DHT-discovered peer {} to cache",
                                                    rec.peer_id
                                                );
                                            }
                                        }
                                    }
                                }
                                libp2p::kad::GetRecordOk::FinishedWithNoAdditionalRecord { .. } => {
                                    tracing::debug!("Kademlia GetRecord query {id} finished");
                                }
                            }
                        }
                        _ => {
                            tracing::debug!("Kademlia query {id} progressed: {result:?}");
                        }
                    }
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
            let _ = ui_tx.send(UiEvent::Info(format!(
                "Identify received from {peer_id}: agent={}",
                info.agent_version
            )));
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
                    // Try to parse as a ChatMessage for display.
                    if let Ok(chat_msg) = serde_json::from_slice::<network::ChatMessage>(&message.data) {
                        let _ = ui_tx.send(UiEvent::ChatMessage {
                            from: chat_msg.from,
                            text: chat_msg.text,
                        });
                    } else {
                        let _ = ui_tx.send(UiEvent::Info(format!(
                            "GossipSub message from {peer_id} (id {message_id}): {} bytes",
                            message.data.len()
                        )));
                    }
                    tracing::info!(
                        "GossipSub message from {peer_id} (id {message_id}): {} bytes",
                        message.data.len()
                    );
                }
                libp2p::gossipsub::Event::Subscribed { peer_id, topic } => {
                    let _ = ui_tx.send(UiEvent::Info(format!("Peer {peer_id} subscribed to {topic}")));
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

        // ── Request-Response (chat) ───────────────────────────────────────
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
                            let _ = ui_tx.send(UiEvent::ChatMessage {
                                from: request.from.clone(),
                                text: request.text.clone(),
                            });
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
                            let _ = ui_tx.send(UiEvent::Info(format!(
                                "Response for req {request_id} from {peer}: received={}",
                                response.received
                            )));
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
                    let _ = ui_tx.send(UiEvent::Warn(format!(
                        "Request-response outbound failure to {peer} (req {request_id}): {error}"
                    )));
                    tracing::warn!(
                        "Request-response outbound failure to {peer} (req {request_id}): {error}"
                    );
                }
                libp2p::request_response::Event::InboundFailure {
                    peer,
                    request_id,
                    error,
                } => {
                    let _ = ui_tx.send(UiEvent::Warn(format!(
                        "Request-response inbound failure from {peer} (req {request_id:?}): {error}"
                    )));
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

        // ── PEX (Peer Exchange) ───────────────────────────────────────────
        SwarmEvent::Behaviour(network::AppBehaviourEvent::Pex(event)) => {
            match event {
                libp2p::request_response::Event::Message {
                    peer,
                    message,
                    ..
                } => {
                    match message {
                        libp2p::request_response::Message::Request {
                            request,
                            channel,
                            ..
                        } => {
                            let _ = ui_tx.send(UiEvent::Info(format!(
                                "PEX message from {peer}: {} peers",
                                request.peers.len()
                            )));
                            tracing::info!(
                                "PEX message from {peer}: {} peers",
                                request.peers.len()
                            );
                            // Report discovered peers from PEX.
                            for record in &request.peers {
                                if !record.peer_id.is_empty() && !record.multiaddrs.is_empty() {
                                    let _ = ui_tx.send(UiEvent::PeerDiscovered {
                                        peer_id: record.peer_id.clone(),
                                        addr: record.multiaddrs.first().cloned().unwrap_or_default(),
                                        source: "PEX".to_string(),
                                    });
                                }
                            }
                            // Validate and save received peers to cache.
                            for record in &request.peers {
                                if !record.peer_id.is_empty()
                                    && !record.multiaddrs.is_empty()
                                    // Don't save our own peer record from PEX.
                                    && record.peer_id != our_peer_id.to_string()
                                {
                                    if let Err(e) = peer_cache.save_peer(record) {
                                        tracing::warn!(
                                            "Failed to save PEX peer {}: {e}",
                                            record.peer_id
                                        );
                                    }
                                }
                            }
                            // Acknowledge.
                            let _ = swarm
                                .behaviour_mut()
                                .pex
                                .send_response(channel, PexResponse { received: true });
                        }
                        libp2p::request_response::Message::Response {
                            response,
                            ..
                        } => {
                            tracing::debug!(
                                "PEX response from {peer}: received={}",
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
                    let _ = ui_tx.send(UiEvent::Warn(format!(
                        "PEX outbound failure to {peer} (req {request_id}): {error}"
                    )));
                    tracing::warn!(
                        "PEX outbound failure to {peer} (req {request_id}): {error}"
                    );
                }
                libp2p::request_response::Event::InboundFailure {
                    peer,
                    request_id,
                    error,
                } => {
                    let _ = ui_tx.send(UiEvent::Warn(format!(
                        "PEX inbound failure from {peer} (req {request_id:?}): {error}"
                    )));
                    tracing::warn!(
                        "PEX inbound failure from {peer} (req {request_id:?}): {error}"
                    );
                }
                libp2p::request_response::Event::ResponseSent {
                    peer,
                    request_id,
                } => {
                    tracing::debug!("PEX response sent to {peer} (req {request_id})");
                }
            }
        }

        // ── AutoNAT ───────────────────────────────────────────────────────
        SwarmEvent::Behaviour(network::AppBehaviourEvent::Autonat(event)) => {
            match event {
                libp2p::autonat::Event::StatusChanged { old, new } => {
                    let status_str = match &new {
                        libp2p::autonat::NatStatus::Public(addr) => {
                            format!("Public ({addr})")
                        }
                        libp2p::autonat::NatStatus::Private => "Private".to_string(),
                        libp2p::autonat::NatStatus::Unknown => "Unknown".to_string(),
                    };
                    let _ = ui_tx.send(UiEvent::NatStatus(status_str.clone()));
                    tracing::info!("AutoNAT status changed: {old:?} -> {new:?}");
                    match &new {
                        libp2p::autonat::NatStatus::Public(addr) => {
                            let _ = ui_tx.send(UiEvent::Info(format!(
                                "AutoNAT: publicly reachable at {addr}"
                            )));
                            tracing::info!(
                                "AutoNAT: publicly reachable at {addr} \
                                 — relay server would be enabled (requires \
                                 swarm restart in libp2p 0.54)"
                            );
                        }
                        libp2p::autonat::NatStatus::Private => {
                            let _ = ui_tx.send(UiEvent::Info(
                                "AutoNAT: behind NAT — need relay + hole punching".to_string(),
                            ));
                            tracing::info!(
                                "AutoNAT: behind NAT — need relay + hole punching"
                            );
                        }
                        libp2p::autonat::NatStatus::Unknown => {
                            let _ = ui_tx.send(UiEvent::Info("AutoNAT: NAT status unknown".to_string()));
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
                    let _ = ui_tx.send(UiEvent::HolePunch {
                        peer_id: event.remote_peer_id.to_string(),
                        success: true,
                    });
                    tracing::info!(
                        "DCUtR: hole punch succeeded with {} (conn {connection_id})",
                        event.remote_peer_id
                    );
                }
                Err(error) => {
                    let _ = ui_tx.send(UiEvent::HolePunch {
                        peer_id: event.remote_peer_id.to_string(),
                        success: false,
                    });
                    tracing::warn!(
                        "DCUtR: hole punch failed with {}: {error}",
                        event.remote_peer_id
                    );
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
                    let _ = ui_tx.send(UiEvent::RelayEvent(format!(
                        "Reservation accepted with relay {relay_peer_id} (renewal={renewal}, limit={limit:?})"
                    )));
                    tracing::info!(
                        "Relay client: reservation accepted with relay {relay_peer_id} \
                         (renewal={renewal}, limit={limit:?})"
                    );
                }
                libp2p::relay::client::Event::OutboundCircuitEstablished {
                    relay_peer_id,
                    limit,
                } => {
                    let _ = ui_tx.send(UiEvent::RelayEvent(format!(
                        "Outbound circuit established via {relay_peer_id} (limit={limit:?})"
                    )));
                    tracing::info!(
                        "Relay client: outbound circuit established via {relay_peer_id} \
                         (limit={limit:?})"
                    );
                }
                libp2p::relay::client::Event::InboundCircuitEstablished {
                    src_peer_id,
                    limit,
                } => {
                    let _ = ui_tx.send(UiEvent::RelayEvent(format!(
                        "Inbound circuit established from {src_peer_id} (limit={limit:?})"
                    )));
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
                    let _ = ui_tx.send(UiEvent::RelayEvent(format!(
                        "Reservation accepted from {src_peer_id} (renewed={renewed})"
                    )));
                    tracing::info!(
                        "Relay server: reservation accepted from {src_peer_id} (renewed={renewed})"
                    );
                }
                libp2p::relay::Event::ReservationReqDenied { src_peer_id } => {
                    let _ = ui_tx.send(UiEvent::RelayEvent(format!(
                        "Reservation denied for {src_peer_id}"
                    )));
                    tracing::info!("Relay server: reservation denied for {src_peer_id}");
                }
                libp2p::relay::Event::ReservationTimedOut { src_peer_id } => {
                    let _ = ui_tx.send(UiEvent::RelayEvent(format!(
                        "Reservation timed out for {src_peer_id}"
                    )));
                    tracing::info!("Relay server: reservation timed out for {src_peer_id}");
                }
                libp2p::relay::Event::CircuitReqAccepted {
                    src_peer_id,
                    dst_peer_id,
                } => {
                    let _ = ui_tx.send(UiEvent::RelayEvent(format!(
                        "Circuit request accepted: {src_peer_id} -> {dst_peer_id}"
                    )));
                    tracing::info!(
                        "Relay server: circuit request accepted: {src_peer_id} -> {dst_peer_id}"
                    );
                }
                libp2p::relay::Event::CircuitReqDenied {
                    src_peer_id,
                    dst_peer_id,
                } => {
                    let _ = ui_tx.send(UiEvent::RelayEvent(format!(
                        "Circuit request denied: {src_peer_id} -> {dst_peer_id}"
                    )));
                    tracing::info!(
                        "Relay server: circuit request denied: {src_peer_id} -> {dst_peer_id}"
                    );
                }
                libp2p::relay::Event::CircuitClosed {
                    src_peer_id,
                    dst_peer_id,
                    error,
                } => {
                    let _ = ui_tx.send(UiEvent::RelayEvent(format!(
                        "Circuit closed: {src_peer_id} -> {dst_peer_id} (error={error:?})"
                    )));
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
            let _ = ui_tx.send(UiEvent::ListenAddr(format!("External address confirmed: {address}")));
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