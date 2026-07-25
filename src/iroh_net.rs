//! iroh-based networking module for hatch-chat.
//!
//! Replaces the libp2p stack with iroh + iroh-gossip +
//! iroh-gossip-rendezvous for zero-knowledge peer discovery.
//!
//! Two nodes sharing only a passphrase find each other via the
//! BitTorrent Mainline DHT — no bootstrap servers, no pre-shared
//! addresses, no manual config.

use iroh_gossip::api::Event as GossipEvent;
use iroh_gossip_rendezvous::Rendezvous;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::peer_cache::{current_timestamp, PeerCache, PeerRecord};
use crate::tui::{UiEvent, UserAction};

// ── Constants ───────────────────────────────────────────────────────────────

/// Application label for the rendezvous DHT namespace.
/// Change this to isolate different applications or versions.
const APP_LABEL: &str = "hatch-chat/v0.7";

// ── Message types ───────────────────────────────────────────────────────────

/// A chat message broadcast over the gossip topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    /// EndpointId of the sender (as a string).
    pub from: String,
    /// Human-readable sender name (set via --name).
    pub name: String,
    /// Message body.
    pub text: String,
    /// Unix timestamp in seconds.
    pub timestamp: u64,
}

impl ChatMessage {
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("ChatMessage serialization is infallible")
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

// ── Node handle ─────────────────────────────────────────────────────────────

// (No explicit Node struct needed — the Rendezvous handle owns everything.)

/// Run the iroh node: bind, rendezvous, join gossip, and enter the
/// combined event loop that merges gossip events with user actions.
pub async fn run(
    passphrase: String,
    name: Option<String>,
    data_dir: String,
    ui_tx: std::sync::mpsc::Sender<UiEvent>,
    mut action_rx: mpsc::Receiver<UserAction>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // ── 1. Rendezvous via Mainline DHT ──────────────────────────────────
    let _ = ui_tx.send(UiEvent::Info(format!(
        "Joining rendezvous with passphrase (app={APP_LABEL})…"
    )));
    tracing::info!("Joining rendezvous with passphrase (app={APP_LABEL})");

    let rendezvous = Rendezvous::join(&passphrase, APP_LABEL).await?;

    let our_id = rendezvous.node_id();
    let sender = rendezvous.sender().clone();
    let mut receiver = rendezvous.subscribe();

    tracing::info!("Local EndpointId: {our_id}");
    let _ = ui_tx.send(UiEvent::Info(format!("Local EndpointId: {our_id}")));
    let _ = ui_tx.send(UiEvent::LocalPeerId(our_id.to_string()));

    let _ = ui_tx.send(UiEvent::Info(
        "Rendezvous joined — listening for peers via DHT".to_string(),
    ));
    tracing::info!("Rendezvous joined");

    // ── 2. Announce our name if provided ────────────────────────────────
    if let Some(ref display_name) = name {
        let msg = ChatMessage {
            from: our_id.to_string(),
            name: display_name.clone(),
            text: String::new(),
            timestamp: current_timestamp(),
        };
        if let Err(e) = sender.broadcast(msg.to_bytes().into()).await {
            tracing::warn!("Failed to broadcast name announcement: {e}");
        }
    }

    // ── 3. Initialize peer cache ────────────────────────────────────────
    let data_dir_path = std::path::PathBuf::from(&data_dir);
    let peer_cache = Arc::new(PeerCache::open(&data_dir_path)?);
    let cached_count = peer_cache.all_peers().map(|p| p.len()).unwrap_or(0);
    let _ = ui_tx.send(UiEvent::CacheCount(cached_count));
    let _ = ui_tx.send(UiEvent::Info(format!(
        "Peer cache initialized with {cached_count} peers"
    )));
    tracing::info!("Peer cache initialized with {cached_count} peers");

    // ── 4. Event loop ──────────────────────────────────────────────────
    let _ = ui_tx.send(UiEvent::Info(
        "Chat ready — waiting for peers…".to_string(),
    ));
    tracing::info!("Entering event loop");

    // Track known peer names: EndpointId string → display name.
    let mut peer_names: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();

    loop {
        tokio::select! {
            // ── User actions from the TUI ──────────────────────────────
            action = action_rx.recv() => {
                match action {
                    Some(UserAction::Quit) => {
                        let _ = ui_tx.send(UiEvent::Info("Shutting down…".to_string()));
                        tracing::info!("Quit received from TUI — shutting down.");
                        break;
                    }
                    Some(UserAction::Broadcast { text }) => {
                        let msg = ChatMessage {
                            from: our_id.to_string(),
                            name: name.clone().unwrap_or_else(|| "anonymous".into()),
                            text: text.clone(),
                            timestamp: current_timestamp(),
                        };
                        match sender.broadcast(msg.to_bytes().into()).await {
                            Ok(_) => {
                                let _ = ui_tx.send(UiEvent::ChatMessage {
                                    from: "me".to_string(),
                                    text,
                                });
                            }
                            Err(e) => {
                                let _ = ui_tx.send(UiEvent::Warn(format!("Broadcast failed: {e}")));
                                tracing::warn!("Broadcast failed: {e}");
                            }
                        }
                    }
                    None => {
                        tracing::info!("TUI disconnected — shutting down.");
                        break;
                    }
                }
            }

            // ── Gossip events ──────────────────────────────────────────
            event = receiver.recv() => {
                match event {
                    Ok(GossipEvent::Received(msg)) => {
                        // Ignore messages that originated from us.
                        if msg.delivered_from == our_id {
                            continue;
                        }
                        match ChatMessage::from_bytes(&msg.content) {
                            Ok(chat_msg) => {
                                let from_short = short_id(&chat_msg.from);

                                // Track name announcements (empty text = name-only).
                                if chat_msg.text.is_empty() && !chat_msg.name.is_empty() {
                                    peer_names.insert(
                                        chat_msg.from.clone(),
                                        chat_msg.name.clone(),
                                    );
                                    let _ = ui_tx.send(UiEvent::Info(format!(
                                        "{from_short} is now known as {}",
                                        chat_msg.name
                                    )));
                                    let _ = ui_tx.send(UiEvent::PeerConnected {
                                        peer_id: chat_msg.from.clone(),
                                        addr: "gossip".to_string(),
                                        direct: true,
                                    });
                                    // Save to peer cache.
                                    let record = PeerRecord {
                                        peer_id: chat_msg.from.clone(),
                                        multiaddrs: vec!["gossip".to_string()],
                                        i2p_destination: None,
                                        last_seen: current_timestamp(),
                                        connection_count: 1,
                                        rtt_ms: None,
                                        is_relay: false,
                                        is_public: false,
                                    };
                                    let _ = peer_cache.save_peer(&record);
                                    continue;
                                }

                                // Display the message.
                                let display_name = peer_names
                                    .get(&chat_msg.from)
                                    .cloned()
                                    .unwrap_or_else(|| from_short.clone());

                                let _ = ui_tx.send(UiEvent::ChatMessage {
                                    from: display_name,
                                    text: chat_msg.text,
                                });
                            }
                            Err(e) => {
                                tracing::debug!("Failed to parse chat message: {e}");
                            }
                        }
                    }
                    Ok(GossipEvent::NeighborUp(peer_id)) => {
                        if peer_id == our_id {
                            continue;
                        }
                        let pid_str = peer_id.to_string();
                        let _ = ui_tx.send(UiEvent::PeerConnected {
                            peer_id: pid_str.clone(),
                            addr: "gossip".to_string(),
                            direct: true,
                        });
                        let _ = ui_tx.send(UiEvent::Info(format!(
                            "Peer joined: {}",
                            short_id(&pid_str)
                        )));
                        tracing::info!("Peer joined: {peer_id}");
                    }
                    Ok(GossipEvent::NeighborDown(peer_id)) => {
                        if peer_id == our_id {
                            continue;
                        }
                        let pid_str = peer_id.to_string();
                        let _ = ui_tx.send(UiEvent::PeerDisconnected {
                            peer_id: pid_str.clone(),
                        });
                        let _ = ui_tx.send(UiEvent::Info(format!(
                            "Peer left: {}",
                            short_id(&pid_str)
                        )));
                        tracing::info!("Peer left: {peer_id}");
                    }
                    Ok(GossipEvent::Lagged) => {
                        let _ = ui_tx.send(UiEvent::Warn(
                            "Event stream lagged — some events were missed".to_string(),
                        ));
                        tracing::warn!("Event stream lagged");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        let _ = ui_tx.send(UiEvent::Warn(format!(
                            "Receiver lagged — missed {n} messages"
                        )));
                        tracing::warn!("Receiver lagged — missed {n} messages");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        let _ = ui_tx.send(UiEvent::Warn(
                            "Gossip event stream closed".to_string(),
                        ));
                        tracing::warn!("Gossip event stream closed");
                        break;
                    }
                }
            }
        }
    }

    // ── 5. Graceful shutdown ───────────────────────────────────────────
    rendezvous.shutdown().await;
    tracing::info!("Shutdown complete");
    Ok(())
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn short_id(id: &str) -> String {
    const MAX: usize = 20;
    if id.chars().count() <= MAX {
        id.to_string()
    } else {
        format!("{}...", id.chars().take(MAX - 3).collect::<String>())
    }
}
