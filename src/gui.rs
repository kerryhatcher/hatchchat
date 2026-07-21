//! Desktop GUI for hatch-chat, built on ply-engine (macroquad backend).
//!
//! Alternate consumer of the same UiEvent/UserAction channel contract the
//! TUI uses. Runs on the main thread (miniquad requires it); the swarm runs
//! on a background thread. Full rendering lands in a later step.
#![cfg(feature = "gui")]

use crate::tui::{UiEvent, UserAction};
use macroquad::prelude::*;
use std::sync::mpsc;
use tokio::sync::mpsc as tokio_mpsc;

#[derive(Clone)]
enum EventKind { System, Chat, Warn, Info }

#[derive(Clone)]
struct LogEntry { text: String, kind: EventKind }

struct ConnPeer { peer_id: String, addr: String, direct: bool }
struct DiscPeer { peer_id: String, addr: String, source: String }

struct GuiState {
    events: Vec<LogEntry>,
    connected_peers: Vec<ConnPeer>,
    discovered_peers: Vec<DiscPeer>,
    our_peer_id: String,
    nat_status: String,
    cache_count: usize,
    input: String,
    selected_peer: usize,
}

impl GuiState {
    fn new(our_peer_id: String) -> Self {
        Self {
            events: Vec::new(),
            connected_peers: Vec::new(),
            discovered_peers: Vec::new(),
            our_peer_id,
            nat_status: "Unknown".to_string(),
            cache_count: 0,
            input: String::new(),
            selected_peer: 0,
        }
    }

    fn push(&mut self, text: String, kind: EventKind) {
        self.events.push(LogEntry { text, kind });
        if self.events.len() > 1000 {
            self.events.remove(0);
        }
    }
}

fn short_pid(id: &str) -> String {
    const MAX: usize = 20;
    if id.len() <= MAX { id.to_string() } else { format!("{}...", &id[..MAX - 3]) }
}

fn apply_ui_event(state: &mut GuiState, event: UiEvent) {
    match event {
        UiEvent::Info(m) => state.push(m, EventKind::Info),
        UiEvent::Warn(m) => state.push(m, EventKind::Warn),
        UiEvent::CacheCount(n) => state.cache_count = n,
        UiEvent::NatStatus(s) => state.nat_status = s.clone(),
        UiEvent::ListenAddr(_a) => {},
        UiEvent::RelayEvent(_m) => {},
        UiEvent::DhtRecord(_m) => {},
        UiEvent::HolePunch { .. } => {},
        UiEvent::ChatMessage { from, text } => {
            state.push(format!("[{}] {}", short_pid(&from), text), EventKind::Chat);
        }
        UiEvent::PeerConnected { peer_id, addr, direct } => {
            if !state.connected_peers.iter().any(|p| p.peer_id == peer_id) {
                state.connected_peers.push(ConnPeer { peer_id: peer_id.clone(), addr: addr.clone(), direct });
            }
        }
        UiEvent::PeerDisconnected { peer_id } => {
            state.connected_peers.retain(|p| p.peer_id != peer_id);
            if state.selected_peer >= state.connected_peers.len() {
                state.selected_peer = state.connected_peers.len().saturating_sub(1);
            }
        }
        UiEvent::PeerDiscovered { peer_id, addr, source } => {
            if !state.discovered_peers.iter().any(|p| p.peer_id == peer_id && p.addr == addr) {
                state.discovered_peers.push(DiscPeer { peer_id: peer_id.clone(), addr: addr.clone(), source: source.clone() });
                if state.discovered_peers.len() > 100 { state.discovered_peers.remove(0); }
            }
        }
    }
}

fn window_conf() -> Conf {
    Conf {
        window_title: "hatch-chat".to_owned(),
        window_width: 1000,
        window_height: 700,
        high_dpi: true,
        sample_count: 4,
        ..Default::default()
    }
}

/// Launch the GUI window on the current (main) thread. Blocks until the
/// window closes. `ui_rx`/`action_tx` are the UI ends of the swarm channels.
pub fn run_gui(
    ui_rx: mpsc::Receiver<UiEvent>,
    action_tx: tokio_mpsc::Sender<UserAction>,
    our_peer_id: String,
) -> Result<(), Box<dyn std::error::Error>> {
    macroquad::Window::from_config(window_conf(), gui_main(ui_rx, action_tx, our_peer_id));
    Ok(())
}

async fn gui_main(
    ui_rx: mpsc::Receiver<UiEvent>,
    action_tx: tokio_mpsc::Sender<UserAction>,
    _our_peer_id: String,
) {
    // Cooperate with the OS close button instead of hard-exiting.
    prevent_quit();
    loop {
        // Drain swarm events (stub: discard; Task 5 renders them).
        loop {
            match ui_rx.try_recv() {
                Ok(_ev) => {}
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
                    let _ = action_tx.try_send(UserAction::Quit);
                    return;
                }
            }
        }

        clear_background(BLACK);
        draw_text("hatch-chat GUI — loading (stub)", 20.0, 40.0, 28.0, WHITE);

        if is_key_pressed(KeyCode::Escape) || is_quit_requested() {
            let _ = action_tx.try_send(UserAction::Quit);
            return;
        }
        next_frame().await;
    }
}

#[cfg(test)]
mod tests {
    use crate::tui::UiEvent;
    use super::{GuiState, apply_ui_event};

    #[test]
    fn maps_events_to_state() {
        let mut s = GuiState::new("me".to_string());

        apply_ui_event(&mut s, UiEvent::CacheCount(7));
        assert_eq!(s.cache_count, 7);

        apply_ui_event(&mut s, UiEvent::NatStatus("Public".into()));
        assert_eq!(s.nat_status, "Public");

        apply_ui_event(&mut s, UiEvent::PeerConnected {
            peer_id: "12D3KooWabc".into(), addr: "/ip4/1.2.3.4/tcp/4001".into(), direct: true,
        });
        assert_eq!(s.connected_peers.len(), 1);
        // Idempotent on duplicate peer_id.
        apply_ui_event(&mut s, UiEvent::PeerConnected {
            peer_id: "12D3KooWabc".into(), addr: "/ip4/1.2.3.4/tcp/4001".into(), direct: true,
        });
        assert_eq!(s.connected_peers.len(), 1);

        apply_ui_event(&mut s, UiEvent::PeerDisconnected { peer_id: "12D3KooWabc".into() });
        assert_eq!(s.connected_peers.len(), 0);

        apply_ui_event(&mut s, UiEvent::ChatMessage { from: "bob".into(), text: "hi".into() });
        assert_eq!(s.events.len(), 1);
        assert!(s.events[0].text.contains("hi"));
    }

    #[test]
    fn event_log_is_capped() {
        let mut s = GuiState::new("me".to_string());
        for i in 0..1100 {
            apply_ui_event(&mut s, UiEvent::Info(format!("line {i}")));
        }
        assert!(s.events.len() <= 1000);
    }
}
