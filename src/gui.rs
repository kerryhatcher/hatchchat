//! Desktop GUI for hatch-chat, built on ply-engine (macroquad backend).
//!
//! Alternate consumer of the same UiEvent/UserAction channel contract the
//! TUI uses. Runs on the main thread (miniquad requires it); the swarm runs
//! on a background thread. Full rendering lands in a later step.
#![cfg(feature = "gui")]

use crate::tui::{UiEvent, UserAction};
use libp2p::PeerId;
use macroquad::prelude::*;
use ply_engine::prelude::*;
use std::sync::mpsc;
use tokio::sync::mpsc as tokio_mpsc;

static DEFAULT_FONT: FontAsset = FontAsset::Bytes {
    file_name: "JetBrainsMono-Regular.ttf",
    data: include_bytes!("../assets/fonts/JetBrainsMono-Regular.ttf"),
};

#[derive(Clone)]
enum EventKind { System, Chat, Warn, Info }

#[derive(Clone)]
struct LogEntry { text: String, kind: EventKind }

#[allow(dead_code)]
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
    if id.chars().count() <= MAX {
        id.to_string()
    } else {
        format!("{}...", id.chars().take(MAX - 3).collect::<String>())
    }
}

fn apply_ui_event(state: &mut GuiState, event: UiEvent) {
    match event {
        UiEvent::Info(m) => state.push(m, EventKind::Info),
        UiEvent::Warn(m) => state.push(m, EventKind::Warn),
        UiEvent::CacheCount(n) => state.cache_count = n,
        UiEvent::LocalPeerId(id) => state.our_peer_id = id,
        UiEvent::NatStatus(s) => {
            state.nat_status = s.clone();
            state.push(format!("NAT status: {s}"), EventKind::System);
        }
        UiEvent::ListenAddr(a) => state.push(format!("Listening on {a}"), EventKind::System),
        UiEvent::RelayEvent(m) => state.push(format!("Relay: {m}"), EventKind::Info),
        UiEvent::DhtRecord(m) => state.push(format!("DHT: {m}"), EventKind::Info),
        UiEvent::HolePunch { peer_id, success } => {
            let (msg, kind) = if success {
                (format!("Hole punch succeeded with {}", short_pid(&peer_id)), EventKind::System)
            } else {
                (format!("Hole punch failed with {}", short_pid(&peer_id)), EventKind::Warn)
            };
            state.push(msg, kind);
        }
        UiEvent::ChatMessage { from, text } => {
            state.push(format!("[{}] {}", short_pid(&from), text), EventKind::Chat);
        }
        UiEvent::PeerConnected { peer_id, addr, direct } => {
            if !state.connected_peers.iter().any(|p| p.peer_id == peer_id) {
                state.connected_peers.push(ConnPeer { peer_id: peer_id.clone(), addr: addr.clone(), direct });
            }
            let kind_str = if direct { "direct" } else { "relayed" };
            state.push(format!("Connected to {} ({kind_str}) at {}", short_pid(&peer_id), addr), EventKind::System);
        }
        UiEvent::PeerDisconnected { peer_id } => {
            state.connected_peers.retain(|p| p.peer_id != peer_id);
            if state.selected_peer >= state.connected_peers.len() {
                state.selected_peer = state.connected_peers.len().saturating_sub(1);
            }
            state.push(format!("Disconnected from {}", short_pid(&peer_id)), EventKind::System);
        }
        UiEvent::PeerDiscovered { peer_id, addr, source } => {
            if !state.discovered_peers.iter().any(|p| p.peer_id == peer_id && p.addr == addr) {
                state.discovered_peers.push(DiscPeer { peer_id: peer_id.clone(), addr: addr.clone(), source: source.clone() });
                if state.discovered_peers.len() > 100 { state.discovered_peers.remove(0); }
            }
            state.push(format!("Discovered {} at {} via {}", short_pid(&peer_id), addr, source), EventKind::System);
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
    our_peer_id: String,
) {
    // Cooperate with the OS close button instead of hard-exiting.
    prevent_quit();
    let mut ply = Ply::<()>::new(&DEFAULT_FONT).await;
    let mut state = GuiState::new(our_peer_id);

    loop {
        // 1. Drain swarm events into state.
        loop {
            match ui_rx.try_recv() {
                Ok(ev) => apply_ui_event(&mut state, ev),
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
                    let _ = action_tx.try_send(UserAction::Quit);
                    return;
                }
            }
        }

        clear_background(BLACK);

        // 2. Build the UI tree.
        let mut ui = ply.begin();
        ui.element().width(grow!()).height(grow!())
            .layout(|l| l.direction(LeftToRight).gap(8).padding(8))
            .children(|ui| {
                // Left column: event log (scroll) + input.
                ui.element().width(fixed!(650.0)).height(grow!())
                    .layout(|l| l.direction(TopToBottom).gap(8))
                    .children(|ui| {
                        // Scrollable log.
                        ui.element().width(grow!()).height(grow!())
                            .background_color(0x1A1A1A)
                            .layout(|l| l.direction(TopToBottom).gap(4).padding(8))
                            .overflow(|o| o.scroll_y().scrollbar(|s| s.width(4.0).thumb_color(0x666666).track_color(0x222222)))
                            .children(|ui| {
                                for entry in &state.events {
                                    let color = match entry.kind {
                                        EventKind::System => 0x66CCFF,
                                        EventKind::Chat => 0x66FF88,
                                        EventKind::Warn => 0xFFCC44,
                                        EventKind::Info => 0xAAAAAA,
                                    };
                                    ui.text(&entry.text, |t| t.font_size(18).color(color));
                                }
                            });
                        // Input box (hand-rolled — render state.input as text; cursor is a trailing '_').
                        ui.element().width(grow!()).height(fixed!(40.0))
                            .background_color(0x000000)
                            .layout(|l| l.padding(8).align(Left, CenterY))
                            .children(|ui| {
                                ui.text(&format!("> {}_", state.input), |t| t.font_size(20).color(0xFFFFFF));
                            });
                    });

                // Right column: connected + discovered + status.
                ui.element().width(grow!()).height(grow!())
                    .layout(|l| l.direction(TopToBottom).gap(8))
                    .children(|ui| {
                        // Connected peers (selectable).
                        ui.element().width(grow!()).height(fixed!(220.0))
                            .background_color(0x1A1A1A)
                            .layout(|l| l.direction(TopToBottom).gap(2).padding(8))
                            .overflow(|o| o.scroll_y())
                            .children(|ui| {
                                ui.text("Peers (connected)", |t| t.font_size(16).color(0xFFFFFF));
                                for (i, p) in state.connected_peers.iter().enumerate() {
                                    let tag = if p.direct { "D" } else { "R" };
                                    let sel = i == state.selected_peer;
                                    let color = if sel { 0xFFFF66 } else { 0xCCCCCC };
                                    ui.text(&format!("{tag} {}", short_pid(&p.peer_id)), |t| t.font_size(16).color(color));
                                }
                            });
                        // Discovered.
                        ui.element().width(grow!()).height(fixed!(160.0))
                            .background_color(0x1A1A1A)
                            .layout(|l| l.direction(TopToBottom).gap(2).padding(8))
                            .overflow(|o| o.scroll_y())
                            .children(|ui| {
                                ui.text("Discovered", |t| t.font_size(16).color(0xFFFFFF));
                                for p in &state.discovered_peers {
                                    ui.text(&format!("{} ({})", short_pid(&p.peer_id), p.source), |t| t.font_size(16).color(0xAAAAAA));
                                }
                            });
                        // Status.
                        ui.element().width(grow!()).height(grow!())
                            .background_color(0x1A1A1A)
                            .layout(|l| l.direction(TopToBottom).gap(2).padding(8))
                            .children(|ui| {
                                ui.text("Status", |t| t.font_size(16).color(0xFFFFFF));
                                ui.text(&format!("PeerId: {}", short_pid(&state.our_peer_id)), |t| t.font_size(14).color(0xCCCCCC));
                                ui.text(&format!("NAT: {}", state.nat_status), |t| t.font_size(14).color(0xCCCCCC));
                                ui.text(&format!("Peers: {} connected", state.connected_peers.len()), |t| t.font_size(14).color(0xCCCCCC));
                                ui.text(&format!("Cache: {} peers", state.cache_count), |t| t.font_size(14).color(0xCCCCCC));
                                ui.text("Enter=send  Shift+Enter=broadcast  Tab=next peer  Esc=quit", |t| t.font_size(12).color(0x777777));
                            });
                    });
            });
        ui.show(|_| {}).await;

        // 3. Hand-rolled text input (rustywx pattern; cookbook §7/§9).
        //    Append printable chars; Backspace deletes. Filter control chars
        //    (Enter/Backspace arrive here too on some platforms).
        while let Some(c) = get_char_pressed() {
            if !c.is_control() {
                state.input.push(c);
            }
        }
        if is_key_pressed(KeyCode::Backspace) {
            state.input.pop();
        }

        // 4. Keyboard actions (macroquad key APIs).
        let shift = is_key_down(KeyCode::LeftShift) || is_key_down(KeyCode::RightShift);
        if is_key_pressed(KeyCode::Tab) && !state.connected_peers.is_empty() {
            state.selected_peer = (state.selected_peer + 1) % state.connected_peers.len();
        }
        if is_key_pressed(KeyCode::Enter) && !state.input.is_empty() {
            if shift {
                let text = std::mem::take(&mut state.input);
                let _ = action_tx.try_send(UserAction::Broadcast { text });
            } else if let Some(p) = state.connected_peers.get(state.selected_peer) {
                if let Ok(pid) = p.peer_id.parse::<PeerId>() {
                    let text = std::mem::take(&mut state.input);
                    let _ = action_tx.try_send(UserAction::SendMessage { peer_id: pid, text });
                }
            }
        }
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

        apply_ui_event(&mut s, UiEvent::LocalPeerId("x".to_string()));
        assert_eq!(s.our_peer_id, "x");

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
        assert!(s.events.last().unwrap().text.contains("hi"));
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
