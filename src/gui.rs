//! Desktop GUI for hatch-chat, built on ply-engine (macroquad backend).
//!
//! Alternate consumer of the same UiEvent/UserAction channel contract the
//! TUI uses. Runs on the main thread (miniquad requires it); the iroh node
//! runs on a background thread.
#![cfg(feature = "gui")]

use crate::tui::{UiEvent, UserAction};
use macroquad::prelude::*;
use ply_engine::prelude::*;
use std::sync::mpsc;
use tokio::sync::mpsc as tokio_mpsc;

static DEFAULT_FONT: FontAsset = FontAsset::Bytes {
    file_name: "JetBrainsMono-Regular.ttf",
    data: include_bytes!("../assets/fonts/JetBrainsMono-Regular.ttf"),
};

#[derive(Clone)]
enum EventKind {
    System,
    Chat,
    Warn,
    Info,
}

#[derive(Clone)]
struct LogEntry {
    text: String,
    kind: EventKind,
}

struct ConnPeer {
    peer_id: String,
    #[allow(dead_code)]
    addr: String,
    direct: bool,
}

struct GuiState {
    chat: Vec<LogEntry>,
    events: Vec<LogEntry>,
    connected_peers: Vec<ConnPeer>,
    our_peer_id: String,
    cache_count: usize,
    input: String,
    selected_peer: usize,
}

impl GuiState {
    fn new(our_peer_id: String) -> Self {
        Self {
            chat: Vec::new(),
            events: Vec::new(),
            connected_peers: Vec::new(),
            our_peer_id,
            cache_count: 0,
            input: String::new(),
            selected_peer: 0,
        }
    }

    fn push(&mut self, text: String, kind: EventKind) {
        let entry = LogEntry {
            text,
            kind: kind.clone(),
        };
        let (buf, cap) = match kind {
            EventKind::Chat => (&mut self.chat, 1000),
            _ => (&mut self.events, 500),
        };
        buf.push(entry);
        if buf.len() > cap {
            buf.remove(0);
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
        UiEvent::ChatMessage { from, text } => {
            state.push(
                format!("[{}] {}", from, text),
                EventKind::Chat,
            );
        }
        UiEvent::PeerConnected {
            peer_id,
            addr,
            direct,
        } => {
            if !state.connected_peers.iter().any(|p| p.peer_id == peer_id) {
                state.connected_peers.push(ConnPeer {
                    peer_id: peer_id.clone(),
                    addr: addr.clone(),
                    direct,
                });
            }
            let kind_str = if direct { "direct" } else { "relayed" };
            state.push(
                format!("Connected to {} ({kind_str})", short_pid(&peer_id)),
                EventKind::System,
            );
        }
        UiEvent::PeerDisconnected { peer_id } => {
            state.connected_peers.retain(|p| p.peer_id != peer_id);
            if state.selected_peer >= state.connected_peers.len() {
                state.selected_peer = state.connected_peers.len().saturating_sub(1);
            }
            state.push(
                format!("Disconnected from {}", short_pid(&peer_id)),
                EventKind::System,
            );
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
        ui.element()
            .width(grow!())
            .height(grow!())
            .layout(|l| l.direction(LeftToRight).gap(8).padding(8))
            .children(|ui| {
                // Left column: chat log + input.
                ui.element()
                    .width(fixed!(650.0))
                    .height(grow!())
                    .layout(|l| l.direction(TopToBottom).gap(8))
                    .children(|ui| {
                        // Scrollable chat log.
                        ui.element()
                            .width(grow!())
                            .height(grow!())
                            .background_color(0x1A1A1A)
                            .layout(|l| l.direction(TopToBottom).gap(4).padding(8))
                            .overflow(|o| {
                                o.scroll_y().scrollbar(|s| {
                                    s.width(4.0)
                                        .thumb_color(0x666666)
                                        .track_color(0x222222)
                                })
                            })
                            .children(|ui| {
                                ui.text("Chat", |t| t.font_size(16).color(0xFFFFFF));
                                for entry in &state.chat {
                                    ui.text(&entry.text, |t| {
                                        t.font_size(18).color(0x66FF88)
                                    });
                                }
                            });
                        // Input box.
                        ui.element()
                            .width(grow!())
                            .height(fixed!(40.0))
                            .background_color(0x000000)
                            .layout(|l| l.padding(8).align(Left, CenterY))
                            .children(|ui| {
                                ui.text(&format!("> {}_", state.input), |t| {
                                    t.font_size(20).color(0xFFFFFF)
                                });
                            });
                    });

                // Right column: connected + status + events.
                ui.element()
                    .width(grow!())
                    .height(grow!())
                    .layout(|l| l.direction(TopToBottom).gap(8))
                    .children(|ui| {
                        // Connected peers.
                        ui.element()
                            .width(grow!())
                            .height(fixed!(180.0))
                            .background_color(0x1A1A1A)
                            .layout(|l| l.direction(TopToBottom).gap(2).padding(8))
                            .overflow(|o| o.scroll_y())
                            .children(|ui| {
                                ui.text("Peers (connected)", |t| {
                                    t.font_size(16).color(0xFFFFFF)
                                });
                                for (i, p) in state.connected_peers.iter().enumerate() {
                                    let tag = if p.direct { "D" } else { "R" };
                                    let sel = i == state.selected_peer;
                                    let color = if sel { 0xFFFF66 } else { 0xCCCCCC };
                                    ui.text(
                                        &format!("{tag} {}", short_pid(&p.peer_id)),
                                        |t| t.font_size(16).color(color),
                                    );
                                }
                            });
                        // Status.
                        ui.element()
                            .width(grow!())
                            .height(fixed!(120.0))
                            .background_color(0x1A1A1A)
                            .layout(|l| l.direction(TopToBottom).gap(2).padding(8))
                            .children(|ui| {
                                ui.text("Status", |t| t.font_size(16).color(0xFFFFFF));
                                ui.text(
                                    &format!("ID: {}", short_pid(&state.our_peer_id)),
                                    |t| t.font_size(14).color(0xCCCCCC),
                                );
                                ui.text(
                                    &format!(
                                        "Peers: {} connected",
                                        state.connected_peers.len()
                                    ),
                                    |t| t.font_size(14).color(0xCCCCCC),
                                );
                                ui.text(
                                    &format!("Cache: {} peers", state.cache_count),
                                    |t| t.font_size(14).color(0xCCCCCC),
                                );
                                ui.text(
                                    "Enter=broadcast  Tab=next peer  Esc=quit",
                                    |t| t.font_size(12).color(0x777777),
                                );
                            });
                        // Events log.
                        ui.element()
                            .width(grow!())
                            .height(grow!())
                            .background_color(0x1A1A1A)
                            .layout(|l| l.direction(TopToBottom).gap(2).padding(8))
                            .overflow(|o| {
                                o.scroll_y().scrollbar(|s| {
                                    s.width(4.0)
                                        .thumb_color(0x666666)
                                        .track_color(0x222222)
                                })
                            })
                            .children(|ui| {
                                ui.text("Events", |t| t.font_size(16).color(0xFFFFFF));
                                for entry in &state.events {
                                    let color = match entry.kind {
                                        EventKind::System => 0x66CCFF,
                                        EventKind::Chat => 0x66FF88,
                                        EventKind::Warn => 0xFFCC44,
                                        EventKind::Info => 0xAAAAAA,
                                    };
                                    ui.text(&entry.text, |t| t.font_size(14).color(color));
                                }
                            });
                    });
            });
        ui.show(|_| {}).await;

        // 3. Text input.
        while let Some(c) = get_char_pressed() {
            if !c.is_control() {
                state.input.push(c);
            }
        }
        if is_key_pressed(KeyCode::Backspace) {
            state.input.pop();
        }

        // 4. Keyboard actions.
        if is_key_pressed(KeyCode::Tab) && !state.connected_peers.is_empty() {
            state.selected_peer =
                (state.selected_peer + 1) % state.connected_peers.len();
        }
        if is_key_pressed(KeyCode::Enter) && !state.input.is_empty() {
            let text = std::mem::take(&mut state.input);
            let _ = action_tx.try_send(UserAction::Broadcast { text });
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
    use super::{apply_ui_event, GuiState};
    use crate::tui::UiEvent;

    #[test]
    fn maps_events_to_state() {
        let mut s = GuiState::new("me".to_string());

        apply_ui_event(&mut s, UiEvent::CacheCount(7));
        assert_eq!(s.cache_count, 7);

        apply_ui_event(&mut s, UiEvent::LocalPeerId("x".to_string()));
        assert_eq!(s.our_peer_id, "x");

        apply_ui_event(&mut s, UiEvent::PeerConnected {
            peer_id: "abc".into(),
            addr: "gossip".into(),
            direct: true,
        });
        assert_eq!(s.connected_peers.len(), 1);
        // Idempotent on duplicate peer_id.
        apply_ui_event(&mut s, UiEvent::PeerConnected {
            peer_id: "abc".into(),
            addr: "gossip".into(),
            direct: true,
        });
        assert_eq!(s.connected_peers.len(), 1);

        apply_ui_event(&mut s, UiEvent::PeerDisconnected {
            peer_id: "abc".into(),
        });
        assert_eq!(s.connected_peers.len(), 0);

        // Chat goes to the chat buffer.
        apply_ui_event(&mut s, UiEvent::ChatMessage {
            from: "bob".into(),
            text: "hi".into(),
        });
        assert!(s.chat.last().unwrap().text.contains("hi"));
        assert_eq!(s.chat.len(), 1);
        assert!(!s.events.is_empty());
    }

    #[test]
    fn event_log_is_capped() {
        let mut s = GuiState::new("me".to_string());
        for i in 0..600 {
            apply_ui_event(&mut s, UiEvent::Info(format!("line {i}")));
        }
        assert!(s.events.len() <= 500);
    }

    #[test]
    fn chat_and_events_are_separate() {
        let mut s = GuiState::new("me".to_string());
        for i in 0..1100 {
            apply_ui_event(&mut s, UiEvent::Info(format!("evt {i}")));
        }
        apply_ui_event(&mut s, UiEvent::ChatMessage {
            from: "a".into(),
            text: "hello".into(),
        });
        assert!(s.events.len() <= 500);
        assert_eq!(s.chat.len(), 1);
    }
}
