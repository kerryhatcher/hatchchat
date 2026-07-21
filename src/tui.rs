//! TUI module for hatch-chat.
//!
//! Uses ratatui + crossterm to render a terminal user interface that
//! shows the P2P connection/discovery process in real-time.  The TUI
//! runs in a dedicated OS thread with blocking crossterm event polling,
//! while the main tokio task drives the libp2p swarm.

use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use libp2p::PeerId;
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap},
    Frame, Terminal,
};
use std::io::stdout;
use std::sync::mpsc;
use tokio::sync::mpsc as tokio_mpsc;

// ── Channel message types ───────────────────────────────────────────────────

/// Events sent from the swarm task → TUI thread.
#[derive(Debug, Clone)]
pub enum UiEvent {
    /// General info message (like a log line).
    Info(String),
    /// Warning message.
    Warn(String),
    /// A peer connected (directly or via relay).
    PeerConnected {
        peer_id: String,
        addr: String,
        direct: bool,
    },
    /// A peer disconnected.
    PeerDisconnected {
        peer_id: String,
    },
    /// A peer was discovered via some mechanism.
    PeerDiscovered {
        peer_id: String,
        addr: String,
        source: String,
    },
    /// A chat message was received.
    ChatMessage {
        from: String,
        text: String,
    },
    /// NAT status changed.
    NatStatus(String),
    /// A new listen address is available.
    ListenAddr(String),
    /// A DCUtR hole-punch result.
    HolePunch {
        peer_id: String,
        success: bool,
    },
    /// Relay event summary.
    RelayEvent(String),
    /// DHT discovery result.
    DhtRecord(String),
    /// Number of peers currently in the persistent cache.
    CacheCount(usize),
}

/// Actions sent from the TUI thread → swarm task.
#[derive(Debug, Clone)]
pub enum UserAction {
    /// Send a direct message to a specific peer via request-response.
    SendMessage {
        peer_id: PeerId,
        text: String,
    },
    /// Broadcast a message to all peers via gossipsub.
    Broadcast {
        text: String,
    },
    /// Quit the application.
    Quit,
}

// ── TUI state ───────────────────────────────────────────────────────────────

/// Kind of log entry — controls prefix and colour.
#[derive(Debug, Clone, Copy)]
enum EventKind {
    System,
    Chat,
    Warn,
    Info,
}

/// A connected peer entry.
#[allow(dead_code)]
struct ConnectedPeer {
    peer_id: PeerId,
    peer_id_str: String,
    addr: String,
    direct: bool,
}

/// A discovered (not necessarily connected) peer entry.
#[allow(dead_code)]
struct DiscoveredPeer {
    peer_id_str: String,
    addr: String,
    source: String,
}

/// All state owned by the TUI thread.
struct TuiState {
    /// Pre-formatted, styled log lines.
    events: Vec<Line<'static>>,
    connected_peers: Vec<ConnectedPeer>,
    discovered_peers: Vec<DiscoveredPeer>,
    our_peer_id: String,
    nat_status: String,
    cache_count: usize,
    input: String,
    selected_peer: usize,
    should_quit: bool,
}

/// RAII guard that restores the terminal on drop.
struct TerminalGuard;
impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(stdout(), LeaveAlternateScreen);
    }
}

// ── Public entry point ──────────────────────────────────────────────────────

/// Run the TUI in the current thread.  Blocks until the user quits or the
/// swarm task disconnects.
///
/// * `ui_rx` — receives events from the swarm.
/// * `action_tx` — sends user actions to the swarm.
/// * `our_peer_id` — the local node's PeerId string (for the status panel).
/// * `initial_cache_count` — number of peers in the persistent cache at startup.
pub fn run_tui(
    ui_rx: mpsc::Receiver<UiEvent>,
    action_tx: tokio_mpsc::Sender<UserAction>,
    our_peer_id: String,
    initial_cache_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    // Setup terminal — guard ensures restoration even on early return / panic.
    let _guard = TerminalGuard;
    enable_raw_mode()?;
    execute!(stdout(), EnterAlternateScreen)?;

    let backend = ratatui::backend::CrosstermBackend::new(stdout());
    let mut terminal: Terminal<_> = Terminal::new(backend)?;

    let mut state = TuiState {
        events: Vec::new(),
        connected_peers: Vec::new(),
        discovered_peers: Vec::new(),
        our_peer_id,
        nat_status: "Unknown".to_string(),
        cache_count: initial_cache_count,
        input: String::new(),
        selected_peer: 0,
        should_quit: false,
    };

    add_event(&mut state, "hatch-chat TUI started", EventKind::System);

    // Main render / input loop.
    loop {
        // Drain all pending UI events from the swarm.
        loop {
            match ui_rx.try_recv() {
                Ok(event) => handle_ui_event(&mut state, event),
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
                    state.should_quit = true;
                    break;
                }
            }
        }

        if state.should_quit {
            break;
        }

        // Render.
        terminal.draw(|f| render(f, &state))?;

        // Poll for keyboard input (50 ms timeout so we can still process
        // incoming UI events promptly).
        if event::poll(std::time::Duration::from_millis(50))? {
            if let Event::Key(key) = event::read()? {
                handle_key_event(&mut state, &action_tx, key);
            }
        }
    }

    Ok(())
}

// ── Event handling ──────────────────────────────────────────────────────────

/// Append a styled log line to the event buffer.
fn add_event(state: &mut TuiState, text: &str, kind: EventKind) {
    let (prefix, color) = match kind {
        EventKind::System => ("[System] ", Color::Cyan),
        EventKind::Chat => ("", Color::Green),
        EventKind::Warn => ("[WARN] ", Color::Yellow),
        EventKind::Info => ("", Color::Gray),
    };

    let line = Line::from(vec![
        Span::styled(
            prefix.to_string(),
            Style::default().fg(color).add_modifier(Modifier::BOLD),
        ),
        Span::styled(text.to_string(), Style::default().fg(color)),
    ]);

    state.events.push(line);
    // Cap at 1000 entries to bound memory.
    if state.events.len() > 1000 {
        state.events.remove(0);
    }
}

/// Process a single [`UiEvent`] from the swarm.
fn handle_ui_event(state: &mut TuiState, event: UiEvent) {
    match event {
        UiEvent::Info(msg) => add_event(state, &msg, EventKind::Info),
        UiEvent::Warn(msg) => add_event(state, &msg, EventKind::Warn),
        UiEvent::PeerConnected {
            peer_id,
            addr,
            direct,
        } => {
            if !state.connected_peers.iter().any(|p| p.peer_id_str == peer_id) {
                if let Ok(pid) = peer_id.parse::<PeerId>() {
                    state.connected_peers.push(ConnectedPeer {
                        peer_id: pid,
                        peer_id_str: peer_id.clone(),
                        addr: addr.clone(),
                        direct,
                    });
                }
            }
            let kind_str = if direct { "direct" } else { "relayed" };
            add_event(
                state,
                &format!("Connected to {} ({kind_str}) at {}", short_pid(&peer_id), addr),
                EventKind::System,
            );
        }
        UiEvent::PeerDisconnected { peer_id } => {
            state
                .connected_peers
                .retain(|p| p.peer_id_str != peer_id);
            if state.selected_peer >= state.connected_peers.len() {
                state.selected_peer = state.connected_peers.len().saturating_sub(1);
            }
            add_event(
                state,
                &format!("Disconnected from {}", short_pid(&peer_id)),
                EventKind::System,
            );
        }
        UiEvent::PeerDiscovered {
            peer_id,
            addr,
            source,
        } => {
            if !state
                .discovered_peers
                .iter()
                .any(|p| p.peer_id_str == peer_id && p.addr == addr)
            {
                state.discovered_peers.push(DiscoveredPeer {
                    peer_id_str: peer_id.clone(),
                    addr: addr.clone(),
                    source: source.clone(),
                });
                if state.discovered_peers.len() > 100 {
                    state.discovered_peers.remove(0);
                }
            }
            add_event(
                state,
                &format!("Discovered {} at {} via {}", short_pid(&peer_id), addr, source),
                EventKind::System,
            );
        }
        UiEvent::ChatMessage { from, text } => {
            add_event(state, &format!("[{}] {}", short_pid(&from), text), EventKind::Chat);
        }
        UiEvent::NatStatus(status) => {
            state.nat_status = status.clone();
            add_event(state, &format!("NAT status: {status}"), EventKind::System);
        }
        UiEvent::ListenAddr(addr) => {
            add_event(state, &format!("Listening on {addr}"), EventKind::System);
        }
        UiEvent::HolePunch { peer_id, success } => {
            if success {
                add_event(
                    state,
                    &format!("Hole punch succeeded with {}", short_pid(&peer_id)),
                    EventKind::System,
                );
            } else {
                add_event(
                    state,
                    &format!("Hole punch failed with {}", short_pid(&peer_id)),
                    EventKind::Warn,
                );
            }
        }
        UiEvent::RelayEvent(msg) => add_event(state, &format!("Relay: {msg}"), EventKind::Info),
        UiEvent::DhtRecord(msg) => add_event(state, &format!("DHT: {msg}"), EventKind::Info),
        UiEvent::CacheCount(n) => {
            state.cache_count = n;
        }
    }
}

// ── Key handling ────────────────────────────────────────────────────────────

fn handle_key_event(state: &mut TuiState, action_tx: &tokio_mpsc::Sender<UserAction>, key: KeyEvent) {
    match key {
        // Quit — Esc or Ctrl+C
        KeyEvent {
            code: KeyCode::Esc, ..
        } => {
            state.should_quit = true;
            let _ = action_tx.try_send(UserAction::Quit);
        }
        KeyEvent {
            code: KeyCode::Char('c'),
            modifiers,
            ..
        } if modifiers.contains(KeyModifiers::CONTROL) => {
            state.should_quit = true;
            let _ = action_tx.try_send(UserAction::Quit);
        }

        // Enter — send direct message to selected peer
        KeyEvent {
            code: KeyCode::Enter,
            ..
        } => {
            if !state.input.is_empty() && !state.connected_peers.is_empty() {
                let peer = &state.connected_peers[state.selected_peer];
                let text = std::mem::take(&mut state.input);
                let _ = action_tx.try_send(UserAction::SendMessage {
                    peer_id: peer.peer_id,
                    text,
                });
            }
        }

        // Tab — cycle to next connected peer
        KeyEvent {
            code: KeyCode::Tab, ..
        } => {
            if !state.connected_peers.is_empty() {
                state.selected_peer = (state.selected_peer + 1) % state.connected_peers.len();
            }
        }

        // Up / Down — navigate peer list
        KeyEvent {
            code: KeyCode::Up, ..
        } => {
            if state.selected_peer > 0 {
                state.selected_peer -= 1;
            }
        }
        KeyEvent {
            code: KeyCode::Down,
            ..
        } => {
            if state.selected_peer + 1 < state.connected_peers.len() {
                state.selected_peer += 1;
            }
        }

        // Backspace
        KeyEvent {
            code: KeyCode::Backspace,
            ..
        } => {
            state.input.pop();
        }

        // Broadcast (Shift+B)
        KeyEvent {
            code: KeyCode::Char('B'),
            ..
        } => {
            if !state.input.is_empty() {
                let text = std::mem::take(&mut state.input);
                let _ = action_tx.try_send(UserAction::Broadcast { text });
            }
        }

        // Regular character input
        KeyEvent {
            code: KeyCode::Char(c),
            ..
        } => {
            state.input.push(c);
        }

        _ => {}
    }
}

// ── Rendering ───────────────────────────────────────────────────────────────

fn render(f: &mut Frame, state: &TuiState) {
    let size = f.size();

    // Vertical: [content, help_bar]
    let main_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(3), Constraint::Length(1)])
        .split(size);

    let content_area = main_chunks[0];
    let help_area = main_chunks[1];

    // Horizontal: [left, right]
    let content_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(65), Constraint::Percentage(35)])
        .split(content_area);

    let left_area = content_chunks[0];
    let right_area = content_chunks[1];

    // Left vertical: [events, input]
    let left_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(3), Constraint::Length(3)])
        .split(left_area);

    let events_area = left_chunks[0];
    let input_area = left_chunks[1];

    // Right vertical: [connected, discovered, status]
    let right_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(40),
            Constraint::Percentage(30),
            Constraint::Percentage(30),
        ])
        .split(right_area);

    let connected_area = right_chunks[0];
    let discovered_area = right_chunks[1];
    let status_area = right_chunks[2];

    render_events(f, events_area, state);
    render_input(f, input_area, state);
    render_connected(f, connected_area, state);
    render_discovered(f, discovered_area, state);
    render_status(f, status_area, state);
    render_help(f, help_area);
}

fn render_events(f: &mut Frame, area: Rect, state: &TuiState) {
    let block = Block::default().borders(Borders::ALL).title(" Chat / Events Log ");

    let inner = block.inner(area);
    f.render_widget(block, area);

    let visible_h = inner.height as usize;
    let total = state.events.len();
    let start = total.saturating_sub(visible_h);

    let lines: Vec<Line> = state.events[start..].to_vec();
    let paragraph = Paragraph::new(lines).wrap(Wrap { trim: false });
    f.render_widget(paragraph, inner);
}

fn render_input(f: &mut Frame, area: Rect, state: &TuiState) {
    let block = Block::default().borders(Borders::ALL).title(" Input ");

    let text = format!("> {}", state.input);
    let paragraph = Paragraph::new(text).block(block);
    f.render_widget(paragraph, area);

    // Position the terminal cursor at the end of the input text.
    let cursor_x = area.x + 1 + 2 + state.input.len() as u16;
    let cursor_y = area.y + 1;
    // Clamp to inner area.
    let max_x = area.x + area.width.saturating_sub(1);
    let clamped_x = cursor_x.min(max_x);
    f.set_cursor(clamped_x, cursor_y);
}

fn render_connected(f: &mut Frame, area: Rect, state: &TuiState) {
    let block = Block::default().borders(Borders::ALL).title(" Peers (connected) ");

    let items: Vec<ListItem> = state
        .connected_peers
        .iter()
        .map(|p| {
            let tag = if p.direct { "D" } else { "R" };
            ListItem::new(format!("{} {}", tag, short_pid(&p.peer_id_str)))
        })
        .collect();

    let mut list_state = ListState::default();
    if !state.connected_peers.is_empty() && state.selected_peer < state.connected_peers.len() {
        list_state.select(Some(state.selected_peer));
    }

    let list = List::new(items)
        .block(block)
        .highlight_style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))
        .highlight_symbol("> ");

    f.render_stateful_widget(list, area, &mut list_state);
}

fn render_discovered(f: &mut Frame, area: Rect, state: &TuiState) {
    let block = Block::default().borders(Borders::ALL).title(" Discovered ");

    let items: Vec<ListItem> = state
        .discovered_peers
        .iter()
        .map(|p| {
            ListItem::new(format!("{} ({})", short_pid(&p.peer_id_str), p.source))
        })
        .collect();

    let list = List::new(items).block(block);
    f.render_widget(list, area);
}

fn render_status(f: &mut Frame, area: Rect, state: &TuiState) {
    let block = Block::default().borders(Borders::ALL).title(" Status ");

    let lines = vec![
        Line::from(format!("PeerId: {}", short_pid(&state.our_peer_id))),
        Line::from(format!("NAT: {}", state.nat_status)),
        Line::from(format!("Peers: {} connected", state.connected_peers.len())),
        Line::from(format!("Cache: {} peers", state.cache_count)),
    ];

    let paragraph = Paragraph::new(lines).block(block);
    f.render_widget(paragraph, area);
}

fn render_help(f: &mut Frame, area: Rect) {
    let help_text =
        " ESC=quit | Enter=send to selected peer | Tab=next peer | Up/Down=navigate | B=broadcast ";
    let paragraph = Paragraph::new(help_text).style(Style::default().fg(Color::DarkGray));
    f.render_widget(paragraph, area);
}

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Truncate a PeerId string to a readable length.
fn short_pid(id: &str) -> String {
    const MAX: usize = 20;
    if id.len() <= MAX {
        id.to_string()
    } else {
        format!("{}...", &id[..MAX - 3])
    }
}