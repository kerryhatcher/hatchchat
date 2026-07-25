//! TUI module for hatch-chat.
//!
//! Uses ratatui + crossterm to render a terminal user interface.
//! The TUI runs in a dedicated OS thread with blocking crossterm event
//! polling, while the main tokio task drives the iroh node.

use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
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

/// Events sent from the iroh node → TUI thread.
#[derive(Debug, Clone)]
pub enum UiEvent {
    /// General info message.
    Info(String),
    /// Warning message.
    Warn(String),
    /// A peer connected.
    PeerConnected {
        peer_id: String,
        addr: String,
        direct: bool,
    },
    /// A peer disconnected.
    PeerDisconnected {
        peer_id: String,
    },
    /// A chat message was received (or echoed from us).
    ChatMessage {
        from: String,
        text: String,
    },
    /// Number of peers in the persistent cache.
    CacheCount(usize),
    /// The local node's EndpointId.
    LocalPeerId(String),
}

/// Actions sent from the TUI thread → iroh node.
#[derive(Debug, Clone)]
pub enum UserAction {
    /// Broadcast a message to all peers via gossip.
    Broadcast {
        text: String,
    },
    /// Quit the application.
    Quit,
}

// ── TUI state ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
enum EventKind {
    System,
    Chat,
    Warn,
    Info,
}

struct ConnectedPeer {
    peer_id_str: String,
    #[allow(dead_code)]
    addr: String,
    direct: bool,
}

struct TuiState {
    chat: Vec<Line<'static>>,
    events: Vec<Line<'static>>,
    connected_peers: Vec<ConnectedPeer>,
    our_peer_id: String,
    cache_count: usize,
    input: String,
    selected_peer: usize,
    should_quit: bool,
}

struct TerminalGuard;
impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(stdout(), LeaveAlternateScreen);
    }
}

// ── Public entry point ──────────────────────────────────────────────────────

pub fn run_tui(
    ui_rx: mpsc::Receiver<UiEvent>,
    action_tx: tokio_mpsc::Sender<UserAction>,
    our_peer_id: String,
    initial_cache_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let _guard = TerminalGuard;
    enable_raw_mode()?;
    execute!(stdout(), EnterAlternateScreen)?;

    let backend = ratatui::backend::CrosstermBackend::new(stdout());
    let mut terminal: Terminal<_> = Terminal::new(backend)?;

    let mut state = TuiState {
        chat: Vec::new(),
        events: Vec::new(),
        connected_peers: Vec::new(),
        our_peer_id,
        cache_count: initial_cache_count,
        input: String::new(),
        selected_peer: 0,
        should_quit: false,
    };

    add_event(&mut state, "hatch-chat TUI started", EventKind::System);

    loop {
        // Drain all pending UI events.
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

        terminal.draw(|f| render(f, &state))?;

        if event::poll(std::time::Duration::from_millis(50))? {
            if let Event::Key(key) = event::read()? {
                handle_key_event(&mut state, &action_tx, key);
            }
        }
    }

    Ok(())
}

// ── Event handling ──────────────────────────────────────────────────────────

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

    let (buf, cap) = match kind {
        EventKind::Chat => (&mut state.chat, 1000),
        _ => (&mut state.events, 500),
    };
    buf.push(line);
    if buf.len() > cap {
        buf.remove(0);
    }
}

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
                state.connected_peers.push(ConnectedPeer {
                    peer_id_str: peer_id.clone(),
                    addr: addr.clone(),
                    direct,
                });
            }
            let kind_str = if direct { "direct" } else { "relayed" };
            add_event(
                state,
                &format!("Connected to {} ({kind_str})", short_pid(&peer_id)),
                EventKind::System,
            );
        }
        UiEvent::PeerDisconnected { peer_id } => {
            state.connected_peers.retain(|p| p.peer_id_str != peer_id);
            if state.selected_peer >= state.connected_peers.len() {
                state.selected_peer = state.connected_peers.len().saturating_sub(1);
            }
            add_event(
                state,
                &format!("Disconnected from {}", short_pid(&peer_id)),
                EventKind::System,
            );
        }
        UiEvent::ChatMessage { from, text } => {
            add_event(state, &format!("[{}] {}", from, text), EventKind::Chat);
        }
        UiEvent::CacheCount(n) => {
            state.cache_count = n;
        }
        UiEvent::LocalPeerId(id) => {
            state.our_peer_id = id;
        }
    }
}

// ── Key handling ────────────────────────────────────────────────────────────

fn handle_key_event(
    state: &mut TuiState,
    action_tx: &tokio_mpsc::Sender<UserAction>,
    key: KeyEvent,
) {
    match key {
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

        // Enter — broadcast message
        KeyEvent {
            code: KeyCode::Enter,
            ..
        } => {
            if !state.input.is_empty() {
                let text = std::mem::take(&mut state.input);
                let _ = action_tx.try_send(UserAction::Broadcast { text });
            }
        }

        // Tab — cycle to next connected peer
        KeyEvent {
            code: KeyCode::Tab, ..
        } => {
            if !state.connected_peers.is_empty() {
                state.selected_peer =
                    (state.selected_peer + 1) % state.connected_peers.len();
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

    let main_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(3), Constraint::Length(1)])
        .split(size);

    let content_area = main_chunks[0];
    let help_area = main_chunks[1];

    let content_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(65), Constraint::Percentage(35)])
        .split(content_area);

    let left_area = content_chunks[0];
    let right_area = content_chunks[1];

    let left_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(3), Constraint::Length(3)])
        .split(left_area);

    let chat_area = left_chunks[0];
    let input_area = left_chunks[1];

    let right_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(30),
            Constraint::Length(6),
            Constraint::Min(3),
        ])
        .split(right_area);

    let connected_area = right_chunks[0];
    let status_area = right_chunks[1];
    let events_area = right_chunks[2];

    render_chat(f, chat_area, state);
    render_input(f, input_area, state);
    render_connected(f, connected_area, state);
    render_status(f, status_area, state);
    render_eventlog(f, events_area, state);
    render_help(f, help_area);
}

fn render_chat(f: &mut Frame, area: Rect, state: &TuiState) {
    render_log(f, area, " Chat ", &state.chat);
}

fn render_eventlog(f: &mut Frame, area: Rect, state: &TuiState) {
    render_log(f, area, " Events ", &state.events);
}

fn render_log(f: &mut Frame, area: Rect, title: &str, lines: &[Line<'static>]) {
    let block = Block::default().borders(Borders::ALL).title(title.to_string());
    let inner = block.inner(area);
    f.render_widget(block, area);

    let visible_h = inner.height as usize;
    let start = lines.len().saturating_sub(visible_h);
    let visible: Vec<Line> = lines[start..].to_vec();
    let paragraph = Paragraph::new(visible).wrap(Wrap { trim: false });
    f.render_widget(paragraph, inner);
}

fn render_input(f: &mut Frame, area: Rect, state: &TuiState) {
    let block = Block::default().borders(Borders::ALL).title(" Input ");
    let text = format!("> {}", state.input);
    let paragraph = Paragraph::new(text).block(block);
    f.render_widget(paragraph, area);

    let cursor_x = area.x + 1 + 2 + state.input.len() as u16;
    let cursor_y = area.y + 1;
    let max_x = area.x + area.width.saturating_sub(1);
    let clamped_x = cursor_x.min(max_x);
    f.set_cursor(clamped_x, cursor_y);
}

fn render_connected(f: &mut Frame, area: Rect, state: &TuiState) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Peers (connected) ");

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
        .highlight_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("> ");

    f.render_stateful_widget(list, area, &mut list_state);
}

fn render_status(f: &mut Frame, area: Rect, state: &TuiState) {
    let block = Block::default().borders(Borders::ALL).title(" Status ");

    let lines = vec![
        Line::from(format!("ID: {}", short_pid(&state.our_peer_id))),
        Line::from(format!("Peers: {} connected", state.connected_peers.len())),
        Line::from(format!("Cache: {} peers", state.cache_count)),
    ];

    let paragraph = Paragraph::new(lines).block(block);
    f.render_widget(paragraph, area);
}

fn render_help(f: &mut Frame, area: Rect) {
    let help_text = " ESC=quit | Enter=broadcast | Tab=next peer | Up/Down=navigate ";
    let paragraph = Paragraph::new(help_text).style(Style::default().fg(Color::DarkGray));
    f.render_widget(paragraph, area);
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn short_pid(id: &str) -> String {
    const MAX: usize = 20;
    if id.chars().count() <= MAX {
        id.to_string()
    } else {
        format!("{}...", id.chars().take(MAX - 3).collect::<String>())
    }
}
