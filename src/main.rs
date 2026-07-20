use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use ratatui::{
    backend::CrosstermBackend,
    widgets::{Block, Borders, List, ListItem, Paragraph},
    layout::{Layout, Constraint, Direction},
    style::{Style, Color, Modifier},
    Terminal,
};
use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use std::io;

mod network;
use network::{NetNode, WirePacket};

// ── App state ──────────────────────────────────────────────────────────────

struct Peer {
    name: String,
    addr: String,
    last_seen: Instant,
}

struct ChatRequest {
    from_name: String,
    from_addr: String,
}

struct App {
    my_name: String,
    my_addr: String,
    net: Arc<NetNode>,

    /// People we've accepted and can chat with
    contacts: HashMap<String, Peer>,  // keyed by addr hex

    /// People we've seen announces from but haven't connected yet
    discovered: HashMap<String, Peer>,  // keyed by addr hex

    /// Incoming chat requests waiting for our accept/decline
    pending_requests: Vec<ChatRequest>,

    /// Chat messages
    messages: Vec<(String, String, Instant)>,  // (sender_name, text, timestamp)

    /// Input buffer
    input: String,

    /// Which sidebar item is selected (for sending chat requests)
    selected_discovered: usize,

    /// Scroll offset for messages
    scroll: usize,
}

impl App {
    fn new(name: String, addr: String, net: Arc<NetNode>) -> Self {
        Self {
            my_name: name.clone(),
            my_addr: addr,
            net,
            contacts: HashMap::new(),
            discovered: HashMap::new(),
            pending_requests: Vec::new(),
            messages: vec![("System".into(), format!("Welcome, {}! Discovering other nodes...", name), Instant::now())],
            input: String::new(),
            selected_discovered: 0,
            scroll: 0,
        }
    }

    fn send_message(&mut self) {
        if self.input.is_empty() || self.contacts.is_empty() {
            if self.contacts.is_empty() {
                self.messages.push(("System".into(), "No contacts yet. Accept a chat request first!".into(), Instant::now()));
            }
            return;
        }
        let text = self.input.clone();
        self.input.clear();

        // Show our own message immediately
        self.messages.push((self.my_name.clone(), text.clone(), Instant::now()));

        // Broadcast to everyone (only contacts will display it)
        let packet = WirePacket::Message {
            from_name: self.my_name.clone(),
            from_addr: self.my_addr.clone(),
            text,
        };
        let net = self.net.clone();
        tokio::spawn(async move {
            net.send(&packet).await;
        });
    }

    fn send_chat_request(&mut self) {
        let discovered_list: Vec<String> = self.discovered.keys().cloned().collect();
        if discovered_list.is_empty() {
            return;
        }
        let idx = self.selected_discovered.min(discovered_list.len() - 1);
        let target_addr = discovered_list[idx].clone();
        let target_name = self.discovered[&target_addr].name.clone();

        let packet = WirePacket::ChatRequest {
            from_name: self.my_name.clone(),
            from_addr: self.my_addr.clone(),
            to_addr: target_addr.clone(),
        };
        let net = self.net.clone();
        tokio::spawn(async move {
            net.send(&packet).await;
        });

        self.messages.push(("System".into(), format!("Sent chat request to {}", target_name), Instant::now()));
    }

    fn accept_request(&mut self) {
        if self.pending_requests.is_empty() {
            return;
        }
        let req = self.pending_requests.remove(0);

        // Add them as a contact
        self.contacts.insert(req.from_addr.clone(), Peer {
            name: req.from_name.clone(),
            addr: req.from_addr.clone(),
            last_seen: Instant::now(),
        });

        // Remove from discovered
        self.discovered.remove(&req.from_addr);

        // Send accept packet
        let packet = WirePacket::ChatAccept {
            from_name: self.my_name.clone(),
            from_addr: self.my_addr.clone(),
            to_addr: req.from_addr.clone(),
        };
        let net = self.net.clone();
        tokio::spawn(async move {
            net.send(&packet).await;
        });

        // Send our contact list to the new member so they know about everyone
        if self.contacts.len() > 1 {
            let contacts_vec: Vec<(String, String)> = self.contacts
                .values()
                .map(|p| (p.name.clone(), p.addr.clone()))
                .collect();
            let packet = WirePacket::ContactList {
                from_addr: self.my_addr.clone(),
                to_addr: req.from_addr.clone(),
                contacts: contacts_vec,
            };
            let net = self.net.clone();
            tokio::spawn(async move {
                net.send(&packet).await;
            });
        }

        // Notify existing contacts about the new member
        let add_packet = WirePacket::AddContact {
            from_addr: self.my_addr.clone(),
            new_name: req.from_name.clone(),
            new_addr: req.from_addr.clone(),
        };
        let net = self.net.clone();
        tokio::spawn(async move {
            net.send(&add_packet).await;
        });

        self.messages.push(("System".into(), format!("Accepted chat request from {}", req.from_name), Instant::now()));
    }

    fn decline_request(&mut self) {
        if self.pending_requests.is_empty() {
            return;
        }
        let req = self.pending_requests.remove(0);
        let packet = WirePacket::ChatDecline {
            from_name: self.my_name.clone(),
            from_addr: self.my_addr.clone(),
            to_addr: req.from_addr.clone(),
        };
        let net = self.net.clone();
        tokio::spawn(async move {
            net.send(&packet).await;
        });
        self.messages.push(("System".into(), format!("Declined chat request from {}", req.from_name), Instant::now()));
    }

    fn handle_packet(&mut self, packet: WirePacket) {
        match packet {
            WirePacket::Announce { name, addr } => {
                if addr != self.my_addr {
                    if !self.contacts.contains_key(&addr) {
                        let is_new = !self.discovered.contains_key(&addr);
                        let name_for_msg = name.clone();
                        self.discovered.insert(addr.clone(), Peer {
                            name,
                            addr: addr.clone(),
                            last_seen: Instant::now(),
                        });
                        if is_new {
                            self.messages.push(("System".into(), format!("Discovered node: {}", name_for_msg), Instant::now()));
                        }
                    } else {
                        // Update last seen for known contact
                        if let Some(p) = self.contacts.get_mut(&addr) {
                            p.last_seen = Instant::now();
                        }
                    }
                }
            }

            WirePacket::ChatRequest { from_name, from_addr, to_addr } => {
                if to_addr == self.my_addr && !self.contacts.contains_key(&from_addr) {
                    // Check if we already have a pending request from this person
                    let already_pending = self.pending_requests.iter().any(|r| r.from_addr == from_addr);
                    if !already_pending {
                        self.pending_requests.push(ChatRequest { from_name, from_addr });
                    }
                }
            }

            WirePacket::ChatAccept { from_name, from_addr, to_addr } => {
                if to_addr == self.my_addr {
                    self.contacts.insert(from_addr.clone(), Peer {
                        name: from_name.clone(),
                        addr: from_addr.clone(),
                        last_seen: Instant::now(),
                    });
                    self.discovered.remove(&from_addr);
                    self.messages.push(("System".into(), format!("{} accepted your chat request", from_name), Instant::now()));
                }
            }

            WirePacket::ChatDecline { from_name, from_addr, to_addr } => {
                if to_addr == self.my_addr {
                    self.messages.push(("System".into(), format!("{} declined your chat request", from_name), Instant::now()));
                }
            }

            WirePacket::Message { from_name, from_addr, text } => {
                // Only display messages from our contacts
                if self.contacts.contains_key(&from_addr) {
                    self.messages.push((from_name, text, Instant::now()));
                    // Update last seen
                    if let Some(p) = self.contacts.get_mut(&from_addr) {
                        p.last_seen = Instant::now();
                    }
                }
            }

            WirePacket::ContactList { from_addr, to_addr, contacts } => {
                if to_addr == self.my_addr {
                    // Add all contacts from the list (excluding ourselves)
                    for (name, addr) in contacts {
                        if addr != self.my_addr && !self.contacts.contains_key(&addr) {
                            self.contacts.insert(addr.clone(), Peer {
                                name,
                                addr: addr.clone(),
                                last_seen: Instant::now(),
                            });
                            self.discovered.remove(&addr);
                        }
                    }
                    self.messages.push(("System".into(), "Received group contact list".into(), Instant::now()));
                }
            }

            WirePacket::AddContact { from_addr, new_name, new_addr } => {
                // Only accept AddContact from our contacts
                if self.contacts.contains_key(&from_addr) && new_addr != self.my_addr {
                    if !self.contacts.contains_key(&new_addr) {
                        self.contacts.insert(new_addr.clone(), Peer {
                            name: new_name.clone(),
                            addr: new_addr.clone(),
                            last_seen: Instant::now(),
                        });
                        self.discovered.remove(&new_addr);
                        self.messages.push(("System".into(), format!("{} joined the group", new_name), Instant::now()));
                    }
                }
            }
        }
    }

    fn prune_stale(&mut self) {
        let now = Instant::now();
        let timeout = Duration::from_secs(15);

        // Remove discovered nodes we haven't heard from in a while
        let stale_addrs: Vec<String> = self.discovered.iter()
            .filter(|(_, p)| now.duration_since(p.last_seen) > timeout)
            .map(|(addr, _)| addr.clone())
            .collect();

        for addr in stale_addrs {
            if let Some(p) = self.discovered.remove(&addr) {
                self.messages.push(("System".into(), format!("Node {} disappeared", p.name), Instant::now()));
            }
        }

        // Mark contacts as stale (but don't remove them)
        // In a real app, you might want to show them as offline
    }
}

// ── TUI rendering ───────────────────────────────────────────────────────────

fn render(app: &App, terminal: &mut Terminal<CrosstermBackend<io::Stdout>>) -> io::Result<()> {
    terminal.draw(|f| {
        let main_layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(3),   // chat + sidebar
                Constraint::Length(3), // input
                Constraint::Length(1), // help bar
            ])
            .split(f.size());

        // Top: chat (left) + sidebar (right)
        let top_layout = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(70),
                Constraint::Percentage(30),
            ])
            .split(main_layout[0]);

        // Chat messages
        let msgs: Vec<ListItem> = app.messages.iter().rev()
            .take(main_layout[0].height as usize - 2)
            .map(|(user, text, _)| {
                let style = if user == "System" {
                    Style::default().fg(Color::DarkGray)
                } else {
                    Style::default()
                };
                ListItem::new(format!("{}: {}", user, text)).style(style)
            })
            .collect();

        let chat_list = List::new(msgs)
            .block(Block::default()
                .title(format!(" Chat ({}) ", app.my_name))
                .borders(Borders::ALL));
        f.render_widget(chat_list, top_layout[0]);

        // Sidebar: contacts + discovered + requests
        let sidebar_layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Percentage(30), // contacts
                Constraint::Percentage(40), // discovered
                Constraint::Percentage(30), // requests
            ])
            .split(top_layout[1]);

        // Contacts list
        let contacts_items: Vec<ListItem> = app.contacts.values()
            .map(|p| ListItem::new(format!(" {} ", p.name)))
            .collect();
        let contacts_list = List::new(contacts_items)
            .block(Block::default()
                .title(format!(" Contacts ({}) ", app.contacts.len()))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Green)));
        f.render_widget(contacts_list, sidebar_layout[0]);

        // Discovered nodes
        let discovered_items: Vec<ListItem> = app.discovered.values()
            .enumerate()
            .map(|(i, p)| {
                let prefix = if i == app.selected_discovered { ">" } else { " " };
                ListItem::new(format!("{} {} ", prefix, p.name))
            })
            .collect();
        let discovered_list = List::new(discovered_items)
            .block(Block::default()
                .title(format!(" Discovered ({}) ", app.discovered.len()))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Yellow)));
        f.render_widget(discovered_list, sidebar_layout[1]);

        // Pending requests
        let request_items: Vec<ListItem> = app.pending_requests.iter()
            .map(|r| ListItem::new(format!(" {} wants to chat ", r.from_name)))
            .collect();
        let request_list = List::new(request_items)
            .block(Block::default()
                .title(format!(" Requests ({}) ", app.pending_requests.len()))
                .borders(Borders::ALL)
                .border_style(if app.pending_requests.is_empty() {
                    Style::default().fg(Color::DarkGray)
                } else {
                    Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
                }));
        f.render_widget(request_list, sidebar_layout[2]);

        // Input field
        let input = Paragraph::new(app.input.as_str())
            .block(Block::default()
                .title(" Message (Enter=send, Tab=request, Ctrl+Y=accept, Ctrl+N=decline) ")
                .borders(Borders::ALL));
        f.render_widget(input, main_layout[1]);

        // Help bar
        let help = Paragraph::new(" ESC=quit | Enter=send | Tab=chat request | Ctrl+Y=accept | Ctrl+N=decline | ↑↓=select ")
            .style(Style::default().fg(Color::DarkGray));
        f.render_widget(help, main_layout[2]);
    })?;
    Ok(())
}

// ── Main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    // Parse port from command line (default 4242)
    let port: u16 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(4242);

    println!("=== Reticulum LAN Chat ===");
    println!("Enter your name: ");
    let mut name = String::new();
    io::stdin().read_line(&mut name)?;
    let name = name.trim().to_string();

    if name.is_empty() {
        println!("Name cannot be empty!");
        return Ok(());
    }

    let net = NetNode::new(name.clone(), port).await;
    let my_addr = net.addr_hex.clone();

    println!("Node address: {}", my_addr);
    println!("Listening on UDP port {} (broadcast mode)", port);
    println!("Press Enter to enter the chatroom...");
    let mut dummy = String::new();
    io::stdin().read_line(&mut dummy)?;

    let mut app = App::new(name, my_addr, net.clone());

    // Setup TUI
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Announcement loop — broadcast our presence every 3 seconds
    let net_announce = net.clone();
    let my_name = net_announce.my_name.clone();
    let my_addr_for_announce = net_announce.addr_hex.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        loop {
            let packet = WirePacket::Announce {
                name: my_name.clone(),
                addr: my_addr_for_announce.clone(),
            };
            net_announce.send(&packet).await;
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    });

    let mut last_prune = Instant::now();

    loop {
        render(&app, &mut terminal)?;

        // Poll for keyboard input (50ms timeout)
        if event::poll(Duration::from_millis(50))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    // Ctrl+key commands work even while typing
                    if key.modifiers.contains(KeyModifiers::CONTROL) {
                        match key.code {
                            KeyCode::Char('y') => {
                                app.accept_request();
                                continue;
                            }
                            KeyCode::Char('n') => {
                                app.decline_request();
                                continue;
                            }
                            KeyCode::Char('c') => {
                                break; // Ctrl+C = quit
                            }
                            _ => {}
                        }
                    }

                    match key.code {
                        KeyCode::Enter => {
                            app.send_message();
                        }
                        KeyCode::Tab => {
                            app.send_chat_request();
                        }
                        KeyCode::Up => {
                            if app.selected_discovered > 0 {
                                app.selected_discovered -= 1;
                            }
                        }
                        KeyCode::Down => {
                            let count = app.discovered.len();
                            if count > 0 && app.selected_discovered < count - 1 {
                                app.selected_discovered += 1;
                            }
                        }
                        KeyCode::Char(c) => {
                            app.input.push(c);
                        }
                        KeyCode::Backspace => {
                            app.input.pop();
                        }
                        KeyCode::Esc => break,
                        _ => {}
                    }
                }
            }
        }

        // Receive any pending UDP packets
        loop {
            match net.try_recv().await {
                Some(packet) => app.handle_packet(packet),
                None => break,
            }
        }

        // Prune stale discovered nodes every 5 seconds
        if last_prune.elapsed() > Duration::from_secs(5) {
            app.prune_stale();
            // Reset selected index if out of bounds
            if app.selected_discovered >= app.discovered.len() && app.discovered.len() > 0 {
                app.selected_discovered = app.discovered.len() - 1;
            } else if app.discovered.is_empty() {
                app.selected_discovered = 0;
            }
            last_prune = Instant::now();
        }
    }

    // Cleanup
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    Ok(())
}