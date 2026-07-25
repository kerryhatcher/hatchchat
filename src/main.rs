//! hatch-chat — hearty peer-to-peer chat over iroh.
//!
//! Two front-ends share one networking core over an internal event/action
//! channel contract:
//!
//! - **TUI** (default) — a ratatui terminal interface.
//! - **GUI** (optional) — a ply-engine desktop window.
//!
//! Peer discovery uses iroh-gossip-rendezvous: two nodes sharing only a
//! passphrase find each other via the BitTorrent Mainline DHT.  No
//! bootstrap servers, no pre-shared addresses, no manual config.

mod iroh_net;
#[cfg(feature = "gui")]
mod gui;
mod peer_cache;
mod tui;

use clap::Parser;
use std::sync::mpsc;
use tui::{UiEvent, UserAction};

// ── CLI ─────────────────────────────────────────────────────────────────────

#[derive(clap::Subcommand, Debug, Clone)]
enum Command {
    /// Launch the desktop GUI instead of the terminal UI.
    Gui,
}

#[derive(Parser, Debug)]
#[command(name = "hatch-chat", about = "Hearty P2P chat — iroh edition")]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,

    /// Shared passphrase for rendezvous.  Two nodes with the same
    /// passphrase and app label will find each other automatically
    /// via the Mainline DHT — no other config needed.
    #[arg(short, long, default_value = "hatch-chat-default")]
    passphrase: String,

    /// Your display name in the chat.
    #[arg(short, long)]
    name: Option<String>,

    /// Data directory for persistent state (peer cache).
    #[arg(long, default_value = ".hatch-chat")]
    data_dir: String,
}

// ── main ────────────────────────────────────────────────────────────────────

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
        .with_writer(std::sync::Mutex::new(log_file))
        .init();

    let args = Args::parse();

    // Channels: UI ends held here, iroh ends handed to run_node.
    let (ui_tx, ui_rx) = mpsc::channel::<UiEvent>();
    let (action_tx, action_rx) = tokio::sync::mpsc::channel::<UserAction>(64);

    match args.command {
        Some(Command::Gui) => run_gui_path(args, ui_tx, ui_rx, action_tx, action_rx),
        None => run_tui_path(args, ui_tx, ui_rx, action_tx, action_rx),
    }
}

// ── TUI path ────────────────────────────────────────────────────────────────

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

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(iroh_net::run(
        args.passphrase,
        args.name,
        args.data_dir,
        ui_tx,
        action_rx,
    ));
    let _ = tui_thread.join();
    result
}

// ── GUI path ────────────────────────────────────────────────────────────────

#[cfg(feature = "gui")]
fn run_gui_path(
    args: Args,
    ui_tx: mpsc::Sender<UiEvent>,
    ui_rx: mpsc::Receiver<UiEvent>,
    action_tx: tokio::sync::mpsc::Sender<UserAction>,
    action_rx: tokio::sync::mpsc::Receiver<UserAction>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // iroh node on a background thread; GUI on main.
    let swarm_thread = std::thread::Builder::new()
        .name("hatch-chat-iroh".into())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    eprintln!("runtime error: {e}");
                    return;
                }
            };
            if let Err(e) = rt.block_on(iroh_net::run(
                args.passphrase,
                args.name,
                args.data_dir,
                ui_tx,
                action_rx,
            )) {
                eprintln!("iroh error: {e}");
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
