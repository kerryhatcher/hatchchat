# hatch-chat GUI (ply-engine) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an optional desktop GUI to hatch-chat, launched via a `gui` CLI subcommand, while the ratatui TUI remains the default UI.

**Architecture:** The network layer already talks to the UI over two channels (`UiEvent` swarm→UI via `std::sync::mpsc`, `UserAction` UI→swarm via `tokio::sync::mpsc`). The GUI is a drop-in alternate consumer of that same contract. The swarm setup + event loop is extracted into a runtime-agnostic `run_node`; the TUI path runs the swarm on the main thread with the TUI in a spawned thread (unchanged), while the GUI path runs the swarm on a background thread and the ply/macroquad window on the main thread (required by miniquad on macOS).

**Tech Stack:** Rust 2021, tokio, libp2p 0.54, ratatui 0.26 (TUI), ply-engine 1.x + forked macroquad (GUI), clap 4 (derive).

## Global Constraints

- `network.rs`, `discovery.rs`, `peer_cache.rs` MUST NOT change. The UI is decoupled via the `UiEvent`/`UserAction` channel contract only.
- The TUI remains the default UI: `hatch-chat` with no subcommand launches the TUI, exactly as it does today.
- ply-engine + macroquad are gated behind a cargo feature `gui`, enabled by default. `cargo build --no-default-features` MUST compile a lean binary with no ply/macroquad/GPU dependencies.
- macroquad/miniquad MUST own the main thread. In the GUI path the swarm runs on a spawned OS thread; in the TUI path the swarm stays on the main thread (unchanged).
- All existing tests in `tests/` MUST continue to pass unchanged.
- Commit type must follow Conventional Commits (`feat`/`fix`/`docs`/`refactor`/`test`/`build`/`chore`…), imperative subject, no trailing period (repo enforces this via a commit hook). End commit bodies with `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.
- Never invent ply APIs. The methods used here are taken from ply-engine's `SKILL.md`; anything not listed there is a macroquad API and must be confirmed against macroquad, not guessed.

---

### Task 1: Extract `run_node` from `main` (behavior-preserving refactor)

Move the swarm construction + `tokio::select!` loop out of `main` into a runtime-agnostic async fn, so both the TUI and (later) GUI paths can drive the node. No behavior change: the TUI still runs exactly as today.

**Files:**
- Modify: `src/main.rs` (currently `main` at lines 63–360; helpers 362–1088 unchanged)

**Interfaces:**
- Produces:
  ```rust
  async fn run_node(
      args: Args,
      ui_tx: std::sync::mpsc::Sender<tui::UiEvent>,
      mut action_rx: tokio::sync::mpsc::Receiver<tui::UserAction>,
  ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
  ```
  `run_node` owns keypair/PeerId generation, transport/behaviour build, `listen_on`, `PeerCache::open`, discovery orchestration, bootstrap dialing, the periodic intervals, and the full `select!` loop. It does NOT create the channels, spawn the UI, or set up tracing — the caller does that.

- [ ] **Step 1: Change `main` to a plain fn and add `run_node`**

Replace the current `#[tokio::main] async fn main` signature and move its body. The new `main` sets up tracing + channels + the TUI thread, then blocks on `run_node`. Everything from "Generate an Ed25519 keypair" through the end of the `loop { tokio::select! { ... } }` moves verbatim into `run_node`; only the variable it reads for args changes from the local `args`/`no_local` to the `args` parameter.

New top of `src/main.rs` `main`:

```rust
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

    // TUI path (default): swarm on main thread, TUI in a spawned thread.
    let tui_thread = std::thread::Builder::new()
        .name("hatch-chat-tui".into())
        .spawn(move || {
            // our_peer_id + cache_count are sent to the TUI as UiEvents by
            // run_node; the TUI starts with placeholders and updates live.
            if let Err(e) = tui::run_tui(ui_rx, action_tx, String::new(), 0) {
                eprintln!("TUI error: {e}");
            }
        })?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(run_node(args, ui_tx, action_rx));

    let _ = tui_thread.join();
    result
}
```

Then paste the moved body into:

```rust
async fn run_node(
    args: Args,
    ui_tx: mpsc::Sender<UiEvent>,
    mut action_rx: tokio::sync::mpsc::Receiver<UserAction>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let no_local = args.no_local;
    // ... everything from "Generate an Ed25519 keypair" (old line 82)
    //     through the end of the select! loop (old line 352) goes here,
    //     UNCHANGED, EXCEPT:
    //       - delete the `let args = Args::parse();` line (now a param)
    //       - delete the tracing setup block (now in main)
    //       - delete the TUI-thread spawn block (old lines 123–131) and the
    //         `drop(ui_tx); let _ = tui_thread.join();` cleanup (old 356–357)
    //       - `our_peer_id_str` / cached_count are still computed here and
    //         sent to the UI via ui_tx (see Task 2 for CacheCount + PeerId).
    Ok(())
}
```

> NOTE: the old `main` sent `our_peer_id` and `cached_count` to `run_tui` as constructor args. Now the TUI starts with `String::new()` / `0`; run_node must surface both over the channel. The PeerId is already sent as `UiEvent::Info(format!("Local PeerId: {peer_id}"))`. The live PeerId/cache-count display for the status panel is wired in Task 2. This task only needs to compile and keep the TUI functional.

- [ ] **Step 2: Build and run the existing tests**

Run: `cargo test`
Expected: PASS — all of `tests/discovery_test.rs`, `tests/network_test.rs`, `tests/peer_cache_test.rs` still green (they don't touch `main`).

- [ ] **Step 3: Manual smoke — TUI still default**

Run: `cargo run` (in one terminal), `cargo run -- --port 0` (in another).
Expected: both open the TUI; they discover each other via mDNS (a "Discovered … via mDNS" line appears); typing + Enter to a selected peer delivers a message. Esc quits and restores the terminal.

- [ ] **Step 4: Commit**

```bash
git add src/main.rs
git commit -m "refactor: extract run_node from main so UIs can share the swarm driver" \
  -m "Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Add `UiEvent::CacheCount` + live PeerId/cache display

The GUI (Task 5) starts on the main thread before `run_node` opens the peer cache, so it can't read the initial count itself. Add a channel event carrying the count, plus reuse the existing PeerId Info line. Wire both UIs to update live.

**Files:**
- Modify: `src/tui.rs` (enum `UiEvent` ~lines 29–68; `handle_ui_event` ~lines 234–325; `TuiState.our_peer_id`/`cache_count` already exist)
- Modify: `src/main.rs` (`run_node` — send `CacheCount` after `PeerCache::open`)

**Interfaces:**
- Consumes: `run_node` from Task 1.
- Produces: new enum variant `UiEvent::CacheCount(usize)`. Both `run_tui` and (later) `run_gui` handle it.

- [ ] **Step 1: Add the enum variant**

In `src/tui.rs`, add to `enum UiEvent` (after `DhtRecord(String)`):

```rust
    /// Number of peers currently in the persistent cache.
    CacheCount(usize),
```

- [ ] **Step 2: Handle it in the TUI**

In `src/tui.rs` `handle_ui_event`, add a match arm (the match is exhaustive, so this is required to compile):

```rust
        UiEvent::CacheCount(n) => {
            state.cache_count = n;
        }
```

- [ ] **Step 3: Send it from run_node**

In `src/main.rs` `run_node`, immediately after the existing `let cached_count = peer_cache.all_peers()...` line, send both the count and (for the status panel PeerId) reuse the existing Info line. Add:

```rust
    let _ = ui_tx.send(UiEvent::CacheCount(cached_count));
```

(The `UiEvent::Info(format!("Local PeerId: {peer_id}"))` send already exists and is kept.)

- [ ] **Step 4: Build + test**

Run: `cargo build && cargo test`
Expected: PASS, no warnings about a non-exhaustive match.

- [ ] **Step 5: Commit**

```bash
git add src/tui.rs src/main.rs
git commit -m "feat: add UiEvent::CacheCount so UIs can show live cache size" \
  -m "Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Add the `gui` cargo feature, ply/macroquad deps, font asset, and the `gui` subcommand wiring

Harvest the exact ply-engine + forked-macroquad dependency lines and a bundled font from `plyx init` (the tool of record — avoids guessing the fork's git source), gate them behind a default-on `gui` feature, add the clap subcommand, and wire the GUI thread model with a stub `run_gui` that opens a window and quits on Esc. Full rendering is Task 5.

**Files:**
- Modify: `Cargo.toml`
- Create: `assets/fonts/<font>.ttf` (harvested)
- Create: `src/gui.rs` (stub in this task)
- Modify: `src/main.rs` (clap subcommand + GUI thread model)

**Interfaces:**
- Consumes: `run_node` (Task 1), `UiEvent`/`UserAction` (`src/tui.rs`).
- Produces:
  ```rust
  #[cfg(feature = "gui")]
  pub fn run_gui(
      ui_rx: std::sync::mpsc::Receiver<crate::tui::UiEvent>,
      action_tx: tokio::sync::mpsc::Sender<crate::tui::UserAction>,
      our_peer_id: String,
  ) -> Result<(), Box<dyn std::error::Error>>
  ```

- [ ] **Step 1: Harvest deps + font from plyx**

```bash
cargo install plyx
cd /private/tmp/claude-502/-Users-kerry-hatcher-projects-hatchchat/*/scratchpad
plyx init   # answer prompts: project name "plyprobe", pick a font (e.g. "JetBrains Mono"), default features
```

Open the generated `plyprobe/Cargo.toml` and note the EXACT `[dependencies]` lines for `ply-engine` and `macroquad` (the fork — it will point at a git or a `=x.y.z` version of `macroquad-ply`/`macroquad-fix`), and whether it generated a `build.rs` / `[build-dependencies]`. Copy the downloaded TTF from `plyprobe/assets/fonts/*.ttf` into this repo:

```bash
mkdir -p assets/fonts
cp plyprobe/assets/fonts/*.ttf /Users/kerry.hatcher/projects/hatchchat/assets/fonts/
```

> If `plyx init` refuses to run non-interactively, run it manually and transcribe the two dependency lines. The point is to use the versions/source ply itself pins, not to hand-author the fork URL.

- [ ] **Step 2: Add the feature + optional deps to `Cargo.toml`**

Add (using the exact version/source strings harvested in Step 1 in place of the `<...>` placeholders — do not invent them):

```toml
[features]
default = ["gui"]
gui = ["dep:ply-engine", "dep:macroquad"]

[dependencies]
# ... existing deps unchanged ...
ply-engine = { version = "<harvested>", optional = true }
macroquad  = { <harvested source, e.g. git = "https://github.com/TheRedDeveloper/macroquad-fix", rev = "..."/version = "..."> , optional = true }
```

If Step 1 revealed a required `build.rs`/`[build-dependencies]` (only if the `shader-build` feature is on — it should NOT be for our use, we don't need shaders), leave it out; we only need default ply features (a11y + text). Confirm no `build.rs` is required for the default feature set.

- [ ] **Step 3: Verify both build configurations compile (deps only, no gui code yet)**

Run: `cargo build --no-default-features`
Expected: PASS, and `cargo tree --no-default-features | grep -E 'ply-engine|macroquad'` prints nothing (deps excluded).

Run: `cargo build`
Expected: PASS, ply-engine + macroquad compile. (First build is slow — GPU stack.)

- [ ] **Step 4: Create the stub `src/gui.rs`**

```rust
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
```

> `macroquad::Window::from_config`, `prevent_quit`, `is_quit_requested`, `is_key_pressed`, `KeyCode`, `clear_background`, `draw_text`, `next_frame`, `Conf`, colors — all macroquad APIs (confirmed against macroquad's own docs; NOT ply APIs). If `Window::from_config`'s signature differs in the fork, `cargo build` in Step 6 will surface it; the correct symbol is `macroquad::window::Window::from_config(conf: Conf, future: impl Future<Output = ()> + 'static)`.

- [ ] **Step 5: Add the subcommand + GUI thread model to `src/main.rs`**

Change `Args` to carry a subcommand. Add above `struct Args`:

```rust
#[derive(clap::Subcommand, Debug, Clone)]
enum Command {
    /// Launch the desktop GUI instead of the terminal UI.
    Gui,
}
```

Add to `struct Args`:

```rust
    #[command(subcommand)]
    command: Option<Command>,
```

Add `mod gui;` near the other `mod` declarations (gate it so `--no-default-features` still compiles):

```rust
#[cfg(feature = "gui")]
mod gui;
```

Restructure `main` to branch on the subcommand. Replace the TUI-only `main` body (from Task 1 Step 1) with:

```rust
    let args = Args::parse();

    let (ui_tx, ui_rx) = mpsc::channel::<UiEvent>();
    let (action_tx, action_rx) = tokio::sync::mpsc::channel::<UserAction>(64);

    match args.command {
        Some(Command::Gui) => run_gui_path(args, ui_tx, ui_rx, action_tx, action_rx),
        None => run_tui_path(args, ui_tx, ui_rx, action_tx, action_rx),
    }
```

Add the two path fns:

```rust
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
```

> The `--port`/`--bootstrap`/etc. flags live on `Args` and apply to both paths (a GUI node can bootstrap/seed identically). clap with an optional subcommand keeps the flags at the top level.

- [ ] **Step 6: Build all three configurations**

Run: `cargo build` — Expected: PASS (gui code compiles).
Run: `cargo build --no-default-features` — Expected: PASS; the `gui` subcommand handler prints the not-built message when run.
Run: `cargo test` — Expected: PASS (existing tests unaffected).

- [ ] **Step 7: Manual smoke — window opens, TUI still default**

Run: `cargo run -- gui`
Expected: a 1000×700 window titled "hatch-chat" opens showing "loading (stub)"; Esc or the window close button quits; the process exits cleanly (swarm thread joins).

Run: `cargo run`
Expected: TUI as before (no window).

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml Cargo.lock assets/fonts src/gui.rs src/main.rs
git commit -m "build: add gui feature, ply-engine deps, and gui subcommand wiring" \
  -m "Swarm runs on a background thread; ply/macroquad window on main. Stub renderer for now." \
  -m "Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: `GuiState` + pure `apply_ui_event` mapping (unit-tested, no window)

Isolate the event→state logic so it's testable without a GPU/window. This is the one piece with real automated coverage.

**Files:**
- Modify: `src/gui.rs` (add `GuiState` + `apply_ui_event` + `#[cfg(test)]` module)

**Interfaces:**
- Consumes: `UiEvent` (`src/tui.rs`).
- Produces:
  ```rust
  struct GuiState { /* fields below */ }
  impl GuiState { fn new(our_peer_id: String) -> Self }
  fn apply_ui_event(state: &mut GuiState, event: UiEvent)
  ```

- [ ] **Step 1: Write the failing test**

Add to the bottom of `src/gui.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::UiEvent;

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
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test --features gui gui::tests`
Expected: FAIL — `GuiState`, `apply_ui_event` not defined.

- [ ] **Step 3: Implement `GuiState` + `apply_ui_event`**

Add near the top of `src/gui.rs` (below the imports):

```rust
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test --features gui gui::tests`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add src/gui.rs
git commit -m "feat: add GuiState and pure apply_ui_event mapping with unit tests" \
  -m "Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Render the ply UI + wire input in `gui_main`

Replace the stub loop with a real ply UI (event log + input + peer lists + status) and input handling. ply APIs below are exactly as listed in ply-engine's `SKILL.md`; macroquad key/quit APIs are as in Task 3.

**Files:**
- Modify: `src/gui.rs` (`gui_main`, imports, font asset)

**Interfaces:**
- Consumes: `GuiState`, `apply_ui_event` (Task 4); `UserAction` (`src/tui.rs`); the harvested font at `assets/fonts/<font>.ttf` (Task 3).
- Produces: full interactive GUI.

- [ ] **Step 1: Add the font asset + ply imports**

At the top of `src/gui.rs`, add ply's prelude and the embedded font (use the actual harvested filename):

```rust
use ply_engine::prelude::*;

static DEFAULT_FONT: FontAsset = FontAsset::Bytes {
    file_name: "hatch-chat-font.ttf",
    data: include_bytes!("../assets/fonts/<HARVESTED_FILENAME>.ttf"),
};
```

> `FontAsset::Bytes { file_name, data }` is the recommended variant per SKILL.md Part 10.1/16.11 — a struct variant, not a tuple. `data` must be `&'static [u8]`; `include_bytes!` yields that. Rename or reference the harvested TTF accordingly.

- [ ] **Step 2: Rewrite `gui_main` to build the UI each frame**

```rust
async fn gui_main(
    ui_rx: mpsc::Receiver<UiEvent>,
    action_tx: tokio_mpsc::Sender<UserAction>,
    our_peer_id: String,
) {
    prevent_quit();
    let mut ply = Ply::<()>::new(&DEFAULT_FONT).await;
    let mut state = GuiState::new(our_peer_id);
    const INPUT_ID: &str = "chat-input";

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

        clear_background(Color::from_hex(0x111111));

        // 2. Build the UI tree.
        let mut ui = ply.begin();
        ui.element().width(grow!()).height(grow!())
            .layout(|l| l.direction(LeftToRight).gap(8).padding(8))
            .children(|ui| {
                // Left column: event log (scroll) + input.
                ui.element().width(percent!(0.65)).height(grow!())
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
                        // Input box.
                        ui.element().id(INPUT_ID).width(grow!()).height(fixed!(40.0))
                            .background_color(0x000000)
                            .layout(|l| l.padding(8).align(Left, CenterY))
                            .text_input(|t| t.font_size(20))
                            .empty();
                    });

                // Right column: connected + discovered + status.
                ui.element().width(grow!()).height(grow!())
                    .layout(|l| l.direction(TopToBottom).gap(8))
                    .children(|ui| {
                        // Connected peers (selectable).
                        ui.element().width(grow!()).height(percent!(0.4))
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
                        ui.element().width(grow!()).height(percent!(0.3))
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
                                ui.text("Enter=send  Shift+B=broadcast  Tab=next peer  Esc=quit", |t| t.font_size(12).color(0x777777));
                            });
                    });
            });
        ui.show(|_| {}).await;

        // 3. Read the input widget's current value.
        state.input = ply.get_text_value(INPUT_ID).to_string();

        // 4. Keyboard actions (macroquad key APIs).
        let shift = is_key_down(KeyCode::LeftShift) || is_key_down(KeyCode::RightShift);
        if is_key_pressed(KeyCode::Tab) && !state.connected_peers.is_empty() {
            state.selected_peer = (state.selected_peer + 1) % state.connected_peers.len();
        }
        if is_key_pressed(KeyCode::Enter) && !state.input.is_empty() {
            let text = std::mem::take(&mut state.input);
            ply.set_text_value(INPUT_ID, "");
            if shift {
                let _ = action_tx.try_send(UserAction::Broadcast { text });
            } else if let Some(p) = state.connected_peers.get(state.selected_peer) {
                if let Ok(pid) = p.peer_id.parse::<libp2p::PeerId>() {
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
```

> ply APIs used — all from SKILL.md: `Ply::<()>::new(&FontAsset).await` (5.1), `ply.begin() -> Ui` (5.1), `ui.element()` + `.id/.width/.height/.layout/.background_color/.overflow/.children/.text_input` (5.3), `.empty()` to close a childless element (16.8), `ui.text(text, |t| t.font_size(_).color(_))` (5.2), `.overflow(|o| o.scroll_y().scrollbar(...))` (6.3/16.10), sizing macros `grow!()/fixed!()/percent!()` (6.1), layout enums `LeftToRight/TopToBottom/Left/CenterY` from prelude (4.4), `ui.show(|_| {}).await` (5.1), `ply.get_text_value(id)`/`set_text_value(id, v)` (9.4). `Color::from_hex`, `clear_background`, `is_key_pressed`, `is_key_down`, `KeyCode`, `is_quit_requested`, `prevent_quit`, `next_frame` are macroquad. If any ply method name differs at build time, consult the local `SKILL.md` copy — do NOT invent replacements.
>
> `libp2p::PeerId` is already a dependency; add `use libp2p::PeerId;` or fully-qualify as shown.

- [ ] **Step 3: Build**

Run: `cargo build --features gui`
Expected: PASS. Resolve any ply method-name mismatch against `SKILL.md` (e.g. `.empty()` vs the exact childless-close call, `Color::from_hex` vs a ply color helper). Do not guess — the local `SKILL.md` from Task 3 Step 1 harvesting is the authority.

- [ ] **Step 4: Keep the unit tests green**

Run: `cargo test --features gui`
Expected: PASS (Task 4 tests unaffected by rendering changes).

- [ ] **Step 5: Manual smoke test — two instances chat**

Run in terminal A: `cargo run -- gui`
Run in terminal B: `cargo run` (TUI) — or a second `cargo run -- gui`.
Expected:
- The GUI window shows the event log filling with discovery/connection lines.
- The right panel lists the connected peer (mDNS on LAN); Tab selects it.
- Typing in the input box + Enter sends a direct message; it appears in B.
- Shift+Enter broadcasts (arrives via gossipsub).
- A message from B appears in the GUI's log.
- Esc / window close quits; both threads exit cleanly.

- [ ] **Step 6: Commit**

```bash
git add src/gui.rs
git commit -m "feat: render the ply-engine chat GUI and wire keyboard input" \
  -m "Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Notes for the executor

- If `macroquad::Window::from_config` is absent or differently named in the forked macroquad, that is the single riskiest integration point. Check the fork's `window` module (`cargo doc --open -p macroquad`). The window loop MUST run in-process on the main thread — a separate GUI binary is NOT an option because it would break the shared in-process channels.
- ply's `SKILL.md` (harvested in Task 3, or `plyx skill` to install it) is the authoritative API reference. The rule from SKILL.md applies: never invent a ply method not listed there.
- The `justfile` may get a convenience recipe later (`gui: cargo run -- gui`); not required by this plan.
