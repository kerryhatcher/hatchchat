# hatch-chat GUI (ply-engine) — Design

**Date:** 2026-07-20
**Status:** Approved (design), pending implementation plan

## Goal

Add an optional desktop GUI to hatch-chat, launched via a `gui` CLI subcommand.
The existing ratatui TUI remains the **default** UI (invoked with no subcommand).
The GUI is built with [`ply-engine`](https://github.com/TheRedDeveloper/ply-engine)
— a cross-platform, immediate-mode Rust UI engine (macroquad → miniquad backend).

## Constraints & key facts

- The network layer is already cleanly decoupled from the UI via two channels:
  - `UiEvent` — swarm → UI (`std::sync::mpsc`), drained by the UI.
  - `UserAction` — UI → swarm (`tokio::sync::mpsc`), sent by the UI.
  - Defined in `src/tui.rs`. The TUI is just one consumer (`run_tui`) running in
    a spawned OS thread while the swarm runs on the main tokio task (`main.rs`).
- **A GUI is a drop-in alternate consumer of that same contract.**
  `network.rs`, `discovery.rs`, `peer_cache.rs` require **zero changes**.
- ply-engine's per-frame rebuild model maps directly onto the existing
  `ui_rx.try_recv()` drain loop the TUI already uses.
- **Threading flip:** ply (macroquad/miniquad) MUST own the main thread on macOS.
  Today the swarm owns the main thread and the TUI runs in a spawned thread.
  For the GUI this flips: the GUI owns the main thread; the swarm moves to a
  background OS thread with its own tokio runtime. The TUI path is unchanged.
- ply is young (v1.1.1, single author) and depends on a personal fork of
  macroquad — accepted risk, isolated behind a cargo feature (see §5).

## Architecture

### 1. Entry point (`src/main.rs`)

`main` drops the `#[tokio::main]` attribute and becomes a plain `fn main()`:

1. Initialize tracing to `hatch-chat.log` (unchanged).
2. Parse clap args. Add a `gui` subcommand. No subcommand → TUI (default).
3. **TUI path:** build a multi-thread tokio runtime, spawn the TUI OS thread,
   `runtime.block_on(run_node(...))` on the main thread. Behavior identical to today.
4. **GUI path:** spawn an OS thread that builds a tokio runtime and runs
   `run_node(...)`; run the ply frame loop on the **main thread** via `run_gui(...)`.

The `Args` struct keeps all existing flags (`--port`, `--no-local`, `--bootstrap`,
`--bootstrap-seed`, `--data-dir`). The `gui` subcommand accepts the same flags so a
GUI node can bootstrap/seed identically to a TUI node.

### 2. Refactor: extract `run_node`

Move the swarm construction + the `tokio::select!` event loop currently inside
`main` into:

```rust
async fn run_node(
    args: Args,
    ui_tx: std::sync::mpsc::Sender<UiEvent>,
    action_rx: tokio::sync::mpsc::Receiver<UserAction>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
```

`run_node` owns: keypair/PeerId generation, transport/behaviour build, listen,
peer-cache open, discovery orchestration, bootstrap dialing, periodic intervals,
and the full `select!` loop. It is runtime-agnostic — called by both paths.
The channel *ends held by the UI* (`ui_rx`, `action_tx`) are created by `main`
and handed to whichever UI runs; the swarm ends (`ui_tx`, `action_rx`) go to `run_node`.

Helper fns (`extract_peer_id`, `safe_dial`, `save_peer_to_cache`,
`handle_swarm_event`) are unchanged.

### 3. New module: `src/gui.rs`

Public entry mirrors `run_tui`'s role:

```rust
#[cfg(feature = "gui")]
pub fn run_gui(
    ui_rx: std::sync::mpsc::Receiver<UiEvent>,
    action_tx: tokio::sync::mpsc::Sender<UserAction>,
    our_peer_id: String,
) -> Result<(), Box<dyn std::error::Error>>
```

- Builds the macroquad `Conf` (window title, size, high_dpi, sample_count) and
  launches the loop with `macroquad::Window::from_config(conf, gui_main(...))`
  — deliberately NOT the `#[macroquad::main]` attribute, so `main` stays ours.
  `from_config` runs the miniquad event loop on the calling (main) thread and
  blocks until the window closes.
- `gui_main` is the async frame loop:
  1. Drain `ui_rx.try_recv()` into `GuiState` (same field shape and mapping as
     `TuiState`; reuse the `handle_ui_event` logic, adapted to ply state). On
     `Disconnected`, quit.
  2. Build the ply UI tree for the frame.
  3. Poll input; translate to `UserAction` via `action_tx.try_send(...)`.
  4. `next_frame().await`.
  5. On quit (Esc / window close), send `UserAction::Quit` and return.

**`GuiState`** carries: `events` (from/text/kind log entries, capped at 1000),
`connected_peers`, `discovered_peers` (capped at 100), `our_peer_id`,
`nat_status`, `cache_count`, `input`, `selected_peer`.

**Layout (parity with the TUI):**
- Left column: scrollable Chat/Events log (top) + input box (bottom).
- Right column: connected peers (selectable list) + discovered peers + status panel
  (PeerId, NAT, connected count, cache count).
- Bottom: help bar.

**Input mapping (parity with the TUI):**
- Text input element bound to `GuiState.input`.
- Enter → `UserAction::SendMessage { peer_id: selected, text }` (if input non-empty
  and a peer is selected).
- `B` (shift) or an explicit Broadcast affordance → `UserAction::Broadcast { text }`.
- Click / Up-Down / Tab → change `selected_peer`.
- Esc or window close → `UserAction::Quit` + return.

### 4. New channel event: `UiEvent::CacheCount(usize)`

The GUI starts on the main thread before `run_node` opens the peer cache, so it
cannot read the initial cache count itself. Add one variant to `UiEvent`:

```rust
CacheCount(usize),
```

`run_node` sends it immediately after `PeerCache::open` succeeds. Both UIs handle it:
the GUI sets `cache_count`; the TUI gets one new match arm that updates its
`cache_count` (a small correctness improvement — the TUI currently only shows the
startup count). The existing `initial_cache_count` parameter to `run_tui` is kept
for the startup value; `CacheCount` updates it live.

### 5. Cargo: optional `gui` feature (default on)

```toml
[features]
default = ["gui"]
gui = ["dep:ply-engine"]

[dependencies]
ply-engine = { version = "1", optional = true }
```

- Default builds include the GUI (so `hatch-chat gui` works out of the box —
  the GUI is a first-class CLI command).
- `cargo build --no-default-features` produces a lean binary for headless /
  bootstrap-seed / server deployments, with **no** forked-macroquad or GPU
  dependencies. This isolates the ply supply-chain risk to GUI-enabled builds.
- The `gui` subcommand handler and `src/gui.rs` are `#[cfg(feature = "gui")]`.
  When compiled without the feature, invoking `gui` prints a clear message:
  `"This binary was built without GUI support (rebuild with --features gui)."`
  and exits non-zero.

### 6. Font asset

ply requires a font. Bundle one open-license TTF (e.g. a permissively licensed
sans such as the font `plyx init` downloads, or an existing OFL font) and load it
via `include_bytes!` + the ply bytes-font variant if one exists (keeps a single
self-contained binary). If ply only supports `FontAsset::Path`, ship the TTF under
`assets/fonts/` and document the path requirement. The exact API is confirmed
against ply's `SKILL.md` during implementation.

## Data flow (GUI path)

```
main thread                         background OS thread
-----------                         --------------------
fn main()
  parse clap -> gui subcommand
  create channels:
    (ui_tx, ui_rx), (action_tx, action_rx)
  spawn thread ----------------->   tokio runtime
                                      run_node(args, ui_tx, action_rx)
                                        - build swarm, open peer cache
                                        - ui_tx.send(CacheCount(n))
                                        - select! loop:
                                            swarm events -> ui_tx.send(UiEvent)
                                            action_rx.recv() -> drive swarm
  run_gui(ui_rx, action_tx, pid)
    macroquad::Window::from_config(...)
      each frame:
        ui_rx.try_recv() -> GuiState   <----- UiEvent
        render ply UI
        input -> action_tx.try_send() -----> UserAction
      quit -> action_tx.try_send(Quit)
```

The TUI path is the mirror image: swarm on main via `block_on(run_node)`, TUI in a
spawned thread — exactly as it works today.

## Error handling

- Font load failure → log and exit non-zero with a clear message (GUI cannot run
  without a font).
- `ui_rx` disconnected (swarm thread ended) → GUI quits gracefully, restoring
  nothing special (macroquad owns the window; drop closes it).
- `action_tx.try_send` failure (swarm gone / full) → ignored, same as the TUI's
  `try_send` pattern; a disconnected channel leads to quit on the next drain.
- Swarm thread panics → the process should exit; the GUI notices `ui_rx`
  disconnect and quits.

## Testing

- Existing tests (`tests/*.rs`) must still pass unchanged — the refactor into
  `run_node` must not alter swarm behavior.
- The GUI is inherently interactive; automated coverage is limited to:
  - A unit check that `GuiState`'s event-mapping produces the expected entries
    for each `UiEvent` variant (mirror of the TUI mapping), runnable without a
    window (pure state transition, no ply/GPU).
  - Manual smoke test: `hatch-chat gui` opens a window, two local instances
    (`gui` + default TUI, or two `gui`) discover each other via mDNS and exchange
    a message.
- `cargo build --no-default-features` must compile and the `gui` subcommand must
  print the not-built message.

## Out of scope (YAGNI)

- Trait-abstracting the TUI and GUI behind a common `Ui` trait — only one GUI
  engine; add if a second consumer appears.
- Mobile / web (WASM) targets — hatch-chat is a desktop app.
- GUI-specific settings, themes, or persistence.
- Rich text / markup, shader effects, audio — ply offers them; none are needed.
