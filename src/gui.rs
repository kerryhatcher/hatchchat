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
