# hatch-chat

Hearty peer-to-peer chat over [iroh](https://iroh.computer). Peers discover each
other automatically via the BitTorrent Mainline DHT using
[iroh-gossip-rendezvous](https://crates.io/crates/iroh-gossip-rendezvous) —
**two nodes sharing only a passphrase find each other with zero configuration.**

No bootstrap servers. No pre-shared addresses. No manual config. Just a
passphrase.

Two front-ends share one networking core over an internal event/action
channel contract:

- **TUI** (default) — a [ratatui](https://ratatui.rs/) terminal interface.
- **GUI** (optional) — a [ply-engine](https://github.com/TheRedDeveloper/ply-engine)
  desktop window, behind the default-on `gui` cargo feature.

## How it works

```
Alice                    Mainline DHT                   Bob
  │                           │                           │
  │── publish EndpointId ────▶│                           │
  │                           │◀──── publish EndpointId ──│
  │                           │                           │
  │◀───────── discover ──────│────── discover ──────────▶│
  │                           │                           │
  │────────────── direct QUIC connection ────────────────▶│
  │◀───────────── (NAT traversal + relay fallback) ──────│
```

1. Each node derives DHT slot keys from the shared passphrase via HKDF
2. Nodes publish their iroh `EndpointId` to the Mainline DHT
3. Nodes periodically scan DHT slots for unknown peers
4. Discovered peers are dialed via iroh (QUIC + NAT traversal + relay fallback)
5. Chat messages flow over iroh-gossip (epidemic broadcast trees)

## Install / run

```bash
# Terminal UI (default passphrase)
cargo run

# With a custom passphrase and display name
cargo run -- --passphrase "our-secret-room" --name alice

# Desktop GUI
cargo run -- gui --passphrase "our-secret-room" --name bob

# Lean, headless build (no GUI / GPU dependencies)
cargo build --no-default-features
```

## Options

```
-p, --passphrase <PHRASE>   Shared passphrase for rendezvous [default: hatch-chat-default]
-n, --name <NAME>            Your display name in the chat
    --data-dir <DIR>         Persistent state directory [default: .hatch-chat]
```

## Requirements

- Rust 1.75+
- Internet connection (UDP access for DHT + QUIC)
- Same passphrase on all peers

No port forwarding, firewall configuration, or bootstrap nodes needed.
iroh handles NAT traversal and falls back to public relay servers when
direct connections aren't possible.

## License

MIT — see [LICENSE](LICENSE).
