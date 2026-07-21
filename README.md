# hatch-chat

Hearty peer-to-peer chat over [libp2p](https://libp2p.io/). Peers discover each
other on the LAN (mDNS), the DHT (Kademlia), via bootstrap nodes, and peer
exchange (PEX), with NAT traversal (AutoNAT, relay, DCUtR hole punching). State
(known peers) persists in an embedded [redb](https://www.redb.org/) cache.

Two front-ends share one networking core over an internal event/action channel
contract:

- **TUI** (default) — a [ratatui](https://ratatui.rs/) terminal interface.
- **GUI** (optional) — a [ply-engine](https://github.com/TheRedDeveloper/ply-engine)
  desktop window, behind the default-on `gui` cargo feature.

Both UIs keep chat messages in a dedicated pane and route system/discovery
events to a separate log.

## Install / run

```bash
# Terminal UI (default)
cargo run

# Desktop GUI
cargo run -- gui

# Lean, headless build (no GUI / GPU dependencies)
cargo build --no-default-features
```

## Options

```
--port <PORT>          Port to listen on (0 = random)
--no-local             Internet-only mode (disables mDNS + local addresses)
--bootstrap <MADDR>    Bootstrap node multiaddr (repeatable)
--bootstrap-seed       Act as a bootstrap seed node
--data-dir <DIR>       Persistent state directory (default: .hatch-chat)
```

## License

MIT — see [LICENSE](LICENSE).
