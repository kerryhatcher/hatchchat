# Default recipe — show available commands
default:
    @just --list

# Build the project
build:
    cargo build

# Run in LAN mode (mDNS discovery — two instances on the same machine find each other instantly)
run:
    cargo run

# Run with a specific port (LAN mode)
run-port port:
    cargo run -- --port {{port}}

# Run with a bootstrap node (LAN mode)
run-bootstrap bootstrap:
    cargo run -- --bootstrap {{bootstrap}}

# Run as a bootstrap seed node (LAN mode)
run-seed:
    cargo run -- --bootstrap-seed

# Run in internet-only mode (--no-local, disables mDNS, filters local addresses)
# Requires a public bootstrap node to discover peers.
# Example: just run-no-local --bootstrap /ip4/1.2.3.4/tcp/4001/p2p/Qm...
run-no-local *args:
    cargo run -- --no-local {{args}}

# Run tests
test:
    cargo test