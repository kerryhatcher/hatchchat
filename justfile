# Default recipe — show available commands
default:
    @just --list

# Build the project
build:
    cargo build

# Run with default passphrase (two instances on any network find each other)
run:
    cargo run

# Run with a custom passphrase
run-passphrase passphrase:
    cargo run -- --passphrase {{passphrase}}

# Run with a display name
run-named passphrase name:
    cargo run -- --passphrase {{passphrase}} --name {{name}}

# Run the desktop GUI
run-gui:
    cargo run -- gui

# Run GUI with custom passphrase
run-gui-passphrase passphrase:
    cargo run -- gui --passphrase {{passphrase}}

# Run tests
test:
    cargo test

# Build without GUI dependencies (headless)
build-headless:
    cargo build --no-default-features
