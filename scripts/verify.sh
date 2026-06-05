#!/usr/bin/env bash
# flare-core quality gate — run before release or large transport changes.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "==> cargo fmt --check"
cargo fmt --all -- --check

echo "==> cargo clippy (lib, deny warnings)"
cargo clippy --lib -- -D warnings

echo "==> cargo test --lib (native)"
cargo test --lib

echo "==> cargo test --tests (native integration)"
cargo test --tests

echo "==> cargo test --no-default-features --lib"
cargo test --no-default-features --lib

echo "==> cargo test --no-default-features --tests"
cargo test --no-default-features --tests

echo "==> cargo check feature matrix"
cargo check --no-default-features --lib
cargo check --no-default-features --features client,websocket --lib
cargo check --no-default-features --features client,quic --lib
cargo check --no-default-features --features server,websocket --lib
cargo check --no-default-features --features server,quic --lib
cargo check --no-default-features --features client,tcp --lib
cargo check --no-default-features --features server,tcp --lib

echo "==> cargo check --target wasm32-unknown-unknown (lib + tests compile)"
cargo check --lib --tests --target wasm32-unknown-unknown

echo "==> cargo build --examples (native)"
cargo build --examples

echo "==> cargo check --benches (native)"
cargo check --benches

echo "✅ flare-core verify passed"
