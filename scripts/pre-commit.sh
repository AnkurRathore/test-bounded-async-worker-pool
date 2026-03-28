#!/bin/bash
set -e

echo "--- Running Pre-commit Guard ---"

echo "Step 1: Formatting..."
cargo fmt --all -- --check

echo "Step 2: Linting (Clippy)..."
cargo clippy -- -D warnings

echo "Step 3: Unit Testing..."
cargo test

echo "--- All local checks passed! ---"