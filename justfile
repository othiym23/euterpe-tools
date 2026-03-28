#!/bin/bash

# Build for local machine with a dev profile to ensure it still can be compiled
build-smoketest:
    cargo build --workspace

# Build for local machine (macOS ARM)
build:
    cargo build --workspace --release

# Build for NAS (x86_64 Linux, statically linked)
build-nas:
    cargo build --workspace --release --target x86_64-unknown-linux-musl

# Build for local machine with profiling instrumentation
build-profile:
    cargo build --workspace --release --features profiling

# Build for NAS with profiling instrumentation
build-nas-profile:
    cargo build --workspace --release --target x86_64-unknown-linux-musl --features profiling

# Build for NAS using cross (if musl toolchain not installed)
build-nas-cross:
    cross build --workspace --release --target x86_64-unknown-linux-musl

# Run a CSV scan on a given directory
run dir:
    cargo run --release --bin etp-csv -- "{{dir}}" -v

# Format sources
format:
    # Rust
    cargo fmt --all
    # Python
    uv run ruff format
    # Markdown
    prettier --write "**/*.md"

# Lint, format-check, and typecheck source files
check:
    # Rust
    cargo fmt --all --check
    cargo clippy --workspace -- -D warnings
    # Python
    uv run ruff check && \
      uv run ruff format --check
    uv run pyright
    uv run ty check pylib/ cmd/etp/
    # Markdown
    prettier --check "**/*.md"

# Run all tests (Rust + Python)
test:
    cargo nextest run --workspace
    uv run pytest pylib/tests/ -q

nas_home := "/Volumes/home"
nas_data := "/Volumes/data"
nas_host := "euterpe.local"
local_test_db := "test-data/db"

# Copy catalog databases from NAS for local smoke testing.
# SQLite over SMB is unreliable — always work with local copies.
# Mount the NAS shares in Finder before running this.
fetch-test-dbs:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ ! -d "{{ nas_data }}" ]; then
        echo "error: {{ nas_data }} is not mounted. Mount it in Finder first." >&2
        exit 1
    fi
    src="{{ nas_data }}/downloads/(music)/catalogs/trees/db"
    dest="{{ local_test_db }}"
    mkdir -p "$dest"
    echo "Copying databases from $src..."
    rsync -av --include='*.db' --exclude='*-wal' --exclude='*-shm' "$src/" "$dest/"
    echo "Databases copied to $dest/"

# Build for NAS and deploy binaries + Python package to NAS.
# Mount the NAS home share in Finder before running this.
deploy: check test build-nas
    #!/usr/bin/env bash
    set -euo pipefail
    if [ ! -d "{{ nas_home }}" ]; then
        echo "error: {{ nas_home }} is not mounted. Mount it in Finder first." >&2
        exit 1
    fi
    # Rust plumbing → libexec
    mkdir -p "{{ nas_home }}/.local/libexec/etp"
    for bin in etp-csv etp-tree etp-find etp-meta etp-cas etp-query; do
        cp "target/x86_64-unknown-linux-musl/release/$bin" "{{ nas_home }}/.local/libexec/etp/"
    done
    # Python package → mirror repo structure so pyproject.toml paths work
    dest="{{ nas_home }}/.local/src/etp"
    mkdir -p "$dest/pylib" "$dest/cmd/etp"
    rsync -a --delete \
        --exclude __pycache__ --exclude .pytest_cache \
        --exclude .ruff_cache --exclude '*.pyc' \
        pylib/ "$dest/pylib/"
    rsync -a --delete \
        --exclude __pycache__ --exclude '*.pyc' \
        cmd/etp/ "$dest/cmd/etp/"
    cp -X pyproject.toml uv.lock "$dest/"
    ssh ogd@{{ nas_host }} "cd ~/.local/src/etp && ~/.local/bin/uv tool install --force --python python3.14 ."
    # Config ($HOME/.config/euterpe-tools/) — don't overwrite existing
    mkdir -p "{{ nas_home }}/.config/euterpe-tools"
    if [ ! -f "{{ nas_home }}/.config/euterpe-tools/catalog.kdl" ]; then
        cp conf/catalog.kdl "{{ nas_home }}/.config/euterpe-tools"
    else
        echo "catalog.kdl already exists, skipping"
    fi
