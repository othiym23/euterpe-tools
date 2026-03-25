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
    cd scripts && uv run ruff format
    cd etp && uv run ruff format
    # Markdown
    prettier --write "**/*.md"

# Lint, format-check, and typecheck source files
check:
    # Rust
    cargo fmt --all --check
    cargo clippy --workspace -- -D warnings
    # Python (scripts)
    cd scripts && \
      uv run ruff check && \
      uv run ruff format --check
    cd scripts && \
      uv run pyright
    cd scripts && \
      uv run ty check
    # Python (etp)
    cd etp && \
      uv run ruff check && \
      uv run ruff format --check
    cd etp && \
      uv run pyright
    cd etp && \
      uv run ty check
    # Markdown
    prettier --check "**/*.md"

# Run all tests (Rust + Python)
test:
    cargo nextest run --workspace
    cd scripts && uv run pytest test_catalog.py -q
    cd etp && uv run pytest test_catalog.py test_anime.py -q

nas_home := "/Volumes/home"

# Mount NAS home directory via SMB if not already mounted
mount-home:
    #!/usr/bin/env bash
    set -euo pipefail
    if mount | grep -q "{{ nas_home }}"; then
        echo "{{ nas_home }} already mounted"
    else
        sudo mkdir -p "{{ nas_home }}"
        sudo mount_smbfs "//ogd@euterpe.local/home" "{{ nas_home }}"
        echo "Mounted {{ nas_home }}"
    fi

# Build for NAS and deploy binaries + scripts to NAS home directory
deploy: check test build-nas mount-home
    #!/usr/bin/env bash
    set -euo pipefail
    # binaries
    mkdir -p "{{ nas_home }}/bin"
    # clean out legacy binaries
    rm -f "{{ nas_home }}/bin/fsscan"
    rm -f "{{ nas_home }}/bin/cached-tree"
    rm -f "{{ nas_home }}/bin/dir-tree-scanner"
    cp target/x86_64-unknown-linux-musl/release/etp-csv "{{ nas_home }}/bin"
    cp target/x86_64-unknown-linux-musl/release/etp-tree "{{ nas_home }}/bin"
    cp target/x86_64-unknown-linux-musl/release/etp-find "{{ nas_home }}/bin"
    # etp porcelain
    mkdir -p "{{ nas_home }}/bin"
    cp etp/etp "{{ nas_home }}/bin"
    cp etp/etp-catalog "{{ nas_home }}/bin"
    cp etp/etp-anime "{{ nas_home }}/bin"
    chmod +x "{{ nas_home }}/bin/etp" "{{ nas_home }}/bin/etp-catalog" "{{ nas_home }}/bin/etp-anime"
    # shared Python libraries ($HOME/.local/lib/etp/)
    mkdir -p "{{ nas_home }}/.local/lib/etp"
    cp etp/paths.py "{{ nas_home }}/.local/lib/etp"
    cp -R etp/kdl "{{ nas_home }}/.local/lib/etp/kdl"
    # config ($HOME/.config/euterpe-tools/) — don't overwrite existing
    mkdir -p "{{ nas_home }}/.config/euterpe-tools"
    if [ ! -f "{{ nas_home }}/.config/euterpe-tools/catalog.kdl" ]; then
        cp conf/catalog.kdl "{{ nas_home }}/.config/euterpe-tools"
    else
        echo "catalog.kdl already exists, skipping"
    fi
    # clean out legacy paths
    rm -f "{{ nas_home }}/scripts/catalog-nas.py"
    rm -f "{{ nas_home }}/scripts/catalog.toml"
    rm -rf "{{ nas_home }}/bin/kdl"
    rm -f "{{ nas_home }}/conf/catalog.kdl"
