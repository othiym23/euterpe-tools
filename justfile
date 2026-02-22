# Build for local machine (macOS ARM)
build:
    cargo build --workspace --release

# Build for NAS (x86_64 Linux, statically linked)
build-nas:
    cargo build --workspace --release --target x86_64-unknown-linux-musl

# Build for NAS using cross (if musl toolchain not installed)
build-nas-cross:
    cross build --workspace --release --target x86_64-unknown-linux-musl

# Run a CSV scan on a given directory
run dir:
    cargo run --release --bin etp-csv -- {{dir}} -v

# Format sources
format:
    # Rust
    cargo fmt --all
    # Python
    cd scripts && uv run ruff format
    # Markdown
    prettier --write "**/*.md"

# Lint, format-check, and typecheck source files
check:
    # Rust
    cargo fmt --all --check
    cargo clippy --workspace -- -D warnings
    # Python
    cd scripts && \
      uv run ruff check && \
      uv run ruff format --check
    cd scripts && \
      uv run pyright
    # Markdown
    prettier --check "**/*.md"

# Run all tests (Rust + Python)
test:
    cargo nextest run --workspace
    cd scripts && uv run pytest test_catalog.py -q

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
    # catalog-nas
    mkdir -p "{{ nas_home }}/scripts"
    cp scripts/catalog-nas.py "{{ nas_home }}/scripts"
    cp scripts/catalog.toml "{{ nas_home }}/scripts"
    # permissions – current invocation is via the python interpreter
    chmod 0640 "{{ nas_home }}/scripts/catalog-nas.py"
