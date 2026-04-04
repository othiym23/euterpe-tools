FROM cgr.dev/chainguard/wolfi-base:latest

ARG USERNAME=ogd
ARG USER_UID=501
ARG USER_GID=20

ENV RUST_VERSION=1.94.1
ENV RUSTUP_HOME=/usr/local/rustup
ENV CARGO_HOME=/usr/local/cargo

# System packages: rustup (not a full toolchain), Node LTS, dev tools
RUN apk update && apk add --no-cache \
    rustup \
    uv \
    gh \
    just \
    nodejs-22 \
    npm \
    bun \
    neovim \
    git \
    openssh-client \
    openssh-keygen \
    build-base \
    glibc-dev \
    curl \
    bash \
    sudo \
    posix-libc-utils \
    docker \
    iptables \
    && rm -rf /var/cache/apk/*

# Install the Rust toolchain via rustup
RUN rustup-init -y --no-modify-path --default-toolchain "${RUST_VERSION}"
ENV PATH="/usr/local/cargo/bin:${PATH}"

# cargo-nextest (shared toolchain, user can run it)
RUN cargo install cargo-nextest --locked

# Prime ld.so.cache for VS Code Server's check-requirements.sh
RUN ldconfig

# Create user matching host UID/GID for NAS share access.
# GID 20 is 'staff' on macOS; create it if it doesn't exist.
RUN { grep -q ":${USER_GID}:" /etc/group || addgroup -g ${USER_GID} staff; } && \
    { grep -q "^docker:" /etc/group || addgroup docker; } && \
    GROUP_NAME=$(awk -F: -v gid=${USER_GID} '$3 == gid {print $1}' /etc/group) && \
    adduser -D -u ${USER_UID} -G "${GROUP_NAME}" -s /bin/bash ${USERNAME} && \
    addgroup ${USERNAME} docker && \
    echo "${USERNAME} ALL=(root) NOPASSWD: /usr/bin/dockerd" > /etc/sudoers.d/${USERNAME}

# Install npm-based tools globally
RUN npm install -g @anthropic-ai/claude-code prettier ccstatusline

# Hand rustup/cargo directories to the container user so they can
# download crates and add targets without root.
RUN chown -R ${USER_UID}:${USER_GID} ${RUSTUP_HOME} ${CARGO_HOME}

ENV PATH="/home/${USERNAME}/.local/bin:${PATH}"

# Pre-create directories that bind mounts will target so they're
# owned by the user, not root.  Docker creates missing mount targets
# as root, which blocks uv/cargo from writing siblings.
RUN mkdir -p /home/${USERNAME}/.local/share /home/${USERNAME}/.local/state \
             /home/${USERNAME}/.config && \
    chown -R ${USER_UID}:${USER_GID} /home/${USERNAME}/.local /home/${USERNAME}/.config

# ccstatusline config (used by the statusline shim inside the container)
COPY ccstatusline-settings.json /home/${USERNAME}/.config/ccstatusline/settings.json
RUN chown -R ${USER_UID}:${USER_GID} /home/${USERNAME}/.config/ccstatusline

# Install Python and dev tools via uv so they're ready before
# VS Code extensions start (avoids "Python not found" errors).
USER ${USERNAME}
RUN uv python install 3.14 && \
    ln -s python3.14 /home/${USERNAME}/.local/bin/python3 && \
    ln -s python3.14 /home/${USERNAME}/.local/bin/python && \
    uv tool install ruff && \
    uv tool install pyright && \
    uv tool install ty

WORKDIR /workspaces/euterpe-tools
