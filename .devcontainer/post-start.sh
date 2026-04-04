#!/bin/bash
# Runs inside the container every time it starts.
# Launches dockerd in the background for docker-in-docker support.
set -euo pipefail

# Start dockerd if not already running.
if ! docker info >/dev/null 2>&1; then
  sudo /usr/bin/dockerd --host=unix:///var/run/docker.sock &>/tmp/dockerd.log &
  # Wait for the socket to appear (up to 10s).
  for i in $(seq 1 20); do
    if docker info >/dev/null 2>&1; then
      echo "dockerd started."
      break
    fi
    sleep 0.5
  done
  if ! docker info >/dev/null 2>&1; then
    echo "warning: dockerd did not start. Check /tmp/dockerd.log" >&2
  fi
fi
