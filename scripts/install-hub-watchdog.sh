#!/usr/bin/env bash
# Install the user-scoped systemd timer that probes the hub MCP endpoint every two minutes.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
unit_dir="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
lib_dir="$HOME/.local/lib/zeroshot"

install -d -m 700 "$lib_dir" "$unit_dir"
install -m 700 "$root/scripts/hub-watchdog.sh" "$lib_dir/hub-watchdog.sh"
install -m 644 "$root/systemd/zeroshot-hub-watchdog.service" "$unit_dir/"
install -m 644 "$root/systemd/zeroshot-hub-watchdog.timer" "$unit_dir/"

systemctl --user daemon-reload
systemctl --user enable --now zeroshot-hub-watchdog.timer
printf 'installed zeroshot-hub-watchdog.timer; inspect it with: systemctl --user status zeroshot-hub-watchdog.timer\n'
