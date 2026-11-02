#!/usr/bin/env bash
# Probe the hub's Streamable HTTP MCP endpoint and restart the daemon after two consecutive
# hangs. `/v1/hub/health` and a bare TCP connect both answer while the MCP queue is jammed, so
# the only useful probe is a real JSON-RPC request with the hub's bearer token.
set -u

if ! command -v curl >/dev/null 2>&1; then
    printf 'curl is unavailable; not probing or restarting the hub\n' >&2
    exit 0
fi

HUB_DIR="${ZEROSHOT_HUB_DIR:-${ZEROSHOT_CONFIG_DIR:-${XDG_CONFIG_HOME:-$HOME/.config}/zeroshot}/hub}"
URL="${ZEROSHOT_HUB_MCP_URL:-}"
TOKEN_FILE="${ZEROSHOT_HUB_TOKEN_FILE:-$HUB_DIR/api-token}"

json_string() {
    # Print the first string value of a top-level heartbeat field, when present.
    sed -n "s/.*\"$1\"[[:space:]]*:[[:space:]]*\"\\([^\"]*\\)\".*/\\1/p" "$2" 2>/dev/null | head -n 1
}

if [ -r "$HUB_DIR/daemon.json" ]; then
    if [ -z "$URL" ]; then
        URL="$(json_string mcp "$HUB_DIR/daemon.json")"
    fi
    if [ -z "${ZEROSHOT_HUB_TOKEN_FILE:-}" ]; then
        heartbeat_token="$(json_string tokenFile "$HUB_DIR/daemon.json")"
        if [ -n "$heartbeat_token" ]; then
            TOKEN_FILE="$heartbeat_token"
        fi
    fi
fi
URL="${URL:-http://127.0.0.1:7383/mcp}"
case "$TOKEN_FILE" in
    "~/"*) TOKEN_FILE="$HOME/${TOKEN_FILE#\~/}" ;;
esac

STATE_DIR="${ZEROSHOT_WATCHDOG_STATE_DIR:-${XDG_STATE_HOME:-$HOME/.local/state}/zeroshot}"
mkdir -p "$STATE_DIR" 2>/dev/null || true
STATE="$STATE_DIR/hub-watchdog.failures"
failures=0
if [ -r "$STATE" ]; then
    failures="$(tr -dc '0-9' < "$STATE" 2>/dev/null || true)"
fi
case "$failures" in
    ''|*[!0-9]*) failures=0 ;;
esac

body="$(mktemp "${TMPDIR:-/tmp}/zeroshot-hub-watchdog.XXXXXX")" || exit 1
trap 'rm -f "$body"' EXIT

arguments=(
    -sS -m 8 -o "$body" -w '%{http_code}'
    -H 'content-type: application/json'
    -H 'accept: application/json, text/event-stream'
    -X POST
    --data '{"jsonrpc":"2.0","method":"tools/list"}'
)
if [ -r "$TOKEN_FILE" ]; then
    token="$(tr -d '\r\n' < "$TOKEN_FILE")"
    arguments+=(-H "Authorization: Bearer $token")
fi

status="$(curl "${arguments[@]}" "$URL" 2>/dev/null)"
code=$?

# A 401 proves the endpoint is answering. A missing or stale token is an authentication
# problem, never a hang, and must never restart the daemon.
if [ "$status" = "401" ]; then
    printf 'hub MCP answered 401; leaving the daemon alone\n' >&2
    printf '0\n' > "$STATE"
    exit 0
fi

# The probe is the task-specified id-less JSON-RPC message, so the server acknowledges it with an
# empty `202 Accepted` body just like any other notification. That acknowledgement proves the
# dispatcher answered; only a timeout or an empty response to anything else is a hang.
if [ "$status" = "202" ]; then
    printf 'hub MCP acknowledged the probe with 202\n' >&2
    printf '0\n' > "$STATE"
    exit 0
fi

# A missing probe tool, DNS failure, or refused connection is a broken probe environment, not a
# wedged dispatcher. Never let it restart a healthy hub.
if [ "$code" -ne 0 ] && [ "$code" -ne 28 ]; then
    printf 'hub MCP probe could not run (curl exit %s, HTTP %s); leaving the daemon alone\n' \
        "$code" "${status:-000}" >&2
    printf '0\n' > "$STATE"
    exit 0
fi

if [ "$code" -eq 28 ] || [ ! -s "$body" ]; then
    failures=$((failures + 1))
    printf '%s\n' "$failures" > "$STATE"
    printf 'hub MCP probe failed (%s consecutive): curl exit %s, HTTP %s\n' \
        "$failures" "$code" "${status:-000}" >&2
    if [ "$failures" -ge 2 ]; then
        printf 'restarting zeroshot-hub.service after %s consecutive MCP failures\n' \
            "$failures" >&2
        if command -v systemctl >/dev/null 2>&1 \
            && systemctl --user restart zeroshot-hub.service; then
            printf '0\n' > "$STATE"
        fi
    fi
    exit 1
fi

case "$status" in
    2[0-9][0-9])
        printf '0\n' > "$STATE"
        exit 0
        ;;
    *)
        # A non-2xx answer with a body is still an answer. It is not the wedge this watchdog
        # exists for, so record reachability without restarting the daemon.
        printf 'hub MCP answered HTTP %s; not a hang\n' "${status:-000}" >&2
        printf '0\n' > "$STATE"
        exit 0
        ;;
esac
