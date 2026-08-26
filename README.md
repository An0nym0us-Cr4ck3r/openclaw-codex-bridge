# openclaw-codex-bridge

Single-owner UDS bridge between OpenClaw (Miku) and Codex app-server — C' ver.2.

## Components

| Path | Role |
|------|------|
| `daemon/daemon.py` | single owner of `app-server.sock` (WS 1本), `ThreadStore`, fanout → Telegram+Miku, reconnect+buffer |
| `daemon/ws_client.py` | minimal async WS over UDS |
| `daemon/thread_store.py` | `activeThreadId` + rotation (`systemError` / `turns>50` / `items>12000`) |
| `clients/miku-to-codex` | thin UDS client (no WS) — `miku-to-codex "hello"` |
| `clients/telegram_reader.py` | reader-only (poll → UDS submit) |
| `systemd/codex-bridge.service` | daemon unit |
| `docs/architecture.md` | C' ver.2 design |

## Quick start

```sh
systemctl --user enable --now codex-bridge.service
systemctl --user enable --now telegram-codex-bridge.service
miku-to-codex "hello"  # → Codex reply on stdout + Telegram + Miku
echo '{"id":1,"method":"ping"}' | socat - UNIX-CONNECT:/run/user/1000/codex-bridge.sock
echo '{"id":1,"method":"status"}' | socat - UNIX-CONNECT:/run/user/1000/codex-bridge.sock
```

## State

- `~/.local/state/codex-bridge/thread-state.json` — `activeThreadId`
- `~/.local/state/codex-bridge/offset.json` — `pendingReplies` / `delivered` dedup
- `~/.local/state/codex-telegram-bridge/state.json` — reader `processed_ids` (polling offset)

## E2E

```
miku-to-codex "[E2E-1] 1+1は？"  → [Codex] 2  (same thread context preserved)
app-server restart              → daemon reconnects, queue preserved
systemError (16384 items)       → ThreadStore picks fresh small thread
```

## Status

C' ver.2 implemented 2026-08-26 — replaces `019fc308`/`01a03ec6` (systemError chain) with fresh `01a03f04-e1aa-7af0-8d7e-84d963cf03bf`.
