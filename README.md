# openclaw-codex-bridge

Single-owner UDS bridge between OpenClaw (Miku) and Codex app-server — C' ver.2.

## Components

| Path | Role |
|------|------|
| `daemon/daemon.py` | single owner of `app-server.sock` (WS 1本), `ThreadStore`, reconnect, durable fanout worker |
| `daemon/ws_client.py` | minimal async WS over UDS |
| `daemon/thread_store.py` | `activeThreadId` + rotation (`systemError` / `turns>50` / `items>12000`) |
| `daemon/offset_store.py` | atomic request idempotency ledger + per-target reply outbox |
| `daemon/verification.py` | fsynced, secret-free JSONL verification events |
| `clients/miku-to-codex` | thin UDS client (no WS) — `miku-to-codex "hello"` |
| `clients/telegram_reader.py` | reader-only (poll → UDS submit) |
| `tools/bridge_report.py` | text/JSON dashboard for offset and verification state |
| `systemd/codex-bridge.service` | daemon unit |
| `docs/architecture.md` | C' ver.2 design |

## Quick start

```sh
systemctl --user enable --now codex-bridge.service
systemctl --user enable --now telegram-codex-bridge.service
miku-to-codex "hello"  # → Codex reply on stdout + Telegram + Miku
python3 -c "import socket; s=socket.socket(1,1); s.connect('/run/user/1000/codex-bridge.sock'); s.sendall(b'{\"id\":1,\"method\":\"ping\"}\n'); print(s.recv(4096).decode())"
```

## State

- `~/.local/state/codex-bridge/thread-state.json` — `activeThreadId`
- `~/.local/state/codex-bridge/offset.json` — schema v2 request ledger and `pendingReplies` outbox
- `~/.local/state/codex-bridge/verification.jsonl` — durable delivery/rotation/replay events (no message text)
- `~/.local/state/codex-telegram-bridge/state.json` — reader `processed_ids` (polling offset)

## Tests

```sh
python3 tests/test_thread_store.py   # unit: ThreadStore rotation
python3 tests/test_offset_store.py   # restart/idempotency/outbox recovery
python3 tests/test_verification.py   # JSONL event log/report inputs
python3 tests/test_daemon.py         # request replay + fanout drain (mocked)
python3 tests/test_reader.py         # stable reader request IDs/state
python3 tests/soak_bridge.py          # offline 1000-request durability soak
bash tests/test_bridge.sh            # smoke: daemon ping/status via UDS
python3 tools/bridge_report.py       # human-readable verification dashboard
```

## E2E

```
miku-to-codex "[E2E-1] 1+1は？"  → [Codex] 2  (same thread context preserved)
app-server restart              → daemon reconnects, queue preserved
systemError (16384 items)        → ThreadStore picks fresh small thread
same requestId retry              → stored response replayed, no second turn
fanout target failure             → target remains in offset.json and retries
```

## Status

C' ver.2 implemented 2026-08-26 — replaces `019fc308`/`01a03ec6` (systemError chain) with fresh `01a03f04-e1aa-7af0-8d7e-84d963cf03bf`.
Fanout cgroup fix (timeout 45s) + tests added 2026-08-26 17:10.
PR2 durability/observability added 2026-08-26: atomic offset v2, stable request IDs,
per-target retry outbox, verification JSONL, dashboard, and offline soak test.
