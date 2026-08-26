# Architecture C' ver.2 — Single-Owner UDS Bridge

Goal: single owner of Codex app-server.sock. See wall discussion 2026-08-26 (Miku's C' ver.2).

```
Telegram reader ─┐
                 ├─ UDS (/run/user/1000/codex-bridge.sock) ── codex-bridge-daemon ── WS(1) ── app-server.sock
miku-to-codex ───┘                                           └─ ThreadStore(thread-state.json)
                                                              └─ fanout → Telegram + Miku
```

Docs to be filled by Codex + Miku.
