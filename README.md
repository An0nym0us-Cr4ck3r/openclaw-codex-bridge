# openclaw-codex-bridge

Single-owner UDS bridge between OpenClaw (Miku) and Codex app-server.

- `codex-bridge-daemon`: single owner of `app-server.sock` (WS 1本), ThreadStore, fanout → Telegram + Miku
- `miku-to-codex`: thin UDS client (no WS)
- `telegram-reader`: JSONL tail → UDS submit

Architecture: C' ver.2 (see docs/architecture.md)

Status: scaffolding — Miku & Codex co-authoring.
