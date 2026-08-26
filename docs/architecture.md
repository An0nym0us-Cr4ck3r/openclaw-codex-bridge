# Architecture C' ver.2 — Single-Owner UDS Bridge

## Goal

`codex-bridge-daemon` が Codex `app-server.sock` の唯一の所有者(single owner)となり、WS を 1 本だけ保持する。競合する WS を排除し、ThreadStore によるスレッドローテーションで 16384 item 上限(systemError)を回避する。

Fixes Miku wall discussion 2026-08-26.

## Diagram

```
                   ┌─────────────────────────────────────────────────────────┐
                   │            codex-bridge-daemon (single owner)           │
                   │                                                         │
Telegram reader ───┤─ UDS ─┐                                ┌── WS(1) ──┼── app-server.sock
                   │       ├─ asyncio.Queue (直列化) ─ queue ─┤             │
miku-to-codex  ────┤─ UDS ─┘                                │ ThreadStore │
                   │       │                                 └──┬──────────┘
                   │       │  fanout ── Telegram (codex-tg-send)│
                   │       │         └─ Miku (openclaw --deliver)│
                   │       │  buffer ── offset/再送              │
                   │       │  reconnect ── WS 再接続 + 指数バックオフ
                   │       └──────── thread-state.json ──────────┘
                   └─────────────────────────────────────────────────────────┘
```

## Components

### 1. `codex-bridge-daemon` (`daemon/daemon.py`)

Single-owner daemon. Responsibilities:

- **UDS server**: `XDG_RUNTIME_DIR/codex-bridge.sock` (`/run/user/1000/codex-bridge.sock`) で LISTEN。`SO_REUSEADDR` ではない、排他バインド。起動時に stale sock を unlink。
- **Single WS**: `~/.codex/app-server-control/app-server-control.sock` への WebSocket 1 本だけを保持。`AppServer` クラスが `initialize` → `initialized` → `thread/resume` までを管理。切断時は指数バックオフ(1s,2s,4s..max 30s)で再接続。WS の `ping/pong` は自前 WS 実装で処理。
- **Queue 直列化**: UDS で受けた `submit` を `asyncio.Queue` に enqueue。ワーカは 1 つだけが WS を使う。`flock` は使わない(queue が直列化を保証)。
- **ThreadStore** (`~/.local/state/codex-bridge/thread-state.json`):
  - `{"activeThreadId": "…", "createdAt": 172416…, "itemCount": N, "forkedFrom": "…"}`
  - 起動時に読み込み。なければ `thread/list` から最新 idle/notLoaded を選ぶか、新規 fork 元を決定。
  - `thread/read` で `turns[].status` と item 数を監視。`systemError` 検知または `items > 12000` / `turns > 50` で `thread/fork` し `activeThreadId` を更新して永続化。fork 後は新 thread で `thread/read` が `idle` であることを確認。
  - Fork 失敗時は古い thread で `thread/compact/start` を試み、失敗なら error を UDS クライアントに返す。
- **Turn 実行**:
  - `thread/read` で `inProgress/pending/started` の active turn があれば `turn/steer` (with `expectedTurnId`)、なければ `turn/start`。
  - `item/agentMessage/delta` を集約。`turn/completed` で完了。
  - `turn/interrupt` は UDS の `cancel` メソッドで露出(将来)。
- **Fanout**:
  - Codex 返信を durable outbox に記録してから、1) Telegram (`codex-tg-send` 経由、3500 文字/chunk) 2) Miku (`openclaw agent --deliver --session-key agent:miku:telegram:direct:7536160870`) に配信。
  - bounded subprocess を `asyncio.to_thread` で実行し、`redact()` で credential パターンを除去してから配信。ターゲットごとに成功/失敗を記録し、指数バックオフで再試行する。
- **通知バッファ / offset**:
  - `~/.local/state/codex-bridge/offset.json` は schema v2。`requests` に request fingerprint と完了レスポンス、`pendingReplies` に `deliveryId` と Telegram/Miku 各ターゲットの状態を保持する。
  - requestId と入力 fingerprint が一致する再送は保存済みレスポンスを返し、Codex turn を二重実行しない。異なる入力で同じ requestId を使うと conflict にする。
  - 完了レスポンスと outbox 追加は同じ atomic replace で保存する。fanout は両ターゲットが成功するまで outbox を削除しないため、daemon 異常終了後も再送できる。
  - `pending` / `delivered` は旧 C' ver.2 との互換ミラーとして残す。
- **検証ログ**:
  - `~/.local/state/codex-bridge/verification.jsonl` に request replay、thread usage/rotation、outbox、fanout 成否を本文なしで fsync 記録する。
  - `python3 tools/bridge_report.py` が offset とイベント集計を表形式または `--format json` で出力する。
- **UDS protocol** (JSON line, `\n` delimited):
  - Client → Daemon: `{"id":1,"method":"submit","params":{"text":"…","source":"miku"|"telegram","requestId":"…"}}`
  - Client → Daemon: `{"id":2,"method":"status"}`
  - Client → Daemon: `{"id":3,"method":"ping"}`
  - Daemon → Client: `{"id":1,"result":{"reply":"…","threadId":"…"}}` or `{"id":1,"error":{"code":…, "message":"…"}}`

### 2. `miku-to-codex` (`clients/miku-to-codex`)

Thin UDS client. No WS, no ThreadStore, no fanout.

```sh
miku-to-codex "hello"        # → UDS submit → daemon queue → WS → Codex → fanout → stdout
echo "hello" | miku-to-codex # stdin fallback
```

- `XDG_RUNTIME_DIR/codex-bridge.sock` に connect。なければ `codex-bridge.sock not found — is daemon running?` を stderr に出して exit 3。
- 5s 以内に connect、turn 完了は最大 30min タイムアウト(WS 側で持つが、UDS クライアントもソケットタイムアウトを伸ばす)。
- `LOCK_PATH` の `flock` は廃止(queue が直列化)。`TURN_LOCK_PATH` も daemon 内 queue に移譲。
- stdout に Codex reply を出すのみ。Telegram/Miku への fanout は daemon が所有。

### 3. `telegram-reader` (`clients/telegram_reader.py`)

Reader-only. Polling → UDS submit に削減。

- 旧 `telegram_codex_bridge.py` (592行, 自前WS, ThreadStore, fanout) から WS/ThreadStore/fanout を削除。
- 残す: `SESSIONS_INDEX` → `SESSION_DIR` 解決、`read_records()`、`label_record()`、`is_stop_instruction()`、4s ポーリング、offset/state (`~/.local/state/codex-telegram-bridge/state.json`)。
- 新: 各 `User:` 行を stable `requestId` 付きの `{"method":"submit","params":{"text":…, "source":"telegram","requestId":"…"}}` で UDS に submit。Miku コンテキストも同様に submit(順序保証のため queue 経由)。
- Daemon 死亡時は UDS connect 失敗を `bridge retry` として 5s 待機して再試行。`STATE_PATH` の `processed_ids` は reader 側で持ち、daemon 側の request ledger と合わせて二重投入を防ぐ。
- `deliver_both` / `deliver_to_miku` / `AppServer` クラスは削除。

## Failure Modes

| Failure | Detection | Recovery |
|---------|-----------|----------|
| app-server.sock 死亡 | WS `recv` が `close` opcode or `ECONN` | 指数バックオフ再接続、queue は保持、クライアントはブロック継続 |
| systemError (16384 items) | `thread/read` の `status.type == "systemError"` or item count > 12000 | `thread/fork` → new `activeThreadId` → retry submit |
| daemon クラッシュ | systemd `Restart=always`; UDS clients は connect 失敗 → 5s リトライ | offset.json の per-target `pendingReplies` を fanout worker が再送 |
| UDS stale sock | 起動時に `unlink` 前に `connect` 試行 → 成功なら他 daemon 生存とみなして exit 1 | `/run/user/1000` は tmpfs、再起動で自然に消える |
| Telegram JSONL ロック | `sessions.json` の `sessionFile` 変更検知で行儀よく reader が session 切替 | `processed.clear()` + `bootstrap_done=False` |

## Systemd

```
codex-bridge.service      — daemon (UDS + WS owner)
telegram-codex-bridge.service — reader (poll → UDS)  ※ daemon After= 依存
codex-remote-control.service  — app-server 本体 (既存、維持)
```

`codex-bridge.service` は `PartOf=codex-remote-control.service` ではなく独立。`After=codex-remote-control.service` で順序だけ保証。再起動は `Restart=always RestartSec=5`.

## Security

- UDS は `0700` の `XDG_RUNTIME_DIR` 配下なので同 UID のみアクセス可。追加認証は不要。
- `redact()` は daemon fanout 前に適用。
- `codex-tg-send` の 4000 文字/guard は維持(daemon が呼び出す)。
- `openclaw --deliver` は daemon の bounded worker(`asyncio.to_thread`, `timeout --kill-after`)で実行、失敗しても daemon 本体は落ちない。

## E2E Test Plan

1. **双方向**: `miku-to-codex "PING"` → Codex reply が stdout に戻り、Telegram に `[Codex] PONG` が届く。
2. **Miku 経由**: Telegram `User:` 行 → reader が UDS submit → daemon が Codex turn → fanout で Miku session に `Codex:` が inject される。
3. **欠落なし**: reader が UDS 失敗中に溜めた `pending_users` を daemon 復帰後に再送し、stable requestId で二重投与を避ける。返信は両 fanout target の成功まで outbox に残る。
4. **コンテキスト保持**: 同一 `activeThreadId` で 2 連続 submit が同じ thread の続きとして Codex に届く(`turn/read` で threadId 一致確認)。
5. **systemError 回復**: 人工的に `status=systemError` の thread を active に設定し、次の submit で fork が起きることを確認。
6. **再接続**: `systemctl --user restart codex-remote-control` で app-server を再起動し、daemon が queue を保持したまま再接続することを確認。
7. **長期検証**: `python3 tests/soak_bridge.py --count 10000 --crash-every 37` で state 再読込・再送・重複 request を繰り返し、完了 request/delivery 数と pending=0 を確認する。

## Heartbeat Integration

- 既存の `miku-heartbeat` (55m) は維持。追加で daemon 自身が `thread/read` で systemError を heartbeat とは独立に検知。
- daemon の liveness は `UDS ping` で確認可能(`miku-heartbeat` から `echo '{"id":1,"method":"ping"}' | socat - UNIX-CONNECT:/run/user/1000/codex-bridge.sock` 相当の簡易 check を追加できるが必須ではない)。
