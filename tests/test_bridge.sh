#!/bin/bash
# E2E smoke: ping/status via UDS — python only, no socat needed
set -e
UDS="${CODEX_BRIDGE_SOCK:-/run/user/1000/codex-bridge.sock}"
if [ ! -S "$UDS" ]; then echo "SKIP: $UDS not found (daemon not running)"; exit 0; fi
probe() {
  python3 - "$UDS" "$1" <<'PY'
import json
import socket
import sys

sock_path, method = sys.argv[1:]
with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
    sock.settimeout(3)
    sock.connect(sock_path)
    sock.sendall((json.dumps({"id": 1, "method": method}) + "\n").encode())
    response = json.loads(sock.recv(65536).decode().split("\n", 1)[0])
if "error" in response:
    raise SystemExit(response["error"])
result = response.get("result") or {}
if method == "ping" and result.get("ok") is not True:
    raise SystemExit(f"unexpected ping response: {response}")
if method == "status" and "activeThreadId" not in result:
    raise SystemExit(f"unexpected status response: {response}")
print(f"PASS: daemon {method}")
PY
}
probe ping
probe status
echo "E2E smoke OK"
