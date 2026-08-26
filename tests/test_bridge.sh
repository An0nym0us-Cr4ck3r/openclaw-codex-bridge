#!/bin/bash
# E2E smoke: ping/status via UDS — python only, no socat needed
set -e
UDS="/run/user/1000/codex-bridge.sock"
if [ ! -S "$UDS" ]; then echo "SKIP: $UDS not found (daemon not running)"; exit 0; fi
RESP=$(python3 -c "import socket,json; s=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM); s.settimeout(3); s.connect('$UDS'); s.sendall(b'{\"id\":1,\"method\":\"ping\"}\n'); print(s.recv(4096).decode())" 2>&1 || echo "ERR:$RESP")
if echo "$RESP" | grep -q '"ok"'; then echo "PASS: daemon ping"; else echo "FAIL: ping $RESP"; exit 1; fi
RESP2=$(python3 -c "import socket,json; s=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM); s.settimeout(3); s.connect('$UDS'); s.sendall(b'{\"id\":1,\"method\":\"status\"}\n'); print(s.recv(4096).decode())" 2>&1 || echo "ERR:$RESP2")
if echo "$RESP2" | grep -q "activeThreadId"; then echo "PASS: daemon status"; else echo "FAIL: status $RESP2"; exit 1; fi
echo "E2E smoke OK"
