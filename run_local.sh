#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
BACKEND_DIR="$ROOT_DIR/backend"
FRONTEND_DIR="$ROOT_DIR/frontend"
RUN_DIR="$ROOT_DIR/.run"
mkdir -p "$RUN_DIR"

if [[ ! -x "$BACKEND_DIR/.venv/bin/python" ]]; then
  echo "Backend venv missing at $BACKEND_DIR/.venv"
  exit 1
fi

if [[ ! -d "$FRONTEND_DIR/node_modules" ]]; then
  echo "Frontend node_modules missing at $FRONTEND_DIR/node_modules"
  exit 1
fi

if [[ ! -f "$BACKEND_DIR/.env" ]]; then
  cp "$BACKEND_DIR/.env.example" "$BACKEND_DIR/.env"
fi

if [[ -f "$RUN_DIR/backend.pid" ]] && kill -0 "$(cat "$RUN_DIR/backend.pid")" 2>/dev/null; then
  echo "Backend already running on PID $(cat "$RUN_DIR/backend.pid")"
else
  (
    cd "$BACKEND_DIR"
    source .venv/bin/activate
    nohup uvicorn app.main:app --host 127.0.0.1 --port 8000 > "$RUN_DIR/backend.log" 2>&1 &
    echo $! > "$RUN_DIR/backend.pid"
  )
  echo "Started backend PID $(cat "$RUN_DIR/backend.pid")"
fi

if [[ -f "$RUN_DIR/frontend.pid" ]] && kill -0 "$(cat "$RUN_DIR/frontend.pid")" 2>/dev/null; then
  echo "Frontend already running on PID $(cat "$RUN_DIR/frontend.pid")"
else
  (
    cd "$FRONTEND_DIR"
    nohup npm run dev -- --host 127.0.0.1 --port 5173 > "$RUN_DIR/frontend.log" 2>&1 &
    echo $! > "$RUN_DIR/frontend.pid"
  )
  echo "Started frontend PID $(cat "$RUN_DIR/frontend.pid")"
fi

echo "Backend:  http://127.0.0.1:8000"
echo "Frontend: http://127.0.0.1:5173"
echo "Logs:     $RUN_DIR/backend.log and $RUN_DIR/frontend.log"
