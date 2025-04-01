#!/bin/sh

PID_FILE="app.pid"

if [ ! -f "$PID_FILE" ]; then
    echo "Application is not running (no PID file found)."
    exit 1
fi

PID=$(cat "$PID_FILE")

if ps -p $PID > /dev/null; then
    echo "Application is running with PID $PID."
    exit 0
else
    echo "Application is not running (stale PID file found)."
    exit 1
fi
