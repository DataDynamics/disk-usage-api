#!/bin/sh

APP_NAME="server.py"
CONFIG="config.yaml"
PID_FILE="app.pid"
LOG_FILE="stdout.log"

# Check if already running
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p $PID > /dev/null; then
        echo "Application already running with PID $PID"
        exit 1
    fi
fi

# Start the application in background
nohup python3 "$APP_NAME" --config="$CONFIG" > "$LOG_FILE" 2>&1 &
PID=$!
echo $PID > "$PID_FILE"
echo "Started $APP_NAME with PID $PID"
