#!/bin/sh

PID_FILE="app.pid"

if [ ! -f "$PID_FILE" ]; then
    echo "No PID file found. Is the app running?"
    exit 1
fi

PID=$(cat "$PID_FILE")

if ps -p $PID > /dev/null; then
    echo "Stopping application with PID $PID"
    kill $PID
    rm "$PID_FILE"
else
    echo "No process found with PID $PID"
    rm "$PID_FILE"
fi
