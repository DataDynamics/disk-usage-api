#!/bin/sh
#
# Author: KIM BYOUNGGON(architect@data-dynamics.io)
# Description: 백그라운드로 실행중인 Python 애플리케이션을 종료하는 스크립트
# Usage: sh shutdown.sh
#

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
