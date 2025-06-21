#!/bin/sh
#
# Author: KIM BYOUNGGON(architect@data-dynamics.io)
# Description: Python 애플리케이션을 백그라운드에서 실행하고 PID를 저장하는 스크립트
# Usage: sh startup.sh
#

APP_NAME="disk_daily_report.py"
CONFIG="config.yaml"
LOG_FILE="stdout.log"

# Run
python3 "$APP_NAME" --config="$CONFIG" > "$LOG_FILE"
