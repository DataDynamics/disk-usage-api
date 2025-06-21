#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Author: KIM BYOUNGGON (architect@data-dynamics.io)
Description: 서버의 Disk Usage를 수집하여 80%가 넘는지 확인하는 배치 작업
"""

import argparse
import subprocess
import logging
import yaml
import json
import socket
import psycopg2
from datetime import date
from logging.handlers import TimedRotatingFileHandler
from lxml import etree


# YAML 설정 파일 로드 함수
def load_config(path='config.yaml'):
    with open(path, 'r') as f:
        return yaml.safe_load(f)


# argparse를 사용해 커맨드라인 인자 처리
def parse_args():
    parser = argparse.ArgumentParser(description="Disk Daily Job with YAML config")
    parser.add_argument('--config', type=str, default='config.yaml', help='Path to the config YAML file')
    return parser.parse_args()


args = parse_args()
config = load_config(args.config)
app_config = config['app']
postgres_config = app_config['postgresql']


# Logger 설정
log_file = app_config['logfile-path']
disk_usage_conf_path = app_config['disk-usage-conf-path']
logger = logging.getLogger("daily_logger")
logger.setLevel(logging.INFO)

handler = TimedRotatingFileHandler(
    filename=log_file,
    when='midnight',  # 자정 기준 롤링
    interval=1,  # 매 1일마다
    backupCount=14,  # 최근 7일치만 보관
    encoding='utf-8',
    utc=False  # 로컬 시간 기준 (True면 UTC 기준)
)

formatter = logging.Formatter('%(asctime)s - [%(levelname)s] %(filename)s:%(lineno)d - %(message)s')  # Formatter
handler.setFormatter(formatter)

logger.addHandler(handler)  # Log Handler



def get_disk_usage():
    hostname = socket.gethostname()
    result = subprocess.run(['df', '-kP'], capture_output=True, text=True)
    lines = result.stdout.strip().split('\n')

    partitions = []
    over_80p_partitions = []
    summary_disk_80p_over = False

    for line in lines[1:]:
        parts = line.split()
        if len(parts) < 6:
            continue

        partition_name, total_kb, used_kb, _, percent_str, mount_point = parts

        try:
            total = int(total_kb) * 1024
            used = int(used_kb) * 1024
            usage_percent = int(percent_str.strip('%'))
        except ValueError:
            continue

        over_80p = usage_percent > 80
        if over_80p:
            summary_disk_80p_over = True
            over_80p_partitions.append(mount_point)

        partitions.append({
            'partition': mount_point,
            'total': total,
            'used': used,
            'disk_80p_over': over_80p
        })

    output = {
        'hostname': hostname,
        'partitions': partitions,
        'over_80p_partitions': over_80p_partitions,
        'summary_disk_80p_over': summary_disk_80p_over
    }

    logger.info(f"Hostname: {hostname}, Summary Disk 80% Over: {summary_disk_80p_over}")

    return hostname, summary_disk_80p_over, json.dumps(output, ensure_ascii=False)

def insert_into_postgres():
    hostname, summary_disk_80p_over, json_data = get_disk_usage()

    conn = psycopg2.connect(
        host=postgres_config['hostname'],
        database=postgres_config['database'],
        user=postgres_config['username'],
        password=postgres_config['password'],
    )

    cursor = conn.cursor()

    insert_query = """
    INSERT INTO daily_report (CURRENT_YMD, TYPE, KEY, VALUE, HOSTNAME, SERVICE, JSON)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    current_date = str(date.today())
    record = (
        current_date,
        'SYSTEM',
        'File System 용량',
        str(summary_disk_80p_over),
        hostname,
        '',
        json_data
    )

    cursor.execute(insert_query, record)
    conn.commit()
    cursor.close()
    conn.close()
    print("Insert completed successfully.")

if __name__ == '__main__':
    insert_into_postgres()
