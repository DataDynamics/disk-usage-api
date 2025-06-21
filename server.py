#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Author: KIM BYOUNGGON (architect@data-dynamics.io)
Description: Hadoop Cluster의 Disk Usage를 수집하는 API
"""

import argparse
import yaml
from flask import Flask, request, jsonify
from flasgger import Swagger
import subprocess
import logging
from logging.handlers import TimedRotatingFileHandler
from lxml import etree
import json


# YAML 설정 파일 로드 함수
def load_config(path='config.yaml'):
    with open(path, 'r') as f:
        return yaml.safe_load(f)


# argparse를 사용해 커맨드라인 인자 처리
def parse_args():
    parser = argparse.ArgumentParser(description="Flask server with YAML config")
    parser.add_argument('--config', type=str, default='config.yaml', help='Path to the config YAML file')
    return parser.parse_args()

args = parse_args()
config = load_config(args.config)
app_config = config['app']

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

# Flask App
app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

# Swagger
swagger = Swagger(app, template={
    "swagger": "2.0",
    "info": {
        "title": "Disk Usage API",
        "description": "Disk Usage API",
        "version": "1.0.0",
        "contact": {
            "name": "KIM BYOUNGGON",
            "email": "architect@data-dynamics.io",
            "url": "https://github.com/DataDynamics"
        }
    },
    "basePath": "/",
    "schemes": ["http"]
})

# 디스크 파티션이 특정 임계치를 넘는지 확인
def get_high_usage_partitions(threshold=80):
    # df -P: POSIX 형식으로 출력 (파싱에 유리)
    result = subprocess.run(['df', '-P'], stdout=subprocess.PIPE, text=True)
    lines = result.stdout.strip().split('\n')

    high_usage = []

    for line in lines[1:]:  # 첫 줄은 헤더이므로 건너뜀
        parts = line.split()
        if len(parts) < 6:
            continue  # 예외 처리 (정상적인 df 라인이 아닐 경우)

        filesystem = parts[0]
        size = int(parts[1])
        used = int(parts[2])
        available = int(parts[3])
        use_percent = int(parts[4].strip('%'))
        mount_point = parts[5]

        if use_percent > threshold:
            high_usage.append({
                'filesystem': filesystem,
                'mount_point': mount_point,
                'used_percent': use_percent
            })

    return high_usage

# 디스크 파티션이 임계치를 넘는 것이 하나라도 있는지 확인
def is_any_partition_over_threshold(threshold=80):
    try:
        # POSIX 포맷으로 df 출력
        result = subprocess.run(['df', '-P'], stdout=subprocess.PIPE, text=True, check=True)
        lines = result.stdout.strip().split('\n')

        for line in lines[1:]:  # 첫 줄은 헤더
            parts = line.split()
            if len(parts) < 6:
                continue  # 이상한 줄 무시

            use_percent_str = parts[4]
            try:
                use_percent = int(use_percent_str.strip('%'))
                if use_percent > threshold:
                    return True
            except ValueError:
                continue  # 비정상적인 % 값 무시

        return False

    except subprocess.CalledProcessError as e:
        print(f"df 명령어 실패: {e}")
        return False

@app.route('/api/disk/usage', methods=['GET'])
def disk_usage():
    """
    디스크 사용량
    ---
    parameters:
      - name: X-ACCESS-TOKEN
        in: header
        type: string
        required: true
        description: Access Token
    responses:
      200:
        description: 성공적으로 디스크 사용량 정보를 반환합니다.
        schema:
          type: object
          properties:
            kudu_used:
              type: integer
              example: 550005488
            kudu_total:
              type: integer
              example: 362438204
            hdfs_used:
              type: integer
              example: 550005488
            hdfs_total:
              type: integer
              example: 362438204
      401:
        description: 권한 없음 (Access Token 필요)
    """

    # Token 검증
    access_token = request.headers.get('X-ACCESS-TOKEN')

    if access_token != app_config['access-token']:
        return jsonify({"error": "Unauthorized"}), 401

    # XML 파일 로딩 및 파싱
    tree = etree.parse(disk_usage_conf_path)

    kudu_disk_paths = []
    kudu_paths = tree.xpath("//kudu/paths/path")
    for path in kudu_paths:
        kudu_disk_paths.append(path.text)

    hdfs_disk_paths = []
    hdfs_paths = tree.xpath("//hdfs/paths/path")
    for path in hdfs_paths:
        hdfs_disk_paths.append(path.text)

    # name 속성이 있는 path 추출
    dir_paths = {}
    named_paths = tree.xpath("//paths/path[@name]")
    for path in named_paths:
        dir_paths[path.get('name')] = path.text

    # df 커맨드 실행 및 결과 파싱
    result = subprocess.run(['df'], stdout=subprocess.PIPE, text=True)
    df_string = result.stdout.strip()
    lines = result.stdout.strip().split('\n')

    logger.info("OS의 Disk 현황\n{}".format(df_string))

    headers = lines[0].split()
    partitions = []

    for line in lines[1:]:
        parts = line.split()
        # 헤더 개수보다 컬럼이 많을 경우 (e.g. mount point에 공백 포함)
        if len(parts) > len(headers):
            # mount point 부분을 다시 붙임
            parts = parts[:5] + [' '.join(parts[5:])]

        entry = dict(zip(headers, parts))
        partitions.append(entry)

    logger.info("추출한 OS의 Disk 현황\n{}".format(json.dumps(partitions, indent=4)))

    # Disk Path로 찾기 위해서 reverse index 구성
    reverse_partitions = {}
    for p in partitions:
        reverse_partitions[p['Mounted']] = p

    # Kudu Disk Usage
    kudu_used = 0
    kudu_available = 0
    for path in kudu_disk_paths:
        if reverse_partitions.get(path) is not None:
            kudu_used = kudu_used + int(reverse_partitions.get(path)['Used'])
            kudu_available = kudu_available + int(reverse_partitions.get(path)['Available'])

    # HDFS Disk Usage
    hdfs_used = 0
    hdfs_available = 0
    for path in kudu_disk_paths:
        if reverse_partitions.get(path) is not None:
            hdfs_used = hdfs_used + int(reverse_partitions.get(path)['Used'])
            hdfs_available = hdfs_available + int(reverse_partitions.get(path)['Available'])

    # Disk Usage of Path
    path_usage = []
    for name in dir_paths:
        path = dir_paths[name]
        size = get_directory_size(path)

        if size is not None:
            path_usage.append({
                "name": name,
                "path": path,
                "size": size
            })

    # Return JSON
    json_string = {}
    json_string['kudu_used'] = kudu_used
    json_string['kudu_total'] = kudu_used + hdfs_available
    json_string['hdfs_used'] = hdfs_used
    json_string['hdfs_total'] = hdfs_used + hdfs_available
    json_string['paths'] = path_usage

    logger.info('Disk Usage 처리 결과\n{}'.format(json.dumps(json_string, indent=4)))

    return jsonify(json_string)


@app.route('/api/disk/usage_threshold', methods=['GET'])
def disk_usage_threshold():
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

    json_string = {}
    json_string['hostname'] = hostname
    json_string['partitions'] = output
    json_string['over_80p_partitions'] = over_80p_partitions
    
    logger.info('Disk Usage 처리 결과\n{}'.format(json.dumps(json_string, indent=4)))

    return jsonify(json_string)


def get_directory_size(directory_path):
    try:
        result = subprocess.run(['du', '-sh', directory_path],
                                capture_output=True,
                                text=True,
                                check=True)

        size_str, path = result.stdout.strip().split('\t')

        size = size_str[:-1]  # 숫자 부분
        unit = size_str[-1]  # 단위 부분 (K, M, G, T 등)

        # 단위를 바이트로 변환하기 위한 딕셔너리
        unit_multiplier = {
            'K': 1024,
            'M': 1024 ** 2,
            'G': 1024 ** 3,
            'T': 1024 ** 4
        }

        # 용량을 바이트 단위로 변환
        try:
            size_in_bytes = float(size) * unit_multiplier.get(unit, 1)
            return int(size_in_bytes);
        except ValueError:
            return None

    except subprocess.CalledProcessError as e:
        logger.warn(f"디렉토리의 용량을 측정할 수 없습니다. 에러: {e}")
        return None


if __name__ == '__main__':
    app.run(
        host=app_config.get('host', '127.0.0.1'),
        port=app_config.get('port', 5000),
        debug=app_config.get('debug', False)
    )
