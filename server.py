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
    tree = etree.fromstring(disk_usage_conf_path)

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

    # Return JSON
    json_string = {}
    json_string['kudu_used'] = kudu_used
    json_string['kudu_total'] = kudu_used + hdfs_available
    json_string['hdfs_used'] = hdfs_used
    json_string['hdfs_total'] = hdfs_used + hdfs_available

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
        print(f"오류 발생: {e}")
        return None


if __name__ == '__main__':
    app.run(
        host=app_config.get('host', '127.0.0.1'),
        port=app_config.get('port', 5000),
        debug=app_config.get('debug', False)
    )
