#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import yaml
from flask import Flask, request, jsonify
from flasgger import Swagger
import subprocess
import logging
from logging.handlers import TimedRotatingFileHandler
from lxml import etree
import json
import socket
import math
import re


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

    # XML 파일 로딩 및 파싱
    tree = etree.parse(disk_usage_conf_path)

    ########################################
    # 패턴 매칭을 위한 Ant-style 패턴
    ########################################

    patterns = {}
    named_patterns = tree.xpath("//patterns/pattern[@name]")
    for pattern in named_patterns:
        patterns[pattern.get('name')] = pattern.text

    ########################################
    # 파티션의 크기 확인
    ########################################

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

        if parts:
            partition_name, total_kb, used_kb, _, percent_str, mount_point = parts

            if partition_name.startswith("/dev") :
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

                qualifier = matches(patterns, mount_point)

                partitions.append({
                    'qualifer': '' if qualifier is None else qualifier,
                    'partition': mount_point,
                    'total': total,
                    'total_mb': round(bytes_to_megabytes(total), 0),
                    'used': used,
                    'used_mb': round(bytes_to_megabytes(used), 0),
                    'usage': truncate_float(used * 100 / total),
                    'disk_80p_over': over_80p
                })

    ########################################
    # 사용자가 지정한 디렉토리의 크기 확인
    ########################################

    # name 속성이 있는 path 추출
    dir_paths = {}
    named_paths = tree.xpath("//paths/path[@name]")
    for path in named_paths:
        dir_paths[path.get('name')] = path.text

    # Disk Usage of Path
    path_usage = []
    for name in dir_paths:
        path = dir_paths[name]
        size = get_directory_size(path)
        qualifier = matches(patterns, path)

        # 허가 거부가 발생하면 None이 반환됨
        if size is not None:
            path_usage.append({
                'qualifer': '' if qualifier is None else qualifier,
                "name": name,
                "path": path,
                "size": size
            })
    
    ########################################
    # 결과 처리
    ########################################

    logger.info(f"Hostname: {hostname}, Summary Disk 80% Over: {summary_disk_80p_over}")

    json_string = {}
    json_string['hostname'] = hostname
    json_string['partitions'] = partitions
    json_string['paths'] = path_usage
    json_string['over_80p_partitions'] = over_80p_partitions
    json_string['summary_disk_80p_over'] = summary_disk_80p_over
    
    logger.info('Disk Usage 처리 결과\n{}'.format(json.dumps(json_string, indent=4)))

    return jsonify(json_string)


# 소숫점 2자리 이하를 제거
def truncate_float(num, decimals=2):
    factor = 10 ** decimals
    return math.trunc(num * factor) / factor


# 바이트 단위를 메가바이트 단위로 변경
def bytes_to_megabytes(byte_size):
    return byte_size / (1024 * 1024)


# Ant-style 패턴 매칭 함수
def ant_match(path: str, pattern: str) -> bool:
    # Ant-style to regex 변환
    pattern = pattern.replace(".", r"\.")
    pattern = pattern.replace("**", ".*")
    pattern = pattern.replace("*", "[^/]*")
    pattern = pattern.replace("?", ".")
    pattern = "^" + pattern + "$"
    return re.match(pattern, path) is not None


# 주어진 경로가 패턴과 일치하는지 확인하는 함수
def matches(patterns, path):
    for name, pattern in patterns.items():
        if ant_match(path, pattern):
            logger.debug(f"Path '{path}' matches pattern '{name}' with regex '{pattern}'")
            return name
    return None


# 디렉토리의 크기를 측정하는 함수
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
        except ValueError as ve:
            logger.warning(f"용량을 변환할 수 없습니다. 에러: {ve}")
            return None

    except subprocess.CalledProcessError as e:
        logger.warning(f"디렉토리의 용량을 측정할 수 없습니다. 에러: {e}")
        return None


if __name__ == '__main__':
    app.run(
        host=app_config.get('host', '0.0.0.0'),
        port=app_config.get('port', 5000),
        debug=app_config.get('debug', False)
    )
