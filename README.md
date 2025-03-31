# Disk Usage API

지정한 파티션의 디스크 사용량을 수집하여 REST로 제공하는 API입니다.

## PyPi 패키지 설치

```
# pip3 install flask argparse flasgger
```

## 커맨드로 실행

```
# python3 server.py --config config.yaml
```

## Linux Systemd 설정

다음과 같이 서비스 파일을 작성합니다.

```
# vi /etc/systemd/system/disk-usage-api.service
[Unit]
Description=Disk Usage API
After=network.target

[Service]
User=cloudera
WorkingDirectory=/sw/disk-usage-api
ExecStart=/usr/bin/python3 /sw/disk-usage-api/server.py --config /sw/disk-usage-api/config.yaml

Restart=always
RestartSec=5

Environment="FLASK_ENV=production"
Environment="PYTHONUNBUFFERED=1"

[Install]
WantedBy=multi-user.target
```

다음의 커맨드로 실행합니다.

```
# sudo systemd restart disk-usage-api
```
