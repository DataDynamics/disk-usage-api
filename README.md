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

## API 호출

```
# wget -O - http://localhost:5000/api/disk/usage
--2025-03-31 21:54:44--  http://localhost:5000/api/disk/usage
Resolving localhost (localhost)... ::1, 127.0.0.1
Connecting to localhost (localhost)|::1|:5000... failed: 연결이 거부됨.
Connecting to localhost (localhost)|127.0.0.1|:5000... connected.
HTTP request sent, awaiting response... 200 OK
Length: 109 [application/json]
Saving to: ‘STDOUT’

 0% [                                                                    ] 0           --.-K/s              {
  "hdfs_total": 362438204,
  "hdfs_used": 550005488,
  "kudu_total": 362438204,
  "kudu_used": 550005488
}
100%[==================================================================>] 109         --.-K/s   in 0s
```
