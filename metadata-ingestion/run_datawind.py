#!/usr/bin/env python3
"""直接运行 DataWind 采集脚本"""
import sys
import logging

logging.basicConfig(level=logging.INFO)

from datahub.ingestion.run.pipeline import Pipeline

config = {
    "source": {
        "type": "datawind",
        "config": {
            "api_base": "https://console.volcengine.com/bi/datawind/aeolus",
            "region": "cn-beijing",
            "app_id": 20074764,
            "client_id": "892af4444b6321e72e9ab23bf39e9f07",
            "client_secret": "f7d85be463dc7560c78c2782c9249312",
            "proxy_user": "liyunze@2105712913",
            "request_timeout_sec": 180,
            "max_workers": 8,
            "extract_dim_metrics": True,
        },
    },
    "sink": {
        "type": "datahub-rest",
        "config": {
            "server": "http://localhost:8080",
        },
    },
}

print("Starting DataWind ingestion...")
print("Config:", config["source"]["config"].get("client_id"), config["source"]["config"].get("client_secret"))
pipeline = Pipeline.create(config)
pipeline.run()
result = pipeline.raise_from_status()
print(f"Done. Result: {result}")
