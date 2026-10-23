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
            "client_id": "c28c6f2a46766522edab67732e7c1cdf",
            "client_secret": "16vsL5vmVEO0yvxr",
            "proxy_user": "liyunze",
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
pipeline = Pipeline.create(config)
pipeline.run()
result = pipeline.raise_from_status()
print(f"Done. Result: {result}")
