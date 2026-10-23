#!/usr/bin/env python3
"""直接运行 DolphinScheduler 采集脚本"""
import sys
import logging

logging.basicConfig(level=logging.INFO)

from datahub.ingestion.run.pipeline import Pipeline

config = {
    "source": {
        "type": "dolphinscheduler",
        "config": {
            "api_url": "http://192.168.2.15:12345/dolphinscheduler",
            "token": "25d8cca5b5baa051a9ccadd2e833ba92",
            "request_timeout_sec": 20,
            "extract_task_sql_lineage": True,
        },
    },
    "sink": {
        "type": "datahub-rest",
        "config": {
            "server": "http://localhost:8080",
        },
    },
}

print("Starting DolphinScheduler ingestion...")
pipeline = Pipeline.create(config)
pipeline.run()
result = pipeline.raise_from_status()
print(f"Done. Result: {result}")
