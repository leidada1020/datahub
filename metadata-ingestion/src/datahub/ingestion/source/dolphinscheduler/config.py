from typing import List, Optional

from pydantic import Field, SecretStr

from datahub.configuration.source_common import DatasetLineageProviderConfigBase
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


class DolphinSchedulerConfig(
    DatasetLineageProviderConfigBase,
    StatefulIngestionConfigBase,
):
    """DolphinScheduler REST API 连接器配置。

    令牌获取: DolphinScheduler 安全中心 -> 令牌管理 -> 创建 API 令牌
    支持 DS 3.x 版本。
    """

    api_url: str = Field(
        description="DolphinScheduler API 地址, e.g. http://192.168.2.15:12345/dolphinscheduler",
    )
    token: SecretStr = Field(description="API 访问令牌 (token 请求头)")
    project_codes: Optional[List[int]] = Field(
        default=None,
        description="指定采集的项目 code 列表, 不填则采集全部项目",
    )
    request_timeout_sec: float = Field(
        default=20.0, description="单次 HTTP 请求超时(秒)", gt=0
    )
    extract_task_sql_lineage: bool = Field(
        default=True,
        description="是否从 SQL 任务中解析表级血缘",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None
