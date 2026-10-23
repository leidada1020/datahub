from typing import Optional

from pydantic import Field, SecretStr

from datahub.configuration.source_common import (
    DatasetLineageProviderConfigBase,
    LowerCaseDatasetUrnConfigMixin,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


class DataWindConfig(
    DatasetLineageProviderConfigBase,
    StatefulIngestionConfigBase,
    LowerCaseDatasetUrnConfigMixin,
):
    """DataWind OpenAPI 连接器配置。

    凭证获取路径: 火山引擎控制台 -> DataWind -> 应用管理 -> OpenAPI 凭证
    """

    api_base: str = Field(
        description="DataWind OpenAPI 基础地址, e.g. https://datawind.volcengineapi.com",
    )
    region: str = Field(
        default="cn-north-1",
        description="DataWind 区域标识 (x-vedi-region 请求头)",
    )
    app_id: int = Field(description="DataWind 应用 ID")
    client_id: str = Field(description="OpenAPI clientId")
    client_secret: SecretStr = Field(description="OpenAPI clientSecret")
    proxy_user: Optional[str] = Field(
        default=None, description="代理用户 (proxyUser), 通常留空"
    )
    request_timeout_sec: float = Field(
        default=180.0, description="单次 HTTP 请求超时(秒)", gt=0
    )
    max_workers: int = Field(
        default=8, description="并发拉取数据集详情的线程数", ge=1, le=32
    )
    extract_dim_metrics: bool = Field(
        default=True, description="是否采集维度/指标元数据"
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None
