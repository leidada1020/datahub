import logging
from typing import Iterable, List, Optional

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import add_entity_to_container, gen_containers
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.datawind.client import DataWindClient
from datahub.ingestion.source.datawind.config import DataWindConfig
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ContainerClass,
    DatasetPropertiesClass,
    EdgeClass,
    GlobalTagsClass,
    OwnerClass,
    OwnershipClass,
    OwnerTypeClass,
    StatusClass,
    TagAssociationClass,
    UpstreamLineageClass,
    UpstreamClass,
)

logger = logging.getLogger(__name__)


class DataWindSourceReport(SourceReport):
    total_datasets: int = 0
    datasets_with_sql: int = 0
    datasets_with_lineage: int = 0
    folders_discovered: int = 0


@platform_name("DataWind")
@config_class(DataWindConfig)
@support_status(SupportStatus.INCUBATING)
class DataWindSource(StatefulIngestionSourceBase):
    """DataWind (火山引擎 BI) 元数据连接器。

    采集:
      - 数据集列表 (作为 Dataset 实体, platform=datawind)
      - 文件夹树 (作为 Container)
      - 加工 SQL 血缘 (上游 Doris 表 -> DataWind 数据集)
      - 所有者、同步状态等属性
    """

    def __init__(self, config: DataWindConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = DataWindSourceReport()
        self.client = DataWindClient(config)
        self._folder_map = {}

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext):
        config = DataWindConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_platform_display_name(self) -> str:
        return "DataWind"

    # ---------- 容器 (文件夹) ----------
    def _wrap_as_folder_workunits(self) -> Iterable[MetadataWorkUnit]:
        if not self._folder_map:
            return
        container_key = gen_containers(
            parent_key=None,
            name="datawind_folders",
            display_name="DataWind Folders",
            sub_type="DATABASE",
        )
        yield ContainerClass(
            name="datawind_folders",
            qualified_name="datawind:folders",
        )
        # 简化: 不逐文件夹建 container, 后续按需扩展

    # ---------- 主入口 ----------
    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        logger.info("Starting DataWind ingestion...")

        # 1. 拉文件夹树
        try:
            self._folder_map = self.client.folder_tree()
            self.report.folders_discovered = len(self._folder_map)
            logger.info("Discovered %d folders", len(self._folder_map))
        except Exception as e:
            self.report.report_warning("folder_tree", f"拉取文件夹树失败: {e}")

        # 2. 拉数据集列表
        try:
            datasets = self.client.list_datasets(need_sync_info=True)
        except Exception as e:
            self.report.report_failure("list_datasets", f"拉取数据集列表失败: {e}")
            return

        self.report.total_datasets = len(datasets)
        logger.info("Discovered %d datasets", len(datasets))

        # 3. 并发拉详情 + 加工逻辑
        ds_ids = [d.get("id") for d in datasets if d.get("id")]
        details = self.client.batch_fetch(ds_ids, self._fetch_dataset_detail)

        # 4. 逐数据集生成 workunit
        for ds in datasets:
            dsid = ds.get("id")
            if dsid is None:
                continue
            detail = details.get(dsid) or {}
            yield from self._process_dataset(ds, detail)

        logger.info(
            "DataWind ingestion done: %d datasets, %d with sql, %d with lineage",
            self.report.total_datasets,
            self.report.datasets_with_sql,
            self.report.datasets_with_lineage,
        )

    def _fetch_dataset_detail(self, dsid: int) -> dict:
        """并发拉取单个数据集的详情 + 加工逻辑"""
        result: dict = {"info": {}, "model": ("", "", [], None), "dim_met": []}
        try:
            result["info"] = self.client.dataset_info(dsid)
        except Exception as e:
            logger.debug("dataset_info %s failed: %s", dsid, e)
        try:
            result["model"] = self.client.model_logic(dsid)
        except Exception as e:
            logger.debug("model_logic %s failed: %s", dsid, e)
        if self.config.extract_dim_metrics:
            try:
                result["dim_met"] = self.client.dim_met(dsid)
            except Exception as e:
                logger.debug("dim_met %s failed: %s", dsid, e)
        return result

    def _process_dataset(self, ds: dict, detail: dict) -> Iterable[MetadataWorkUnit]:
        dsid = ds.get("id")
        name = ds.get("name") or f"dataset_{dsid}"
        platform = "datawind"

        # Dataset URN
        dataset_urn = builder.make_dataset_urn(platform, f"dataset_{dsid}", "PROD")

        # --- Status ---
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        # --- Properties ---
        upstreams_str = ""
        sql, upstreams_str, fields, data_source = detail.get("model", ("", "", [], None))
        if sql:
            self.report.datasets_with_sql += 1

        folder_id = None
        info = detail.get("info") or {}
        bc = info.get("baseConf") or {}
        folder_id = bc.get("parentId")
        folder_path = self._folder_map.get(folder_id, "")

        properties = DatasetPropertiesClass(
            name=name,
            description=ds.get("description") or "",
            customProperties={
                "dataset_id": str(dsid),
                "type": (ds.get("type") or {}).get("name", ""),
                "folder_path": folder_path,
                "sync_type": str(ds.get("syncType") or ""),
                "frequency": str(ds.get("frequency") or ""),
                "last_sync_time": str(ds.get("lastSyncTime") or ""),
                "rows_last_sync": str(ds.get("rowsForLastSuccessTaskTime") or ""),
                "url": ds.get("url") or "",
            },
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=properties,
        ).as_workunit()

        # --- Ownership ---
        owner_email = ds.get("owner")
        if owner_email:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=OwnershipClass(
                    owners=[
                        OwnerClass(
                            owner=builder.make_user_urn(owner_email.split("@")[0]),
                            type=OwnerTypeClass.DATAOWNER,
                        )
                    ]
                ),
            ).as_workunit()

        # --- Upstream Lineage ---
        if upstreams_str:
            upstreams = []
            for tbl in upstreams_str.split(","):
                tbl = tbl.strip()
                if not tbl or "." not in tbl:
                    continue
                db_name, table_name = tbl.split(".", 1)
                # 上游表默认映射到 Doris 平台
                upstream_urn = builder.make_dataset_urn(
                    "doris", f"{db_name}.{table_name}", "PROD"
                )
                upstreams.append(
                    UpstreamClass(
                        dataset=upstream_urn,
                        type="TRANSFORMED",
                    )
                )
            if upstreams:
                self.report.datasets_with_lineage += 1
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=UpstreamLineageClass(upstreams=upstreams),
                ).as_workunit()
