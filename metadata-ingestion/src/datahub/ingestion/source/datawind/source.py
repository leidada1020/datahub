import logging
from typing import Dict, Iterable, List, Optional

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
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
    ChartInfoClass,
    DashboardInfoClass,
    DatasetPropertiesClass,
    OwnerClass,
    OwnershipClass,
    OwnerTypeClass,
    StatusClass,
    UpstreamLineageClass,
    UpstreamClass,
)

logger = logging.getLogger(__name__)


class DataWindSourceReport(SourceReport):
    total_datasets: int = 0
    datasets_with_sql: int = 0
    datasets_with_lineage: int = 0
    folders_discovered: int = 0
    total_dashboards: int = 0
    total_charts: int = 0
    dashboards_with_charts: int = 0


@platform_name("DataWind")
@config_class(DataWindConfig)
@support_status(SupportStatus.INCUBATING)
class DataWindSource(StatefulIngestionSourceBase):
    """DataWind (火山引擎 BI) 元数据连接器。

    采集实体:
      - Dataset: 数据集 (含加工 SQL 血缘 -> Doris 上游表)
      - Chart: 图表 (关联数据集)
      - Dashboard: 仪表盘 (包含图表, 关联数据集)
      - Owner, 同步状态, 文件夹路径等属性

    血缘链: Doris表 -> DataWind数据集 -> DataWind图表 -> DataWind仪表盘
    """

    def __init__(self, config: DataWindConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = DataWindSourceReport()
        self.client = DataWindClient(config)
        self._folder_map: Dict[int, str] = {}
        # 数据集 id -> dataset_urn 映射 (供图表/看板关联用)
        self._dataset_urn_map: Dict[int, str] = {}

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext):
        config = DataWindConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_platform_display_name(self) -> str:
        return "DataWind"

    # ---------- 主入口 ----------
    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        logger.info("Starting DataWind ingestion...")

        # 1. 文件夹树
        try:
            self._folder_map = self.client.folder_tree()
            self.report.folders_discovered = len(self._folder_map)
            logger.info("Discovered %d folders", len(self._folder_map))
        except Exception as e:
            self.report.report_warning("folder_tree", f"拉取文件夹树失败: {e}")

        # 2. 数据集列表 + 详情
        try:
            datasets = self.client.list_datasets(need_sync_info=True)
        except Exception as e:
            self.report.report_failure("list_datasets", f"拉取数据集列表失败: {e}")
            return

        self.report.total_datasets = len(datasets)
        logger.info("Discovered %d datasets", len(datasets))

        ds_ids = [d.get("id") for d in datasets if d.get("id")]
        details = self.client.batch_fetch(ds_ids, self._fetch_dataset_detail)

        for ds in datasets:
            dsid = ds.get("id")
            if dsid is None:
                continue
            detail = details.get(dsid) or {}
            yield from self._process_dataset(ds, detail)

        # 3. 仪表盘 (Dashboard)
        try:
            dashboards = self.client.list_dashboards()
            self.report.total_dashboards = len(dashboards)
            logger.info("Discovered %d dashboards", len(dashboards))

            # 拉看板->图表映射
            dash_charts = self.client.dashboard_reports()
            # 拉图表->数据集映射
            chart_datasets = self.client.report_datasets()

            for dash in dashboards:
                yield from self._process_dashboard(dash, dash_charts, chart_datasets)
        except Exception as e:
            self.report.report_warning("dashboards", f"拉取看板/图表失败: {e}")

        logger.info(
            "DataWind ingestion done: %d datasets, %d dashboards, %d charts",
            self.report.total_datasets,
            self.report.total_dashboards,
            self.report.total_charts,
        )

    def _fetch_dataset_detail(self, dsid: int) -> dict:
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

    # ---------- Dataset ----------
    def _process_dataset(self, ds: dict, detail: dict) -> Iterable[MetadataWorkUnit]:
        dsid = ds.get("id")
        name = ds.get("name") or f"dataset_{dsid}"
        platform = "datawind"

        dataset_urn = builder.make_dataset_urn(platform, f"dataset_{dsid}", "PROD")
        self._dataset_urn_map[dsid] = dataset_urn

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        sql, upstreams_str, fields, data_source = detail.get("model", ("", "", [], None))
        if sql:
            self.report.datasets_with_sql += 1

        info = detail.get("info") or {}
        bc = info.get("baseConf") or {}
        folder_id = bc.get("parentId")
        folder_path = self._folder_map.get(folder_id, "")

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DatasetPropertiesClass(
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
            ),
        ).as_workunit()

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

        # 上游 Doris 表血缘
        if upstreams_str:
            upstreams = []
            for tbl in upstreams_str.split(","):
                tbl = tbl.strip()
                if not tbl or "." not in tbl:
                    continue
                db_name, table_name = tbl.split(".", 1)
                upstream_urn = builder.make_dataset_urn(
                    "doris", f"{db_name}.{table_name}", "PROD"
                )
                upstreams.append(UpstreamClass(dataset=upstream_urn, type="TRANSFORMED"))
            if upstreams:
                self.report.datasets_with_lineage += 1
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=UpstreamLineageClass(upstreams=upstreams),
                ).as_workunit()

    # ---------- Dashboard ----------
    def _process_dashboard(
        self,
        dash: dict,
        dash_charts: Dict[str, List[dict]],
        chart_datasets: Dict[str, List[dict]],
    ) -> Iterable[MetadataWorkUnit]:
        dash_id = dash.get("id")
        name = dash.get("name") or f"dashboard_{dash_id}"
        if dash_id is None:
            return

        dashboard_urn = builder.make_dashboard_urn("datawind", f"dashboard_{dash_id}")

        yield MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        # 看板包含的图表
        charts = dash_charts.get(str(dash_id)) or []
        self.report.total_charts += len(charts)
        if charts:
            self.report.dashboards_with_charts += 1

        chart_urns: List[str] = []
        # 看板直接关联的数据集 (通过图表间接关联)
        dashboard_dataset_urns: List[str] = []

        for chart in charts:
            chart_id = chart.get("id")
            chart_name = chart.get("name") or f"chart_{chart_id}"
            chart_urn = builder.make_chart_urn("datawind", f"chart_{chart_id}")
            chart_urns.append(chart_urn)

            # Chart 实体
            yield MetadataChangeProposalWrapper(
                entityUrn=chart_urn,
                aspect=StatusClass(removed=False),
            ).as_workunit()

            # 图表关联的数据集
            ds_list = chart_datasets.get(str(chart_id)) or []
            input_urns = []
            for ds_item in ds_list:
                ds_id = ds_item.get("id")
                if ds_id and ds_id in self._dataset_urn_map:
                    ds_urn = self._dataset_urn_map[ds_id]
                    input_urns.append(ds_urn)
                    if ds_urn not in dashboard_dataset_urns:
                        dashboard_dataset_urns.append(ds_urn)

            yield MetadataChangeProposalWrapper(
                entityUrn=chart_urn,
                aspect=ChartInfoClass(
                    name=chart_name,
                    title=chart_name,
                    description="",
                    lastModified=chart.get("mtime") or "",
                    externalUrl=chart.get("url") or "",
                    inputDatasets=input_urns,
                    inputs=[],
                    access="PUBLIC",
                ),
            ).as_workunit()

            chart_owner = chart.get("owner")
            if chart_owner:
                yield MetadataChangeProposalWrapper(
                    entityUrn=chart_urn,
                    aspect=OwnershipClass(
                        owners=[
                            OwnerClass(
                                owner=builder.make_user_urn(chart_owner.split("@")[0]),
                                type=OwnerTypeClass.DATAOWNER,
                            )
                        ]
                    ),
                ).as_workunit()

        # Dashboard 实体
        yield MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=DashboardInfoClass(
                name=name,
                description="",
                externalUrl=dash.get("url") or "",
                datasetUrns=dashboard_dataset_urns,
                charts=chart_urns,
                access="PUBLIC",
                lastModified=dash.get("mtime") or "",
            ),
        ).as_workunit()

        dash_owner = dash.get("owner")
        if dash_owner:
            yield MetadataChangeProposalWrapper(
                entityUrn=dashboard_urn,
                aspect=OwnershipClass(
                    owners=[
                        OwnerClass(
                            owner=builder.make_user_urn(dash_owner.split("@")[0]),
                            type=OwnerTypeClass.DATAOWNER,
                        )
                    ]
                ),
            ).as_workunit()
