import json
import logging
from typing import Iterable, List, Optional

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
from datahub.ingestion.source.dolphinscheduler.client import DSClient
from datahub.ingestion.source.dolphinscheduler.config import DolphinSchedulerConfig
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    StatusClass,
)
from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result

logger = logging.getLogger(__name__)


class DolphinSchedulerSourceReport(SourceReport):
    total_projects: int = 0
    total_workflows: int = 0
    total_jobs: int = 0
    jobs_with_sql_lineage: int = 0


@platform_name("DolphinScheduler")
@config_class(DolphinSchedulerConfig)
@support_status(SupportStatus.ALPHA)
class DolphinSchedulerSource(StatefulIngestionSourceBase):
    """DolphinScheduler 调度系统元数据连接器。

    采集:
      - DataFlow (工作流)
      - DataJob (任务节点)
      - SQL 任务的表级血缘 (输入表 -> 作业 -> 输出表)
    """

    def __init__(self, config: DolphinSchedulerConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = DolphinSchedulerSourceReport()
        self.client = DSClient(
            api_url=config.api_url,
            token=config.token.get_secret_value(),
            timeout=config.request_timeout_sec,
        )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext):
        config = DolphinSchedulerConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_platform_display_name(self) -> str:
        return "DolphinScheduler"

    def get_report(self) -> DolphinSchedulerSourceReport:
        return self.report

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        logger.info("Starting DolphinScheduler ingestion...")

        # 1. 项目列表
        try:
            projects = self.client.list_projects()
        except Exception as e:
            self.report.report_failure("list_projects", f"拉取项目列表失败: {e}")
            return

        # 过滤指定项目
        if self.config.project_codes:
            projects = [p for p in projects if p.get("code") in self.config.project_codes]

        self.report.total_projects = len(projects)
        logger.info("Discovered %d projects", len(projects))

        # 2. 遍历项目 -> 工作流 -> 任务
        for proj in projects:
            proj_code = proj.get("code")
            proj_name = proj.get("name") or str(proj_code)
            if proj_code is None:
                continue

            try:
                workflows = self.client.list_workflows(proj_code)
            except Exception as e:
                self.report.report_warning(
                    f"workflows_{proj_code}", f"拉取工作流失败: {e}"
                )
                continue

            self.report.total_workflows += len(workflows)
            logger.info(
                "Project %s: %d workflows", proj_name, len(workflows)
            )

            for wf in workflows:
                yield from self._process_workflow(proj_code, proj_name, wf)

        logger.info(
            "DolphinScheduler ingestion done: %d projects, %d workflows, %d jobs",
            self.report.total_projects,
            self.report.total_workflows,
            self.report.total_jobs,
        )

    def _process_workflow(
        self, project_code: int, project_name: str, wf: dict
    ) -> Iterable[MetadataWorkUnit]:
        wf_code = wf.get("code")
        wf_name = wf.get("name") or f"workflow_{wf_code}"
        if wf_code is None:
            return

        # DataFlow URN
        flow_urn = builder.make_data_flow_urn(
            orchestrator="dolphinscheduler",
            flow_id=f"{project_code}:{wf_code}",
            cluster="PROD",
        )

        # --- DataFlow Status ---
        yield MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        # --- DataFlow Info ---
        yield MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=DataFlowInfoClass(
                name=wf_name,
                description=wf.get("description") or "",
                project=project_name,
                customProperties={
                    "project_code": str(project_code),
                    "workflow_code": str(wf_code),
                    "release_state": str(wf.get("releaseState") or ""),
                    "schedule_state": str(wf.get("scheduleReleaseState") or ""),
                },
            ),
        ).as_workunit()

        # 3. 拉工作流详情 (任务节点)
        try:
            detail = self.client.get_workflow_detail(project_code, wf_code)
        except Exception as e:
            self.report.report_warning(
                f"workflow_{wf_code}", f"拉取工作流详情失败: {e}"
            )
            return

        task_list = detail.get("taskDefinitionList") or []
        self.report.total_jobs += len(task_list)

        # 收集作业输入输出表 (用于血缘)
        job_inputs: dict = {}  # job_code -> [dataset_urn]
        job_outputs: dict = {}

        for task in task_list:
            yield from self._process_task(
                flow_urn, project_code, wf_code, task, job_inputs, job_outputs
            )

    def _process_task(
        self,
        flow_urn: str,
        project_code: int,
        workflow_code: int,
        task: dict,
        job_inputs: dict,
        job_outputs: dict,
    ) -> Iterable[MetadataWorkUnit]:
        task_code = task.get("code")
        task_name = task.get("name") or f"task_{task_code}"
        task_type = task.get("taskType") or "UNKNOWN"
        if task_code is None:
            return

        # DataJob URN
        job_urn = builder.make_data_job_urn(
            orchestrator="dolphinscheduler",
            flow_id=f"{project_code}:{workflow_code}",
            job_id=str(task_code),
            cluster="PROD",
        )

        # --- DataJob Status ---
        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        # 解析 taskParams
        task_params = task.get("taskParams") or {}
        if isinstance(task_params, str):
            try:
                task_params = json.loads(task_params)
            except Exception:
                task_params = {}

        sql = task_params.get("sql") or ""
        # --- DataJob Info ---
        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInfoClass(
                name=task_name,
                type=task_type,
                description=task_params.get("description") or "",
                customProperties={
                    "task_code": str(task_code),
                    "task_type": task_type,
                },
            ),
        ).as_workunit()

        # --- SQL 血缘解析 (基于 DataHub SQLGlot) ---
        input_urns: List[str] = []
        output_urns: List[str] = []
        if sql and self.config.extract_task_sql_lineage:
            input_urns, output_urns = self._parse_sql_lineage(sql)
            if input_urns or output_urns:
                self.report.jobs_with_sql_lineage += 1

        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInputOutputClass(
                inputDatasets=input_urns,
                outputDatasets=output_urns,
                inputDatajobs=[],
            ),
        ).as_workunit()

    def _parse_sql_lineage(self, sql: str) -> tuple:
        """使用 DataHub 自带的 SQLGlot 解析器提取 SQL 表级血缘。

        返回 (input_urns, output_urns), 元素为 DataHub dataset URN 字符串。
        解析失败时返回空列表, 不中断采集。
        """
        try:
            result = create_lineage_sql_parsed_result(
                query=sql,
                default_db=None,
                platform="doris",
                platform_instance=None,
                env="PROD",
                override_dialect="mysql",  # Doris 兼容 MySQL 方言
                generate_column_lineage=False,
            )
            # in_tables / out_tables 已经是 URN 对象列表
            input_urns = [str(urn) for urn in result.in_tables]
            output_urns = [str(urn) for urn in result.out_tables]
            logger.debug(
                "SQL lineage parsed: %d inputs, %d outputs",
                len(input_urns), len(output_urns),
            )
            return input_urns, output_urns
        except Exception as e:
            logger.debug("SQL lineage parse failed: %s", e)
            return [], []
