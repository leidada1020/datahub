"""DolphinScheduler REST API 客户端 (只读)。

移植自 datawind-asset-center 项目。
认证: 请求头 token=<API 令牌>
已验证接口 (DS 3.x):
  GET /projects                              项目列表
  GET /projects/{code}/process-definition    工作流定义列表
  GET /projects/{code}/process-definition/{code}  工作流详情
  GET /projects/{code}/process-instances     工作流实例
"""
import json
import logging
import time
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class DSClient:
    def __init__(self, api_url: str, token: str, timeout: float = 20.0, retries: int = 3):
        self.base_url = api_url.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.retries = retries

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        url = self.base_url + path
        if params:
            url += "?" + urllib.parse.urlencode(params)
        last_err: Optional[Exception] = None
        for i in range(self.retries):
            try:
                req = urllib.request.Request(
                    url, headers={"token": self.token}
                )
                with urllib.request.urlopen(req, timeout=self.timeout) as r:
                    data = json.loads(r.read().decode("utf-8"))
                if data.get("code") != 0:
                    raise RuntimeError(
                        f"DS API code={data.get('code')} msg={data.get('msg')} path={path}"
                    )
                return data.get("data") or {}
            except Exception as e:
                last_err = e
                logger.debug("DS request failed (attempt %d): %s", i + 1, e)
                time.sleep(2 * (i + 1))
        raise last_err  # type: ignore

    def _paged(self, path: str, page_size: int = 100) -> List[dict]:
        out: List[dict] = []
        page = 1
        while True:
            d = self._get(path, {"pageNo": page, "pageSize": page_size})
            items = d.get("totalList") or []
            out.extend(items)
            total_page = d.get("totalPage") or 1
            if not items or page >= total_page:
                break
            page += 1
        return out

    def list_projects(self) -> List[dict]:
        return self._paged("/projects")

    def list_workflows(self, project_code: int) -> List[dict]:
        return self._paged(f"/projects/{project_code}/process-definition")

    def get_workflow_detail(self, project_code: int, workflow_code: int) -> dict:
        return self._get(
            f"/projects/{project_code}/process-definition/{workflow_code}"
        )
