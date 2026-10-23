"""DataWind OpenAPI 只读客户端。

移植自 datawind-asset-center 项目, 仅保留只读接口:
  - exchange_jwt    : 凭证换 JWT (带缓存, 1h 有效)
  - list_datasets   : dataSetList (列表 + 同步信息)
  - folder_tree     : dataSetFolderTreeV2 (文件夹树)
  - dataset_info    : /dataset/{id}/info
  - model_logic     : getDataModelInfo (加工 SQL + 来源表 + 字段)
  - dim_met         : dimMet (维度/指标)
仅依赖标准库 urllib, 无第三方依赖。
"""
import json
import logging
import os
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

from pydantic import SecretStr

logger = logging.getLogger(__name__)


class DataWindClient:
    def __init__(self, config):
        self.cfg = config
        self.base = config.api_base.rstrip("/")
        self.region = config.region
        self.app_id = config.app_id
        self.timeout = config.request_timeout_sec
        self.max_workers = config.max_workers
        self._jwt: Optional[str] = None
        self._jwt_expire_at: float = 0.0

    # ---------- HTTP 基础 ----------
    def _headers(self, with_auth: bool = True) -> Dict[str, str]:
        h = {
            "x-vedi-region": self.region,
            "x-request-id": os.urandom(8).hex(),
            "Content-Type": "application/json; charset=utf-8",
        }
        if with_auth:
            h["Authorization"] = "Bearer " + self.jwt()
        return h

    def _http_json(
        self,
        path: str,
        method: str = "GET",
        body: Optional[dict] = None,
        timeout: Optional[float] = None,
        with_auth: bool = True,
    ) -> dict:
        url = self.base + path
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        for k, v in self._headers(with_auth).items():
            req.add_header(k, v)
        with urllib.request.urlopen(req, timeout=timeout or self.timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))

    # ---------- JWT 认证 ----------
    def jwt(self, force: bool = False) -> str:
        now = time.time()
        if self._jwt and not force and self._jwt_expire_at - 300 > now:
            return self._jwt
        body = {
            "metadata": {
                "clientId": self.cfg.client_id,
                "clientSecret": self.cfg.client_secret.get_secret_value(),
                "proxyUser": self.cfg.proxy_user or "",
                "expire": 3600,
            }
        }
        r = self._http_json(
            "/api/v3/openapi/jwtToken", "POST", body, timeout=60, with_auth=False
        )
        if r.get("code") != "aeolus/ok":
            raise RuntimeError(f"换取 JWT 失败: {r.get('code')} {r.get('msg')}")
        self._jwt = r["data"]["jwtToken"]
        self._jwt_expire_at = now + 3600
        logger.debug("DataWind JWT refreshed, expires in 3600s")
        return self._jwt

    # ---------- 数据集 ----------
    def list_datasets(self, need_sync_info: bool = True) -> List[dict]:
        r = self._http_json(
            "/api/v3/open/metadata/dataSetList",
            "POST",
            {"appId": self.app_id, "needSyncInfo": need_sync_info},
            timeout=300,
        )
        if r.get("code") not in (None, "aeolus/ok"):
            raise RuntimeError(f"拉取数据集列表失败: {r.get('code')} {r.get('msg')}")
        return r.get("data") or []

    # ---------- 文件夹树 ----------
    def folder_tree(self) -> Dict[int, str]:
        r = self._http_json(
            f"/api/v4/open/app/{self.app_id}/dataSetFolderTreeV2", timeout=120
        )
        data = r.get("data") or {}
        nodes: Dict[int, str] = {}

        def walk(items, path):
            for it in items or []:
                fid = it.get("id")
                name = it.get("name") or ""
                nodes[fid] = "/".join(path + [name])
                walk(it.get("childList"), path + [name])

        walk(data.get("public"), [])
        walk(data.get("private"), [])
        return nodes

    # ---------- 单数据集详情 ----------
    def dataset_info(self, dsid: int) -> dict:
        r = self._http_json(f"/api/v4/open/dataset/{dsid}/info", timeout=120)
        return r.get("data") or {}

    def model_logic(self, dsid: int) -> Tuple[str, str, List[dict], Optional[dict]]:
        """返回 (sql, upstream_tables_csv, fields_list, data_source)"""
        r = self._http_json(
            f"/api/v3/open/openDataFactory/getDataModelInfo?dataSetId={dsid}",
            timeout=120,
        )
        d = r.get("data") or {}
        nc = d.get("nodeConf") or []
        if not isinstance(nc, list):
            nc = [nc]
        sql_parts: List[str] = []
        src_set = set()
        fields: List[dict] = []
        data_source: Optional[dict] = None
        for node in nc:
            if not isinstance(node, dict):
                continue
            if data_source is None:
                ds_type = node.get("dataSourceType") or ""
                if ds_type:
                    data_source = {
                        "type": ds_type,
                        "id": node.get("dataSourceId"),
                        "cluster": node.get("clusterName") or "",
                        "db": node.get("displayDbName") or node.get("dbName") or "",
                    }
            q = node.get("query")
            if q:
                sql_parts.append(str(q))
            for st in node.get("sourceTableList") or []:
                db = (st.get("dbName") or "").strip()
                tb = (st.get("tableName") or "").strip()
                if db and tb:
                    src_set.add(f"{db}.{tb}")
            for f in node.get("fields") or []:
                if isinstance(f, dict):
                    fields.append(
                        {
                            "name": f.get("name"),
                            "business": f.get("dsColumnNames"),
                            "type": f.get("prepType") or f.get("type"),
                        }
                    )
        return (
            "\n;\n".join(sql_parts),
            ", ".join(sorted(src_set)),
            fields,
            data_source,
        )

    # ---------- 维度指标 ----------
    def dim_met(self, dsid: int) -> List[dict]:
        try:
            r = self._http_json(f"/api/v4/open/dataset/{dsid}/dimMet", timeout=60)
        except Exception:
            return []
        data = r.get("data") or {}
        dims = data.get("dimMetList") or []
        return dims if isinstance(dims, list) else []

    # ---------- 并发批量 ----------
    def batch_fetch(self, ids: List[int], fetch_fn) -> Dict[int, Any]:
        out: Dict[int, Any] = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            futs = {ex.submit(fetch_fn, i): i for i in ids}
            for fu in as_completed(futs):
                i = futs[fu]
                try:
                    out[i] = fu.result()
                except Exception as e:
                    logger.warning("fetch failed for dataset %s: %s", i, e)
                    out[i] = None
        return out
