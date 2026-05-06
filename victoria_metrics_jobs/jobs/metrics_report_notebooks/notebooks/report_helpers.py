"""Helpers for extractor self-report notebook."""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import requests
from sqlalchemy import create_engine, text

_helper_dir = Path(__file__).parent.resolve()
_project_root = None
_cwd = Path.cwd()
if (_cwd / "victoria_metrics_jobs").exists():
    _project_root = _cwd
else:
    _current = _helper_dir
    for _ in range(6):
        if (_current / "victoria_metrics_jobs").exists():
            _project_root = _current
            break
        _current = _current.parent
        if _current == _current.parent:
            break

if _project_root and str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from victoria_metrics_jobs.scheduler.config import ConfigLoader


@dataclass
class VmClient:
    query_url: str
    token: str = ""
    timeout: int = 30

    def _headers(self) -> Dict[str, str]:
        headers = {}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def query(self, query: str, ts: float | None = None) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"query": query}
        if ts is not None:
            params["time"] = ts
        response = requests.get(
            f"{self.query_url.rstrip('/')}/api/v1/query",
            params=params,
            headers=self._headers(),
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json().get("data", {}).get("result", [])

    def query_range(
        self, query: str, start_ts: float, end_ts: float, step_seconds: int
    ) -> List[Dict[str, Any]]:
        response = requests.get(
            f"{self.query_url.rstrip('/')}/api/v1/query_range",
            params={
                "query": query,
                "start": start_ts,
                "end": end_ts,
                "step": step_seconds,
            },
            headers=self._headers(),
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json().get("data", {}).get("result", [])


def _parse_strict_date(value: str, fmt: str) -> datetime | None:
    try:
        return datetime.strptime(value, fmt)
    except (TypeError, ValueError):
        return None


def _classify_biz_date_label(raw_biz_date: str) -> Dict[str, Any]:
    ddmm_dt = _parse_strict_date(raw_biz_date, "%d/%m/%Y")
    mmdd_dt = _parse_strict_date(raw_biz_date, "%m/%d/%Y")

    if ddmm_dt and mmdd_dt:
        # Ambiguous values are treated as expected format as requested.
        return {
            "biz_date_raw": raw_biz_date,
            "biz_date": raw_biz_date,
            "biz_date_format": "ddmm_ambiguous",
            "biz_date_normalized": ddmm_dt.date().isoformat(),
            "is_wrong_format": False,
        }
    if ddmm_dt:
        return {
            "biz_date_raw": raw_biz_date,
            "biz_date": raw_biz_date,
            "biz_date_format": "ddmm",
            "biz_date_normalized": ddmm_dt.date().isoformat(),
            "is_wrong_format": False,
        }
    if mmdd_dt:
        return {
            "biz_date_raw": raw_biz_date,
            "biz_date": raw_biz_date,
            "biz_date_format": "mmdd",
            "biz_date_normalized": mmdd_dt.date().isoformat(),
            "is_wrong_format": True,
        }
    return {
        "biz_date_raw": raw_biz_date,
        "biz_date": raw_biz_date,
        "biz_date_format": "invalid",
        "biz_date_normalized": "",
        "is_wrong_format": False,
    }


def compute_report_rows(
    vm_query_url: str,
    vm_token: str,
    extractor_job_ids: List[str],
    expected_metric_name: str,
    business_date: str,
    lookback_days: int,
) -> List[Dict[str, Any]]:
    client = VmClient(vm_query_url, vm_token)
    end_dt = datetime.strptime(business_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    start_dt = end_dt - timedelta(days=max(1, lookback_days - 1))

    # For long lookbacks, 1-minute resolution can produce large responses and VM may reject.
    # We widen the step/window while keeping the same "attempt" clustering semantics.
    if lookback_days >= 21:
        step_seconds = 300  # 5 minutes
        window = "5m"
    else:
        step_seconds = 60  # 1 minute
        window = "1m"

    all_rows: List[Dict[str, Any]] = []
    for job_id in extractor_job_ids:
        expected = _read_expected_value(client, expected_metric_name, job_id)
        all_rows.extend(
            _read_actual_rows_for_job(
                client=client,
                job_id=job_id,
                expected_value=expected,
                start_ts=start_dt.timestamp(),
                end_ts=(end_dt + timedelta(hours=23, minutes=59)).timestamp(),
                step_seconds=step_seconds,
                window=window,
            )
        )
    return sorted(all_rows, key=lambda item: (item["job_id"], item["biz_date"], item["attempt"]))


def _read_expected_value(client: VmClient, expected_metric_name: str, job_id: str) -> float:
    query = f'{expected_metric_name}{{job="{job_id}"}}'
    result = client.query(query)
    if not result:
        return 0.0
    value = result[0].get("value", [0, 0])[1]
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _read_actual_rows_for_job(
    client: VmClient,
    job_id: str,
    expected_value: float,
    start_ts: float,
    end_ts: float,
    step_seconds: int,
    window: str,
) -> List[Dict[str, Any]]:
    # Build per-step counts grouped by biz_date; contiguous non-zero steps represent attempts.
    # For long windows we use a larger step/window to avoid VM errors due to max points/series.
    query = f'sum by (biz_date) (count_over_time({{job="{job_id}",biz_date!=""}}[{window}]))'
    try:
        series = client.query_range(
            query, start_ts=start_ts, end_ts=end_ts, step_seconds=step_seconds
        )
    except requests.RequestException:
        # Fallback: daily total per biz_date (attempt information lost; attempt=1)
        daily_query = f'sum by (biz_date) (count_over_time({{job="{job_id}",biz_date!=""}}[1d]))'
        series = client.query_range(
            daily_query, start_ts=start_ts, end_ts=end_ts, step_seconds=86400
        )
    rows: List[Dict[str, Any]] = []

    for item in series:
        raw_biz_date = item.get("metric", {}).get("biz_date", "")
        if not raw_biz_date:
            continue
        biz_date_meta = _classify_biz_date_label(raw_biz_date)
        values = item.get("values", [])
        # If this is daily fallback (step 1d), force attempt=1 with total sum.
        if step_seconds >= 86400:
            total = 0.0
            for _, value_raw in values:
                try:
                    total += float(value_raw)
                except (TypeError, ValueError):
                    continue
            rows.append(
                {
                    "job_id": job_id,
                    "biz_date": biz_date_meta["biz_date"],
                    "attempt": 1,
                    "actual_count": total,
                    "expected_count": expected_value,
                    "biz_date_raw": biz_date_meta["biz_date_raw"],
                    "biz_date_format": biz_date_meta["biz_date_format"],
                    "biz_date_normalized": biz_date_meta["biz_date_normalized"],
                    "is_wrong_format": biz_date_meta["is_wrong_format"],
                }
            )
        else:
            rows.extend(
                _attempt_rows_for_series(
                    job_id=job_id,
                    biz_date_meta=biz_date_meta,
                    expected_value=expected_value,
                    values=values,
                )
            )
    return rows


def _attempt_rows_for_series(
    job_id: str, biz_date_meta: Dict[str, Any], expected_value: float, values: List[List[Any]]
) -> List[Dict[str, Any]]:
    attempt = 0
    in_attempt = False
    current_sum = 0.0
    attempt_rows: List[Dict[str, Any]] = []

    for _, value_raw in values:
        try:
            value = float(value_raw)
        except (TypeError, ValueError):
            value = 0.0
        if value > 0:
            if not in_attempt:
                attempt += 1
                in_attempt = True
                current_sum = 0.0
            current_sum += value
        elif in_attempt:
            attempt_rows.append(
                {
                    "job_id": job_id,
                    "biz_date": biz_date_meta["biz_date"],
                    "attempt": attempt,
                    "actual_count": current_sum,
                    "expected_count": expected_value,
                    "biz_date_raw": biz_date_meta["biz_date_raw"],
                    "biz_date_format": biz_date_meta["biz_date_format"],
                    "biz_date_normalized": biz_date_meta["biz_date_normalized"],
                    "is_wrong_format": biz_date_meta["is_wrong_format"],
                }
            )
            in_attempt = False
            current_sum = 0.0

    if in_attempt:
        attempt_rows.append(
            {
                "job_id": job_id,
                "biz_date": biz_date_meta["biz_date"],
                "attempt": attempt,
                "actual_count": current_sum,
                "expected_count": expected_value,
                "biz_date_raw": biz_date_meta["biz_date_raw"],
                "biz_date_format": biz_date_meta["biz_date_format"],
                "biz_date_normalized": biz_date_meta["biz_date_normalized"],
                "is_wrong_format": biz_date_meta["is_wrong_format"],
            }
        )
    return attempt_rows


def _load_database_config(config_path: str, environment: str) -> Dict[str, Any]:
    if not config_path:
        raise ValueError("config_path is required to load database config")
    if not environment:
        raise ValueError("environment is required to load database config")
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    config_loader = ConfigLoader()
    env_config = config_loader.load(config_path, environment=environment)
    database_config = env_config.get("database", {})
    if not database_config:
        raise ValueError(
            f"No 'database' section found in environment '{environment}' configuration"
        )

    password = os.getenv("VM_JOBS_DB_PASSWORD")
    if not password:
        raise ValueError("VM_JOBS_DB_PASSWORD environment variable must be set")

    return {
        "host": database_config.get("host", "localhost"),
        "port": int(database_config.get("port", 5432)),
        "name": database_config.get("name", "scheduler_local"),
        "user": database_config.get("user", "scheduler"),
        "password": password,
        "ssl_mode": database_config.get("ssl_mode", "prefer"),
    }


def get_latest_exported_biz_dates(
    extractor_job_ids: List[str], config_path: str, environment: str
) -> Dict[str, str]:
    """Return latest exported biz_date per extractor job from PostgreSQL."""
    if not extractor_job_ids:
        return {}

    db_cfg = _load_database_config(config_path=config_path, environment=environment)
    conn_str = (
        f"postgresql://{db_cfg['user']}:{db_cfg['password']}@{db_cfg['host']}:"
        f"{db_cfg['port']}/{db_cfg['name']}?sslmode={db_cfg['ssl_mode']}"
    )

    query = text(
        """
        SELECT job_id, MAX(biz_date) AS latest_biz_date
        FROM public.vm_extraction_job
        WHERE job_id = ANY(:job_ids)
        GROUP BY job_id
        """
    )

    latest_dates: Dict[str, str] = {}
    engine = create_engine(conn_str)
    with engine.connect() as conn:
        rows = conn.execute(query, {"job_ids": extractor_job_ids}).fetchall()
        for row in rows:
            latest = row[1]
            if latest is not None:
                latest_dates[str(row[0])] = latest.isoformat()
    engine.dispose()
    return latest_dates

