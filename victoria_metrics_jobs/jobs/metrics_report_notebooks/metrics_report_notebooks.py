#!/usr/bin/env python3
"""
Metrics Report Notebooks Job - Executes Jupyter notebooks for self-reporting.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from victoria_metrics_jobs.jobs.common import BaseJob, BaseJobState, Err, Ok, Result
from victoria_metrics_jobs.scheduler.notebooks_file_manager import NotebooksFileManager


@dataclass
class MetricsReportNotebooksState(BaseJobState):
    notebooks_dir: Path = Path("notebooks")
    notebooks_output_dir: Path = Path("/var/lib/scheduler/notebooks_output")
    notebooks_found: List[str] = field(default_factory=list)
    notebooks_executed: int = 0
    notebooks_succeeded: int = 0
    notebooks_failed: int = 0
    reports_generated: int = 0
    rows_total: int = 0
    rows_expected_format: int = 0
    rows_wrong_format_mmdd: int = 0
    rows_ambiguous_ddmm: int = 0
    rows_invalid_biz_date: int = 0
    wrong_format_distinct_dates: int = 0
    wrong_format_dates_existing_in_expected: int = 0
    current_business_date: Optional[date] = None
    vm_query_url: str = ""
    vm_token: str = ""
    config_path: str = ""
    extractor_job_ids: List[str] = field(default_factory=list)

    def to_results(self) -> Dict[str, Any]:
        results = super().to_results()
        results.update(
            {
                "notebooks_found": len(self.notebooks_found),
                "notebooks_executed": self.notebooks_executed,
                "notebooks_succeeded": self.notebooks_succeeded,
                "notebooks_failed": self.notebooks_failed,
                "reports_generated": self.reports_generated,
                "rows_total": self.rows_total,
                "rows_expected_format": self.rows_expected_format,
                "rows_wrong_format_mmdd": self.rows_wrong_format_mmdd,
                "rows_ambiguous_ddmm": self.rows_ambiguous_ddmm,
                "rows_invalid_biz_date": self.rows_invalid_biz_date,
                "wrong_format_distinct_dates": self.wrong_format_distinct_dates,
                "wrong_format_dates_existing_in_expected": self.wrong_format_dates_existing_in_expected,
                "extractor_jobs_count": len(self.extractor_job_ids),
                "current_business_date": self.current_business_date.isoformat()
                if self.current_business_date
                else None,
            }
        )
        return results


class MetricsReportNotebooksJob(BaseJob):
    def __init__(self, config_path: str = None, verbose: bool = False):
        super().__init__("metrics_report_notebooks", config_path, verbose)

    def create_initial_state(
        self, job_id: str
    ) -> Result[MetricsReportNotebooksState, Exception]:
        try:
            job_config = self.get_job_config(job_id)
            notebooks_dir = job_config.get("notebooks_directory", "notebooks")
            notebooks_dir_path = Path(notebooks_dir)
            if not notebooks_dir_path.is_absolute():
                notebooks_dir_path = Path(__file__).parent / notebooks_dir_path

            notebooks_output_dir = job_config.get(
                "notebooks_output_directory", "/var/lib/scheduler/notebooks_output"
            )
            notebooks_output_dir_path = Path(notebooks_output_dir)

            victoria_metrics_cfg = job_config.get("victoria_metrics", {})
            config_path = getattr(self, "config_path", None) or ""
            extractor_job_ids = self._resolve_extractor_jobs(job_config)

            state = MetricsReportNotebooksState(
                job_id=job_id,
                job_config=job_config,
                started_at=datetime.now(),
                notebooks_dir=notebooks_dir_path,
                notebooks_output_dir=notebooks_output_dir_path,
                vm_query_url=victoria_metrics_cfg.get("query_url", ""),
                vm_token=victoria_metrics_cfg.get("token", ""),
                config_path=config_path,
                extractor_job_ids=extractor_job_ids,
            )
            return Ok(state)
        except Exception as exc:
            return Err(exc)

    def get_workflow_steps(self) -> List[Callable]:
        return [
            self._derive_current_business_date,
            self._discover_notebooks,
            self._execute_notebooks,
        ]

    def finalize_state(
        self, state: MetricsReportNotebooksState
    ) -> MetricsReportNotebooksState:
        state.completed_at = datetime.now()
        if state.notebooks_failed > 0 and state.notebooks_succeeded == 0:
            state.status = "error"
            state.message = "All report notebooks failed"
        elif state.notebooks_failed > 0:
            state.status = "partial_success"
            state.message = (
                f"Report notebooks completed with warnings: "
                f"{state.notebooks_succeeded} succeeded, {state.notebooks_failed} failed"
            )
        elif state.notebooks_executed == 0:
            state.status = "success"
            state.message = "No report notebooks found to execute"
        else:
            state.status = "success"
            state.message = f"Successfully executed {state.notebooks_succeeded} report notebook(s)"
        return state

    def _resolve_extractor_jobs(self, job_config: Dict[str, Any]) -> List[str]:
        env_name = job_config.get("environment", os.getenv("VM_JOBS_ENVIRONMENT", ""))
        configured = self.config_manager.config.get("environments", {}).get(env_name, {}).get("jobs", {})
        extractor_ids: List[str] = []
        if isinstance(configured, dict):
            for configured_job_id, cfg in configured.items():
                if cfg.get("enabled", True) and cfg.get("job_type") == "extractor":
                    extractor_ids.append(configured_job_id)
        return sorted(extractor_ids)

    def _derive_current_business_date(
        self, state: MetricsReportNotebooksState
    ) -> Result[MetricsReportNotebooksState, Exception]:
        try:
            cutoff_hour = int(state.job_config.get("cutoff_hour", 6))
            now = datetime.utcnow()
            if now.weekday() >= 5 or now.hour < cutoff_hour:
                days_back = 1
                if now.weekday() == 6:
                    days_back = 2
                elif now.hour < cutoff_hour and now.weekday() == 0:
                    days_back = 3
                state.current_business_date = (now - timedelta(days=days_back)).date()
            else:
                state.current_business_date = now.date()
            return Ok(state)
        except Exception as exc:
            return Err(exc)

    def _discover_notebooks(
        self, state: MetricsReportNotebooksState
    ) -> Result[MetricsReportNotebooksState, Exception]:
        try:
            if not state.notebooks_dir.exists():
                return Ok(state)
            notebooks = [
                str(nb.relative_to(state.notebooks_dir))
                for nb in state.notebooks_dir.glob("*.ipynb")
                if not nb.name.startswith(".") and not nb.name.startswith("_")
            ]
            state.notebooks_found = sorted(notebooks)
            return Ok(state)
        except Exception as exc:
            return Err(exc)

    def _execute_notebooks(
        self, state: MetricsReportNotebooksState
    ) -> Result[MetricsReportNotebooksState, Exception]:
        try:
            if not state.notebooks_found:
                return Ok(state)
            business_date_str = state.current_business_date.isoformat()
            job_output_dir = state.notebooks_output_dir / state.job_id
            notebooks_manager = NotebooksFileManager(
                notebooks_dir=str(job_output_dir), archive_dir=None, enable_archive=False
            )
            partition_dir = notebooks_manager._get_partition_path(business_date_str)

            for notebook_rel_path in state.notebooks_found:
                notebook_path = state.notebooks_dir / notebook_rel_path
                notebook_name = notebook_path.stem
                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                output_path = partition_dir / f"{notebook_name}_{timestamp}.ipynb"
                html_output_path = partition_dir / f"{notebook_name}_{timestamp}.html"
                output_results_path = partition_dir / f"{notebook_name}_{timestamp}_results.json"

                result = self._execute_with_papermill(
                    notebook_path, output_path, state, output_results_path
                )
                state.notebooks_executed += 1
                if result["success"]:
                    self._convert_to_html(output_path, html_output_path)
                    state.notebooks_succeeded += 1
                    state.reports_generated += 1
                    self._accumulate_notebook_results(state, output_results_path)
                else:
                    state.notebooks_failed += 1
            return Ok(state)
        except Exception as exc:
            return Err(exc)

    def _execute_with_papermill(
        self,
        input_path: Path,
        output_path: Path,
        state: MetricsReportNotebooksState,
        output_results_path: Path,
    ) -> Dict[str, Any]:
        try:
            import papermill as pm

            notebook_parameters = {
                "vm_query_url": state.vm_query_url,
                "vm_token": state.vm_token,
                "vm_jobs_notebook_env": os.getenv("VM_JOBS_NOTEBOOK_ENV", os.getenv("VM_JOBS_ENVIRONMENT", "")),
                "vm_jobs_config_path": state.config_path,
                "extractor_job_ids": state.extractor_job_ids,
                "expected_metric_name": state.job_config.get(
                    "expected_metric_name", "expected_number_of_metrics_count"
                ),
                "business_date": state.current_business_date.isoformat()
                if state.current_business_date
                else "",
                "lookback_days": int(state.job_config.get("lookback_days", 14)),
                "output_results_path": str(output_results_path),
            }

            notebooks_dir = input_path.parent
            original_cwd = os.getcwd()
            original_mplbackend = os.environ.get("MPLBACKEND")
            try:
                os.chdir(str(notebooks_dir))
                # Match the forecast-notebooks behavior: ensure an inline backend so plots
                # are captured into the executed notebook, then exported into HTML.
                os.environ["MPLBACKEND"] = "module://matplotlib_inline.backend_inline"
                pm.execute_notebook(
                    input_path=str(input_path),
                    output_path=str(output_path),
                    parameters=notebook_parameters,
                    kernel_name="python3",
                    log_output=True,
                    start_timeout=300,
                )
            finally:
                os.chdir(original_cwd)
                if original_mplbackend is None:
                    os.environ.pop("MPLBACKEND", None)
                else:
                    os.environ["MPLBACKEND"] = original_mplbackend

            return {"success": True}
        except Exception as exc:
            self.logger.error("Report notebook execution failed: %s", exc)
            return {"success": False, "error": str(exc)}

    def _convert_to_html(self, notebook_path: Path, html_path: Path) -> None:
        try:
            from nbconvert import HTMLExporter
            import nbformat

            with open(notebook_path, "r", encoding="utf-8") as file_obj:
                notebook_content = nbformat.read(file_obj, as_version=4)
            html_exporter = HTMLExporter(template_name="classic")
            body, _ = html_exporter.from_notebook_node(notebook_content)
            with open(html_path, "w", encoding="utf-8") as file_obj:
                file_obj.write(body)
        except Exception as exc:
            self.logger.warning("Failed to convert notebook to HTML: %s", exc)

    def _accumulate_notebook_results(
        self, state: MetricsReportNotebooksState, output_results_path: Path
    ) -> None:
        if not output_results_path.exists():
            return
        try:
            with open(output_results_path, "r", encoding="utf-8") as file_obj:
                data = json.load(file_obj)
            state.rows_total += int(data.get("rows", 0) or 0)
            state.rows_expected_format += int(data.get("rows_expected_format", 0) or 0)
            state.rows_wrong_format_mmdd += int(data.get("rows_wrong_format_mmdd", 0) or 0)
            state.rows_ambiguous_ddmm += int(data.get("rows_ambiguous_ddmm", 0) or 0)
            state.rows_invalid_biz_date += int(data.get("rows_invalid_biz_date", 0) or 0)
            state.wrong_format_distinct_dates += int(
                data.get("wrong_format_distinct_dates", 0) or 0
            )
            state.wrong_format_dates_existing_in_expected += int(
                data.get("wrong_format_dates_existing_in_expected", 0) or 0
            )
        except Exception as exc:
            self.logger.debug(
                "Could not read notebook results file %s: %s",
                output_results_path,
                exc,
            )


def main():
    epilog = """
Examples:
  python -m victoria_metrics_jobs.jobs.metrics_report_notebooks --config victoria_metrics_jobs/victoria_metrics_jobs.yml --list-jobs
  python -m victoria_metrics_jobs.jobs.metrics_report_notebooks --config victoria_metrics_jobs/victoria_metrics_jobs.yml --job-id metrics_report_notebooks
    """
    return MetricsReportNotebooksJob.main(
        description="Metrics Report Notebooks Job - Executes Jupyter notebooks for self reporting",
        epilog=epilog,
    )


if __name__ == "__main__":
    sys.exit(main())

