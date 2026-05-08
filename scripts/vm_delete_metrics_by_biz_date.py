#!/usr/bin/env python3
"""
Maintenance script: list and delete Victoria Metrics series for a given job
filtered by the biz_date label (dd/mm/yyyy) using a comparison operator.

The script lists exact series via /api/v1/series, filters them by parsing the
biz_date label, and then issues one delete per matched series via
/api/v1/admin/tsdb/delete_series with a fully-qualified match[] containing
every label of that series.

Dry-run is the default; --apply is required to actually delete.
"""

from __future__ import annotations

import argparse
import logging
import operator
import os
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional, Tuple

import requests


OPS: Dict[str, Callable[[date, date], bool]] = {
    "gt": operator.gt,
    "lt": operator.lt,
    "ge": operator.ge,
    "le": operator.le,
    "eq": operator.eq,
}


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="vm_delete_metrics_by_biz_date.py",
        description=(
            "Delete Victoria Metrics series for a job, filtered by the "
            "biz_date label (dd/mm/yyyy) using a comparison operator. "
            "Defaults to dry-run."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  # Preview series with biz_date older than 01/05/2025\n"
            "  vm_delete_metrics_by_biz_date.py --job apex_collector --op lt --biz-date 01/05/2025\n\n"
            "  # Actually delete them (interactive confirmation)\n"
            "  vm_delete_metrics_by_biz_date.py --job apex_collector --op lt --biz-date 01/05/2025 --apply\n\n"
            "  # Apply non-interactively\n"
            "  vm_delete_metrics_by_biz_date.py --job apex_collector --op lt --biz-date 01/05/2025 --apply --yes\n"
        ),
    )

    parser.add_argument("--job", required=True, help="Job label value to filter by (job=<JOB>).")
    parser.add_argument(
        "--op",
        required=True,
        choices=sorted(OPS.keys()),
        help="Comparison operator applied to biz_date: gt, lt, ge, le, eq.",
    )
    parser.add_argument(
        "--biz-date",
        required=True,
        dest="biz_date",
        help="Threshold biz_date in dd/mm/yyyy format.",
    )
    parser.add_argument(
        "--vm-url",
        default=os.environ.get("VM_JOBS_VM_QUERY_URL", ""),
        help="Victoria Metrics base URL (defaults to $VM_JOBS_VM_QUERY_URL).",
    )
    parser.add_argument(
        "--vm-token",
        default=os.environ.get("VM_JOBS_VM_TOKEN", ""),
        help="Bearer token for VM auth (defaults to $VM_JOBS_VM_TOKEN).",
    )
    parser.add_argument(
        "--metric",
        default="",
        help="Optional metric name filter (e.g. my_metric); default no filter.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=30,
        dest="lookback_days",
        help=(
            "How far back the /api/v1/series listing window extends, in days. "
            "Defaults to 30 ('last month' of submitted samples)."
        ),
    )

    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        default=True,
        help="Only print what would be deleted (default).",
    )
    mode.add_argument(
        "--apply",
        dest="dry_run",
        action="store_false",
        help="Actually delete the matched series.",
    )

    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip the interactive y/N confirmation when running with --apply.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging.")

    args = parser.parse_args(argv)

    if not args.vm_url:
        parser.error("--vm-url is required (or set $VM_JOBS_VM_QUERY_URL)")

    try:
        args.threshold_date = datetime.strptime(args.biz_date, "%d/%m/%Y").date()
    except ValueError:
        parser.error(f"--biz-date must be in dd/mm/yyyy format, got: {args.biz_date}")

    return args


def normalize_base_url(url: str) -> str:
    base = url.rstrip("/")
    if base.endswith("/api/v1"):
        base = base[: -len("/api/v1")]
    elif base.endswith("/api"):
        base = base[: -len("/api")]
    return base


def build_session(vm_token: str) -> requests.Session:
    session = requests.Session()
    if vm_token:
        session.headers["Authorization"] = f"Bearer {vm_token}"
    return session


def list_series(
    session: requests.Session,
    base_url: str,
    job: str,
    metric_name: str,
    lookback_days: int,
    logger: logging.Logger,
) -> List[Dict[str, str]]:
    """List every series for {job=...} that had a sample submitted in the lookback window.

    Intentionally does NOT filter by biz_date label: all biz_date comparison is
    done locally by the caller after this function returns.
    """
    selector = f'{{job="{job}"}}'
    if metric_name:
        selector = f"{metric_name}{selector}"

    end_ts = int(datetime.now(timezone.utc).timestamp())
    start_ts = int((datetime.now(timezone.utc) - timedelta(days=lookback_days)).timestamp())

    params = {"match[]": selector, "start": str(start_ts), "end": str(end_ts)}
    logger.debug("GET %s/api/v1/series params=%s", base_url, params)

    resp = session.get(f"{base_url}/api/v1/series", params=params, timeout=60)
    resp.raise_for_status()

    payload = resp.json()
    if payload.get("status") != "success":
        raise RuntimeError(f"VM /api/v1/series returned non-success: {payload}")

    return payload.get("data", []) or []


def safe_parse_biz_date(raw: Optional[str]) -> Optional[date]:
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%d/%m/%Y").date()
    except ValueError:
        return None


def filter_series_by_biz_date(
    series_list: List[Dict[str, str]],
    op_name: str,
    threshold: date,
    logger: logging.Logger,
) -> Tuple[List[Dict[str, str]], int, int]:
    """Compare biz_date label locally for every series in series_list.

    Returns:
        (matched, skipped_no_biz_date, skipped_unparseable)
        - matched: series whose biz_date satisfies `op` against `threshold`.
        - skipped_no_biz_date: series with no biz_date label at all (expected
          for jobs that emit non-bd-aware metrics; logged at debug).
        - skipped_unparseable: series with a biz_date label that did not parse
          as dd/mm/yyyy (logged at warning since that's a data quality signal).
    """
    cmp = OPS[op_name]
    matched: List[Dict[str, str]] = []
    skipped_no_biz_date = 0
    skipped_unparseable = 0
    for labels in series_list:
        raw = labels.get("biz_date")
        if raw is None or raw == "":
            skipped_no_biz_date += 1
            logger.debug("Skipping series with no biz_date label: %s", labels)
            continue
        bd = safe_parse_biz_date(raw)
        if bd is None:
            skipped_unparseable += 1
            logger.warning(
                "Skipping series with unparseable biz_date %r: %s", raw, labels
            )
            continue
        if cmp(bd, threshold):
            matched.append(labels)
    return matched, skipped_no_biz_date, skipped_unparseable


def labels_to_match_selector(labels: Dict[str, str]) -> str:
    """Build a fully-qualified match[] string from a series' label set."""
    name = labels.get("__name__", "")
    parts: List[str] = []
    for key in sorted(labels):
        if key == "__name__":
            continue
        value = labels[key]
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        parts.append(f'{key}="{escaped}"')
    return f'{name}{{{",".join(parts)}}}'


def format_series_line(labels: Dict[str, str]) -> str:
    """Human-readable line that includes __name__ alongside the other labels."""
    parts: List[str] = []
    for key in sorted(labels):
        value = labels[key]
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        parts.append(f'{key}="{escaped}"')
    return "{" + ",".join(parts) + "}"


def delete_series(
    session: requests.Session,
    base_url: str,
    match_selector: str,
    logger: logging.Logger,
) -> Tuple[bool, int, str]:
    """Delete a single series by exact match[]. Returns (ok, status_code, reason)."""
    try:
        resp = session.post(
            f"{base_url}/api/v1/admin/tsdb/delete_series",
            params={"match[]": match_selector},
            timeout=60,
        )
    except requests.RequestException as exc:
        logger.error("delete request failed for %s: %s", match_selector, exc)
        return False, 0, str(exc)

    if 200 <= resp.status_code < 300:
        return True, resp.status_code, ""
    return False, resp.status_code, resp.text.strip()[:200]


def confirm_interactive(matched_count: int, op_name: str, threshold: date) -> bool:
    prompt = (
        f"About to delete {matched_count} series where biz_date {op_name} "
        f"{threshold.strftime('%d/%m/%Y')}. Continue? [y/N]: "
    )
    try:
        answer = input(prompt)
    except EOFError:
        return False
    return answer.strip().lower() in {"y", "yes"}


def configure_logging(verbose: bool) -> logging.Logger:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    return logging.getLogger("vm_delete_by_biz_date")


def run(args: argparse.Namespace) -> int:
    logger = configure_logging(args.verbose)
    base_url = normalize_base_url(args.vm_url)
    session = build_session(args.vm_token)

    logger.info(
        "Listing series for job=%s op=%s biz_date_threshold=%s lookback_days=%d (vm=%s)",
        args.job,
        args.op,
        args.threshold_date.strftime("%d/%m/%Y"),
        args.lookback_days,
        base_url,
    )

    try:
        series_list = list_series(
            session=session,
            base_url=base_url,
            job=args.job,
            metric_name=args.metric,
            lookback_days=args.lookback_days,
            logger=logger,
        )
    except (requests.RequestException, RuntimeError, ValueError) as exc:
        logger.error("Failed to list series: %s", exc)
        return 2

    logger.info("Listed %d series", len(series_list))

    matched, skipped_no_biz_date, skipped_unparseable = filter_series_by_biz_date(
        series_list=series_list,
        op_name=args.op,
        threshold=args.threshold_date,
        logger=logger,
    )

    logger.info(
        "Matched %d series (skipped %d with no biz_date label, %d with unparseable biz_date)",
        len(matched),
        skipped_no_biz_date,
        skipped_unparseable,
    )

    if not matched:
        print("No series matched the biz_date filter; nothing to do.")
        return 0

    if args.dry_run:
        print(
            f"[DRY RUN] {len(matched)} series would be deleted "
            f"(biz_date {args.op} {args.threshold_date.strftime('%d/%m/%Y')}):"
        )
        for labels in matched:
            print(format_series_line(labels))
        print(f"[DRY RUN] Total: {len(matched)} series. Re-run with --apply to delete.")
        return 0

    if not args.yes and not confirm_interactive(len(matched), args.op, args.threshold_date):
        print("Aborted by user; no series deleted.")
        return 1

    deleted = 0
    failed = 0
    for labels in matched:
        match_selector = labels_to_match_selector(labels)
        ok, status_code, reason = delete_series(session, base_url, match_selector, logger)
        if ok:
            deleted += 1
            print(f"OK   [{status_code}] {match_selector}")
        else:
            failed += 1
            print(f"FAIL [{status_code}] {match_selector} :: {reason}")

    print(
        f"Done. matched={len(matched)} deleted={deleted} failed={failed} "
        f"skipped_no_biz_date={skipped_no_biz_date} "
        f"skipped_unparseable={skipped_unparseable}"
    )

    return 0 if failed == 0 else 1


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
