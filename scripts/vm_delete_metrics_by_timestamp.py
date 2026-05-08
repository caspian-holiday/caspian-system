#!/usr/bin/env python3
"""
Maintenance script: list and delete Victoria Metrics series for a given job
that had a sample at a specific integer-second timestamp (no ms part).

The user supplies --ts as a whole number of seconds (e.g. 1775409800). The
script translates that into a [ts, ts+0.999] window on /api/v1/series, so
every sample timestamp inside that integer second matches -- including
ts itself (no fractional), ts.0, ts.123, ts.999, etc.

Note on deletion semantics:
    Victoria Metrics' /api/v1/admin/tsdb/delete_series API drops the ENTIRE
    series matching a selector; it does not accept a time range. So matched
    series are deleted in their entirety, not just the samples in the
    targeted second. The dry-run output and final summary say this loudly.

Dry-run is the default; --apply is required to actually delete.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="vm_delete_metrics_by_timestamp.py",
        description=(
            "Delete Victoria Metrics series for a job that had a sample at a "
            "specific integer-second timestamp. The integer second matches the "
            "bare value AND every fractional/millisecond variant within that "
            "second (ts, ts.0, ts.123, ts.999, ...). Defaults to dry-run."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  # Preview series with a sample at integer second 1775409800\n"
            "  vm_delete_metrics_by_timestamp.py --job apex_collector --ts 1775409800\n\n"
            "  # Actually delete them (interactive confirmation)\n"
            "  vm_delete_metrics_by_timestamp.py --job apex_collector --ts 1775409800 --apply\n\n"
            "  # Apply non-interactively\n"
            "  vm_delete_metrics_by_timestamp.py --job apex_collector --ts 1775409800 --apply --yes\n\n"
            "WARNING: VM's delete_series drops the entire matched series, not\n"
            "only the sample at the targeted second. Any other samples those\n"
            "series have at other timestamps will be deleted as well.\n"
        ),
    )

    parser.add_argument("--job", required=True, help="Job label value to filter by (job=<JOB>).")
    parser.add_argument(
        "--ts",
        required=True,
        help=(
            "Integer Unix timestamp in seconds, with NO fractional/ms part "
            "(e.g. 1775409800). The script auto-matches every .xxx variation "
            "within that second."
        ),
    )
    parser.add_argument(
        "--vm-url",
        default=os.environ.get("VM_JOBS_VM_QUERY_URL", ""),
        help="Victoria Metrics base URL (defaults to $VM_JOBS_VM_QUERY_URL).",
    )
    parser.add_argument(
        "--vm-token",
        default=os.environ.get("VM_JOBS_VM_TOKEN", ""),
        help=(
            "Full Authorization header value, sent verbatim. Include the "
            "scheme prefix, e.g. 'Bearer abc123' or 'Basic dXNlcjpwYXNz'. "
            "Defaults to $VM_JOBS_VM_TOKEN. Omit to send no Authorization."
        ),
    )
    parser.add_argument(
        "--metric",
        default="",
        help="Optional metric name filter (e.g. my_metric); default no filter.",
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

    if "." in args.ts:
        parser.error(
            f"--ts must be an integer number of seconds with NO ms part; got {args.ts!r}. "
            "All .xxx variations are matched automatically."
        )
    try:
        args.ts_int = int(args.ts)
    except ValueError:
        parser.error(f"--ts must be a whole integer (seconds since epoch); got {args.ts!r}")

    if args.ts_int <= 0:
        parser.error(f"--ts must be a positive integer; got {args.ts_int}")

    return args


def normalize_base_url(url: str) -> str:
    base = url.rstrip("/")
    if base.endswith("/api/v1"):
        base = base[: -len("/api/v1")]
    elif base.endswith("/api"):
        base = base[: -len("/api")]
    return base


def build_session(vm_token: str) -> requests.Session:
    """Build a requests session, setting Authorization to vm_token verbatim.

    The caller is expected to pass the full header value including the scheme
    (e.g. 'Bearer xyz' or 'Basic xyz'); empty/None means no auth header.
    """
    session = requests.Session()
    auth_header = (vm_token or "").strip()
    if auth_header:
        session.headers["Authorization"] = auth_header
    return session


def list_series_at_second(
    session: requests.Session,
    base_url: str,
    job: str,
    metric_name: str,
    ts_int: int,
    logger: logging.Logger,
) -> List[Dict[str, str]]:
    """List every series for {job=...} with at least one sample in [ts, ts+0.999].

    The half-open-ish window over the integer second is what gives us "match
    the bare ts AND every .xxx ms variation" without doing any local filter
    work or PromQL gymnastics.
    """
    selector = f'{{job="{job}"}}'
    if metric_name:
        selector = f"{metric_name}{selector}"

    start_param = str(ts_int)
    end_param = f"{ts_int}.999"

    params = {"match[]": selector, "start": start_param, "end": end_param}
    logger.debug("GET %s/api/v1/series params=%s", base_url, params)

    resp = session.get(f"{base_url}/api/v1/series", params=params, timeout=60)
    resp.raise_for_status()

    payload = resp.json()
    if payload.get("status") != "success":
        raise RuntimeError(f"VM /api/v1/series returned non-success: {payload}")

    return payload.get("data", []) or []


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


def confirm_interactive(matched_count: int, ts_int: int) -> bool:
    iso = datetime.fromtimestamp(ts_int, tz=timezone.utc).isoformat()
    prompt = (
        f"About to delete {matched_count} series that had a sample at integer second "
        f"{ts_int} ({iso}). VM deletes drop the WHOLE series, not just that second. "
        f"Continue? [y/N]: "
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
    return logging.getLogger("vm_delete_by_timestamp")


def run(args: argparse.Namespace) -> int:
    logger = configure_logging(args.verbose)
    base_url = normalize_base_url(args.vm_url)
    session = build_session(args.vm_token)

    iso = datetime.fromtimestamp(args.ts_int, tz=timezone.utc).isoformat()
    logger.info(
        "Listing series for job=%s ts=%d (%s) window=[%d, %d.999] (vm=%s)",
        args.job,
        args.ts_int,
        iso,
        args.ts_int,
        args.ts_int,
        base_url,
    )

    try:
        series_list = list_series_at_second(
            session=session,
            base_url=base_url,
            job=args.job,
            metric_name=args.metric,
            ts_int=args.ts_int,
            logger=logger,
        )
    except (requests.RequestException, RuntimeError, ValueError) as exc:
        logger.error("Failed to list series: %s", exc)
        return 2

    logger.info("Matched %d series at second %d", len(series_list), args.ts_int)

    if not series_list:
        print(f"No series had a sample at second {args.ts_int}; nothing to do.")
        return 0

    if args.dry_run:
        print(
            f"[DRY RUN] {len(series_list)} series had a sample at second "
            f"{args.ts_int} ({iso}) and would be DELETED ENTIRELY:"
        )
        for labels in series_list:
            print(format_series_line(labels))
        print(
            f"[DRY RUN] Total: {len(series_list)} series. Re-run with --apply "
            f"to delete. Note: delete_series drops the whole series, not just "
            f"the targeted second."
        )
        return 0

    if not args.yes and not confirm_interactive(len(series_list), args.ts_int):
        print("Aborted by user; no series deleted.")
        return 1

    deleted = 0
    failed = 0
    for labels in series_list:
        match_selector = labels_to_match_selector(labels)
        ok, status_code, reason = delete_series(session, base_url, match_selector, logger)
        if ok:
            deleted += 1
            print(f"OK   [{status_code}] {match_selector}")
        else:
            failed += 1
            print(f"FAIL [{status_code}] {match_selector} :: {reason}")

    print(f"Done. matched={len(series_list)} deleted={deleted} failed={failed}")
    return 0 if failed == 0 else 1


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
