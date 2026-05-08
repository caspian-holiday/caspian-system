#!/usr/bin/env python3
"""
Maintenance script: list and delete Victoria Metrics series for a given job
that had a sample at a specific integer-second timestamp (no ms part).

The user supplies --ts as a whole number of seconds (e.g. 1775409800). The
script auto-matches the bare value AND every fractional/millisecond variant
within that second (ts, ts.0, ts.123, ts.999).

Why /api/v1/export and not /api/v1/series:
    /api/v1/series is an index lookup with built-in staleness tolerance, so
    a series whose nearest sample is slightly outside the requested window
    can still appear in its result. /api/v1/export, on the other hand,
    returns the raw per-sample timestamps for each series, which we then
    filter locally with strict ms-precision: keep only series with at least
    one sample in [ts*1000, ts*1000+999] inclusive. That gives an exact
    match independent of VM's index behaviour.

Note on deletion semantics:
    Victoria Metrics' /api/v1/admin/tsdb/delete_series API drops the ENTIRE
    series matching a selector; it does not accept a time range. So matched
    series are deleted in their entirety, not just the samples in the
    targeted second. The dry-run output and final summary say this loudly.

Dry-run is the default; --apply is required to actually delete.
"""

from __future__ import annotations

import argparse
import json
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


def list_series_with_sample_at_second(
    session: requests.Session,
    base_url: str,
    job: str,
    metric_name: str,
    ts_int: int,
    logger: logging.Logger,
) -> List[Tuple[Dict[str, str], List[int]]]:
    """List series for {job=...} that have at least one sample in [ts, ts+0.999].

    Uses /api/v1/export so we see every sample's actual ms timestamp, then
    filters locally with exact ms precision. The server-side window is set
    slightly wider ([ts-1, ts+1] seconds) to absorb any rounding VM might
    apply to the export start/end, since the strict filter is then re-applied
    locally.

    Returns:
        A list of (labels, matched_ts_ms) pairs, where:
        - labels includes __name__ and every label of the series
        - matched_ts_ms is the sorted list of sample timestamps (in ms) that
          fell inside the [ts*1000, ts*1000+999] window
    """
    selector = f'{{job="{job}"}}'
    if metric_name:
        selector = f"{metric_name}{selector}"

    server_start_s = max(0, ts_int - 1)
    server_end_s = ts_int + 1

    window_ms_start = ts_int * 1000
    window_ms_end = ts_int * 1000 + 999

    params = {
        "match[]": selector,
        "start": str(server_start_s),
        "end": str(server_end_s),
    }
    logger.debug("GET %s/api/v1/export params=%s", base_url, params)

    resp = session.get(f"{base_url}/api/v1/export", params=params, timeout=60)
    resp.raise_for_status()

    matched: List[Tuple[Dict[str, str], List[int]]] = []
    for line in resp.text.splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            logger.warning("Skipping un-parseable export line: %s", exc)
            continue

        metric = row.get("metric") or {}
        if not metric.get("__name__"):
            continue

        timestamps = row.get("timestamps") or []
        in_window = [
            int(ts)
            for ts in timestamps
            if window_ms_start <= int(ts) <= window_ms_end
        ]
        if not in_window:
            continue

        labels = dict(metric)
        matched.append((labels, sorted(in_window)))

    return matched


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
    window_ms_start = args.ts_int * 1000
    window_ms_end = args.ts_int * 1000 + 999
    logger.info(
        "Listing series for job=%s ts=%d (%s) window_ms=[%d, %d] (vm=%s)",
        args.job,
        args.ts_int,
        iso,
        window_ms_start,
        window_ms_end,
        base_url,
    )

    try:
        matched_series = list_series_with_sample_at_second(
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

    logger.info(
        "Matched %d series with at least one sample in ms window [%d, %d]",
        len(matched_series),
        window_ms_start,
        window_ms_end,
    )

    if not matched_series:
        print(f"No series had a sample at second {args.ts_int}; nothing to do.")
        return 0

    if args.dry_run:
        print(
            f"[DRY RUN] {len(matched_series)} series had a sample in ms window "
            f"[{window_ms_start}, {window_ms_end}] (second {args.ts_int}, {iso}) "
            f"and would be DELETED ENTIRELY:"
        )
        for labels, in_window_ms in matched_series:
            ts_repr = ",".join(str(t) for t in in_window_ms)
            print(f"{format_series_line(labels)}  matched_ts_ms=[{ts_repr}]")
        print(
            f"[DRY RUN] Total: {len(matched_series)} series. Re-run with --apply "
            f"to delete. Note: delete_series drops the whole series, not just "
            f"the targeted second."
        )
        return 0

    if not args.yes and not confirm_interactive(len(matched_series), args.ts_int):
        print("Aborted by user; no series deleted.")
        return 1

    deleted = 0
    failed = 0
    for labels, _in_window_ms in matched_series:
        match_selector = labels_to_match_selector(labels)
        ok, status_code, reason = delete_series(session, base_url, match_selector, logger)
        if ok:
            deleted += 1
            print(f"OK   [{status_code}] {match_selector}")
        else:
            failed += 1
            print(f"FAIL [{status_code}] {match_selector} :: {reason}")

    print(f"Done. matched={len(matched_series)} deleted={deleted} failed={failed}")
    return 0 if failed == 0 else 1


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
