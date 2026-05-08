from datetime import date, datetime, timedelta, timezone

import pytest

from victoria_metrics_jobs.scheduler.sidecar_service import SidecarService


@pytest.mark.unit
def test_convert_biz_date_timestamp_maps_window_start_to_biz_date_start(monkeypatch):
    monkeypatch.setenv("VM_JOBS_SIDECAR_MAX_STALENESS_DAYS", "365")
    service = SidecarService(database_manager=None, runtime_config={})
    biz_date = date(2026, 5, 1)
    submission_ts = datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc)

    converted = service._convert_biz_date_timestamp(
        biz_date=biz_date,
        submission_ts=submission_ts,
    )

    assert converted == datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc)


@pytest.mark.unit
def test_convert_biz_date_timestamp_maps_window_end_to_next_day(monkeypatch):
    monkeypatch.setenv("VM_JOBS_SIDECAR_MAX_STALENESS_DAYS", "365")
    service = SidecarService(database_manager=None, runtime_config={})
    biz_date = date(2026, 5, 1)
    biz_start = datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc)
    submission_ts = biz_start + timedelta(days=365)

    converted = service._convert_biz_date_timestamp(
        biz_date=biz_date,
        submission_ts=submission_ts,
    )

    assert converted == biz_start + timedelta(days=1)


@pytest.mark.unit
def test_convert_biz_date_timestamp_uses_default_when_staleness_non_positive(monkeypatch):
    monkeypatch.setenv("VM_JOBS_SIDECAR_MAX_STALENESS_DAYS", "0")
    service = SidecarService(database_manager=None, runtime_config={})
    biz_date = date(2026, 5, 1)
    biz_start = datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc)
    submission_ts = biz_start + timedelta(days=365)

    converted = service._convert_biz_date_timestamp(
        biz_date=biz_date,
        submission_ts=submission_ts,
    )

    assert converted == biz_start + timedelta(days=1)


@pytest.mark.unit
def test_convert_biz_date_timestamp_raises_when_submission_out_of_window(monkeypatch):
    monkeypatch.setenv("VM_JOBS_SIDECAR_MAX_STALENESS_DAYS", "30")
    service = SidecarService(database_manager=None, runtime_config={})
    biz_date = date(2026, 5, 1)
    submission_ts = datetime(2026, 6, 1, 0, 0, 1, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="out of allowed window"):
        service._convert_biz_date_timestamp(
            biz_date=biz_date,
            submission_ts=submission_ts,
        )
