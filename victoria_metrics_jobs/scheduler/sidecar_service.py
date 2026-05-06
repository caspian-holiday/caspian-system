#!/usr/bin/env python3
"""
Sidecar ingestion service for VMJ remote-write endpoints.
"""

import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Optional

import requests
import snappy
from flask import request
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from sqlalchemy import text

from .database import DatabaseManager


class SidecarService:
    """Handles remote-write decode, validation, PG writes, and VM forwarding."""

    def __init__(self, database_manager: Optional[DatabaseManager], runtime_config: Dict[str, Any]):
        self.database_manager = database_manager
        self.runtime_config = runtime_config
        self.logger = logging.getLogger(__name__)
        self._write_request_cls = self._build_write_request_class()
    
    def _build_write_request_class(self):
        file_proto = descriptor_pb2.FileDescriptorProto()
        file_proto.name = "remote_write_v1.proto"
        file_proto.package = "prometheus"
        file_proto.syntax = "proto3"

        label_msg = file_proto.message_type.add()
        label_msg.name = "Label"
        f = label_msg.field.add()
        f.name = "name"
        f.number = 1
        f.type = descriptor_pb2.FieldDescriptorProto.TYPE_STRING
        f.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
        f = label_msg.field.add()
        f.name = "value"
        f.number = 2
        f.type = descriptor_pb2.FieldDescriptorProto.TYPE_STRING
        f.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL

        sample_msg = file_proto.message_type.add()
        sample_msg.name = "Sample"
        f = sample_msg.field.add()
        f.name = "value"
        f.number = 1
        f.type = descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE
        f.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
        f = sample_msg.field.add()
        f.name = "timestamp"
        f.number = 2
        f.type = descriptor_pb2.FieldDescriptorProto.TYPE_INT64
        f.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL

        ts_msg = file_proto.message_type.add()
        ts_msg.name = "TimeSeries"
        f = ts_msg.field.add()
        f.name = "labels"
        f.number = 1
        f.type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
        f.type_name = ".prometheus.Label"
        f.label = descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED
        f = ts_msg.field.add()
        f.name = "samples"
        f.number = 2
        f.type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
        f.type_name = ".prometheus.Sample"
        f.label = descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED

        req_msg = file_proto.message_type.add()
        req_msg.name = "WriteRequest"
        f = req_msg.field.add()
        f.name = "timeseries"
        f.number = 1
        f.type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
        f.type_name = ".prometheus.TimeSeries"
        f.label = descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED

        pool = descriptor_pool.DescriptorPool()
        pool.Add(file_proto)
        return message_factory.GetMessageClass(
            pool.FindMessageTypeByName("prometheus.WriteRequest")
        )

    def handle_write(self):
        if request.method != "POST":
            return {"error": "Method not allowed"}, 405
        if not self.database_manager:
            return {"error": "Database not configured"}, 503

        content_encoding = (request.headers.get("Content-Encoding") or "").lower()
        if content_encoding != "snappy":
            return {
                "error": f"unsupported content-encoding {content_encoding or '<empty>'}; expected snappy"
            }, 415

        try:
            timeseries = self._decode_remote_write(request.get_data())
            sidecar_result = self._process_remote_write_timeseries(timeseries)
            if sidecar_result["vm_forward_payload"]:
                self._forward_to_vm_best_effort(sidecar_result["vm_forward_payload"])
            return {
                "status": "ok",
                "exported_metrics": sidecar_result["exported_metrics"],
                "rejected_metrics": sidecar_result["rejected_metrics"],
            }, 200
        except ValueError as exc:
            self.logger.warning("Sidecar rejected remote-write payload: %s", exc)
            return {"error": str(exc)}, 400
        except Exception as exc:
            self.logger.exception("Sidecar write failed: %s", exc)
            return {"error": "sidecar processing failed"}, 500

    def _decode_remote_write(self, compressed_payload: bytes):
        try:
            # Strict Prometheus remote-write contract: snappy block format.
            raw_payload = snappy.decompress(compressed_payload)
            write_request = self._write_request_cls()
            write_request.ParseFromString(raw_payload)
            return write_request.timeseries
        except Exception as exc:
            raise ValueError(f"failed to decode remote-write payload: {exc}") from exc

    def _process_remote_write_timeseries(self, timeseries) -> Dict[str, Any]:
        processed = 0
        rejected = 0
        vm_forward = []
        engine = self.database_manager.get_engine()
        if engine is None:
            raise RuntimeError("database engine is not available")

        with engine.begin() as conn:
            for series in timeseries:
                labels = {label.name: label.value for label in series.labels}
                metric_name = labels.get("__name__", "")
                metric_job_name = labels.get("job", "")
                normalized_labels = self._normalize_labels(labels)

                for sample in series.samples:
                    try:
                        if not metric_job_name:
                            raise ValueError("missing job label")
                        submission_ts = self._timestamp_from_ms(sample.timestamp)
                        biz_date_raw = labels.get("biz_date")
                        if not biz_date_raw:
                            raise ValueError("missing biz_date label")
                        biz_date = self._parse_biz_date(biz_date_raw)
                        target_ts = self._convert_biz_date_timestamp(
                            biz_date=biz_date,
                            submission_ts=submission_ts,
                        )

                        metric_id = self._upsert_metric_metadata(
                            conn=conn,
                            metric_name=metric_name,
                            normalized_labels=normalized_labels,
                            first_seen_labels=labels,
                            metric_job_name=metric_job_name,
                        )
                        self._upsert_metric_data(
                            conn=conn,
                            metric_id=metric_id,
                            biz_date=biz_date,
                            metric_value=float(sample.value),
                            metric_timestamp=target_ts,
                            submission_timestamp=submission_ts,
                        )
                        processed += 1

                        vm_forward_labels = {k: v for k, v in labels.items() if k != "biz_date"}
                        vm_forward.append(
                            {
                                "metric_name": metric_name,
                                "labels": vm_forward_labels,
                                "value": float(sample.value),
                                "timestamp_ms": int(target_ts.timestamp() * 1000),
                            }
                        )
                    except ValueError as val_err:
                        rejected += 1
                        self._insert_rejected_metric(
                            conn=conn,
                            endpoint_path=request.path,
                            reason=str(val_err),
                            metric_name=metric_name,
                            metric_job_name=metric_job_name,
                            biz_date_raw=labels.get(
                                "biz_date", biz_date_raw if "biz_date_raw" in locals() else ""
                            ),
                            provided_labels=labels,
                            sample_value=float(sample.value),
                            sample_timestamp_ms=int(sample.timestamp),
                        )

        return {
            "exported_metrics": processed,
            "rejected_metrics": rejected,
            "vm_forward_payload": vm_forward,
        }

    def _normalize_labels(self, labels: Dict[str, str]) -> Dict[str, str]:
        return {k: labels[k] for k in sorted(labels.keys()) if k not in {"biz_date", "job"}}

    def _timestamp_from_ms(self, timestamp_ms: int) -> datetime:
        try:
            return datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
        except Exception as exc:
            raise ValueError(f"invalid submission timestamp: {timestamp_ms}") from exc

    def _parse_biz_date(self, biz_date_raw: str) -> date:
        try:
            return datetime.strptime(biz_date_raw, "%d/%m/%Y").date()
        except ValueError as exc:
            raise ValueError(
                f"invalid biz_date format: {biz_date_raw}. expected dd/mm/yyyy"
            ) from exc

    def _convert_biz_date_timestamp(self, biz_date: date, submission_ts: datetime) -> datetime:
        max_staleness_days = int(os.getenv("VM_JOBS_SIDECAR_MAX_STALENESS_DAYS", "365"))
        if max_staleness_days <= 0:
            max_staleness_days = 365

        biz_start = datetime.combine(biz_date, datetime.min.time(), tzinfo=timezone.utc)
        max_staleness = timedelta(days=max_staleness_days)
        window_end = biz_start + max_staleness
        if submission_ts < biz_start or submission_ts > window_end:
            raise ValueError(
                f"submission timestamp {submission_ts.isoformat()} out of allowed window for biz_date {biz_date.isoformat()}"
            )

        offset_ratio = (submission_ts - biz_start).total_seconds() / max_staleness.total_seconds()
        return biz_start + timedelta(milliseconds=offset_ratio * 86400000.0)

    def _upsert_metric_metadata(
        self,
        conn,
        metric_name: str,
        normalized_labels: Dict[str, str],
        first_seen_labels: Dict[str, str],
        metric_job_name: str,
    ) -> int:
        normalized_labels_json = json.dumps(normalized_labels, sort_keys=True)
        first_seen_labels_json = json.dumps(first_seen_labels, sort_keys=True)

        select_sql = text(
            """
            SELECT metric_id
            FROM vm_direct_metric_metadata
            WHERE metric_name = :metric_name
              AND metric_job_name = :metric_job_name
              AND normalized_labels = CAST(:normalized_labels AS jsonb)
            LIMIT 1
            """
        )
        row = conn.execute(
            select_sql,
            {
                "metric_name": metric_name,
                "normalized_labels": normalized_labels_json,
                "metric_job_name": metric_job_name,
            },
        ).fetchone()
        if row:
            return int(row[0])

        insert_sql = text(
            """
            INSERT INTO vm_direct_metric_metadata (
                metric_name,
                normalized_labels,
                first_seen_labels,
                metric_job_name
            )
            VALUES (
                :metric_name,
                CAST(:normalized_labels AS jsonb),
                CAST(:first_seen_labels AS jsonb),
                :metric_job_name
            )
            ON CONFLICT (metric_name, metric_job_name, normalized_labels)
            DO NOTHING
            RETURNING metric_id
            """
        )
        inserted_row = conn.execute(
            insert_sql,
            {
                "metric_name": metric_name,
                "normalized_labels": normalized_labels_json,
                "first_seen_labels": first_seen_labels_json,
                "metric_job_name": metric_job_name,
            },
        ).fetchone()
        if inserted_row:
            return int(inserted_row[0])

        race_row = conn.execute(
            select_sql,
            {
                "metric_name": metric_name,
                "normalized_labels": normalized_labels_json,
                "metric_job_name": metric_job_name,
            },
        ).fetchone()
        if race_row:
            return int(race_row[0])
        raise RuntimeError("failed to retrieve metric_id from metadata select/insert flow")

    def _upsert_metric_data(
        self,
        conn,
        metric_id: int,
        biz_date: date,
        metric_value: float,
        metric_timestamp: datetime,
        submission_timestamp: datetime,
    ) -> None:
        sql = text(
            """
            INSERT INTO vm_direct_metric_data (
                metric_id,
                biz_date,
                metric_value,
                metric_timestamp,
                submission_timestamp,
                updated_at
            )
            VALUES (
                :metric_id,
                :biz_date,
                :metric_value,
                :metric_timestamp,
                :submission_timestamp,
                NOW()
            )
            ON CONFLICT (metric_id, biz_date)
            DO UPDATE SET
                metric_value = EXCLUDED.metric_value,
                metric_timestamp = EXCLUDED.metric_timestamp,
                submission_timestamp = EXCLUDED.submission_timestamp,
                updated_at = NOW()
            """
        )
        conn.execute(
            sql,
            {
                "metric_id": metric_id,
                "biz_date": biz_date,
                "metric_value": metric_value,
                "metric_timestamp": metric_timestamp,
                "submission_timestamp": submission_timestamp,
            },
        )

    def _insert_rejected_metric(
        self,
        conn,
        endpoint_path: str,
        reason: str,
        metric_name: str,
        metric_job_name: str,
        biz_date_raw: str,
        provided_labels: Dict[str, str],
        sample_value: float,
        sample_timestamp_ms: int,
    ) -> None:
        sql = text(
            """
            INSERT INTO vm_direct_metric_rejected (
                endpoint_path,
                reason,
                metric_name,
                metric_job_name,
                biz_date_raw,
                provided_labels,
                sample_value,
                sample_timestamp_ms
            )
            VALUES (
                :endpoint_path,
                :reason,
                :metric_name,
                :metric_job_name,
                :biz_date_raw,
                CAST(:provided_labels AS jsonb),
                :sample_value,
                :sample_timestamp_ms
            )
            """
        )
        conn.execute(
            sql,
            {
                "endpoint_path": endpoint_path,
                "reason": reason,
                "metric_name": metric_name,
                "metric_job_name": metric_job_name,
                "biz_date_raw": biz_date_raw,
                "provided_labels": json.dumps(provided_labels, sort_keys=True),
                "sample_value": sample_value,
                "sample_timestamp_ms": sample_timestamp_ms,
            },
        )

    def _forward_to_vm_best_effort(self, vm_forward_payload: list) -> None:
        vm_url = os.getenv("VM_JOBS_SIDECAR_VM_IMPORT_URL", "").strip()
        if not vm_url:
            vm_cfg = self.runtime_config.get("victoria_metrics", {})
            base_url = (vm_cfg.get("query_url") or "").rstrip("/")
            if base_url:
                vm_url = f"{base_url}/api/v1/import/prometheus"
        if not vm_url:
            self.logger.info("Skipping VM forward: no VM import URL configured")
            return

        lines = []
        for sample in vm_forward_payload:
            labels = sample.get("labels", {})
            label_parts = []
            for key in sorted(labels.keys()):
                escaped = str(labels[key]).replace("\\", "\\\\").replace("\"", "\\\"")
                label_parts.append(f'{key}="{escaped}"')
            labels_str = ",".join(label_parts)
            metric_name = sample["metric_name"]
            value = sample["value"]
            ts_ms = sample["timestamp_ms"]
            if labels_str:
                lines.append(f"{metric_name}{{{labels_str}}} {value} {ts_ms}")
            else:
                lines.append(f"{metric_name} {value} {ts_ms}")

        payload = "\n".join(lines)
        try:
            response = requests.post(
                vm_url,
                data=payload.encode("utf-8"),
                headers={"Content-Type": "text/plain"},
                timeout=10,
            )
            if response.status_code >= 400:
                self.logger.warning(
                    "Best-effort VM forward failed with status %s",
                    response.status_code,
                )
        except Exception as exc:
            self.logger.warning("Best-effort VM forward failed: %s", exc)
