#!/usr/bin/env python3
"""
Generate local test payloads for VMJ sidecar write endpoints.

Outputs:
- Binary remote-write payload (protobuf + snappy), e.g. tmp/biz_date_remote_write.bin
- Optional Prometheus text payload, e.g. tmp/biz_date_prometheus_text.txt
"""

from __future__ import annotations

import argparse
from pathlib import Path
from datetime import datetime, timedelta, timezone

import snappy
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory


def _build_write_request_class():
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


def _build_payload(
    write_request_cls,
    series_entries: list[tuple[dict[str, str], float, int]],
) -> bytes:
    request_obj = write_request_cls()
    for labels, value, ts_ms in series_entries:
        series = request_obj.timeseries.add()
        for key in sorted(labels.keys()):
            label = series.labels.add()
            label.name = key
            label.value = labels[key]

        sample = series.samples.add()
        sample.value = value
        sample.timestamp = ts_ms

    return snappy.compress(request_obj.SerializeToString())


def _build_prometheus_text_payload(
    series_entries: list[tuple[dict[str, str], float, int]],
) -> str:
    lines: list[str] = []
    for labels, value, _ts_ms in series_entries:
        metric_name = labels.get("__name__")
        if not metric_name:
            raise ValueError("Each series entry must include __name__ label")

        text_labels: list[str] = []
        for key in sorted(labels.keys()):
            if key == "__name__":
                continue
            label_value = labels[key]
            escaped_value = (
                label_value.replace("\\", "\\\\")
                .replace("\n", "\\n")
                .replace('"', '\\"')
            )
            text_labels.append(f'{key}="{escaped_value}"')

        label_suffix = f'{{{",".join(text_labels)}}}' if text_labels else ""
        lines.append(f"{metric_name}{label_suffix} {value}")

    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate remote-write payloads for sidecar/vmauth tests")
    parser.add_argument(
        "--count",
        type=int,
        default=1000,
        help="Number of unique metrics to generate (default: 1000)",
    )
    parser.add_argument(
        "--job-name",
        default="intellij_local_test",
        help="Value for the job label (default: intellij_local_test)",
    )
    parser.add_argument(
        "--metric-prefix",
        default="vmj_capacity_metric_test",
        help="Metric name prefix (default: vmj_capacity_metric_test)",
    )
    parser.add_argument(
        "--biz-date-span-days",
        type=int,
        default=365,
        help="How many past days to spread biz_date over (default: 365)",
    )
    parser.add_argument(
        "--output",
        default="tmp/biz_date_remote_write_1k.bin",
        help="Output binary payload path (default: tmp/biz_date_remote_write_1k.bin)",
    )
    parser.add_argument(
        "--text-output",
        default=None,
        help="Optional output path for Prometheus text payload (e.g. tmp/biz_date_prometheus_text_1k.txt)",
    )
    args = parser.parse_args()

    if args.count <= 0:
        raise ValueError("--count must be > 0")
    if args.biz_date_span_days <= 0:
        raise ValueError("--biz-date-span-days must be > 0")

    write_request_cls = _build_write_request_class()
    now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

    # Generate many unique series with varying biz_date values.
    biz_date_entries: list[tuple[dict[str, str], float, int]] = []
    for idx in range(args.count):
        days_back = idx % args.biz_date_span_days
        biz_date = (datetime.now(tz=timezone.utc) - timedelta(days=days_back)).strftime("%d/%m/%Y")
        biz_date_entries.append(
            (
                {
                    "__name__": f"{args.metric_prefix}_{idx:04d}",
                    "job": args.job_name,
                    "biz_date": biz_date,
                    "env": "local",
                    "series_group": "stress_1k",
                },
                float(idx) + 0.123,
                now_ms,
            )
        )
    biz_date_payload = _build_payload(write_request_cls, biz_date_entries)

    biz_path = Path(args.output)
    biz_path.parent.mkdir(parents=True, exist_ok=True)
    biz_path.write_bytes(biz_date_payload)

    print(f"Wrote {biz_path}")
    if args.text_output:
        text_payload = _build_prometheus_text_payload(biz_date_entries)
        text_path = Path(args.text_output)
        text_path.parent.mkdir(parents=True, exist_ok=True)
        text_path.write_text(text_payload, encoding="utf-8")
        print(f"Wrote {text_path}")

    print(f"biz_date series count: {len(biz_date_entries)}")


if __name__ == "__main__":
    main()
