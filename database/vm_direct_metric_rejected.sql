-- ============================================================================
-- Victoria Metrics Jobs - Direct Metric Rejected Rows DDL
-- Table: vm_direct_metric_rejected
-- ============================================================================

CREATE TABLE IF NOT EXISTS vm_direct_metric_rejected (
    rejected_id BIGSERIAL PRIMARY KEY,
    endpoint_path VARCHAR NOT NULL,
    reason VARCHAR NOT NULL,
    metric_name VARCHAR,
    metric_job_name VARCHAR,
    biz_date_raw VARCHAR,
    provided_labels JSONB NOT NULL,
    sample_value DOUBLE PRECISION,
    sample_timestamp_ms BIGINT,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_vm_direct_metric_rejected_received_at
    ON vm_direct_metric_rejected (received_at DESC);

CREATE INDEX IF NOT EXISTS idx_vm_direct_metric_rejected_reason
    ON vm_direct_metric_rejected (reason);
