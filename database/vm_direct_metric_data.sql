-- ============================================================================
-- Victoria Metrics Jobs - Direct Metric Data Schema DDL
-- Table: vm_direct_metric_data
-- ============================================================================

CREATE TABLE IF NOT EXISTS vm_direct_metric_data (
    metric_id BIGINT NOT NULL REFERENCES vm_direct_metric_metadata(metric_id),
    biz_date DATE NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    metric_timestamp TIMESTAMPTZ NOT NULL,
    submission_timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_vm_direct_metric_data_metric_biz_date UNIQUE (metric_id, biz_date)
);

CREATE INDEX IF NOT EXISTS idx_vm_direct_metric_data_metric_timestamp
    ON vm_direct_metric_data (metric_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_vm_direct_metric_data_biz_date
    ON vm_direct_metric_data (biz_date DESC);
