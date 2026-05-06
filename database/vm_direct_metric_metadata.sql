-- ============================================================================
-- Victoria Metrics Jobs - Direct Metric Metadata Schema DDL
-- Table: vm_direct_metric_metadata
-- ============================================================================

CREATE TABLE IF NOT EXISTS vm_direct_metric_metadata (
    metric_id BIGSERIAL PRIMARY KEY,
    metric_name VARCHAR NOT NULL,
    metric_job_name VARCHAR NOT NULL,
    normalized_labels JSONB NOT NULL,
    first_seen_labels JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_vm_direct_metric_metadata_identity
        UNIQUE (metric_name, metric_job_name, normalized_labels)
);

CREATE INDEX IF NOT EXISTS idx_vm_direct_metric_metadata_metric_name
    ON vm_direct_metric_metadata (metric_name);

CREATE INDEX IF NOT EXISTS idx_vm_direct_metric_metadata_metric_job_name
    ON vm_direct_metric_metadata (metric_job_name);

CREATE INDEX IF NOT EXISTS idx_vm_direct_metric_metadata_normalized_labels
    ON vm_direct_metric_metadata USING GIN (normalized_labels);

CREATE INDEX IF NOT EXISTS idx_vm_direct_metric_metadata_identity
    ON vm_direct_metric_metadata (metric_name, metric_job_name, normalized_labels);
