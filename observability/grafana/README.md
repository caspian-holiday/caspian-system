# Grafana Stage A Provisioning

This folder contains Stage A observability assets for VM job operations.

## Files

- `dashboards/vmj_operations_dashboard.json`: provisionable baseline dashboard (`uid=vmj_ops_v1`)
- `provisioning/dashboards/vmj_operations.yml`: Grafana dashboard provider definition

## Dashboard scope (Stage A)

- Dynamic `job` filter discovered from VictoriaMetrics label values
- Daily metric population panel using generic `job` label sample counts
- 7-day population table by job using generic `job` label sample counts
- Latest submission timestamp by `biz_date` for selected job using `tlast_over_time(...[30d])`
- `biz_date` for sidecar-ingested producer metrics is validated as strict `dd/mm/yyyy`
- Table is sorted by `latest_submission_ts` (timestamp) for reliable chronology
- Datasource is hard-bound to Grafana datasource UID `victoriametrics`

## Data path

- Read/query path goes directly from Grafana to VictoriaMetrics datasource.
- Write actions are intentionally out of scope for Stage A.

## Job exclusion denylist sync

The dashboard variable currently excludes internal/system jobs with regex:

- `^(?!vmj$)(?!victoria-metrics$)(?!internal_)(?!system_).*`

All panel queries also hard-exclude `job="victoria-metrics"` regardless of selector state.

Keep this aligned with config keys in `victoria_metrics_jobs/victoria_metrics_jobs.yml`:

- `common.dashboard.job_discovery.exclude_jobs_regex`

Job discovery query scans all metrics for distinct `job` labels:

- `label_values({__name__=~".+"}, job)`

If denylist entries change, update the dashboard variable regex accordingly.

## Suggested deployment mapping

1. Copy `dashboards/vmj_operations_dashboard.json` to `/var/lib/grafana/dashboards/vmj/`.
2. Copy `provisioning/dashboards/vmj_operations.yml` into Grafana provisioning dashboards directory.
3. Restart Grafana or reload provisioning.

## Notebook report endpoints

Notebook artifacts are served by the scheduler HTTP API using job-id routes:

- `/vmj/notebooks` lists all notebook-producing jobs
- `/vmj/notebooks/metrics_forecast_notebooks` lists forecast notebook outputs
- `/vmj/notebooks/metrics_report_notebooks` lists self-report notebook outputs
- `/vmj/notebooks/<job_id>/<year>/<month>/<day>/<filename>` serves a specific `.ipynb` or `.html` file

Access control is enforced by upstream `vmauth`; the scheduler does not apply
additional endpoint-level authentication.
