# Business-Date Metric Ingestion — Problem Statement
 
## Functional view
 
- Metrics are collected over a single business day and become available at the end of that day.
- They are visualised by plotting values on a timeline of business days.
- The specific time of day is not relevant — only the date component matters.

## Technical view
 
- The chosen storage is a timeseries database. Each data point comprises:
  - A unique metric identifier (metric name + labels)
  - A metric value
  - A metric timestamp with millisecond precision
- Despite the storage carrying full date-and-time precision, the time-of-day component remains semantically irrelevant — values are plotted along a business-day timeline regardless.

## Integration view
 
The integration view was shaped by two goals: shielding producers from the complexity of aligning metric timestamps to business dates, and allowing multiple submissions for the same date with last-write-wins semantics.
 
- The receiving interface must accept multiple submissions for the same business date. In such cases, the most recent submitted value is treated as the only valid value for that day.
- Producers are not required to construct metric timestamps aligned to the business date or to the visualisation timeline. Instead, they supply the business date as a metric label.
- Producers are also not required to supply the metric timestamp — the receiving interface stamps each sample with its wall-clock submission time.
 
## Problem

Carrying business date as a label means each new date creates a new unique label set — and therefore a new timeseries. A single logical metric is fragmented across as many timeseries as there are business dates, rather than forming one continuous series plotted along a date axis.

# Solution

The integration contract remains unchanged — producers continue to submit metrics with the business date as a label and without constructing timestamps. Instead, a new system component is introduced upstream of the timeseries store that converts incoming metrics by:

1. Removing the business date label from the metric
2. Manufacturing a metric timestamp where the date component is taken from the business date and the time component is derived from the submission timestamp using the algorithm below

This produces a single continuous timeseries per logical metric, with each data point placed on the correct business date.

### Timestamp manufacturing algorithm
 
The submission timestamp is linearly mapped into the millisecond range of the business date's day:
 
```
submission_window = [biz_date, biz_date + max_staleness]
offset            = (submission_ts - biz_date) / max_staleness
target_ts         = biz_date_midnight + offset × 86,400,000ms
```
 
Where:
 
- **`biz_date`** — the business date from the metric label, as epoch midnight
- **`submission_ts`** — the wall-clock time the sample was received (stamped by the receiving interface)
- **`max_staleness`** — the maximum allowed delay between a business date and its submission (e.g. 12 months), configurable per deployment
- **`86,400,000ms`** — the number of milliseconds in a day, defining the target range
**Properties:**
 
- **Monotonicity** — later submissions always produce a later target timestamp within the business date's day. This guarantees that resubmissions for the same business date are stored as distinct data points, even in storage engines that deduplicate on identical label set + timestamp.
- **Last-write-wins resolution** — the data point with the highest manufactured timestamp (i.e. the latest submission) represents the most recent value for that business date.
- **Collision resistance** — at 12-month maximum staleness, two submissions must arrive within approximately 6 minutes of each other to collide at millisecond precision. For daily batch submissions this is effectively impossible.
- **Stateless** — the conversion requires only the business date label and the submission timestamp. No lookups, no stored state, no coordination between requests.

