# Maintenance Charts Reference (Numbered, Technical + Layperson)

## Purpose of this document
This file is a single reference for every maintenance-related chart in `/docs/images`.

For each chart, I provide:
1. A chart number for easy discussion.
2. The chart filename.
3. A layperson explanation.
4. A technical explanation.
5. What inference can be made.
6. What it does not prove.

## Quick index
M01. `maintenance_gap_distribution.png` (legacy model context)
M02. `maintenance_gap_distribution_linear.png` (legacy model context)
M03. `maintenance_gap_distribution_suspected.png` (legacy model context)
M04. `maintenance_gap_log10_normal_reference.png` (legacy model context)
M05. `maintenance_events_monthly.png` (legacy model context)
M06. `maintenance_events_monthly_suspected_zoom.png` (legacy model context)
M07. `maintenance_events_day_hour_heatmap.png` (legacy model context)
M08. `maintenance_top_station_transfers.png` (legacy model context)
M09. `maintenance_duration_comparison.png` (legacy model context)
M10. `maintenance_inferred_state_counts.png` (legacy model context)
M11. `maintenance_inferred_state_monthly_mix.png` (legacy model context)
M12. `maintenance_service_classes_log10_boxplot.png` (legacy model context)
M13. `maintenance_gap_boxplot_overall.png` (distribution diagnostics)
M14. `maintenance_h2_gap_distribution_thresholds.png` (revised hypothesis model)
M15. `maintenance_h2_class_counts.png` (revised hypothesis model)
M16. `maintenance_h2_monthly_mix.png` (revised hypothesis model)
M17. `maintenance_h2_preventive_per_bike_year.png` (revised hypothesis model)
M18. `maintenance_h2_top_demand_share_by_class.png` (revised hypothesis model)

## Model context
The charts come from two different logic versions:
1. Legacy model: p95-tail + broad maintenance inference.
2. Revised model (H2): your updated assumptions:
   - short gaps can be rebalancing,
   - preventive window is narrower,
   - preventive frequency is capped per bike-year,
   - longer gaps are repair/out-of-service candidates.

When presenting conclusions, prioritize M14-M18.

---

## M01 - `maintenance_gap_distribution.png`
**Layperson meaning**
This is the big picture of all relocation gap durations. It shows how long bikes are inactive between one ride ending and the next ride starting at a different station.

**Technical meaning**
Histogram of all relocation `gap_hours` on a log-scale x-axis. Includes a vertical p95 reference line from the legacy threshold model.

**What it supports**
It supports that the data is heavy-tailed (many moderate gaps, fewer very long gaps).

**What it does not prove**
It does not prove maintenance activity by itself.

---

## M02 - `maintenance_gap_distribution_linear.png`
**Layperson meaning**
This is the same gap story but zoomed on the common range so the shape is easier to read.

**Technical meaning**
Linear-scale histogram, clipped at the 99th percentile to avoid tail dominance.

**What it supports**
It helps communicate where most observations sit in raw hours.

**What it does not prove**
It does not identify maintenance vs repair without additional rules.

---

## M03 - `maintenance_gap_distribution_suspected.png`
**Layperson meaning**
This shows only the longest gaps under the old rule.

**Technical meaning**
Distribution of legacy “suspected” events (`gap >= p95`).

**What it supports**
It shows how spread-out extreme inactivity is.

**What it does not prove**
It does not prove those extreme gaps are maintenance; they may include out-of-service periods.

---

## M04 - `maintenance_gap_log10_normal_reference.png`
**Layperson meaning**
This checks whether the log-transformed gap values behave more regularly than raw hours.

**Technical meaning**
Histogram of `log10(gap_hours)` with a normal reference overlay.

**What it supports**
It supports using log-scale methods for thresholding/classification stability.

**What it does not prove**
It does not prove strict normality or causal operations behavior.

---

## M05 - `maintenance_events_monthly.png`
**Layperson meaning**
This compares monthly total relocation volume with monthly long-gap events under the legacy rule.

**Technical meaning**
Dual-axis top panel: relocation counts vs suspected counts. Bottom panel: suspected rate and rolling average.

**What it supports**
It supports time-trend monitoring and seasonality checks.

**What it does not prove**
It does not prove planned maintenance schedules.

---

## M06 - `maintenance_events_monthly_suspected_zoom.png`
**Layperson meaning**
This is a clearer month-by-month view of legacy suspected counts.

**Technical meaning**
Monthly suspected count series with smoothed trend.

**What it supports**
It helps detect trend shifts and periods of unusually high/low tail activity.

**What it does not prove**
It does not identify root cause.

---

## M07 - `maintenance_events_day_hour_heatmap.png`
**Layperson meaning**
This shows what days and hours legacy suspected events happen most.

**Technical meaning**
2D heatmap of event counts by weekday x hour.

**What it supports**
It supports operational-timing hypotheses (for example, overnight patterns).

**What it does not prove**
It does not prove workshop shifts or staffing patterns.

---

## M08 - `maintenance_top_station_transfers.png`
**Layperson meaning**
This shows the most common station-to-station transitions in legacy suspected events.

**Technical meaning**
Top origin→destination pairs ranked by count.

**What it supports**
It supports identifying potentially important network movement corridors.

**What it does not prove**
It does not prove a transfer is maintenance rather than redistribution.

---

## M09 - `maintenance_duration_comparison.png`
**Layperson meaning**
Quick side-by-side duration comparison between all relocations and legacy extreme-tail events.

**Technical meaning**
Bar chart of selected summary metrics (mean all, p95 threshold, median/mean suspected).

**What it supports**
It supports communicating how much longer tail events are than the baseline.

**What it does not prove**
It does not classify event intent.

---

## M10 - `maintenance_inferred_state_counts.png`
**Layperson meaning**
This shows counts for the old classification model’s operational states.

**Technical meaning**
Legacy class count chart derived from log-space z-band rules.

**What it supports**
It supports a first-pass segmentation of downtime regimes.

**What it does not prove**
It does not validate class labels against true maintenance logs.

---

## M11 - `maintenance_inferred_state_monthly_mix.png`
**Layperson meaning**
This shows how old-model state proportions changed over months.

**Technical meaning**
Stacked monthly area chart by legacy inferred class.

**What it supports**
It supports monitoring drift in class composition over time.

**What it does not prove**
It does not prove process policy changes without external evidence.

---

## M12 - `maintenance_service_classes_log10_boxplot.png`
**Layperson meaning**
This compares old-model service-related classes on log-transformed duration scale.

**Technical meaning**
Boxplots of `log10(gap_hours)` for short/preventive/long classes (legacy method).

**What it supports**
It supports seeing whether old classes are statistically separated.

**What it does not prove**
It does not prove those classes map to actual maintenance job types.

---

## M13 - `maintenance_gap_boxplot_overall.png`
**Layperson meaning**
This shows central spread and outlier boundaries for all relocation gaps.

**Technical meaning**
Two-panel boxplot: raw hours and `log10(hours)`, with 1.5*IQR fence diagnostics.

**What it supports**
It supports quantifying tail/outlier presence and explaining mean vs median differences.

**What it does not prove**
It does not prove outliers are “bad data”; many may be real prolonged downtime.

---

## M14 - `maintenance_h2_gap_distribution_thresholds.png`
**Layperson meaning**
This is the revised-model backbone chart. It shows all gaps and where new rule boundaries are placed.

**Technical meaning**
Full histogram on log axis with explicit thresholds at:
- 8h (rebalancing boundary)
- 24h (preventive lower bound)
- 96h (preventive upper bound)
- 336h (out-of-service boundary)

**What it supports**
It supports transparency of rule design and how classes partition the distribution.

**What it does not prove**
Threshold locations are assumption-driven, not externally validated truth.

---

## M15 - `maintenance_h2_class_counts.png`
**Layperson meaning**
This shows how many events fall into each revised class.

**Technical meaning**
Bar chart by revised class labels with share percentages.

**What it supports**
It supports that revised classes produce a practical segmentation, including a small conservative preventive class.

**What it does not prove**
It does not prove class correctness without labels from maintenance records.

---

## M16 - `maintenance_h2_monthly_mix.png`
**Layperson meaning**
This shows how revised classes change month to month across years.

**Technical meaning**
Stacked monthly area chart of revised class counts.

**What it supports**
It supports monitoring operational regime changes and potential anomalies.

**What it does not prove**
It does not prove causality for time shifts.

---

## M17 - `maintenance_h2_preventive_per_bike_year.png`
**Layperson meaning**
This chart answers: how often does each bike get preventive events in a year under the revised cap rule.

**Technical meaning**
Distribution of preventive count buckets per bike-year (`0`, `1`, `2`, `3+` where `3+` should be 0 due to cap).

**What it supports**
It supports that the cap-enforced preventive class obeys your policy assumption (no >2 assigned preventive events per bike-year).

**What it does not prove**
It does not prove true preventive frequency in reality; it proves rule behavior.

---

## M18 - `maintenance_h2_top_demand_share_by_class.png`
**Layperson meaning**
This compares how strongly each class is linked to popular destination stations.

**Technical meaning**
Class-level percentage of events ending at top-demand stations.

**What it supports**
It supports the logistics/rebalancing interpretation for short-gap demand-linked events.

**What it does not prove**
It does not fully separate rebalancing from other operational processes without route/depot/work-order data.

---

## What can be claimed overall
1. Relocation downtime has multiple regimes and a long tail.
2. Short-gap demand-linked behavior is consistent with rebalancing.
3. A narrow, capped preventive class is possible as an operational heuristic.
4. Longer durations are better treated as repair/out-of-service candidates than preventive.

## What cannot be claimed from charts alone
1. Exact maintenance action per event.
2. Exact repair type or severity.
3. Official maintenance schedules.
4. Ground-truth correctness of inferred labels without maintenance logs.

## Recommended chart set for reporting
Use these as the primary set for current conclusions:
1. M14 (`maintenance_h2_gap_distribution_thresholds.png`)
2. M15 (`maintenance_h2_class_counts.png`)
3. M16 (`maintenance_h2_monthly_mix.png`)
4. M17 (`maintenance_h2_preventive_per_bike_year.png`)
5. M18 (`maintenance_h2_top_demand_share_by_class.png`)

Keep M13 as diagnostics appendix.
