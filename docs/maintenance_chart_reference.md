# Maintenance Charts Reference (Numbered, Technical + Layperson)

## Purpose of this document
I use this file as a single reference for every maintenance-related chart in `/docs/images`.

For each chart, I provide:
1. A chart number for easy discussion.
2. The chart filename.
3. A layperson explanation.
4. A technical explanation.
5. What inference can be made.
6. What it does not prove.

## Quick index
M01. `M01_maintenance_gap_distribution.png` (legacy model context)
M02. `M02_maintenance_gap_distribution_linear.png` (legacy model context)
M03. `M03_maintenance_gap_distribution_suspected.png` (legacy model context)
M04. `M04_maintenance_gap_log10_normal_reference.png` (legacy model context)
M05. `M05_maintenance_events_monthly.png` (legacy model context)
M06. `M06_maintenance_events_monthly_suspected_zoom.png` (legacy model context)
M07. `M07_maintenance_events_day_hour_heatmap.png` (legacy model context)
M08. `M08_maintenance_top_station_transfers.png` (legacy model context)
M09. `M09_maintenance_duration_comparison.png` (legacy model context)
M10. `M10_maintenance_inferred_state_counts.png` (legacy model context)
M11. `M11_maintenance_inferred_state_monthly_mix.png` (legacy model context)
M12. `M12_maintenance_service_classes_log10_boxplot.png` (legacy model context)
M13. `M13_maintenance_gap_boxplot_overall.png` (distribution diagnostics)
M14. `M14_maintenance_h2_gap_distribution_thresholds.png` (revised hypothesis model)
M15. `M15_maintenance_h2_class_counts.png` (revised hypothesis model)
M16. `M16_maintenance_h2_monthly_mix.png` (revised hypothesis model)
M17. `M17_maintenance_h2_preventive_per_bike_year.png` (revised hypothesis model)
M18. `M18_maintenance_h2_top_demand_share_by_class.png` (revised hypothesis model)
M19. `M19_maintenance_gap_distribution_linear_body_0_200h.png` (linear body view, 0-200h with coverage disclaimer)
M20. `M20_maintenance_gap_linear_vs_log_comparison.png` (Linear Body 2 vs log view for same range)

## Model context
The charts come from two different logic versions:
1. Legacy model: p95-tail + broad maintenance inference.
2. Revised model (H2): my updated assumptions:
   - short gaps can be rebalancing,
   - preventive window is narrower,
   - preventive frequency is capped per bike-year,
   - longer gaps are repair/out-of-service candidates.

When presenting conclusions, prioritize M14-M18.

---

## M01 - `M01_maintenance_gap_distribution.png`
**Layperson meaning**
I use this as the big picture of all relocation gap durations. I use it to show how long bikes are inactive between one ride ending and the next ride starting at a different station.

**Technical meaning**
Histogram of all relocation `gap_hours` on a log-scale x-axis. Includes a vertical p95 reference line from the legacy threshold model.

**What it supports**
I use this to support that the data is heavy-tailed (many moderate gaps, fewer very long gaps).

**What it does not prove**
I do not claim this proves maintenance activity by itself.

### Log-scale reading guide (important for M01, M03, M14)
I use this chart with a logarithmic x-axis, so axis ticks are powers of 10.

Examples:
- `10^-2 = 0.01`
- `10^-1 = 0.1`
- `10^0 = 1`
- `10^1 = 10`
- `10^2 = 100`

What this means visually:
1. Equal spacing on the axis means equal change in exponent (not equal change in raw value).
2. Moving one tick to the right multiplies value by 10.
3. So the visual step from `0.1` to `1` is the same width as `1` to `10`.

Why this is used:
1. Gap durations span tiny to very large values.
2. Linear spacing would compress small values and stretch large values too much.
3. Log spacing shows the full range more clearly.

How this differs from `1, 2, 4, 8, 16`:
1. `1, 2, 4, 8, 16` are powers of 2 (`2^0, 2^1, 2^2...`), each step is `x2`.
2. `10^-2, 10^-1, 10^0, 10^1...` are powers of 10, each step is `x10`.
3. Both are multiplicative sequences; only the base changes.

What “log-spaced bins” means in this project:
1. Histogram bin edges were created using a geometric sequence (`np.geomspace`).
2. Bin edges grow by multiplication rather than constant addition.
3. Combined with a log x-axis, this prevents almost all observations from collapsing into the first bin.

---

## M02 - `M02_maintenance_gap_distribution_linear.png`
**Layperson meaning**
I use this as the same gap story but zoomed on the common range so the shape is easier to read.

**Technical meaning**
Linear-scale histogram, clipped at the 99th percentile to avoid tail dominance.

**What it supports**
I use this to help communicate where most observations sit in raw hours.

**What it does not prove**
I do not claim this identifies maintenance vs repair without additional rules.

---

## M03 - `M03_maintenance_gap_distribution_suspected.png`
**Layperson meaning**
I use this to show only the longest gaps under the old rule.

**Technical meaning**
Distribution of legacy “suspected” events (`gap >= p95`).

**What it supports**
I use this to show how spread-out extreme inactivity is.

**What it does not prove**
I do not claim this proves those extreme gaps are maintenance; they may include out-of-service periods.

---

## M04 - `M04_maintenance_gap_log10_normal_reference.png`
**Layperson meaning**
I check whether the log-transformed gap values behave more regularly than raw hours.

**Technical meaning**
Histogram of `log10(gap_hours)` with a normal reference overlay.

**What it supports**
I use this to support using log-scale methods for thresholding/classification stability.

**What it does not prove**
I do not claim this proves strict normality or causal operations behavior.

---

## M05 - `M05_maintenance_events_monthly.png`
**Layperson meaning**
I use this to compare monthly total relocation volume with monthly long-gap events under the legacy rule.

**Technical meaning**
Dual-axis top panel: relocation counts vs suspected counts. Bottom panel: suspected rate and rolling average.

**What it supports**
I use this to support time-trend monitoring and seasonality checks.

**What it does not prove**
I do not claim this proves planned maintenance schedules.

---

## M06 - `M06_maintenance_events_monthly_suspected_zoom.png`
**Layperson meaning**
I use this as a clearer month-by-month view of legacy suspected counts.

**Technical meaning**
Monthly suspected count series with smoothed trend.

**What it supports**
I use this to help detect trend shifts and periods of unusually high/low tail activity.

**What it does not prove**
I do not claim this identifies root cause.

---

## M07 - `M07_maintenance_events_day_hour_heatmap.png`
**Layperson meaning**
I use this to show what days and hours legacy suspected events happen most.

**Technical meaning**
2D heatmap of event counts by weekday x hour.

**What it supports**
I use this to support operational-timing hypotheses (for example, overnight patterns).

**What it does not prove**
I do not claim this proves workshop shifts or staffing patterns.

---

## M08 - `M08_maintenance_top_station_transfers.png`
**Layperson meaning**
I use this to show the most common station-to-station transitions in legacy suspected events.

**Technical meaning**
Top origin→destination pairs ranked by count.

**What it supports**
I use this to support identifying potentially important network movement corridors.

**What it does not prove**
I do not claim this proves a transfer is maintenance rather than redistribution.

---

## M09 - `M09_maintenance_duration_comparison.png`
**Layperson meaning**
Quick side-by-side duration comparison between all relocations and legacy extreme-tail events.

**Technical meaning**
Bar chart of selected summary metrics (mean all, p95 threshold, median/mean suspected).

**What it supports**
I use this to support communicating how much longer tail events are than the baseline.

**What it does not prove**
I do not claim this classifies event intent.

---

## M10 - `M10_maintenance_inferred_state_counts.png`
**Layperson meaning**
I use this to show counts for the old classification model’s operational states.

**Technical meaning**
Legacy class count chart derived from log-space z-band rules.

**What it supports**
I use this to support a first-pass segmentation of downtime regimes.

**What it does not prove**
I do not claim this validates class labels against true maintenance logs.

---

## M11 - `M11_maintenance_inferred_state_monthly_mix.png`
**Layperson meaning**
I use this to show how old-model state proportions changed over months.

**Technical meaning**
Stacked monthly area chart by legacy inferred class.

**What it supports**
I use this to support monitoring drift in class composition over time.

**What it does not prove**
I do not claim this proves process policy changes without external evidence.

---

## M12 - `M12_maintenance_service_classes_log10_boxplot.png`
**Layperson meaning**
I use this to compare old-model service-related classes on log-transformed duration scale.

**Technical meaning**
Boxplots of `log10(gap_hours)` for short/preventive/long classes (legacy method).

**What it supports**
I use this to support seeing whether old classes are statistically separated.

**What it does not prove**
I do not claim this proves those classes map to actual maintenance job types.

---

## M13 - `M13_maintenance_gap_boxplot_overall.png`
**Layperson meaning**
I use this to show central spread and outlier boundaries for all relocation gaps.

**Technical meaning**
Two-panel boxplot: raw hours and `log10(hours)`, with 1.5*IQR fence diagnostics.

**What it supports**
I use this to support quantifying tail/outlier presence and explaining mean vs median differences.

**What it does not prove**
I do not claim this proves outliers are “bad data”; many may be real prolonged downtime.

---

## M14 - `M14_maintenance_h2_gap_distribution_thresholds.png`
**Layperson meaning**
I use this as the revised-model backbone chart. I use it to show all gaps and where new rule boundaries are placed.

**Technical meaning**
Full histogram on log axis with explicit thresholds at:
- 8h (rebalancing boundary)
- 24h (preventive lower bound)
- 96h (preventive upper bound)
- 336h (out-of-service boundary)

**What it supports**
I use this to support transparency of rule design and how classes partition the distribution.

**What it does not prove**
Threshold locations are assumption-driven, not externally validated truth.

---

## M15 - `M15_maintenance_h2_class_counts.png`
**Layperson meaning**
I use this to show how many events fall into each revised class.

**Technical meaning**
Bar chart by revised class labels with share percentages.

**What it supports**
I use this to support that revised classes produce a practical segmentation, including a small conservative preventive class.

**What it does not prove**
I do not claim this proves class correctness without labels from maintenance records.

---

## M16 - `M16_maintenance_h2_monthly_mix.png`
**Layperson meaning**
I use this to show how revised classes change month to month across years.

**Technical meaning**
Stacked monthly area chart of revised class counts.

**What it supports**
I use this to support monitoring operational regime changes and potential anomalies.

**What it does not prove**
I do not claim this proves causality for time shifts.

---

## M17 - `M17_maintenance_h2_preventive_per_bike_year.png`
**Layperson meaning**
I use this chart to answer: how often each bike gets preventive events in a year under the revised cap rule.

**Technical meaning**
Distribution of preventive count buckets per bike-year (`0`, `1`, `2`, `3+` where `3+` should be 0 due to cap).

**What it supports**
I use this to support that the cap-enforced preventive class obeys my policy assumption (no >2 assigned preventive events per bike-year).

**What it does not prove**
I do not claim this proves true preventive frequency in reality; it proves rule behavior.

---

## M18 - `M18_maintenance_h2_top_demand_share_by_class.png`
**Layperson meaning**
I use this to compare how strongly each class is linked to popular destination stations.

**Technical meaning**
Class-level percentage of events ending at top-demand stations.

**What it supports**
I use this to support the logistics/rebalancing interpretation for short-gap demand-linked events.

**What it does not prove**
I do not claim this fully separates rebalancing from other operational processes without route/depot/work-order data.

---

## M19 - `M19_maintenance_gap_distribution_linear_body_0_200h.png`
**Layperson meaning**
I use this as a focused linear chart of the core range (`0-200h`) so the main distribution is readable.

**Technical meaning**
Histogram on raw linear hours with x-axis capped at `200h`, plus a disclaimer that reports coverage percentage and full-data max context.

**What it supports**
I use this to support practical reading of the bulk distribution where most events occur.

**What it does not prove**
I do not claim this shows the entire extreme tail on-axis; it is a body-view chart by design.

---

## M20 - `M20_maintenance_gap_linear_vs_log_comparison.png`
**Layperson meaning**
I use this as a side-by-side demonstration using the Linear Body 2 window (`0-200h`) and a log view of that same window.

**Technical meaning**
Two panels using identical relocation events in the same capped range (`<=200h`):
1. Left: linear histogram (`0-200h`).
2. Right: log-scale histogram with log-spaced bins (`<=200h`).

The chart includes summary stats:
- mean
- p50
- p95
- p99
- max
- `p99/p50` ratio as a right-tail intensity indicator.

**What it supports**
I use this to support how a log view reveals structure inside the same body range that looks compressed in linear view.

**What it does not prove**
I do not claim this proves any operational cause (maintenance vs rebalancing vs repair); it only shows statistical shape.

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
I use these as the primary set for current conclusions:
1. M14 (`M14_maintenance_h2_gap_distribution_thresholds.png`)
2. M15 (`M15_maintenance_h2_class_counts.png`)
3. M16 (`M16_maintenance_h2_monthly_mix.png`)
4. M17 (`M17_maintenance_h2_preventive_per_bike_year.png`)
5. M18 (`M18_maintenance_h2_top_demand_share_by_class.png`)

Keep M13 as diagnostics appendix.
