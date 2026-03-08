# Maintenance Assumption Analysis (Presentation-Ready)

## 1) Executive Summary
We built a data-driven proxy for likely maintenance activity because no official maintenance table exists.

Rule used:

1. Bike ends a customer ride at station A.
2. Next customer ride starts at a different station B.
3. Gap between those rides is unusually long (top 5% of relocation gaps, p95).

Headline result:

- `778` suspected maintenance-like events over `8.04` years.
- Mean suspected gap: `80.0` hours.
- Median suspected gap: `4340.5` minutes (~3.0 days).
- Threshold used: `3250.6` minutes (~2.26 days).
- Mean all-relocation gap (benchmark): `15.18` hours.
- Effect size: suspected mean is about `5.27x` the benchmark mean.

## 2) Scale Context (why 778 is not "too small")
Coverage and base volume:

- Time span: `2015-01-04` to `2023-01-17`.
- Total rides: `83,434,866`.
- Relocation events: `15,549`.
- Suspected events: `778`.

Rates:

- `778` is `5.004%` of relocation events.
- `778` is `0.000932%` of all rides.

Interpretation:

- This is rare by design because the method intentionally selects only the extreme tail (top 5% of relocation gaps).

## 3) Why we used log in the visual analysis
### 3.1 Problem with linear scale only
Gap durations are strongly right-skewed with a long tail.
On linear axes, very long gaps stretch the chart and compress most data near zero.

### 3.2 What log axis means
In the log-scale chart we still plot gap hours, but axis positions follow `log10(hours)`.

Key ticks:

- `10^0 = 1` hour
- `10^1 = 10` hours
- `10^2 = 100` hours

So one major tick to the right means 10x larger duration.

### 3.3 How `log10` histogram is created and why
For the transformed chart, each gap is converted:

- `z = log10(gap_hours)`

Examples:

- `1 hour -> 0`
- `10 hours -> 1`
- `100 hours -> 2`

Why this is useful:

- reduces skew
- makes shape diagnostics easier
- allows a normal-reference overlay for visual comparison

Important caveat:

- normal overlay is diagnostic only; it does not prove true normality.

## 4) Graph-by-Graph explanation

### A) Full distribution, log scale
File: `/home/shaun/NTU_M2_Data_Pipeline/docs/images/maintenance_gap_distribution.png`

What it shows:

- all relocation gaps in hours
- red dashed line = p95 cutoff

Why it is important:

- best single chart for seeing both common short gaps and rare long gaps together

### B) Full distribution, linear scale
File: `/home/shaun/NTU_M2_Data_Pipeline/docs/images/maintenance_gap_distribution_linear.png`

What it shows:

- same data as A, but no log scaling

Why it is important:

- plain-unit readability for non-technical audience

Limitation:

- long tail can visually compress the main mass

### C) Suspected-only distribution
File: `/home/shaun/NTU_M2_Data_Pipeline/docs/images/maintenance_gap_distribution_suspected.png`

What it shows:

- only events at/above p95 threshold

Why it is important:

- focuses directly on inferred maintenance-like subset

### D) `log10` histogram + normal reference
File: `/home/shaun/NTU_M2_Data_Pipeline/docs/images/maintenance_gap_log10_normal_reference.png`

What it shows:

- histogram of `log10(gap_hours)`
- fitted normal reference line

Why it is important:

- checks if transformed distribution is closer to bell-shaped than raw hours

### E) Monthly context + monthly rate
File: `/home/shaun/NTU_M2_Data_Pipeline/docs/images/maintenance_events_monthly.png`

What it shows:

Top panel:

- monthly relocation count (blue)
- monthly suspected count (red)

Bottom panel:

- monthly suspected rate (green light)
- 3-month rolling average rate (green dark)

Why it is important:

- avoids false conclusions from count-only trends
- separates volume effects from behavior-rate effects

### F) Day-hour heatmap
File: `/home/shaun/NTU_M2_Data_Pipeline/docs/images/maintenance_events_day_hour_heatmap.png`

What it shows:

- suspected events by weekday and hour

Why it is important:

- highlights potential recurring operational windows

### G) Top station transitions
File: `/home/shaun/NTU_M2_Data_Pipeline/docs/images/maintenance_top_station_transfers.png`

What it shows:

- most frequent origin->destination transitions among suspected events

Why it is important:

- helps identify repeat movement corridors potentially linked to servicing logistics

### H) Duration benchmark comparison
File: `/home/shaun/NTU_M2_Data_Pipeline/docs/images/maintenance_duration_comparison.png`

What it shows:

- mean all-relocation duration
- p95 cutoff
- median suspected duration
- mean suspected duration

Why it is important:

- gives direct evidence that suspected events are not just slightly longer, but materially longer
- communicates maintenance-likeness strength in one slide

## 5) Business-safe conclusion
Most defensible wording:

- "The ride-log evidence shows recurring long-duration relocation patterns that are operationally consistent with maintenance or servicing activity, but does not alone prove an official maintenance schedule."

## 6) Thought process and ascertainment
Reasoning chain (first-person framing):

1. I do not have maintenance logs, so I infer from movement and downtime behavior.
2. A bike ending at one station and next starting at another implies non-customer repositioning.
3. Extremely long such gaps are more plausibly maintenance-like than routine balancing.
4. I need both absolute counts and normalized rates to avoid misleading conclusions.

What these results can ascertain:

1. A measurable, repeatable maintenance-like signal exists.
2. Suspected events are much longer than baseline relocation behavior.
3. The inference is strong enough for operational hypothesis and monitoring use.

What these results cannot ascertain alone:

1. Verified maintenance work-order confirmation per event.
2. Exact repair/service type.
3. Formal maintenance schedule policy without external maintenance data.

## 7) Reproducibility commands
Run maintenance summary query:
```bash
duckdb /home/shaun/NTU_M2_Data_Pipeline/data/warehouse/london_bikes.db < /home/shaun/NTU_M2_Data_Pipeline/queries/maintenance_assumption_percentile.sql
```

Regenerate all charts:
```bash
conda run -n london-bikes-env python /home/shaun/NTU_M2_Data_Pipeline/scripts/plot_maintenance_assumption.py
```
