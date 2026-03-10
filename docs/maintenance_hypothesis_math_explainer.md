# Maintenance Hypothesis Math Explainer (Technical + Layperson)

## 1) What My Analysis Is (and Is Not)

### Technical
I use a workflow that is primarily a **rule-based inference model** with descriptive statistics and correlation diagnostics. I do not yet run a full inferential statistics pipeline with formal null-hypothesis tests (for example p-values from regression or ANOVA).

### Layperson
I am using data rules to classify likely behaviors (rebalancing, repair, preventive, out-of-service), then checking if those rules behave sensibly. I am not yet claiming formal statistical proof with p-values.

## 2) Where the Math Is Implemented

### Technical
- SQL logic: `/home/shaun/NTU_M2_Data_Pipeline/queries/maintenance_hypothesis_revised.sql`
- Python diagnostics and charting: `/home/shaun/NTU_M2_Data_Pipeline/scripts/plot_maintenance_hypothesis_revised.py`

### Layperson
The heavy math/filtering happens in SQL. Python then reads those results, applies the same assumptions, and makes charts/tables.

## 3) Event Construction Math

### Technical
For each `bike_id`, rides are ordered by `(start_date, rental_id)` and paired with the next ride using `LEAD()`:
- `prev_end_time = end_date (current ride)`
- `next_start_time = LEAD(start_date)`
- `prev_end_station = end_station_id`
- `next_start_station = LEAD(start_station_id)`

Gap metrics:
- `gap_minutes = DATE_DIFF('minute', prev_end_time, next_start_time)`
- `gap_hours = gap_minutes / 60.0`

Filters:
- `next_start_time IS NOT NULL`
- `next_start_station IS NOT NULL`
- `next_start_station <> prev_end_station` (relocation only)
- `gap_minutes >= 0` (remove negative time artifacts)

### Layperson
I track each bike’s rides in time order, then compare one ride to the next ride of the same bike. If the bike reappears at a different station and time moves forward, I keep that event and measure the inactivity gap.

## 4) Popular-Station Threshold Math

### Technical
Station popularity is defined by total start rides:
- `station_start_count = COUNT(*) GROUP BY start_station_id`

Top-demand threshold:
- `popular_station_cutoff = quantile_cont(station_start_count, 0.90)`

Flag:
- `is_top_demand_station = station_start_count >= popular_station_cutoff`

### Layperson
I count how busy each station is. The busiest 10% are labeled “top-demand.” If a bike next appears at one of those, that supports a rebalancing interpretation.

## 5) Revised Hypothesis Rules

### Technical
Class boundaries are deterministic:
1. `gap_hours < 8` and top-demand destination
   - `Demand rebalancing likely`
2. `gap_hours < 8` and non-top-demand destination
   - `Operational / unknown short idle`
3. `8 <= gap_hours < 24`
   - `Short repair likely`
4. `24 <= gap_hours <= 96`
   - candidate window for preventive
5. `96 < gap_hours <= 336`
   - `Long repair likely`
6. `gap_hours > 336`
   - `Out-of-service candidate`

### Layperson
Short gaps to popular stations are likely bike movement for demand. Medium-short gaps look like short repairs. Very long gaps look like long repairs or out-of-service periods.

## 6) Preventive Frequency Cap Math

### Technical
Inside `24h-96h` window, per `(bike_id, event_year)`:
- rank candidates by `gap_hours DESC, prev_end_time`
- `rank <= 2` => preventive
- `rank > 2` => repeat downtime beyond preventive cap

Equivalent SQL window function:
- `ROW_NUMBER() OVER (PARTITION BY bike_id, event_year ORDER BY gap_hours DESC, prev_end_time)`

### Layperson
I only allow up to 2 preventive events per bike per year. If the bike shows many more 24h-96h gaps, I do not call all of them preventive.

## 7) Summary Statistics Used

### Technical
Per class:
- `event_count = COUNT(*)`
- `share_pct = 100 * event_count / total_events`
- `mean_gap_hours = AVG(gap_hours)`
- `median_gap_hours = quantile_cont(gap_hours, 0.50)`
- `p90_gap_hours = quantile_cont(gap_hours, 0.90)`

### Layperson
For each class, I report how many events there are, what % they represent, and typical duration values (average, middle value, and high-end 90th percentile).

## 8) Per-Bike Correlation Diagnostics

### Technical
At bike-year level, Pearson correlations were computed:
- `corr(usage, raw_24_96_count) = 0.8801`
- `corr(usage, assigned_preventive) = 0.4831`
- `corr(usage, overflow_beyond_cap) = 0.8753`

Interpretation:
- Raw 24-96h counts are strongly usage-driven.
- Cap reduces (but does not eliminate) usage linkage in assigned preventive counts.

### Layperson
Bikes used more often naturally show more 24h-96h events. So raw duration counts alone can be misleading. The cap makes preventive labels more conservative and less inflated by heavy-use bikes.

## 9) Additional Signal Check (Short Gaps to Top-Demand)

### Technical
For bike-years with at least one `<8h` event:
- mean top-demand share = `0.4314`
- median top-demand share = `0.4667`
- IQR = `0.3333` to `0.5625`

### Layperson
For short gaps, many bikes frequently reappear at busy stations. That supports the idea that short gaps are often rebalancing/operations, not maintenance.

## 10) What the Math Supports

### Technical
1. Strong right-tail structure in relocation gaps.
2. Duration-only preventive labeling is overly permissive.
3. A capped preventive class gives a more controlled operational heuristic.
4. Short-gap events have meaningful linkage to demand stations.

### Layperson
The numbers back my concern: short and medium gaps are not automatically maintenance. The revised model better separates likely logistics from likely repair/maintenance behavior.

## 11) What the Math Does Not Prove

### Technical
1. No ground-truth maintenance labels are present.
2. No causal inference is established.
3. Cap rule is assumption-driven policy, not discovered law.

### Layperson
I treat this model as a smart estimate, not official proof of workshop actions.

## 12) If I Want Formal Hypothesis Testing Next

### Technical
Possible upgrades:
1. Logistic regression: `Pr(top_demand_destination)` vs duration bucket + controls.
2. Bootstrap confidence intervals for class-rate differences.
3. KS/Mann-Whitney tests between duration distributions of candidate classes.
4. Mixed-effects model at bike level to separate usage effects from latent maintenance behavior.

### Layperson
If needed, I can turn this into formal statistical testing with confidence intervals and significance tests, not just rule-based segmentation.

## 13) Reproduce the Numbers

Run revised SQL diagnostics:
```bash
duckdb /home/shaun/NTU_M2_Data_Pipeline/data/warehouse/london_bikes.db < /home/shaun/NTU_M2_Data_Pipeline/queries/maintenance_hypothesis_revised.sql
```

Run revised Python outputs:
```bash
conda run -n london-bikes-env python /home/shaun/NTU_M2_Data_Pipeline/scripts/plot_maintenance_hypothesis_revised.py
```
