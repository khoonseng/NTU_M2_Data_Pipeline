# Revised Maintenance Inference Experiment (Based on My Hypotheses)

## 1) Why I Revised This
I revised the model because my objections were valid:

1. `2h-5.64h` is likely too short to represent maintenance.
2. `5.64h-56.58h` is too broad for one preventive-maintenance class.
3. Rebalancing (moving bikes to popular stations) can look like short downtime.
4. If a bike appears to have preventive maintenance many times per year, the assumption is weak.

So I shifted from the older "single broad preventive band" to a hypothesis-driven model that separates:
- rebalancing,
- short repair,
- preventive maintenance (capped),
- repeated downtime,
- long repair,
- out-of-service.

## 2) Where the Math and Calculations Are Done

### SQL (core math and class logic)
- `/home/shaun/NTU_M2_Data_Pipeline/queries/maintenance_hypothesis_revised.sql`

I use this SQL file to compute:
1. station popularity threshold (top 10% stations by starts),
2. relocation gaps in minutes/hours,
3. per-bike-year preventive candidate ranking,
4. final class assignment,
5. summary statistics,
6. preventive-frequency diagnostics,
7. demand-station correlation by class.

### Python (charting + export tables)
- `/home/shaun/NTU_M2_Data_Pipeline/scripts/plot_maintenance_hypothesis_revised.py`

I use this script to:
1. queries base relocation events,
2. applies revised classification,
3. generates revised charts,
4. exports CSV tables used in documentation.

### Output tables (generated)
- `/home/shaun/NTU_M2_Data_Pipeline/docs/data/maintenance_h2_class_summary.csv`
- `/home/shaun/NTU_M2_Data_Pipeline/docs/data/maintenance_h2_bike_year_preventive_counts.csv`
- `/home/shaun/NTU_M2_Data_Pipeline/docs/data/maintenance_h2_demand_share_by_class.csv`

## 3) Libraries Used

### SQL layer
- DuckDB SQL engine (`duckdb` CLI / Python DuckDB connector)

### Python analysis / plotting layer
- `duckdb` (database connection)
- `pandas` (table transforms)
- `numpy` (quantiles/statistics)
- `matplotlib` (plot canvas and export)
- `seaborn` (plot styling)

## 4) Exact Hypotheses and Decision Rules

### H1: Short gap + popular destination implies rebalancing
- If `gap_hours < 8` and destination station is in top 10% by starts:
  - `Demand rebalancing likely (<8h + top-demand destination)`

### H2: Very short but non-popular destination is operational/unknown short idle
- If `gap_hours < 8` and destination is not top-demand:
  - `Operational / unknown short idle (<8h, non-demand destination)`

### H3: Short repair window
- If `8 <= gap_hours < 24`:
  - `Short repair likely (8h-24h)`

### H4: Preventive window and frequency cap
- Candidate window: `24h-96h`
- Rank candidates within each `bike_id + year` by descending `gap_hours`
- Keep only top `2` candidates as preventive:
  - `Preventive maintenance likely (24h-96h, max 2 per bike-year)`
- Remaining events in same window become:
  - `Repeat downtime beyond preventive cap (24h-96h)`

### H5: Long repair and out-of-service
- If `96 < gap_hours <= 336`: `Long repair likely (96h-336h)`
- If `gap_hours > 336`: `Out-of-service candidate (>336h)`

## 5) Technical Formulas (Plain + Formal)

### 5.1 Gap calculation
- Plain: minutes/hours between previous ride end and next ride start for same bike.
- SQL:
  - `gap_minutes = DATE_DIFF('minute', prev_end_time, next_start_time)`
  - `gap_hours = gap_minutes / 60.0`

### 5.2 Popular station threshold
- Plain: station is "popular" if its start count is at/above the 90th percentile.
- SQL:
  - `popular_station_cutoff = quantile_cont(station_start_count, 0.90)`

### 5.3 Preventive cap logic
- Plain: within 24h-96h, each bike can have at most 2 preventive events per year.
- SQL window:
  - `ROW_NUMBER() OVER (PARTITION BY bike_id, event_year ORDER BY gap_hours DESC, prev_end_time)`

### 5.4 Summary statistics
- Mean gap: `AVG(gap_hours)`
- Median gap: `quantile_cont(gap_hours, 0.50)`
- P90 gap: `quantile_cont(gap_hours, 0.90)`
- Share: `COUNT(class) / COUNT(all)`

## 6) Revised Results (Current Run)

### 6.1 Class summary
1. Demand rebalancing likely (<8h + top-demand destination)
- Count: `946,608`
- Share: `16.4156%`
- Mean gap: `2.87h`
- Median gap: `2.22h`

2. Operational / unknown short idle (<8h, non-demand destination)
- Count: `1,014,006`
- Share: `17.5843%`
- Mean gap: `3.85h`
- Median gap: `3.65h`

3. Short repair likely (8h-24h)
- Count: `1,794,268`
- Share: `31.1152%`
- Mean gap: `15.65h`
- Median gap: `15.23h`

4. Preventive maintenance likely (24h-96h, max 2 per bike-year)
- Count: `228,461`
- Share: `3.9618%`
- Mean gap: `72.93h`
- Median gap: `75.48h`

5. Repeat downtime beyond preventive cap (24h-96h)
- Count: `1,232,785`
- Share: `21.3783%`
- Mean gap: `40.48h`
- Median gap: `37.40h`

6. Long repair likely (96h-336h)
- Count: `442,812`
- Share: `7.6790%`
- Mean gap: `170.71h`
- Median gap: `155.28h`

7. Out-of-service candidate (>336h)
- Count: `107,588`
- Share: `1.8657%`
- Mean gap: `1320.97h`
- Median gap: `565.03h`

### 6.2 Preventive frequency diagnostic per bike-year
- Bike-year rows: `124,161`
- Avg preventive events per bike-year: `1.84`
- Median preventive events per bike-year: `2.0`
- `% with 0 preventive`: `4.91%`
- `% with 1 preventive`: `6.17%`
- `% with 2 preventive`: `88.92%`
- `% with >2 preventive`: `0.00%` (enforced by rule)

### 6.3 Demand-station correlation by class
- Rebalancing class to top-demand station: `100.00%` (by design)
- Short repair class to top-demand station: `33.61%`
- Preventive class to top-demand station: `19.56%`
- Repeat downtime class to top-demand station: `23.11%`

Interpretation:
- I treat this as support for my concern that short gaps are strongly mixed with operational logistics.
- Preventive class is now narrower and frequency-capped.

## 7) New / Revised Charts

Generated revised chart set:
- `/home/shaun/NTU_M2_Data_Pipeline/docs/images/M14_maintenance_h2_gap_distribution_thresholds.png`
- `/home/shaun/NTU_M2_Data_Pipeline/docs/images/M15_maintenance_h2_class_counts.png`
- `/home/shaun/NTU_M2_Data_Pipeline/docs/images/M16_maintenance_h2_monthly_mix.png`
- `/home/shaun/NTU_M2_Data_Pipeline/docs/images/M17_maintenance_h2_preventive_per_bike_year.png`
- `/home/shaun/NTU_M2_Data_Pipeline/docs/images/M18_maintenance_h2_top_demand_share_by_class.png`

Optional context chart still available:
- `/home/shaun/NTU_M2_Data_Pipeline/docs/images/M13_maintenance_gap_boxplot_overall.png`

### What each revised chart answers
1. `M14_maintenance_h2_gap_distribution_thresholds.png`
- Shows full gap shape and where decision boundaries sit (8h, 24h, 96h, 336h).

2. `M15_maintenance_h2_class_counts.png`
- Shows event volume split across revised classes.

3. `M16_maintenance_h2_monthly_mix.png`
- Shows how class composition changes month to month.

4. `M17_maintenance_h2_preventive_per_bike_year.png`
- Shows distribution of preventive counts per bike-year under cap logic.

5. `M18_maintenance_h2_top_demand_share_by_class.png`
- Shows how strongly each class correlates with popular stations.

## 8) Outliers: Removed or Not?
No outliers were removed from core classification.

- Outlier logic is used only for interpretation checks (e.g., boxplot fences).
- Main class assignment uses hypothesis thresholds and cap rules, not row deletion.

## 9) Line-by-Line Script Explanation Requirement
I implemented this directly in code comments.

- Script with line-by-line layperson comments:
  - `/home/shaun/NTU_M2_Data_Pipeline/scripts/plot_maintenance_hypothesis_revised.py`

- SQL with detailed first-person comments:
  - `/home/shaun/NTU_M2_Data_Pipeline/queries/maintenance_hypothesis_revised.sql`

## 10) Reproducibility Commands
Run revised SQL report:
```bash
duckdb /home/shaun/NTU_M2_Data_Pipeline/data/warehouse/london_bikes.db < /home/shaun/NTU_M2_Data_Pipeline/queries/maintenance_hypothesis_revised.sql
```

Run revised Python charts + CSV exports:
```bash
conda run -n london-bikes-env python /home/shaun/NTU_M2_Data_Pipeline/scripts/plot_maintenance_hypothesis_revised.py
```

## 11) Caveats (Important)
1. I still treat this as an inference model, not direct work-order truth.
2. The preventive cap (`max 2`) is assumption-driven; it improves interpretability but is a policy choice.
3. If/when true maintenance logs become available, this model should be validated and recalibrated.

## 12) Final Detailed Conclusion
1. My challenge to the old broad preventive band was correct.
2. The revised model separates short logistics-like behavior from maintenance-like behavior more credibly.
3. Per-bike-year diagnostics show raw `24h-96h` events are strongly usage-driven:
   - `corr(usage, raw 24h-96h count) = 0.8801`
   - raw average `24h-96h` events per bike-year = `11.77`
4. I conclude that duration alone is not enough to call preventive maintenance.
5. The capped rule (`max 2 preventive per bike-year`) makes preventive classification conservative and operationally usable:
   - avg assigned preventive per bike-year = `1.84`
   - median assigned preventive per bike-year = `2`
   - `% bike-year with >2 assigned preventive = 0.00%` (rule-enforced)
6. Rebalancing signal is supported:
   - `<8h` bike-year top-demand share median = `46.67%`
   - mean = `43.14%`
7. Overall verdict:
   - Rebalancing hypothesis: supported.
   - Broad preventive-by-duration hypothesis: not supported.
   - Revised capped preventive hypothesis: acceptable as a practical inference rule, but still not proof without maintenance logs.

## 13) Operational Scheduling Rule I Can Use Now
### Why I need this
I do not have explicit preventive-maintenance records, and my current tests do not prove a true preventive schedule from observed relocation gaps alone.
So I need an operational scheduling rule that is practical, transparent, and robust to skewed data.

### Statistic I choose
I choose a **quantile-based trigger on cumulative usage minutes**, not a mean-based trigger.

Technical definition:
1. Define `T` as cumulative usage minutes before first high-risk downtime event.
2. Preferred high-risk event: `gap_hours >= 96` (long repair/out-of-service risk).
3. Temporary proxy until full trigger study is complete: first `24h-96h` event usage.
4. Schedule interval rule: `S = Qp(T)` where `p` is policy percentile.

### Why quantile is better than mean for this case
1. My distribution is right-skewed with heavy tail behavior.
2. Mean gets pulled by extreme values and is less stable operationally.
3. Quantiles let me directly choose risk appetite:
   - `Q50`: aggressive
   - `Q75`: balanced
   - `Q90`: conservative

### Values from my current proxy analysis
Using first `24h-96h` hit as proxy:
1. `Q50(T) = 426` minutes (`~7.10` usage hours)
2. `Q75(T) = 1035` minutes (`~17.25` usage hours)
3. `Q90(T) = 2288` minutes (`~38.13` usage hours)

### Recommended start policy
1. I start at `Q75` (`~1000-1050` usage minutes) as my default preventive trigger.
2. I apply a calendar backstop (for example monthly) for low-usage bikes.
3. I monitor post-policy rates of `>=96h` and `>336h` gaps and tune percentile if needed.

### What this does and does not mean
1. This gives me a defensible operational schedule proposal from available data.
2. This does not prove true workshop preventive cycles without maintenance logs.
