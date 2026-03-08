# DuckDB Access + Maintenance Inference (Full Session Update)

## 1) What we set out to do
Today we did two things end-to-end:

1. Set up reliable DuckDB access from VS Code terminal (CLI workflow).
2. Build and validate an assumption-based method to detect likely maintenance-like bike downtime from ride logs.

This was needed because your team does not have a confirmed maintenance/work-order table.

## 2) What we did, step by step

1. Confirmed you were in the correct conda environment: `london-bikes-env`.
2. Installed DuckDB CLI in that env so `duckdb` command works in terminal.
3. Opened the DB file with CLI:
```bash
duckdb /home/shaun/NTU_M2_Data_Pipeline/data/warehouse/london_bikes.db
```
4. Checked tables using `SHOW TABLES;` and initially saw `0 rows`.
5. Verified attached DB path with `PRAGMA database_list;`.
6. Discovered tables are in schema `london_bicycles`, so we queried via fully qualified names.
7. Confirmed data exists:
```sql
SELECT COUNT(*) FROM london_bicycles.staging_cycle_hire;
```
Result: `83,434,866` rows.
8. Built reusable SQL files in `/queries`.
9. Ran assumption query and got summary metrics.
10. Added plots and refined chart design after your feedback (full distribution, linear vs log, monthly context/rate).

## 3) Core assumption we used
Because there is no official maintenance table, we used this operational assumption:

1. A bike finishes a customer ride at station A.
2. Its next customer ride starts at a different station B.
3. The idle gap between those rides is unusually long.

To define “unusually long,” we used the dataset’s 95th percentile of relocation gaps.

In plain terms: we kept the longest 5% of relocation gaps.

## 4) Key results from your run
From your run of `maintenance_assumption_percentile.sql`:

- `suspected_events`: `778`
- `mean_gap_minutes`: `4800.29`
- `mean_gap_hours`: `80.0`
- `median_gap_minutes`: `4340.5`
- `max_gap_minutes`: `9285.0`
- `threshold_minutes_used`: `3250.6`

Interpretation in plain language:

- We found 778 events matching your maintenance-like assumption.
- Average downtime for these events is about 80 hours (~3.3 days).
- Typical downtime (median) is about 4340.5 minutes (~3.0 days).
- Longest observed downtime is about 9285 minutes (~6.45 days).
- The “long gap” cutoff used was about 3250.6 minutes (~2.26 days).

Additional benchmark (all relocation events, not tail-only):

- Mean gap for all relocation events: `910.77` minutes (`15.18` hours).
- Mean gap for suspected events: `4800.29` minutes (`80.0` hours).
- Relative magnitude: suspected mean is about `5.27x` larger than normal relocation mean.

## 5) Scale/context check (important)
We validated whether 778 is “small” in context:

- `min_start_date`: `2015-01-04 00:00:00`
- `max_end_date`: `2023-01-17 14:00:00`
- `span_days`: `2935`
- `span_years`: `8.04`
- `total_rides`: `83,434,866`
- `relocation_events`: `15,549`
- `suspected_events`: `778`
- `pct_of_relocations`: `5.004%`
- `pct_of_all_rides`: `0.000932%`

Layperson interpretation:

- 778 is rare relative to all rides, but this is expected because the rule intentionally takes only the extreme tail.
- It is roughly ~97 suspected events per year over the 8.04-year span.

## 6) What this does and does not prove
What this supports:

- There are real long-duration relocation patterns consistent with maintenance/rebalancing operations.

What this does not prove:

- It does not confirm a formal maintenance schedule by itself.
- It does not identify workshop location, technician action, or repair type.
- Some events may still be non-maintenance operations.

## 7) Files created/updated

1. SQL query (station-specific relocation view):
- `/home/shaun/NTU_M2_Data_Pipeline/queries/bike_relocation_gap.sql`

2. SQL query (assumption summary with p95 threshold):
- `/home/shaun/NTU_M2_Data_Pipeline/queries/maintenance_assumption_percentile.sql`

3. Plot generator script:
- `/home/shaun/NTU_M2_Data_Pipeline/scripts/plot_maintenance_assumption.py`

4. Output plots:
- `/home/shaun/NTU_M2_Data_Pipeline/docs/images/maintenance_gap_distribution.png`
- `/home/shaun/NTU_M2_Data_Pipeline/docs/images/maintenance_gap_distribution_linear.png`
- `/home/shaun/NTU_M2_Data_Pipeline/docs/images/maintenance_gap_distribution_suspected.png`
- `/home/shaun/NTU_M2_Data_Pipeline/docs/images/maintenance_gap_log10_normal_reference.png`
- `/home/shaun/NTU_M2_Data_Pipeline/docs/images/maintenance_events_monthly.png`
- `/home/shaun/NTU_M2_Data_Pipeline/docs/images/maintenance_events_day_hour_heatmap.png`
- `/home/shaun/NTU_M2_Data_Pipeline/docs/images/maintenance_top_station_transfers.png`
- `/home/shaun/NTU_M2_Data_Pipeline/docs/images/maintenance_duration_comparison.png`

## 8) Why some plots use log scale
Gap durations are heavily right-skewed and span a wide range.

On log scale:

- `10^0` = `1`
- `10^1` = `10`
- `10^2` = `100`

If x-axis is hours, those are 1 hour, 10 hours, 100 hours.

This helps us see both short and long gaps in one view.

We kept both versions so you can present clearly:

1. Log-scale full distribution (shape visibility).
2. Linear full distribution (plain-scale intuition).
3. `log10(gap)` + normal-reference overlay (to check whether transformed data looks closer to normal).

## 9) About negative gaps
A negative gap means next ride starts before previous ride ends for the same bike.

That usually indicates data/timestamp/order issues, not real bike movement.

We explicitly filtered negative gaps out:
```sql
AND DATE_DIFF('minute', prev_end_time, next_start_time) >= 0
```

## 10) Query run commands
Run station-specific relocation query:
```bash
duckdb /home/shaun/NTU_M2_Data_Pipeline/data/warehouse/london_bikes.db < /home/shaun/NTU_M2_Data_Pipeline/queries/bike_relocation_gap.sql
```

Run maintenance assumption summary query:
```bash
duckdb /home/shaun/NTU_M2_Data_Pipeline/data/warehouse/london_bikes.db < /home/shaun/NTU_M2_Data_Pipeline/queries/maintenance_assumption_percentile.sql
```

Regenerate all plots:
```bash
conda run -n london-bikes-env python /home/shaun/NTU_M2_Data_Pipeline/scripts/plot_maintenance_assumption.py
```

## 11) Full line-by-line layperson walkthrough of Query 1
File: `/home/shaun/NTU_M2_Data_Pipeline/queries/bike_relocation_gap.sql`

```sql
-- I use this query to find bikes that end a customer ride at one station
-- and then start their next customer ride at a different station.
-- I treat this pattern as possible rebalancing or maintenance movement between rides.
-- I only need to change the marked values below to customize the analysis.

-- I start my first CTE block and call it rides.
WITH rides AS (
  -- I select fields needed to build bike-by-bike ride sequences.
  SELECT
    -- I keep rental_id so ordering remains stable when timestamps tie.
    rental_id,
    -- I keep bike_id so I can track each bike across rides.
    bike_id,
    -- I keep start_date so I can identify each next ride.
    start_date,
    -- I keep end_date so I can compute time gaps after each ride.
    end_date,
    -- I rename start station to a shorter alias for readability.
    start_station_logical_terminal AS start_station_id,
    -- I rename end station to a shorter alias for readability.
    end_station_logical_terminal AS end_station_id
  -- I read from the staged hire table in the london_bicycles schema.
  FROM london_bicycles.staging_cycle_hire
  -- I keep only rows with non-null bike_id so tracking is valid.
  WHERE bike_id IS NOT NULL
    -- I can uncomment ONE of these lines to exclude e-bikes if available.
    -- AND is_ebike = FALSE
    -- AND bike_type = 'pedal bike'
),
-- I start my second CTE block and call it seq.
seq AS (
  -- I transform each row so it includes this ride and the next ride for the same bike.
  SELECT
    -- I keep bike_id so each sequence stays inside one bike.
    bike_id,
    -- I keep rental_id for deterministic ordering and traceability.
    rental_id,
    -- I rename end_date as previous end time for the gap.
    end_date AS prev_end_time,
    -- I rename end station as the "from" station for movement detection.
    end_station_id AS prev_end_station,
    -- I use LEAD to fetch next ride start time for the same bike.
    LEAD(start_date) OVER (
      -- I partition by bike_id so look-ahead never mixes different bikes.
      PARTITION BY bike_id
      -- I order by start_date and rental_id so "next ride" is deterministic.
      ORDER BY start_date, rental_id
    ) AS next_start_time,
    -- I use LEAD again to fetch next ride start station for the same bike.
    LEAD(start_station_id) OVER (
      -- I partition by bike_id so look-ahead remains within one bike.
      PARTITION BY bike_id
      -- I use the same ordering for alignment with next_start_time.
      ORDER BY start_date, rental_id
    ) AS next_start_station
  -- I build this from the cleaned rides CTE.
  FROM rides
)
-- I return relocation-style events with their computed gaps.
SELECT
  -- I show which bike appears to move between customer rides.
  bike_id,
  -- I show the prior ride end station as the origin.
  prev_end_station AS from_station,
  -- I show the next ride start station as the destination.
  next_start_station AS to_station,
  -- I show when the prior ride ended.
  prev_end_time,
  -- I show when the next ride started.
  next_start_time,
  -- I compute minutes between the prior end and next start.
  DATE_DIFF('minute', prev_end_time, next_start_time) AS gap_minutes
-- I read from the sequence CTE that already pairs each ride to its next ride.
FROM seq
-- I apply my station-specific business logic.
WHERE prev_end_station = 1132
  -- I skip rows where the bike has no next recorded ride.
  AND next_start_station IS NOT NULL
  -- I keep only true relocations where destination differs from origin.
  AND next_start_station <> prev_end_station
-- I sort by longest gap first so potential servicing windows appear first.
ORDER BY gap_minutes DESC;
```

## 12) Full line-by-line layperson walkthrough of Query 2
File: `/home/shaun/NTU_M2_Data_Pipeline/queries/maintenance_assumption_percentile.sql`

```sql
-- I am assuming there is no official maintenance table, so I need to infer likely maintenance from ride behavior.
-- I am defining likely maintenance-like events as: bike appears at a different station on next ride + unusually long idle gap.
-- I am choosing "unusually long" as the top 5% of these relocation gaps (95th percentile), instead of a fixed time cutoff.

-- I start my first CTE block and call it rides.
WITH rides AS (
  -- I start from customer rides and keep only the fields I need for sequence logic.
  SELECT
    -- I keep rental_id so I can break timestamp ties consistently.
    rental_id,
    -- I keep bike_id because all sequencing is done bike-by-bike.
    bike_id,
    -- I keep start_date because I need the next ride start timestamp.
    start_date,
    -- I keep end_date because I need the current ride end timestamp.
    end_date,
    -- I rename the original start station field to a shorter alias for readability.
    start_station_logical_terminal AS start_station_id,
    -- I rename the original end station field to a shorter alias for readability.
    end_station_logical_terminal AS end_station_id
  -- I read from the staged hire table in the london_bicycles schema.
  FROM london_bicycles.staging_cycle_hire
  -- I only keep rows with a valid bike_id so I can track each bike over time.
  WHERE bike_id IS NOT NULL
  -- I can optionally exclude e-bikes here if the model has a usable column.
  -- AND is_ebike = FALSE
),
-- I start my second CTE block and call it paired.
paired AS (
  -- I pair each ride with the next ride for the same bike.
  SELECT
    -- I keep bike_id to preserve bike-level grouping downstream.
    bike_id,
    -- I keep rental_id so ordering stays deterministic on ties.
    rental_id,
    -- I rename current ride end time to represent the "previous event" end.
    end_date AS prev_end_time,
    -- I rename current ride end station to represent where the prior ride ended.
    end_station_id AS prev_end_station,
    -- I use LEAD to fetch the next ride's start time for this same bike.
    LEAD(start_date) OVER (
      -- I partition by bike_id so each bike is sequenced independently.
      PARTITION BY bike_id
      -- I order by start_date then rental_id so "next ride" is unambiguous.
      ORDER BY start_date, rental_id
    ) AS next_start_time,
    -- I use LEAD again to fetch the next ride's start station for this same bike.
    LEAD(start_station_id) OVER (
      -- I partition by bike_id so I never mix rides from different bikes.
      PARTITION BY bike_id
      -- I use the same ordering to align with the next_start_time logic.
      ORDER BY start_date, rental_id
    ) AS next_start_station
  -- I build this from the cleaned rides CTE.
  FROM rides
),
-- I start my third CTE block and call it candidate_events.
candidate_events AS (
  -- I keep only relocation events and compute the idle gap between rides.
  SELECT
    -- I keep bike_id for entity-level tracking.
    bike_id,
    -- I keep previous end station as relocation origin.
    prev_end_station,
    -- I keep next start station as relocation destination.
    next_start_station,
    -- I keep previous end timestamp for timing context.
    prev_end_time,
    -- I keep next start timestamp for timing context.
    next_start_time,
    -- I compute gap in minutes between previous end and next start.
    DATE_DIFF('minute', prev_end_time, next_start_time) AS gap_minutes
  -- I build this from paired ride sequences.
  FROM paired
  -- I remove rows where there is no next ride timestamp.
  WHERE next_start_time IS NOT NULL
    -- I remove rows where there is no next ride station.
    AND next_start_station IS NOT NULL
    -- I keep only true relocations (station changed).
    AND next_start_station <> prev_end_station
    -- I remove negative gaps, which usually indicate timestamp/data-order issues.
    AND DATE_DIFF('minute', prev_end_time, next_start_time) >= 0
),
-- I start my fourth CTE block and call it threshold.
threshold AS (
  -- I calculate the 95th percentile so my "long gap" rule adapts to real system behavior.
  -- I use quantile_cont for a continuous percentile threshold value.
  SELECT quantile_cont(gap_minutes, 0.95) AS p95_gap_minutes
  -- I compute this threshold over all relocation candidate events.
  FROM candidate_events
),
-- I start my fifth CTE block and call it suspected.
suspected AS (
  -- I tag events as likely maintenance-like when the gap is at or above the 95th percentile threshold.
  SELECT
    -- I keep every column from candidate_events for downstream summary stats.
    c.*
  -- I cross join threshold so every row can be compared to one global p95 cutoff.
  FROM candidate_events c
  -- I join the single-row threshold CTE to apply one consistent cutoff.
  CROSS JOIN threshold t
  -- I keep only events whose gap is at least the p95 threshold.
  WHERE c.gap_minutes >= t.p95_gap_minutes
)
-- I return summary metrics, including the mean time, for events that match my assumption.
SELECT
  -- I count how many events satisfy my suspected-maintenance definition.
  COUNT(*) AS suspected_events,
  -- I compute the average gap in minutes for those events.
  ROUND(AVG(gap_minutes), 2) AS mean_gap_minutes,
  -- I convert the same average gap into hours for easier interpretation.
  ROUND(AVG(gap_minutes) / 60.0, 2) AS mean_gap_hours,
  -- I compute the median gap in minutes to represent a robust "typical" value.
  ROUND(quantile_cont(gap_minutes, 0.50), 2) AS median_gap_minutes,
  -- I report the maximum gap to show the longest observed extreme case.
  ROUND(MAX(gap_minutes), 2) AS max_gap_minutes,
  -- I expose the exact threshold used so I can explain the cutoff transparently.
  (SELECT ROUND(p95_gap_minutes, 2) FROM threshold) AS threshold_minutes_used
-- I finalize the report from the suspected set only.
FROM suspected;
```

## 13) Final practical takeaway
The workflow is now production-friendly for analysis sharing:

1. Query logic is saved in reusable SQL files.
2. Query logic has layperson first-person comments line-by-line.
3. Outputs are reproducible from terminal commands.
4. Plots are reproducible from one script command.

Most defensible statement from current evidence:

- We have a strong maintenance-like operational signal, not a confirmed maintenance schedule.

## 14) Full explanation: log scale vs `log10` histogram
This section explains exactly what we did, why we did it, and how to interpret it.

### 14.1 What problem log solves
Our gap durations are positive and very right-skewed:

- many shorter gaps
- fewer medium gaps
- very few very long gaps

On a plain linear x-axis, the long-tail values stretch the axis so much that most bars bunch up on the left and become hard to read.

### 14.2 How log axis works (chart: `maintenance_gap_distribution.png`)
In this chart, we plot original gap hours, but the x-axis is logarithmic.

If `x` is gap hours:

- axis position is proportional to `log10(x)`
- equal spacing means equal multiplicative change, not equal additive change

So:

- `10^0 = 1` hour
- `10^1 = 10` hours
- `10^2 = 100` hours

Interpretation:

- moving one major tick right means “10x larger gap”
- this lets short and long gaps be visible in one figure

### 14.3 How `log10` transformation works (chart: `maintenance_gap_log10_normal_reference.png`)
Here we explicitly transform the data first:

- transformed value = `z = log10(gap_hours)`

So examples are:

- `gap_hours = 1`  -> `z = 0`
- `gap_hours = 10` -> `z = 1`
- `gap_hours = 100` -> `z = 2`

This “translation” changes the measurement scale from multiplicative to additive:

- equal steps in `z` mean equal powers of 10 in original hours
- heavy-right-tail data often becomes less skewed after log transform

### 14.4 Why we overlaid a normal curve on `log10` values
In that chart, we fit a normal reference curve to `log10(gap_hours)`:

1. compute `mu = mean(log10(gap_hours))`
2. compute `sigma = std(log10(gap_hours))`
3. draw a normal density with that `mu` and `sigma`

Why:

- not to claim data is perfectly normal
- only to visually check whether the transformed shape is closer to bell-like than raw hours

### 14.5 Log-axis histogram vs `log10` histogram: key difference
They may look related but are not the same thing.

1. Log-axis histogram:
- bins are created in original hour space
- then shown on log-scaled x-axis

2. `log10` histogram:
- data is transformed first (`log10`)
- histogram bins are in transformed units

Use both together:

- log-axis chart: intuitive for raw-unit communication
- `log10` chart: better for statistical-shape diagnostics

### 14.6 Why this is appropriate for your case
Your question is about rare long idle windows and extreme tails.
Log-based views are appropriate because:

- they preserve all events
- they prevent tail events from visually disappearing
- they make threshold interpretation (`p95`) clearer

## 15) Full explanation of every graph

### 15.1 `maintenance_gap_distribution.png` (all relocations, log x-axis)
What it shows:

- distribution of all relocation gap durations in hours
- red dashed vertical line = p95 cutoff used for suspected events

Why it matters:

- shows complete landscape of relocation gaps
- makes it obvious where “extreme” starts

How to read:

- left side: common shorter gaps
- right tail: rare long gaps
- events to the right of the red line are the top 5% longest relocation gaps

### 15.2 `maintenance_gap_distribution_linear.png` (all relocations, linear x-axis)
What it shows:

- same data as above, but plain linear hours

Why it matters:

- easier for non-technical audiences to understand raw units

Limitation:

- long tail compresses much of the distribution near the left

### 15.3 `maintenance_gap_distribution_suspected.png` (suspected events only)
What it shows:

- only events at or above p95 threshold

Why it matters:

- focuses on exactly the subset used for maintenance-like inference
- shows spread inside the “extreme tail” itself

How to read:

- center of this curve indicates typical extreme-case downtime
- width indicates variability among suspected events

### 15.4 `maintenance_gap_log10_normal_reference.png` (`log10` values + normal overlay)
What it shows:

- histogram of `log10(gap_hours)` values
- red line = normal reference fit to transformed values

Why it matters:

- checks whether transformation reduces skew
- supports whether normal-based summaries are reasonable as an approximation

Important caveat:

- this does not prove true normality; it is a shape check only

### 15.5 `maintenance_events_monthly.png` (context + rate)
What it shows:

Top panel:

- monthly count of all relocation events (blue)
- monthly count of suspected events (red)

Bottom panel:

- monthly suspected rate = suspected / relocations (green light)
- 3-month rolling average of that rate (green dark)

Why it matters:

- avoids misleading interpretation of raw monthly counts alone
- controls for monthly activity volume differences

How to read:

- if red count changes but blue also changes similarly, operational volume may be the driver
- if rate changes materially, behavior may actually be changing

### 15.6 `maintenance_events_day_hour_heatmap.png` (suspected events by weekday/hour)
What it shows:

- concentration of suspected events by day-of-week and hour-of-day

Why it matters:

- reveals timing clusters that may suggest operational routines

How to read:

- darker cells = more suspected events in that day-hour bucket
- consistent hot zones may indicate schedule-like operations

### 15.7 `maintenance_top_station_transfers.png` (top origin->destination pairs)
What it shows:

- most frequent station transitions among suspected events

Why it matters:

- identifies recurring movement corridors
- helps detect potential service/depot pathways

How to read:

- longer bars = more repeated suspected transitions
- strong concentration in few pairs suggests structured operations

### 15.8 `maintenance_duration_comparison.png` (direct duration benchmark chart)
What it shows:

- mean gap for all relocation events
- p95 cutoff used by the assumption rule
- median suspected duration
- mean suspected duration

Why it matters:

- gives one direct picture of “normal relocation downtime” vs “suspected maintenance-like downtime”
- makes the effect size visible without reading multiple charts

How to read:

- if suspected median/mean are materially above all-relocation mean, suspicion strength increases
- in this run, suspected mean (`80.0h`) is far above all-relocation mean (`15.18h`)

## 16) Your reasoning chain (first-person, as captured in this analysis)
This section records your practical thought process in plain language:

1. I do not have an official maintenance table, so I need to infer behavior from ride logs.
2. If a bike ends at one station and later reappears at another station, it likely moved outside customer rides.
3. If that gap is unusually long, maintenance/servicing/repair becomes a plausible explanation.
4. I need both event-level evidence and system-level context, not isolated examples.
5. I need reproducible SQL and reusable charts so others can validate and reuse the method.
6. I need transparent uncertainty statements so findings are strong but not overstated.

## 17) What these results can ascertain (and cannot)
What we can ascertain with high confidence:

1. Long-duration relocation events exist and are measurable.
2. The extreme-tail subset is quantitatively very different from routine relocations.
3. The suspected subset has much longer downtime (mean `80.0h`) than typical relocation behavior (mean `15.18h`).
4. These patterns are operationally consistent with maintenance-like processes.

What we cannot ascertain from current data alone:

1. Exact maintenance action type (repair vs routine service vs storage).
2. Exact workshop/depot confirmation for each event.
3. Formal maintenance schedule policy (without maintenance logs or external operational metadata).
