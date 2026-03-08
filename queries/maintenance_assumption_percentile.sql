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
