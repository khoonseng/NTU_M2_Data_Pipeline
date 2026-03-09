-- I am building a revised inference model based on these assumptions:
-- 1) Very short relocation gaps to popular stations are likely demand rebalancing.
-- 2) Preventive maintenance should be in a narrower duration window.
-- 3) Preventive maintenance should not happen too many times per bike per year.
-- 4) Durations outside that preventive window are more likely short/long repair or out-of-service.

-- I store all tunable assumptions in one place so I can edit logic safely later.
CREATE OR REPLACE TEMP TABLE h2_params AS
SELECT
  8.0::DOUBLE AS rebalance_max_hours,
  24.0::DOUBLE AS preventive_min_hours,
  96.0::DOUBLE AS preventive_max_hours,
  336.0::DOUBLE AS out_of_service_min_hours,
  2::INTEGER AS max_preventive_events_per_bike_year,
  0.90::DOUBLE AS popular_station_percentile;

-- I count how many ride starts each station receives, as a proxy for station popularity.
CREATE OR REPLACE TEMP TABLE h2_station_popularity AS
SELECT
  start_station_id AS station_id,
  COUNT(*) AS station_start_count
FROM london_bicycles.staging_cycle_hire
WHERE start_station_id IS NOT NULL
GROUP BY start_station_id;

-- I compute the popularity threshold (90th percentile by default).
CREATE OR REPLACE TEMP TABLE h2_popularity_threshold AS
SELECT
  quantile_cont(station_start_count, 0.90) AS popular_station_cutoff
FROM h2_station_popularity;

-- I label stations as top-demand if they are above the threshold.
CREATE OR REPLACE TEMP TABLE h2_station_flags AS
SELECT
  p.station_id,
  p.station_start_count,
  CASE
    WHEN p.station_start_count >= t.popular_station_cutoff THEN TRUE
    ELSE FALSE
  END AS is_top_demand_station
FROM h2_station_popularity p
CROSS JOIN h2_popularity_threshold t;

-- I keep only fields needed for bike-by-bike sequencing.
CREATE OR REPLACE TEMP TABLE h2_rides AS
SELECT
  rental_id,
  bike_id,
  start_date,
  end_date,
  start_station_id,
  end_station_id
FROM london_bicycles.staging_cycle_hire
WHERE bike_id IS NOT NULL;

-- I pair every ride with its next ride for the same bike.
CREATE OR REPLACE TEMP TABLE h2_paired AS
SELECT
  bike_id,
  rental_id,
  end_date AS prev_end_time,
  end_station_id AS prev_end_station,
  LEAD(start_date) OVER (
    PARTITION BY bike_id
    ORDER BY start_date, rental_id
  ) AS next_start_time,
  LEAD(start_station_id) OVER (
    PARTITION BY bike_id
    ORDER BY start_date, rental_id
  ) AS next_start_station
FROM h2_rides;

-- I keep relocation events and compute inactivity duration in hours.
CREATE OR REPLACE TEMP TABLE h2_relocations AS
SELECT
  ROW_NUMBER() OVER () AS relocation_event_id,
  p.bike_id,
  p.prev_end_time,
  p.prev_end_station,
  p.next_start_time,
  p.next_start_station,
  DATE_DIFF('minute', p.prev_end_time, p.next_start_time) AS gap_minutes,
  DATE_DIFF('minute', p.prev_end_time, p.next_start_time) / 60.0 AS gap_hours,
  EXTRACT(year FROM p.prev_end_time) AS event_year,
  DATE_TRUNC('month', p.prev_end_time) AS event_month,
  COALESCE(s.is_top_demand_station, FALSE) AS to_top_demand_station
FROM h2_paired p
LEFT JOIN h2_station_flags s
  ON p.next_start_station = s.station_id
WHERE p.next_start_time IS NOT NULL
  AND p.next_start_station IS NOT NULL
  AND p.next_start_station <> p.prev_end_station
  AND DATE_DIFF('minute', p.prev_end_time, p.next_start_time) >= 0;

-- I rank only true preventive-window candidates per bike-year by gap length (longer first).
-- This avoids non-candidate rows stealing rank positions.
CREATE OR REPLACE TEMP TABLE h2_preventive_candidates AS
SELECT
  r.relocation_event_id,
  ROW_NUMBER() OVER (
    PARTITION BY r.bike_id, r.event_year
    ORDER BY r.gap_hours DESC, r.prev_end_time
  ) AS preventive_candidate_rank
FROM h2_relocations r
WHERE r.gap_hours >= (SELECT preventive_min_hours FROM h2_params)
  AND r.gap_hours <= (SELECT preventive_max_hours FROM h2_params);

-- I join candidate ranks back to all relocation rows.
CREATE OR REPLACE TEMP TABLE h2_ranked AS
SELECT
  r.*,
  c.preventive_candidate_rank
FROM h2_relocations r
LEFT JOIN h2_preventive_candidates c
  ON r.relocation_event_id = c.relocation_event_id;

-- I classify each relocation event according to the revised hypothesis rules.
CREATE OR REPLACE TEMP TABLE h2_classified AS
SELECT
  r.*,
  CASE
    -- I treat short gaps to high-demand stations as likely rebalancing.
    WHEN r.gap_hours < (SELECT rebalance_max_hours FROM h2_params)
      AND r.to_top_demand_station
    THEN 'Demand rebalancing likely (<8h + top-demand destination)'

    -- I treat other sub-8h events as short operational movement or unknown short idle.
    WHEN r.gap_hours < (SELECT rebalance_max_hours FROM h2_params)
      AND NOT r.to_top_demand_station
    THEN 'Operational / unknown short idle (<8h, non-demand destination)'

    -- I treat 8h-24h as short repair-like downtime.
    WHEN r.gap_hours >= (SELECT rebalance_max_hours FROM h2_params)
      AND r.gap_hours < (SELECT preventive_min_hours FROM h2_params)
    THEN 'Short repair likely (8h-24h)'

    -- I treat first two 24h-96h events per bike-year as preventive by assumption.
    WHEN r.gap_hours >= (SELECT preventive_min_hours FROM h2_params)
      AND r.gap_hours <= (SELECT preventive_max_hours FROM h2_params)
      AND r.preventive_candidate_rank <= (SELECT max_preventive_events_per_bike_year FROM h2_params)
    THEN 'Preventive maintenance likely (24h-96h, max 2 per bike-year)'

    -- I treat extra 24h-96h repeats as non-preventive repeated downtime.
    WHEN r.gap_hours >= (SELECT preventive_min_hours FROM h2_params)
      AND r.gap_hours <= (SELECT preventive_max_hours FROM h2_params)
      AND r.preventive_candidate_rank > (SELECT max_preventive_events_per_bike_year FROM h2_params)
    THEN 'Repeat downtime beyond preventive cap (24h-96h)'

    -- I treat 96h-336h as long repair-like downtime.
    WHEN r.gap_hours > (SELECT preventive_max_hours FROM h2_params)
      AND r.gap_hours <= (SELECT out_of_service_min_hours FROM h2_params)
    THEN 'Long repair likely (96h-336h)'

    -- I treat >336h as out-of-service candidate.
    ELSE 'Out-of-service candidate (>336h)'
  END AS inferred_state_h2
FROM h2_ranked r;

-- I return class-level summary statistics.
SELECT
  inferred_state_h2,
  COUNT(*) AS event_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 4) AS share_pct,
  ROUND(AVG(gap_hours), 2) AS mean_gap_hours,
  ROUND(quantile_cont(gap_hours, 0.50), 2) AS median_gap_hours,
  ROUND(quantile_cont(gap_hours, 0.90), 2) AS p90_gap_hours
FROM h2_classified
GROUP BY inferred_state_h2
ORDER BY CASE inferred_state_h2
  WHEN 'Demand rebalancing likely (<8h + top-demand destination)' THEN 1
  WHEN 'Operational / unknown short idle (<8h, non-demand destination)' THEN 2
  WHEN 'Short repair likely (8h-24h)' THEN 3
  WHEN 'Preventive maintenance likely (24h-96h, max 2 per bike-year)' THEN 4
  WHEN 'Repeat downtime beyond preventive cap (24h-96h)' THEN 5
  WHEN 'Long repair likely (96h-336h)' THEN 6
  WHEN 'Out-of-service candidate (>336h)' THEN 7
  ELSE 99
END;

-- I return preventive frequency diagnostics per bike-year.
SELECT
  COUNT(*) AS bike_year_rows,
  ROUND(AVG(preventive_event_count), 2) AS avg_preventive_events_per_bike_year,
  ROUND(quantile_cont(preventive_event_count, 0.50), 2) AS median_preventive_events_per_bike_year,
  ROUND(100.0 * AVG(CASE WHEN preventive_event_count = 0 THEN 1 ELSE 0 END), 2) AS pct_bike_year_preventive_eq_0,
  ROUND(100.0 * AVG(CASE WHEN preventive_event_count = 1 THEN 1 ELSE 0 END), 2) AS pct_bike_year_preventive_eq_1,
  ROUND(100.0 * AVG(CASE WHEN preventive_event_count = 2 THEN 1 ELSE 0 END), 2) AS pct_bike_year_preventive_eq_2,
  ROUND(100.0 * AVG(CASE WHEN preventive_event_count > 2 THEN 1 ELSE 0 END), 2) AS pct_bike_year_preventive_gt_2
FROM (
  SELECT
    bike_id,
    event_year,
    SUM(CASE WHEN inferred_state_h2 = 'Preventive maintenance likely (24h-96h, max 2 per bike-year)' THEN 1 ELSE 0 END) AS preventive_event_count
  FROM h2_classified
  GROUP BY bike_id, event_year
) t;

-- I return demand-station correlation by class to support rebalancing hypothesis checks.
SELECT
  inferred_state_h2,
  COUNT(*) AS event_count,
  ROUND(100.0 * AVG(CASE WHEN to_top_demand_station THEN 1 ELSE 0 END), 2) AS pct_to_top_demand_station
FROM h2_classified
GROUP BY inferred_state_h2
ORDER BY CASE inferred_state_h2
  WHEN 'Demand rebalancing likely (<8h + top-demand destination)' THEN 1
  WHEN 'Operational / unknown short idle (<8h, non-demand destination)' THEN 2
  WHEN 'Short repair likely (8h-24h)' THEN 3
  WHEN 'Preventive maintenance likely (24h-96h, max 2 per bike-year)' THEN 4
  WHEN 'Repeat downtime beyond preventive cap (24h-96h)' THEN 5
  WHEN 'Long repair likely (96h-336h)' THEN 6
  WHEN 'Out-of-service candidate (>336h)' THEN 7
  ELSE 99
END;
