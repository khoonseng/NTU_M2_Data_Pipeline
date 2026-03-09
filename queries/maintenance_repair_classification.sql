-- I want to separate relocation inactivity into likely operational states:
-- preventive maintenance, short repair, long repair, and out-of-service candidates.
-- I do this with z-scores on log10(gap_hours) inside a plausible service window.

WITH params AS (
  -- I define tunable parameters here so I can adjust assumptions in one place.
  SELECT
    2.0::DOUBLE AS min_service_hours,
    336.0::DOUBLE AS max_service_hours,
    1.0::DOUBLE AS z_band
),
rides AS (
  -- I take fields needed to pair each ride with its next ride for the same bike.
  SELECT
    rental_id,
    bike_id,
    start_date,
    end_date,
    start_station_id,
    end_station_id
  FROM london_bicycles.staging_cycle_hire
  WHERE bike_id IS NOT NULL
),
paired AS (
  -- I connect each ride to the next ride for the same bike.
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
  FROM rides
),
candidate_events AS (
  -- I keep relocation events and compute inactivity duration.
  SELECT
    bike_id,
    prev_end_station,
    next_start_station,
    prev_end_time,
    next_start_time,
    DATE_DIFF('minute', prev_end_time, next_start_time) AS gap_minutes,
    DATE_DIFF('minute', prev_end_time, next_start_time) / 60.0 AS gap_hours
  FROM paired
  WHERE next_start_time IS NOT NULL
    AND next_start_station IS NOT NULL
    AND next_start_station <> prev_end_station
    AND DATE_DIFF('minute', prev_end_time, next_start_time) >= 0
),
stats_window AS (
  -- I compute mean and standard deviation in log-space within the service window only.
  SELECT
    AVG(LOG10(gap_hours)) AS mu_log10_gap,
    STDDEV_POP(LOG10(gap_hours)) AS sigma_log10_gap
  FROM candidate_events c
  CROSS JOIN params p
  WHERE c.gap_hours >= p.min_service_hours
    AND c.gap_hours <= p.max_service_hours
    AND c.gap_hours > 0
),
classified AS (
  -- I classify each relocation event into an inferred operational state.
  SELECT
    c.*,
    p.min_service_hours,
    p.max_service_hours,
    p.z_band,
    s.mu_log10_gap,
    s.sigma_log10_gap,
    CASE
      WHEN c.gap_hours <= 0 THEN NULL
      ELSE (LOG10(c.gap_hours) - s.mu_log10_gap) / NULLIF(s.sigma_log10_gap, 0)
    END AS gap_z,
    CASE
      WHEN c.gap_hours < p.min_service_hours THEN 'Operational / short idle (<2h)'
      WHEN c.gap_hours > p.max_service_hours THEN 'Out-of-service candidate (>14d)'
      WHEN s.sigma_log10_gap IS NULL OR s.sigma_log10_gap = 0 THEN 'Preventive maintenance likely'
      WHEN (LOG10(c.gap_hours) - s.mu_log10_gap) / s.sigma_log10_gap < -p.z_band THEN 'Short repair likely'
      WHEN (LOG10(c.gap_hours) - s.mu_log10_gap) / s.sigma_log10_gap <= p.z_band THEN 'Preventive maintenance likely'
      ELSE 'Long repair likely'
    END AS inferred_state
  FROM candidate_events c
  CROSS JOIN params p
  CROSS JOIN stats_window s
)
-- I return summary stats by inferred class.
SELECT
  inferred_state,
  COUNT(*) AS event_count,
  ROUND(AVG(gap_hours), 2) AS mean_gap_hours,
  ROUND(quantile_cont(gap_hours, 0.50), 2) AS median_gap_hours,
  ROUND(quantile_cont(gap_hours, 0.90), 2) AS p90_gap_hours
FROM classified
GROUP BY inferred_state
ORDER BY CASE inferred_state
  WHEN 'Operational / short idle (<2h)' THEN 1
  WHEN 'Short repair likely' THEN 2
  WHEN 'Preventive maintenance likely' THEN 3
  WHEN 'Long repair likely' THEN 4
  WHEN 'Out-of-service candidate (>14d)' THEN 5
  ELSE 99
END;
