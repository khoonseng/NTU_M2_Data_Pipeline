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
    -- I use station_id because it has full multi-year coverage in this dataset.
    start_station_id,
    -- I use station_id because it has full multi-year coverage in this dataset.
    end_station_id
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
