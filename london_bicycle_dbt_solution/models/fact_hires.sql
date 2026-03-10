-- ============================================================
-- FACT TABLE: fact_cycle_hire
-- Purpose: Records each individual bike hire event, replacing
-- raw station ID/name columns with surrogate keys from dim_station
-- ============================================================

SELECT 
    -- Select all columns from the raw hire data EXCEPT the raw station
    -- identifier columns, since we're replacing them with surrogate keys below
    cycle_hire.* EXCEPT(start_station_id, start_station_name, end_station_id, end_station_name),

    -- Replace raw start station columns with the surrogate key from dim_station
    start_station.station_key AS start_station_key,

    -- Replace raw end station columns with the surrogate key from dim_station
    end_station.station_key AS end_station_key

-- Source: Google BigQuery public dataset for London bicycle hires
FROM `bigquery-public-data.london_bicycles.cycle_hire` cycle_hire

-- Join to dim_station once for the START station
-- IFNULL handles NULLs by substituting a default (0 for IDs, '' for names)
-- so that NULL = NULL comparisons don't silently drop rows
INNER JOIN {{ ref('dim_station') }} start_station 
    ON IFNULL(cycle_hire.start_station_id, 0)   = IFNULL(start_station.station_id, 0) 
    AND IFNULL(cycle_hire.start_station_name, '') = IFNULL(start_station.station_name, '')

-- Join to dim_station a second time (aliased differently) for the END station
-- Same NULL-safe logic applied to end station ID and name
INNER JOIN {{ ref('dim_station') }} end_station
    ON IFNULL(cycle_hire.end_station_id, 0)     = IFNULL(end_station.station_id, 0) 
    AND IFNULL(cycle_hire.end_station_name, '')  = IFNULL(end_station.station_name, '')