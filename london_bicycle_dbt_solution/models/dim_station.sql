{{ config(materialized='table') }}

-- CTE to get a unique list of all stations (both start and end stations)
WITH stations AS (

    -- Get all unique stations where a bike trip started
    SELECT DISTINCT 
        start_station_id AS station_id, 
        start_station_name AS station_name
    FROM `bigquery-public-data.london_bicycles.cycle_hire`

    -- Combine with all unique stations where a bike trip ended
    -- UNION DISTINCT ensures no duplicate stations across both sets
    UNION DISTINCT

    SELECT DISTINCT 
        end_station_id, 
        end_station_name
    FROM `bigquery-public-data.london_bicycles.cycle_hire`
)

SELECT 
    -- Generate a unique surrogate key by hashing station_id + station_name
    -- Used as the primary key for the dim_stations table
    {{ dbt_utils.generate_surrogate_key(['station_id', 'station_name']) }} AS station_key,
    
    station_id,     -- Original station ID from the source data
    station_name    -- Human-readable station name
FROM stations
    