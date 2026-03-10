{{ config(materialized='table') }}

WITH stations AS (
    SELECT DISTINCT start_station_id station_id, start_station_name station_name
        FROM `bigquery-public-data.london_bicycles.cycle_hire`
    UNION DISTINCT
    SELECT DISTINCT end_station_id, end_station_name
        FROM `bigquery-public-data.london_bicycles.cycle_hire`
)
SELECT 
    {{ dbt_utils.generate_surrogate_key(['station_id', 'station_name']) }} station_key,
    station_id,
    station_name
FROM stations
    