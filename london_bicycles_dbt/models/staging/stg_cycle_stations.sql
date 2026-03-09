WITH stations_from_hire AS (
    SELECT DISTINCT 
            start_station_id as station_id, 
            start_station_name as station_name
        from {{ ref('stg_cycle_hire') }}
    UNION DISTINCT
    SELECT DISTINCT 
            end_station_id as station_id, 
            end_station_name as station_name
        from {{ ref('stg_cycle_hire') }}
)
SELECT 
    {{ dbt_utils.generate_surrogate_key(['station_id', 'station_name']) }} station_key,
    station_id,
    station_name
FROM stations_from_hire
where station_name is not null