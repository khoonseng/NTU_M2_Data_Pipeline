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
    {{ dbt_utils.generate_surrogate_key(['h.station_id', 'h.station_name']) }} station_key,
    h.station_id,
    h.station_name,
    COALESCE(s.latitude, 0) as latitude,
    COALESCE(s.longitude, 0) as longitude
FROM stations_from_hire h
left outer join {{ source('london_bicycles','cycle_stations')}} s on h.station_id = s.id
where station_name is not null