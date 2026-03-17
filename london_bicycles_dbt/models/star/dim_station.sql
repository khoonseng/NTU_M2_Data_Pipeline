with start_trips as (
    select start_station_id,
            count(*) as total_start_count
    from {{ ref('stg_cycle_hire') }}
    group by start_station_id
),
end_trips as (
    select end_station_id,
            count(*) as total_end_count
    from {{ ref('stg_cycle_hire') }}
    group by end_station_id
)    
select
    station_key,
    station_id,
    station_name,
    latitude,
    longitude,
    COALESCE(st.total_start_count, 0) as total_start_count,
    COALESCE(et.total_end_count, 0) as total_end_count
from {{ ref('stg_cycle_stations') }} s 
left outer join start_trips st on st.start_station_id = s.station_id
left outer join end_trips et on et.end_station_id = s.station_id