with valid_start_stations as (
    select distinct start_station_id
    from {{ ref('stg_cycle_hire') }}
    where start_station_id 
        in (select station_id 
            from {{ ref('stg_cycle_stations') }}
        )
),
valid_end_stations as (
    select distinct end_station_id
    from {{ ref('stg_cycle_hire') }}
    where end_station_id 
        in (select station_id 
            from {{ ref('stg_cycle_stations') }}
        )
)
select
    rental_id,
    floor(duration / 60) as duration_minutes,
    bike_id,
    start_date,
    end_date,
    h.start_station_id,
    h.end_station_id
from  {{ ref('stg_cycle_hire') }} h
inner join valid_start_stations s on s.start_station_id = h.start_station_id
inner join valid_end_stations e on e.end_station_id = h.end_station_id
