with valid_start_stations as (
    select distinct start_station_id
    from {{ ref('stg_cycle_hire') }}
    where start_station_id 
        in (select station_id 
            from {{ ref('dim_station') }}
        )
),
valid_end_stations as (
    select distinct end_station_id
    from {{ ref('stg_cycle_hire') }}
    where end_station_id 
        in (select station_id 
            from {{ ref('dim_station') }}
        )
)
select
    h.rental_id,
    h.duration,
    h.bike_id,
    h.start_date,
    h.end_date,
    h.start_station_id,
    h.end_station_id
from  {{ ref('stg_cycle_hire') }} h
inner join valid_start_stations s on s.start_station_id = h.start_station_id
inner join valid_end_stations e on e.end_station_id = h.end_station_id
