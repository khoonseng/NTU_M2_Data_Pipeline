with missing_start_stations as (
    select start_station_name, count(*)
    from {{ source('london_bicycles','cycle_hire')}}
    where start_station_id is null
    group by start_station_name
),
matching_start_stations as (
    select s.id, s.name 
    from {{ source('london_bicycles','cycle_stations')}} s
    inner join missing_start_stations m on s.name = m.start_station_name
),
missing_end_stations as (
    select end_station_name, count(*)
    from {{ source('london_bicycles','cycle_hire')}}
    where end_station_id is null
    group by end_station_name
),
matching_end_stations as (
    select s.id, s.name 
    from {{ source('london_bicycles','cycle_stations')}} s
    inner join missing_end_stations m on s.name = m.end_station_name
)
select
    h.rental_id,
    h.duration,
    h.bike_id,
    COALESCE(NULLIF(h.bike_model, ''), 'CLASSIC') as bike_model,
    h.start_date,
    h.end_date,
    COALESCE(COALESCE(COALESCE(h.start_station_id, s.id), h.start_station_logical_terminal), 0) as start_station_id,
    COALESCE(h.start_station_name, s.name) as start_station_name,
    CASE
        WHEN h.end_date is not null THEN COALESCE(COALESCE(COALESCE(h.end_station_id, e.id), h.end_station_logical_terminal), 0)
        ELSE COALESCE(h.end_station_id, -1)
    END as end_station_id,
    CASE
        WHEN h.end_date is not null THEN COALESCE(h.end_station_name, e.name)
        ELSE COALESCE(h.end_station_name, 'Unknown')
    END as end_station_name
from  {{ source('london_bicycles','cycle_hire')}} h
left outer join matching_start_stations s on s.name = h.start_station_name
left outer join matching_end_stations e on e.name = h.end_station_name
where h.bike_id is not null