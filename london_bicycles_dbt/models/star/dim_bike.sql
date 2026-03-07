with total_bike_usage as (
    select bike_id, 
            sum(duration) as total_duration
    from {{ ref('stg_cycle_hire') }}
    group by bike_id
)
select distinct 
    h.bike_id, 
    h.bike_model,
    floor(u.total_duration / 60) as total_duration_minutes
from {{ ref('stg_cycle_hire') }} h
inner join total_bike_usage u on u.bike_id = h.bike_id