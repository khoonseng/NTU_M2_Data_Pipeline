select
    rental_id,
    floor(duration / 60) as duration_minutes,
    bike_id,
    start_date,
    end_date,
    start_station_id,
    end_station_id
from  {{ ref('stg_cycle_hire_valid_stations') }}
