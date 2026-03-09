select
    rental_id,
    floor(duration / 60) as duration_minutes,
    bike_id,
    start_date,
    end_date,
    start_station_id,
    end_station_id,
    EXTRACT(HOUR FROM start_date) AS start_hour,
    {{ generate_date_key('start_date') }} as start_date_key,
    COALESCE({{ generate_date_key('end_date') }}, 99990101) as end_date_key
from {{ ref('stg_cycle_hire_valid_stations') }}
