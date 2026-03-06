select
    station_id,
    name
from {{ ref('stg_cycle_stations') }}