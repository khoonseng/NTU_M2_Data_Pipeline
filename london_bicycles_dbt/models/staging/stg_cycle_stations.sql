select
    distinct id as station_id,
    name
from {{ source('london_bicycles','cycle_stations')}}