select
    *
from {{ source('raw_london_bicycles','staging_cycle_hire')}}