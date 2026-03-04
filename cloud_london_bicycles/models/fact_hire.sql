select
    *
from {{ source('london_bicycles','cycle_hire')}}