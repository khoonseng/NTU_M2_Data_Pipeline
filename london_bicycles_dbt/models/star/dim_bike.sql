select distinct 
    bike_id, 
    bike_model
from {{ ref('stg_cycle_hire') }}