{{
  config(
    materialized='table',
    partition_by={
      "field": "start_date_key",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["bike_id", "start_station_id", "end_station_id"]
  )
}}

select
    rental_id,
    COALESCE(floor(duration / 60), 0) as duration_minutes,
    bike_id,
    start_date,
    end_date,
    start_station_id,
    end_station_id,
    EXTRACT(HOUR FROM start_date) AS start_hour,
    {{ generate_date_key('start_date') }} as start_date_key,
    COALESCE({{ generate_date_key('end_date') }}, '9999-01-01') as end_date_key
from {{ ref('stg_cycle_hire') }}
