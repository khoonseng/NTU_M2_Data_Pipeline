{{
  config(
    materialized='table',
    partition_by={
      "field": "date_key",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["bike_id"]
  )
}}

with bikes_hired as (
    select distinct bike_id
    from {{ ref('fact_hire') }}  
),
bike_date_combined as (
    SELECT 
        {# Generate a surrogate key based on the grain (bike + date) #}
        {{ dbt_utils.generate_surrogate_key(['b.bike_id', 'd.date_key']) }} as bike_usage_key,
        d.date_key,
        b.bike_id,
    FROM {{ ref('dim_bike') }} b
    CROSS JOIN {{ ref('dim_date') }} d
    inner join bikes_hired h on h.bike_id = b.bike_id
    where d.date_key != '9999-01-01'
),
daily_hire_info as (
    select bike_id, 
            start_date_key, 
            count(*) as hire_count,
            sum(duration_minutes) as daily_duration_minutes,            
    from {{ ref('fact_hire') }}
    group by bike_id, start_date_key
)
select c.bike_usage_key,
        c.bike_id,
        c.date_key,
        COALESCE(i.hire_count, 0) as hire_count,
        COALESCE(i.daily_duration_minutes, 0) as daily_duration_minutes,
        SUM(COALESCE(daily_duration_minutes, 0)) OVER (PARTITION BY c.bike_id ORDER BY c.date_key) as cumulative_duration_minutes
from bike_date_combined c
left outer join daily_hire_info i on c.bike_id = i.bike_id and c.date_key = i.start_date_key
