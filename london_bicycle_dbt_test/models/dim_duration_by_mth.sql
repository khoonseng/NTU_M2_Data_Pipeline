{{ config(materialized='table') }}

SELECT
    -- Truncates the date to the first day of the month
    DATE_TRUNC(end_date, MONTH) AS rental_month,

    -- Formatted version for readability (e.g., '2023-01')
    FORMAT_DATE('%Y-%m', end_date) AS month_label,

    -- Clean bike_model: replace NULL and empty with 'not_specified'
    COALESCE(NULLIF(TRIM(bike_model), ''), 'not_specified') AS bike_model,

    -- Sums the duration in seconds
    SUM(duration) AS total_duration_seconds,

    -- Count of rentals
    COUNT(rental_id) AS total_rentals

FROM 
    `ntu-project-489202.london_bicycle.fact_hires`

GROUP BY 
    1, 2, 3

ORDER BY 
    rental_month DESC,
    bike_model
