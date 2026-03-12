SELECT rental_id, duration, duration_ms, bike_id, bike_model,
    start_date, end_date, start_station.station_key start_station_key, end_station.station_key end_station_key,
    start_station_logical_terminal, end_station_logical_terminal, end_station_priority_id
    FROM `bigquery-public-data.london_bicycles.cycle_hire` cycle_hire
    INNER JOIN {{ ref('dim_station') }} start_station 
        ON IFNULL(cycle_hire.start_station_id, 0) = IFNULL(start_station.station_id, 0) AND IFNULL(cycle_hire.start_station_name, '') = IFNULL(start_station.station_name, '')
    INNER JOIN {{ ref('dim_station') }} end_station
        ON IFNULL(cycle_hire.end_station_id, 0) = IFNULL(end_station.station_id, 0) AND IFNULL(cycle_hire.end_station_name, '') = IFNULL(end_station.station_name, '')