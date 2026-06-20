{{
  config(
    materialized = 'table',
    )
}}

WITH BASE_TRIPS AS (
  SELECT
    trip_id,
    trip_date,
    truck_id,
    operator_id,
    pit_location,
    dump_location,
    dump_type,
    material_type,
    shift,
    load_timestamp,
    dump_timestamp,
    cycle_time_minutes,
    is_anomalus_cycle_time
  FROM {{ ref('silver_haulage_trips') }}
  WHERE current_status = 'IDLE'
),
TRUCK_AGGREGATE AS (
  SELECT
    trip_date,
    truck_id,
    operator_id,
    shift,
    material_type,
    COUNT(DISTINCT trip_id) AS total_trips,
    COUNT(DISTINCT CASE WHEN material_type = 'ORE' THEN trip_id END) AS ore_trips,
    COUNT(DISTINCT CASE WHEN material_type = 'WASTE' THEN trip_id END) AS waste_trips,
    COUNT(DISTINCT CASE WHEN dump_type = 'CRUSHER' THEN trip_id END) AS crusher_trips,
    COUNT(DISTINCT CASE WHEN dump_type = 'STOCKPILE' THEN trip_id END) AS stockpile_trips,
    ROUND(AVG(cycle_time_minutes), 2) AS avg_cycle_time_minutes,
    MIN(cycle_time_minutes) AS min_cycle_time_minutes,
    MAX(cycle_time_minutes) AS max_cycle_time_minutes,
    ROUND(STDDEV(cycle_time_minutes), 2) AS stddev_cycle_time_minutes,
    ROUND(
      COUNT(DISTINCT trip_id)/NULLIF(DATEDIFF('hour', MIN(load_timestamp), MAX(dump_timestamp)), 0),
      2
    ) AS trips_per_hour,
    SUM(cycle_time_minutes) AS total_cycle_time_minutes,
    COUNT(CASE WHEN is_anomalus_cycle_time = TRUE THEN 1 END) AS total_anomalus_cycle_time
  FROM BASE_TRIPS
  GROUP BY
    trip_date,
    truck_id,
    operator_id,
    shift,
    material_type
)
SELECT
  *,
  RANK() OVER(
    PARTITION BY trip_date
    ORDER BY total_trips DESC
  ) AS daily_truck_rank,
  ROUND(AVG(avg_cycle_time_minutes) OVER(
    PARTITION BY truck_id
    ORDER BY trip_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ), 2) AS rolling_7d_truck_avg_minutes 
FROM TRUCK_AGGREGATE