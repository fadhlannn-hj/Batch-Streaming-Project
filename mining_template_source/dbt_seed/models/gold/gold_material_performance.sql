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
    cycle_time_minutes
  FROM {{ ref('silver_haulage_trips') }}
  WHERE current_status = 'IDLE' 
),
MATERIAL_AGGREGATE AS (
  SELECT
    trip_date,
    pit_location,
    dump_location,
    dump_type,
    material_type,
    COUNT(DISTINCT trip_id)                         AS total_trips,
    COUNT(DISTINCT truck_id)                        AS unique_trucks,
    COUNT(DISTINCT operator_id)                     AS unique_operators,
    ROUND(AVG(cycle_time_minutes), 2)               AS avg_cycle_time_minutes,
    SUM(cycle_time_minutes)                         AS total_haulage_minutes,
    COUNT(DISTINCT CASE WHEN shift = 'DAY'   THEN trip_id END) AS day_shift_trips,
    COUNT(DISTINCT CASE WHEN shift = 'NIGHT' THEN trip_id END) AS night_shift_trips
  FROM BASE_TRIPS
  GROUP BY
    trip_date,
    pit_location,
    dump_location,
    dump_type,
    material_type
)
SELECT
  *,
  ROUND(
    total_trips * 100.0 / NULLIF(SUM(total_trips) OVER (PARTITION BY trip_date), 0),
    2
    ) AS pct_of_daily_trips
FROM MATERIAL_AGGREGATE