{{
  config(
    materialized = 'incremental',
    unique_key = ['trip_id', 'current_status', 'valid_from'],
    on_schema_change = 'append_new_columns'
    )
}}

WITH NEW_BRONZE_DATA AS (
  SELECT
    trip_id,
    current_status,
    CASE current_status
      WHEN 'LOADING' THEN 1
      WHEN 'HAULING' THEN 2
      WHEN 'DUMPING' THEN 3
      WHEN 'IDLE' THEN 4
      ELSE 0
    END AS status_order,
    {{ clean_string('equipment_id') }} AS truck_id,
    operator_id,
    {{ clean_string('source_location_id') }} AS pit_location,
    {{ clean_string('destination_location_id') }} AS dump_location,
    {{ clean_string('material_type') }} AS material_type,
    load_timestamp,
    dump_timestamp,
    event_timestamp AS valid_from,
    ingested_at,
    CURRENT_TIMESTAMP() AS dbt_timestamp 
  FROM {{ ref('bronze_haulage_trips') }}
  
  {% if is_incremental() %}
    WHERE ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
  {% endif %}
),
HISTORICAL_ACTIVE_TRIPS AS (
  {% if not is_incremental() %}
    SELECT 
      NULL::VARCHAR AS trip_id,
      NULL::VARCHAR AS current_status,
      NULL::NUMBER AS status_order,
      NULL::VARCHAR AS truck_id,
      NULL::VARCHAR AS operator_id,
      NULL::VARCHAR AS pit_location,
      NULL::VARCHAR AS dump_location,
      NULL::VARCHAR AS material_type,
      NULL::TIMESTAMP AS load_timestamp,
      NULL::TIMESTAMP AS dump_timestamp,
      NULL::TIMESTAMP AS valid_from,
      NULL::TIMESTAMP AS ingested_at,
      NULL::TIMESTAMP AS dbt_timestamp
    WHERE 1 = 0
  {% else %}
    SELECT 
      trip_id,
      current_status,
      status_order,
      truck_id,
      operator_id,
      pit_location,
      dump_location,
      material_type,
      load_timestamp,
      dump_timestamp,
      valid_from,
      ingested_at,
      dbt_timestamp 
    FROM {{ this }}
    WHERE trip_id IN (SELECT DISTINCT trip_id FROM NEW_BRONZE_DATA)
  {% endif %}
),
COMBINED_POOL AS (
  SELECT
    *
  FROM HISTORICAL_ACTIVE_TRIPS
  UNION ALL
  SELECT
    *
  FROM NEW_BRONZE_DATA
),
DEDUPLICATED_POOL AS (
  SELECT 
    * 
  FROM COMBINED_POOL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY trip_id, current_status, valid_from 
    ORDER BY dbt_timestamp DESC
  ) = 1
),
SCD AS (
  SELECT
  MD5(CONCAT(trip_id::VARCHAR, '-', current_status)) AS surrogate_key,
  trip_id,
  current_status,
  status_order,
  truck_id,
  operator_id,
  pit_location,
  dump_location,
  CASE
    WHEN dump_location ILIKE '%CRUSHER%' THEN 'CRUSHER'
    WHEN dump_location ILIKE '%STOCKPILE%' THEN 'STOCKPILE'
    ELSE 'UNKNOWN'
  END AS dump_type,
  material_type,
  load_timestamp,
  dump_timestamp,
  DATE(load_timestamp) AS trip_date,
  CASE
    WHEN HOUR(load_timestamp) BETWEEN 6 AND 17 THEN 'DAY'
    ELSE 'NIGHT'
  END AS shift,
  valid_from,
  LEAD(valid_from) OVER(
    PARTITION BY trip_id
    ORDER BY status_order ASC
  ) AS valid_to,
  CASE
    WHEN LEAD(valid_from) OVER(
      PARTITION BY trip_id
      ORDER BY status_order ASC
    ) IS NULL THEN TRUE
    ELSE FALSE
  END AS is_current,
  CASE 
    WHEN LEAD(valid_from) OVER(PARTITION BY trip_id ORDER BY status_order ASC) IS NOT NULL
    THEN DATEDIFF(
      'minute',
      valid_from,
      LEAD(valid_from) OVER(PARTITION BY trip_id ORDER BY status_order ASC)
    )
    ELSE NULL
  END AS status_duration_minutes,
    CASE
      WHEN current_status = 'IDLE'
        AND load_timestamp IS NOT NULL
        AND dump_timestamp IS NOT NULL
      THEN DATEDIFF('minute', load_timestamp, dump_timestamp)
      ELSE NULL
    END AS cycle_time_minutes,
    CASE
      WHEN current_status = 'IDLE'
        AND DATEDIFF('minute', load_timestamp, dump_timestamp) > 120
      THEN TRUE ELSE FALSE
    END AS is_anomalus_cycle_time,
    ingested_at,
    dbt_timestamp
  FROM DEDUPLICATED_POOL
)
SELECT
  *
FROM SCD