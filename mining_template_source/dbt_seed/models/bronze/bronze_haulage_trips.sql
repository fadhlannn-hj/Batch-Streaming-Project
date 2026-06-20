{{
  config(
    materialized = 'incremental',
    unique_key = ['trip_id', 'current_status', 'event_timestamp'],
    on_schema_change = 'append_new_columns'
  )
}}

WITH RAW AS (
  SELECT
    RAW_PAYLOAD,
    FILE_NAME,
    ROW_NUMBER,
    INGESTED_AT
  FROM {{ source('raw', 'RAW_HAULAGE_TRIPS') }}

  {% if is_incremental() %}
    WHERE INGESTED_AT > (SELECT MAX(ingested_at) FROM {{ this }})
  {% endif %}
),
EXTRACTED AS (
  SELECT
    RAW_PAYLOAD:value:trip_id::NUMBER AS trip_id,
    RAW_PAYLOAD:value:equipment_id::VARCHAR AS equipment_id,
    RAW_PAYLOAD:value:operator_id::VARCHAR AS operator_id,
    RAW_PAYLOAD:value:source_location_id::VARCHAR AS source_location_id,
    RAW_PAYLOAD:value:destination_location_id::VARCHAR AS destination_location_id,
    CASE
      WHEN RAW_PAYLOAD:value:load_timestamp IS NOT NULL
      THEN TO_TIMESTAMP(RAW_PAYLOAD:value:load_timestamp::NUMBER / 1000000)
      ELSE NULL
    END AS load_timestamp,
    CASE
      WHEN RAW_PAYLOAD:value:dump_timestamp IS NOT NULL
      THEN TO_TIMESTAMP(RAW_PAYLOAD:value:dump_timestamp::NUMBER / 1000000)
      ELSE NULL
    END AS dump_timestamp,
    RAW_PAYLOAD:value:material_type::VARCHAR AS material_type,
    RAW_PAYLOAD:value:current_status::VARCHAR AS current_status,
    CASE
      WHEN RAW_PAYLOAD:value:__deleted::VARCHAR = 'true' THEN TRUE
      ELSE FALSE
    END AS is_deleted,
    FILE_NAME AS source_file,
    ROW_NUMBER AS source_row_number,
    INGESTED_AT AS ingested_at
  FROM RAW
  WHERE RAW_PAYLOAD:value:trip_id IS NOT NULL AND RAW_PAYLOAD:value:__deleted::VARCHAR != 'true'
),
FINAL AS (
  SELECT
    trip_id,
    equipment_id,
    operator_id,
    source_location_id,
    destination_location_id,
    material_type,
    current_status,
    load_timestamp,
    dump_timestamp,
    is_deleted,
    source_file,
    source_row_number,
    ingested_at,
    CASE current_status
      WHEN 'LOADING' THEN COALESCE(load_timestamp, ingested_at)
      WHEN 'HAULING' THEN COALESCE(DATEADD('second', 1, load_timestamp), ingested_at)
      WHEN 'DUMPING' THEN COALESCE(dump_timestamp, ingested_at)
      WHEN 'IDLE'    THEN COALESCE(DATEADD('second', 1, dump_timestamp), ingested_at)
    ELSE ingested_at
    END AS event_timestamp
  FROM extracted
)
SELECT * FROM FINAL