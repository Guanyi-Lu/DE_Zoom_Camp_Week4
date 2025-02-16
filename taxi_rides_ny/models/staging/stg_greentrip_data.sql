{{ config(materialized='view') }} --by default, it is a view

WITH tripdata AS (
  SELECT *,
    ROW_NUMBER() OVER(PARTITION BY vendor_id, pickup_datetime,pickup_location_id) AS rn
  FROM {{ source('staging', 'greentrip_data') }}
  WHERE vendor_id IS NOT NULL 
),
--The ROW_NUMBER() function assigns a unique ranking (rn) to each row partitioned by (vendor_id, pickup_datetime, pickup_location_id). 
--This means that if there are multiple records with the same vendor_id, 
--pickup_datetime, and pickup_location_id, they will be numbered sequentially within that group.

cleaned_data AS (
  SELECT
    -- Identifiers
    {{ dbt_utils.generate_surrogate_key(['vendor_id', 'pickup_datetime','pickup_location_id']) }} AS tripid,    
    {{ dbt.safe_cast("vendor_id", api.Column.translate_type("integer")) }} AS vendorid,
    {{ dbt.safe_cast("rate_code", api.Column.translate_type("integer")) }} AS ratecodeid,
    {{ dbt.safe_cast("pickup_location_id", api.Column.translate_type("integer")) }} AS pickup_locationid,
    {{ dbt.safe_cast("dropoff_location_id", api.Column.translate_type("integer")) }} AS dropoff_locationid,

    -- Timestamps
    CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,

    -- Trip info
    store_and_fwd_flag,
    {{ dbt.safe_cast("passenger_count", api.Column.translate_type("integer")) }} AS passenger_count,
    CAST(trip_distance AS NUMERIC) AS trip_distance,
    1 AS trip_type,

    -- Payment info
    CAST(fare_amount AS NUMERIC) AS fare_amount,
    CAST(extra AS NUMERIC) AS extra,
    CAST(mta_tax AS NUMERIC) AS mta_tax,
    CAST(tip_amount AS NUMERIC) AS tip_amount,
    CAST(tolls_amount AS NUMERIC) AS tolls_amount,
    CAST(0 AS NUMERIC) AS ehail_fee,
    CAST(imp_surcharge AS NUMERIC) AS improvement_surcharge,
    CAST(total_amount AS NUMERIC) AS total_amount,

    -- Cleaned payment type
    CASE
        WHEN REGEXP_CONTAINS(payment_type, r'^[0-9]+(\.0)?$') THEN  
            SAFE_CAST(REPLACE(payment_type, '.0', '') AS INT64) 
        ELSE 0  
    END AS cleaned_payment_type

  FROM tripdata
  WHERE rn = 1
)

SELECT 
    *,
    {{ get_payment_type_description('cleaned_payment_type') }} AS payment_type_description
FROM cleaned_data


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}    

My note: if you  run  dbt build --select stg_greentrip_data --vars '{'is_test_run': 'false'}' in the terminal, this will apply no limit to your staging model.
         if you  run  dbt build --select stg_greentrip_data --vars '{'is_test_run': 'true'}' in the terminal, or not putting a value (default is true) this will apply a limit of 100 only to your staging model.
