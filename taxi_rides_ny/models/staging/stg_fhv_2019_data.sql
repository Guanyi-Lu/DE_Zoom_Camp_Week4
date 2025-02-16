with 

source as (

    select * from {{ source('staging', 'fhv_2019_data') }}

),

stg_fhv_tripdata as (

    select
    dispatching_base_num,
    CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
    CAST(pulocationid AS integer) AS pickup_location_id,
    CAST(dolocationid,AS integer) AS dropoff_location_id,
  
    sr_flag,
    affiliated_base_number

    from source

)

select * from stg_fhv_tripdata
