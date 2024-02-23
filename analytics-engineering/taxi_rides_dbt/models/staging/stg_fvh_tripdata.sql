{{
    config(
        materialized='view'
    )
}}

with tripdata as(
    select *
    from {{ source('staging', 'fvh_tripdata') }}
    where EXTRACT(YEAR FROM pickup_datetime) = 2019
)

select

{{ dbt.safe_cast("dispatching_base_num", api.Column.translate_type("string")) }} as dispatching_base_num,
{{ dbt.safe_cast("PULocationID", api.Column.translate_type("integer")) }} as PULocationID,
{{ dbt.safe_cast("DOLocationID", api.Column.translate_type("integer")) }} as DOLocationID,
{{ dbt.safe_cast("SR_FLAG", api.Column.translate_type("integer")) }} as SR_FLAG,
{{ dbt.safe_cast("pickup_datetime", api.Column.translate_type("timestamp")) }} as pickup_datetime,
{{ dbt.safe_cast("dropOff_datetime", api.Column.translate_type("timestamp")) }} as dropoff_datetime,
{{ dbt.safe_cast("affiliated_base_number", api.Column.translate_type("timestamp")) }} as affiliated_base_num

from tripdata