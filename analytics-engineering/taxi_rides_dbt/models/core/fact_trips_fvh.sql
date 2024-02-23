{{
    config(
        materialized='table'
    )
}}

with fvh_tripdata as (
    select *,
    'FVH' as service_type
    from {{ ref('stg_fvh_tripdata') }}
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select fvh_tripdata.dispatching_base_num,
    fvh_tripdata.pulocationid,
    fvh_tripdata.dolocationid,
    fvh_tripdata.SR_FLAG,
    fvh_tripdata.pickup_datetime,
    fvh_tripdata.dropoff_datetime,
    fvh_tripdata.affiliated_base_num,
    pickup_zones.zone as pickup_zone,
    dropoff_zones.zone as dropoff_zone,
    pickup_zones.borough as pickup_borough,
    dropoff_zones.borough as dropoff_borough

from fvh_tripdata inner join dim_zones as pickup_zones
on fvh_tripdata.pulocationid = pickup_zones.locationid
inner join dim_zones as dropoff_zones
on fvh_tripdata.dolocationid = dropoff_zones.locationid