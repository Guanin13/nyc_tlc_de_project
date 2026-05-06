{{ config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key='trip_key',
    format='parquet',
    partitioned_by=['pickup_month'],
    on_schema_change='sync_all_columns'
) }}

with silver as (

    select *
    from {{ ref('silver_yellow_trip') }}

    {% if is_incremental() %}
        where pickup_datetime > (
            select coalesce(max(pickup_datetime), timestamp '1900-01-01')
            from {{ this }}
        )
    {% endif %}

),

fare_features as (

    select
        trip_key,

        -- Time features
        pickup_datetime,
        hour(pickup_datetime) as pickup_hour,
        day_of_week(pickup_datetime) as pickup_day_of_week,
        month(pickup_datetime) as pickup_month,

        case
            when day_of_week(pickup_datetime) in (6, 7) then 1
            else 0
        end as is_weekend,

        -- Location features
        pu_location_id,
        do_location_id,

        -- Trip features
        passenger_count,
        trip_distance,

        date_diff('minute', pickup_datetime, dropoff_datetime) as trip_duration_minutes,

        -- Target
        fare_amount as target_fare_amount

    from silver

    where pickup_datetime is not null
      and dropoff_datetime is not null
      and fare_amount is not null
      and fare_amount > 0
      and trip_distance > 0
      and passenger_count > 0
      and date_diff('minute', pickup_datetime, dropoff_datetime) between 1 and 180
      and fare_amount between 2.5 and 500

)

select *
from fare_features