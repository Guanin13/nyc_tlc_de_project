{{ config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key='trip_key',
    format='parquet',
    partitioned_by=['month(pickup_datetime)'],
    on_schema_change='sync_all_columns'
) }}

with raw as (

    select
        vendorid,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        ratecodeid,
        store_and_fwd_flag,
        pulocationid,
        dolocationid,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,
        cbd_congestion_fee,
        year,
        month,
        load_ts
    from {{ source('bronze', 'yellow_trip_data_rawraw') }}

),

typed as (

    select
        md5(
            to_utf8(
                coalesce(cast(vendorid as varchar), '') || '|' ||
                coalesce(cast(tpep_pickup_datetime as varchar), '') || '|' ||
                coalesce(cast(tpep_dropoff_datetime as varchar), '') || '|' ||
                coalesce(cast(pulocationid as varchar), '') || '|' ||
                coalesce(cast(dolocationid as varchar), '')
            )
        ) as trip_key,

        cast(vendorid as integer) as vendor_id,
        cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
        cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
        coalesce(cast(passenger_count as bigint), 0) as passenger_count,
        cast(trip_distance as double) as trip_distance,
        cast(ratecodeid as bigint) as rate_code_id,
        cast(store_and_fwd_flag as varchar) as store_and_fwd_flag,
        cast(pulocationid as integer) as pu_location_id,
        cast(dolocationid as integer) as do_location_id,
        cast(payment_type as bigint) as payment_type,
        coalesce(cast(fare_amount as double), 0) as fare_amount,
        coalesce(cast(extra as double), 0) as extra,
        coalesce(cast(mta_tax as double), 0) as mta_tax,
        coalesce(cast(tip_amount as double), 0) as tip_amount,
        coalesce(cast(tolls_amount as double), 0) as tolls_amount,
        coalesce(cast(improvement_surcharge as double), 0) as improvement_surcharge,
        coalesce(cast(total_amount as double), 0) as total_amount,
        coalesce(cast(congestion_surcharge as double), 0) as congestion_surcharge,
        coalesce(cast(airport_fee as double), 0) as airport_fee,
        coalesce(cast(cbd_congestion_fee as double), 0) as cbd_congestion_fee,

        cast(year as varchar) as source_year,
        cast(month as varchar) as source_month,
        date_parse(load_ts, '%Y%m%dT%H%i%sZ') as source_load_ts,
        current_timestamp as dbt_loaded_at
    from raw

    where tpep_pickup_datetime is not null
        and tpep_dropoff_datetime is not null
        and cast(tpep_dropoff_datetime as timestamp) >= cast(tpep_pickup_datetime as timestamp)
        and vendorid is not null
        and pulocationid is not null
        and dolocationid is not null
        and total_amount is not null
        and cast(total_amount as double) >= 0
        and (cast(passenger_count as bigint) >= 0 or passenger_count is null)
        and (cast(ratecodeid as bigint) >= 0 or ratecodeid is null)
        and cast(payment_type as bigint) in (0,1,2,3,4,5)
        and cast(trip_distance as double) >= 0
        and (store_and_fwd_flag in ('Y', 'N') or store_and_fwd_flag is null)
        and fare_amount is not null
        and cast(fare_amount as double) >= 0
        and extra is not null
        and cast(extra as double) >= 0
        and mta_tax is not null
        and cast(mta_tax as double) >= 0
        and tip_amount is not null
        and cast(tip_amount as double) >= 0
        and tolls_amount is not null
        and cast(tolls_amount as double) >= 0
        and improvement_surcharge is not null
        and cast(improvement_surcharge as double) >= 0
        and coalesce(cast(congestion_surcharge as double), 0) >= 0
        and coalesce(cast(airport_fee as double), 0) >= 0
        and coalesce(cast(cbd_congestion_fee as double), 0) >= 0
        and round(cast(total_amount as double), 2) =
            round(
                    cast(fare_amount as double)
                + cast(extra as double)
                + cast(mta_tax as double)
                + cast(tip_amount as double)
                + cast(tolls_amount as double)
                + cast(improvement_surcharge as double)
                + coalesce(cast(congestion_surcharge as double), 0)
                + coalesce(cast(airport_fee as double), 0)
                + coalesce(cast(cbd_congestion_fee as double), 0)
            , 2)
),

deduped as (

    select *
    from (
        select
            *,
            row_number() over (
                partition by trip_key
                order by source_load_ts desc
            ) as row_rn
        from typed
    )
    where row_rn = 1

)

select
    trip_key,
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    rate_code_id,
    store_and_fwd_flag,
    pu_location_id,
    do_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    source_year,
    source_month,
    source_load_ts,
    dbt_loaded_at
from deduped

{% if is_incremental() %}
where source_load_ts >= (
    select coalesce(max(source_load_ts), timestamp '1900-01-01 00:00:00')
    from {{ this }}
)
{% endif %}