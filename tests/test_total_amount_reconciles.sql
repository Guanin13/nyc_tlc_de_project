select *
from {{ ref('silver_yellow_trip') }}
where round(total_amount, 2) != round(
        fare_amount
      + extra
      + mta_tax
      + tip_amount
      + tolls_amount
      + improvement_surcharge
      + coalesce(congestion_surcharge, 0)
      + coalesce(airport_fee, 0)
      + coalesce(cbd_congestion_fee, 0)
    , 2)