select *
from {{ ref('silver_yellow_trip') }}
where total_amount < 0
   or trip_distance < 0
   or fare_amount < 0
   or extra < 0
   or mta_tax < 0
   or tip_amount < 0
   or tolls_amount < 0
   or improvement_surcharge < 0
   or coalesce(congestion_surcharge, 0) < 0
   or coalesce(airport_fee, 0) < 0
   or coalesce(cbd_congestion_fee, 0) < 0
   or (passenger_count is not null and passenger_count < 0)
   or (rate_code_id is not null and rate_code_id < 0)