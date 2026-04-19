select *
from {{ ref('silver_yellow_trip') }}
where dropoff_datetime < pickup_datetime