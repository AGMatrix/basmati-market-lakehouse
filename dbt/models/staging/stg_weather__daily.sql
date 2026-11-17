select
    weather_date,
    state,
    district,
    precipitation_mm,
    temperature_max_c,
    temperature_min_c,
    latitude,
    longitude,
    geocoded_place
from {{ source('silver', 'weather_daily') }}
