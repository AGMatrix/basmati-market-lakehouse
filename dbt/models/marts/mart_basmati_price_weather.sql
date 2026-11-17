-- Monthly basmati price per district and variety group next to that month's weather.
-- Weather is at one geocoded point per district, so it approximates field conditions;
-- price and rain moving together is correlation, not a causal effect.
with prices as (
    select
        date_trunc('month', arrival_date)::date as month_start,
        state,
        district,
        variety_group,
        form,
        avg(modal_price)                        as avg_modal_price,
        count(*)                                as n_market_days
    from {{ ref('int_basmati_prices') }}
    group by all
),

weather as (
    select
        date_trunc('month', weather_date)::date as month_start,
        state,
        district,
        sum(precipitation_mm)                   as precipitation_mm,
        avg(temperature_max_c)                  as avg_temperature_max_c,
        count(*)                                as n_weather_days
    from {{ ref('stg_weather__daily') }}
    group by all
)

select
    p.month_start::date                         as month_start,
    p.state::varchar                            as state,
    p.district::varchar                         as district,
    p.variety_group::varchar                    as variety_group,
    p.form::varchar                             as form,
    round(p.avg_modal_price, 2)::double         as avg_modal_price,
    p.n_market_days::bigint                     as n_market_days,
    round(w.precipitation_mm, 1)::double        as precipitation_mm,
    round(w.avg_temperature_max_c, 1)::double   as avg_temperature_max_c,
    w.n_weather_days::bigint                    as n_weather_days
from prices as p
left join weather as w
    on w.month_start = p.month_start
   and w.state = p.state
   and w.district = p.district
