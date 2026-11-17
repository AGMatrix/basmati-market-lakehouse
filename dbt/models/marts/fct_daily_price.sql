-- Grain: arrival date x market x commodity x raw variety x grade.
-- Agmarknet's API exposes no arrival quantities, so there is no arrivals measure.
select
    p.arrival_date,
    m.market_version_id,
    p.market_id,
    p.commodity_id,
    v.variety_id,
    p.raw_variety,
    p.grade,
    p.min_price,
    p.max_price,
    p.modal_price,
    p.report_count
from {{ ref('stg_agmarknet__daily_price') }} as p
left join {{ ref('dim_market') }} as m
    on m.market_id = p.market_id
   and p.arrival_date >= m.valid_from
   and p.arrival_date <  m.valid_to
left join {{ ref('stg_basmati_variety_map') }} as v
    on v.commodity = p.commodity
   and v.raw_variety = p.raw_variety
