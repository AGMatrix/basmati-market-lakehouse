-- Every raw (commodity, variety) pair that might be basmati, with whether it is mapped yet.
-- A pair is a candidate if the commodity says basmati, or it is a rice/paddy commodity whose
-- variety name carries a basmati signal (the word in any common spelling - basumathi, basamti,
-- bashmati, basmatti... - or a Pusa Basmati variety number).
-- Unmapped candidates are what scripts/variety_mapping.py sends to the LLM, and what the
-- quality gate fails on.
with prices as (
    select * from {{ ref('stg_agmarknet__daily_price') }}
),

pairs as (
    select
        commodity,
        raw_variety,
        count(*)            as n_rows,
        min(arrival_date)   as first_seen,
        max(arrival_date)   as last_seen,
        median(modal_price) as median_modal_price
    from prices
    where regexp_matches(lower(commodity), 'basmati')
       or regexp_matches(lower(commodity), 'paddy|rice|dhan') and regexp_matches(
            lower(raw_variety),
            'bas[aeiou]*h?m[aeiou]*t+h?[aeiou]|1121|1509|1718|1401|1847|1885|1886|1692|pusa|sugandh|taraori|dehradun|\bd\.?b\.?\b|370|386|csr ?30'
          )
    group by commodity, raw_variety
)

select
    p.*,
    m.raw_variety is not null as is_mapped,
    m.is_basmati,
    m.variety_group
from pairs as p
left join {{ ref('stg_basmati_variety_map') }} as m
    on m.commodity = p.commodity
   and m.raw_variety = p.raw_variety
