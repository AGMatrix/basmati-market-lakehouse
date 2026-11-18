-- A table, not a view: every test and downstream model would otherwise re-scan the whole
-- Iceberg table (60M rows) over S3; materialising reads the lake once per build.
{{ config(materialized='table') }}

-- Renaming, typing and surrogate keys only, plus one documented exclusion:
-- prices below var('min_plausible_price') Rs/quintal are a different unit in the price
-- column (flowers per bunch, fish per kg, ...), about 0.1-0.25% of rows. Agmarknet gives no
-- unit to convert by, so they are left out of the warehouse (they stay in silver).
select
    arrival_date,
    md5(lower(state) || '|' || lower(market))  as market_id,
    md5(lower(commodity))                       as commodity_id,
    state,
    district,
    market,
    commodity,
    commodity_code,
    variety                                     as raw_variety,
    grade,
    min_price,
    max_price,
    modal_price,
    modal_price / 100.0                         as modal_price_per_kg,
    price_unit,
    report_count,
    source                                      as source_resource,
    ingested_at
from {{ source('silver', 'daily_price') }}
where modal_price >= {{ var('min_plausible_price') }}
