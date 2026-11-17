select
    commodity_id,
    commodity,
    max(commodity_code)                              as commodity_code,
    regexp_matches(lower(commodity), 'basmati')      as is_basmati_commodity,
    regexp_matches(lower(commodity), 'paddy|rice')   as is_rice_or_paddy
from {{ ref('stg_agmarknet__daily_price') }}
group by commodity_id, commodity
