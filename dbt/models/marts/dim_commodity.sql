-- One row per commodity_id. The id is md5(lower(name)), and the source sometimes changes
-- case for the same commodity ("Kutki" / "kutki", "Tulip" / "tulip"), so the display name
-- is the spelling used on the most rows rather than a grouping key.
select
    commodity_id,
    mode(commodity)                                         as commodity,
    max(commodity_code)                                     as commodity_code,
    bool_or(regexp_matches(lower(commodity), 'basmati'))    as is_basmati_commodity,
    bool_or(regexp_matches(lower(commodity), 'paddy|rice')) as is_rice_or_paddy
from {{ ref('stg_agmarknet__daily_price') }}
group by commodity_id
