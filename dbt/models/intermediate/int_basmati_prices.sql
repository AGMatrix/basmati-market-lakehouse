-- Price rows whose (commodity, variety) a reviewer mapped as basmati, with the clean labels.
select
    p.*,
    m.variety_id,
    m.variety_group,
    m.form,
    m.processing
from {{ ref('stg_agmarknet__daily_price') }} as p
inner join {{ ref('stg_basmati_variety_map') }} as m
    on m.commodity = p.commodity
   and m.raw_variety = p.raw_variety
where m.is_basmati
