select
    commodity,
    raw_variety,
    is_basmati,
    variety_group,
    form,
    processing,
    mapped_by,
    reviewed_on,
    md5(lower(commodity) || '|' || lower(raw_variety)) as variety_id
from {{ ref('basmati_variety_map') }}
