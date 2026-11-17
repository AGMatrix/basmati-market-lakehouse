select
    variety_id,
    commodity,
    raw_variety,
    is_basmati,
    variety_group,
    form,
    processing,
    mapped_by,
    reviewed_on
from {{ ref('stg_basmati_variety_map') }}
