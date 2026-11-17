select
    event_id,
    event_date,
    event_type,
    mep_usd_per_tonne_before,
    mep_usd_per_tonne_after,
    source_url,
    source_type,
    verification_status
from {{ ref('policy_events') }}
