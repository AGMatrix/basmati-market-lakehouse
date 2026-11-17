select
    event_id,
    event_date,
    event_type,
    mep_usd_per_tonne_before,
    mep_usd_per_tonne_after,
    (event_date - lag(event_date) over (order by event_date))::integer
        as days_since_previous_event,
    source_url,
    source_type,
    verification_status
from {{ ref('stg_policy_events') }}
