-- One row per MEP event x window length x side.
--   before: the N days ending the day before the event
--   after:  the N days starting the day after the event
-- The event day itself is excluded from both sides: announcement-day trading is noisy and
-- belongs to neither regime cleanly.
with events as (
    select * from {{ ref('stg_policy_events') }}
),

windows as (
    select unnest({{ var('policy_windows') }}) as window_days
)

select
    e.event_id,
    e.event_date,
    e.event_type,
    w.window_days,
    s.side,
    case s.side
        when 'before' then e.event_date - w.window_days
        else e.event_date + 1
    end as window_start,
    case s.side
        when 'before' then e.event_date - 1
        else e.event_date + w.window_days
    end as window_end
from events as e
cross join windows as w
cross join (values ('before'), ('after')) as s(side)
