-- Average basmati modal price in the N days before vs. after each MEP change,
-- by variety group and state, plus an all-India row per variety group.
-- Averages are over market-days (each market's daily modal price counts once).
-- This is a before/after comparison, not a causal estimate: harvest seasonality, global
-- prices and the overlap between the Aug and Oct 2023 windows all move prices too.
with priced as (
    select
        w.event_id,
        w.event_date,
        w.window_days,
        w.side,
        p.variety_group,
        p.state,
        p.market_id,
        p.modal_price
    from {{ ref('int_policy_windows') }} as w
    inner join {{ ref('int_basmati_prices') }} as p
        on p.arrival_date between w.window_start and w.window_end
),

by_side as (
    select
        event_id,
        event_date,
        window_days,
        variety_group,
        coalesce(state, 'All India')                          as state,
        avg(modal_price) filter (where side = 'before')       as avg_modal_before,
        avg(modal_price) filter (where side = 'after')        as avg_modal_after,
        count(*) filter (where side = 'before')               as n_market_days_before,
        count(*) filter (where side = 'after')                as n_market_days_after,
        count(distinct market_id)                             as n_markets
    from priced
    group by grouping sets (
        (event_id, event_date, window_days, variety_group, state),
        (event_id, event_date, window_days, variety_group)
    )
)

select
    event_id::varchar                                         as event_id,
    event_date::date                                          as event_date,
    window_days::integer                                      as window_days,
    variety_group::varchar                                    as variety_group,
    state::varchar                                            as state,
    round(avg_modal_before, 2)::double                        as avg_modal_before,
    round(avg_modal_after, 2)::double                         as avg_modal_after,
    round(100.0 * (avg_modal_after - avg_modal_before) / avg_modal_before, 2)::double
                                                              as pct_change,
    n_market_days_before::bigint                              as n_market_days_before,
    n_market_days_after::bigint                               as n_market_days_after,
    n_markets::bigint                                         as n_markets
from by_side
