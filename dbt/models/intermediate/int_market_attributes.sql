-- Current attributes of each market (state + market name), as reported most recently.
-- Snapshotted by snap_market to keep a Type 2 history of district changes.
select
    market_id,
    state,
    market,
    district,
    first_seen,
    last_seen
from (
    select
        market_id,
        state,
        market,
        district,
        min(arrival_date) over (partition by market_id) as first_seen,
        max(arrival_date) over (partition by market_id) as last_seen,
        row_number() over (
            partition by market_id
            order by arrival_date desc, district
        ) as rn
    from {{ ref('stg_agmarknet__daily_price') }}
)
where rn = 1
