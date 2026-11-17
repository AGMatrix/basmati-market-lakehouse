-- SCD Type 2 market dimension from snap_market.
-- The snapshot only observes changes from its first run, so each market's earliest version
-- is extended back to 1900-01-01: historical prices then join to the first-known attributes
-- instead of to nothing. Later versions keep their real validity dates.
with versions as (
    select
        *,
        row_number() over (partition by market_id order by dbt_valid_from) as version_number
    from {{ ref('snap_market') }}
)

select
    dbt_scd_id::varchar                                         as market_version_id,
    market_id::varchar                                          as market_id,
    state::varchar                                              as state,
    market::varchar                                             as market,
    district::varchar                                           as district,
    version_number::integer                                     as version_number,
    case when version_number = 1 then date '1900-01-01'
         else dbt_valid_from::date end                          as valid_from,
    coalesce(dbt_valid_to::date, date '9999-12-31')             as valid_to,
    dbt_valid_to is null                                        as is_current,
    first_seen::date                                            as first_seen,
    last_seen::date                                             as last_seen
from versions
