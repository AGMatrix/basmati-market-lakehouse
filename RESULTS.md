# Results

All numbers below were measured on this build (2026-11-17/18, MacBook, Colima VM with 4 CPUs
and 8 GB RAM). Nothing is estimated. The SQL to reproduce each analytical number is included.

## 1. Data platform

| Metric | Value | How it was measured |
|---|---|---|
| Days ingested | **4,339** (2015-01-01 → 2026-11-17), 0 days with no data | raw manifests in MinIO |
| Raw rows (all commodities) | **55,365,892** | sum of manifest row counts |
| Raw size in the lake | **1.26 GB** gzipped JSON | object sizes, `raw/agmarknet/` |
| Silver rows (after combining repeat reports) | **55,147,964**; `sum(report_count)` = 55,365,892, so every raw row is accounted for | `lake.silver.daily_price` |
| Coverage | **8,325** markets, **421** commodities, **34** states/UTs | silver |
| Iceberg snapshots on `silver.daily_price` | **917** (one per atomic day-set replace) | `iceberg_snapshots()` |
| Backfill | **4,330** Airflow runs, **0 failed**; **16 h 45 min** wall clock with 2 concurrent runs through a Mumbai SOCKS proxy; median run **10.1 s** | Airflow run metadata |
| Silver rebuild, 65 days / 1.09M rows | **39.6 s** of Spark work, 66 s for the whole Airflow run | task metrics |
| Warehouse build on the full history | dbt **112.8 s** (64 nodes), quality gate **16 s**, publish **24 s** | task logs |
| dbt checks | **64** (42 data tests, 3 unit tests, contracts on 9 mart tables): all pass | `dbt build` |
| Python unit tests | **39** pass | `make test` |
| Warehouse fact table | **55,039,691** rows from **8,398** market versions; basmati: **138,044** rows from **891** markets (95% paddy) | `marts.fct_daily_price` |

## 2. LLM-assisted variety mapping (offline, human-reviewed)

| Metric | Value |
|---|---|
| Unmapped (commodity, variety) names found by the warehouse gate | 69 |
| API calls | 3 batches (25 + 25 + 19 names), `claude-opus-5-5`, structured output |
| Approved by a person | 69 / 69 |
| Accepted unchanged | **67 (97.1%)** |
| Edited by the reviewer | 2: `Paddy(Basmati) \| Hybrid` and `Paddy(Dhan)(Basmati) \| Other`, both changed from basmati to not basmati |
| Result | 61 names mapped as basmati, 8 as not basmati; warehouse gate went from FAIL (69 unmapped) to PASS |

Source: `review/mapping_log.jsonl`.

## 3. Basmati prices around the Minimum Export Price (MEP) changes

Average modal price in Rs per quintal, all India: the mean over market-day prices in each
window, from `marts.mart_basmati_price_vs_policy`. The event day itself is excluded. All 1121 and 1509
rows here are paddy.

| Event | Window | 1121 before → after | 1509 before → after |
|---|---|---|---|
| 27 Aug 2023: MEP introduced (USD 1,200/t) | 30 d | 2,646 → 2,939 (+11.1%) | 3,155 → 3,337 (+5.8%) |
| | 60 d | 2,594 → 3,138 (**+21.0%**) | 3,056 → 3,221 (+5.4%) |
| | 90 d | 2,682 → 3,618 (+34.9%) | 3,094 → 3,269 (+5.7%) |
| 26 Oct 2023: MEP cut (USD 950/t; news source, official notice pending) | 30 d | 3,180 → 3,888 (+22.3%) | 3,180 → 3,337 (+5.0%) |
| | 60 d | 3,113 → 4,006 (**+28.7%**) | 3,222 → 3,450 (+7.1%) |
| | 90 d | 3,032 → 4,024 (+32.7%) | 3,215 → 3,501 (+8.9%) |
| 13 Sep 2024: MEP removed | 30 d | 2,434 → 2,548 (+4.7%) | 2,538 → 2,631 (+3.7%) |
| | 60 d | 2,619 → 3,068 (**+17.2%**) | 2,608 → 2,664 (+2.2%) |
| | 90 d | 2,858 → 3,317 (+16.1%) | 2,690 → 2,725 (+1.3%) |

Markets per comparison: 23–156 (column `n_markets`).

### Seasonal check: the same 60-day windows one year earlier (paddy)

Basmati paddy is harvested from September to November, so prices move with the season. To
see how much of the change is seasonal, the same calendar windows were measured a year
earlier:

| Event | 1121: event year | 1121: one year earlier | 1509: event year | 1509: one year earlier |
|---|---|---|---|---|
| Aug 2023 introduced | +21.0% | −8.0% (Aug 2022) | +5.4% | −10.4% |
| Oct 2023 cut | +28.7% | +4.6% (Oct 2022) | +7.1% | +1.1% |
| Sep 2024 removed | +17.2% | +31.1% (Sep 2023)* | +2.2% | +1.1% |

\* The Sep 2023 baseline overlaps the Aug and Oct 2023 policy windows, so it is not a clean
seasonal baseline.

### What this does and does not show

- In 2023, 1121 paddy prices rose much faster after both MEP events than in the same weeks of
  2022. The season alone does not explain the 2023 increases.
- It is still a before/after comparison, **not a causal estimate**. Global rice prices
  (India's non-basmati export restrictions from July 2023), the two overlapping 2023 windows,
  and changes in which markets report from one window to the next all move these numbers.
- 1509 moved much less than 1121 in every window.

Reproduce the seasonal check (DuckDB on `data/warehouse_serving.duckdb`):

```sql
with ev as (select event_id, event_date from marts.dim_policy_event),
p as (
  select f.arrival_date, f.modal_price, v.variety_group, v.form
  from marts.fct_daily_price f join marts.dim_variety v using (variety_id)
  where v.is_basmati and v.variety_group in ('1121', '1509')
),
w as (
  select e.event_id, k.shift, p.variety_group, p.form,
    avg(modal_price) filter (where arrival_date <  e.event_date - k.shift * interval 1 year
                               and arrival_date >= e.event_date - k.shift * interval 1 year - interval 60 day) as p_before,
    avg(modal_price) filter (where arrival_date >  e.event_date - k.shift * interval 1 year
                               and arrival_date <= e.event_date - k.shift * interval 1 year + interval 60 day) as p_after
  from ev e cross join (values (0), (1)) k(shift) cross join p
  group by all
)
select event_id, form, variety_group, shift as years_earlier,
       round(100 * (p_after / p_before - 1), 1) as pct
from w where p_before is not null and p_after is not null
order by all;
```

## 4. Problems found only at full scale

Details in [docs/TROUBLESHOOTING_LOG.txt](docs/TROUBLESHOOTING_LOG.txt):

- **#27:** 62 of the silver runs during the backfill failed with *Cannot assign requested address* (ephemeral-port exhaustion). Raw vs silver reconciliation found 65 missing days, and a targeted re-run fixed them.
- **#28:** dbt was OOM-killed on 55M rows. Capping DuckDB at 3 GB and materialising staging brought the full build to 113 s.
- **#29:** a uniqueness test caught duplicate commodity ids (`Kutki`/`kutki`).
- **#30:** mixed CRLF/LF line endings broke `dbt seed` after the mapping merge.

## Not measured yet

- **End-to-end time of a scheduled daily run:** the Mumbai proxy is stopped, so no scheduled daily ingest has run since the backfill.
- **Exports by destination:** the APEDA/DGCI&S source is not verified (troubleshooting #26).
