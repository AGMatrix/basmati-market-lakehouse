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
| Python unit tests | **48** pass | `make test` |
| Warehouse fact table | **55,039,691** rows from **8,398** market versions; basmati: **156,186** rows from **904** markets (95% paddy) | `marts.fct_daily_price` |

## 2. LLM-assisted variety mapping (offline, human-reviewed)

| Metric | Value |
|---|---|
| Review rounds | 2: 69 names from the first warehouse gate, then 3 more found when the basmati spelling check was widened (troubleshooting #33) |
| API calls | 4 batches, `claude-opus-5-5`, structured (typed) output |
| Approved by a person | 72 / 72 |
| Accepted unchanged | **70 (97.2%)** |
| Edited by the reviewer | 2: `Paddy(Basmati) \| Hybrid` and `Paddy(Dhan)(Basmati) \| Other`, both changed from basmati to not basmati |
| Effect of round 2 | +18,142 basmati reports that a spelling list had missed (`Basumathi`); basmati rows 138,044 → 156,186 |

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

## 4. Daily refresh in the cloud (GitHub Actions)

| Metric | Value |
|---|---|
| Schedule | every day 09:00 IST, with the Mac off |
| Run time | 3 min 43 s (first run, 8 days ingested and published); 3 min 33 s (forced rebuild) |
| What it runs | the same `ingest_day`, Spark `build_silver`, dbt models (64 checks) and warehouse gate as the local pipeline, on a lite lake in S3 |
| Lite vs full warehouse | identical: 156,186 basmati rows, 977,918 rice rows, 0 differing policy-mart rows |
| Proxy | the Mumbai instance runs only during the job; SSH is opened for the runner's IP and closed again |
| Hosted extract | 1.1M rows (basmati + all rice), 16 MB |

## 5. Weekly price forecast

One gradient-boosting model (scikit-learn `HistGradientBoostingRegressor`, quantile loss)
across **49** rice and basmati series × regions with at least 300 weeks of history and 3+
reporting mandis a week. It predicts the 1–4 week price change from price history,
seasonality (week of year, last year's move), mandi activity and the MEP regime. The 80%
band is conformally calibrated on a held-out year.

Backtest: trained on targets before Jan 2024 (102,455 rows), scored on Jan 2024 – Nov 2026
(27,030 rows) that the model never saw. Mean absolute % error:

| Horizon | Model | No change | Last year's change |
|---|---|---|---|
| 1 week | **4.01%** | 4.12% | 6.15% |
| 2 weeks | **4.80%** | 5.00% | 7.32% |
| 3 weeks | **5.43%** | 5.73% | 8.18% |
| 4 weeks | **5.84%** | 6.20% | 8.55% |
| All | **5.01%** | 5.25% | 7.54% |

- 80% band coverage on the test period: **79.9%** (before calibration: 75.2%).
- Honest reading: the model beats "price stays the same" at every horizon, but only by
  about 5% of the error. Stable series (Rice Fine, Sona Mansoori; 1–2% error) leave little
  to gain. Volatile ones gain more, e.g. Other Basmati rice 14.7% lower error, before the
  mandi filter.
- The biggest improvement would come from data the API lacks: arrival quantities (supply),
  export volumes, world rice prices and USD/INR.

## 6. Problems found only at full scale

Details in [docs/TROUBLESHOOTING_LOG.txt](docs/TROUBLESHOOTING_LOG.txt):

- **#27:** 62 of the silver runs during the backfill failed with *Cannot assign requested address* (ephemeral-port exhaustion). Raw vs silver reconciliation found 65 missing days, and a targeted re-run fixed them.
- **#28:** dbt was OOM-killed on 55M rows. Capping DuckDB at 3 GB and materialising staging brought the full build to 113 s.
- **#29:** a uniqueness test caught duplicate commodity ids (`Kutki`/`kutki`).
- **#30:** mixed CRLF/LF line endings broke `dbt seed` after the mapping merge.
- **#33:** a spelling *list* missed 18,142 `Basumathi` reports; a spelling *pattern* found them.
- **#34:** the Mac's disk filled up; deleted-but-open files (`lsof +L1`) were holding 6.8 GB.

## Not measured yet

- **Forecast accuracy on live data:** every run's forecasts are logged (`ml.forecast_log`); they can be scored once the predicted weeks have happened.
- **Exports by destination:** the APEDA/DGCI&S source is not verified (troubleshooting #26).
