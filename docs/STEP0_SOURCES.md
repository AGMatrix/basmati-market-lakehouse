# Step 0 — Source verification (2026-11-12)

Status legend: ✅ verified by a live call or official page · ⚠️ partly verified · ❌ blocked / not usable as-is

## 1. Agmarknet daily mandi prices (data.gov.in) — ✅ (probed 2026-11-13 from AWS Mumbai)

| Item | Finding |
|---|---|
| Dataset | "Current Daily Price of Various Commodities from Various Markets (Mandi)" — https://data.gov.in/resource/current-daily-price-various-commodities-various-markets-mandi |
| Endpoint | `https://api.data.gov.in/resource/<resource-id>?api-key=<key>&format=json&offset=&limit=&filters[...]` (standard OGD API shape). The commonly cited resource id `9ef84268-d588-465a-a308-a864a43d0070` was **not confirmed**: search snippets of the listing say "API for this resource does not exist… click Request API". |
| History | This resource serves **only the current day** (it is overwritten daily). It cannot backfill. |
| Key | Free signup at data.gov.in → "My Account" → API key. A shared demo key exists but is capped (reported: 10 records/call) — not usable for "all commodities". |
| Rate limits | Not documented on any page I could reach. Will be measured empirically and the client will throttle conservatively. |
| Fields (per listings) | state, district, market, commodity, variety, grade, arrival_date, min_price, max_price, modal_price (Rs/quintal). Arrivals (tonnes) are **not** in this resource. |
| Access | **Geo-blocked: loads from India, not from the US (confirmed by user 2026-11-12).** Ingestion supports an optional India proxy via `AGMARKNET_PROXY`. **API also blocked:** on 2026-11-13, `api.data.gov.in` (164.100.61.198:443) refused TCP connections from the US while other sites worked, so ingestion must egress from India. |
| Could not verify from here | data.gov.in returned HTTP 403 / connection refused to my fetcher, and a direct shell probe was blocked. **You need to sign up for a key and run the probe script (`scripts/probe_sources.py`) yourself.** |

**Probe results (2026-11-13, from an EC2 instance in ap-south-1, `scripts/probe_sources.py`):**

| Resource | id | Total rows | Fields | Date filter |
|---|---|---|---|---|
| Current Daily Price… (Mandi) | `9ef84268-d588-465a-a308-a864a43d0070` | 17,636 (today only) | lower-case: state, district, market, commodity, variety, grade, arrival_date (dd/mm/yyyy), min/max/modal_price (ints) | `filters[arrival_date]=01/09/2023` → 0 (no history, as expected) |
| Variety-wise Daily Market Prices | `35985678-0d79-46b4-9ed6-6f13308a1d24` | 83,049,851 | Title-case: Arrival_Date, Commodity, **Commodity_Code**, District, Grade, Market, Min/Max/Modal_Price (**strings**), State, Variety | `filters[Arrival_Date]=01/09/2023` → 13,515 rows ✅ |

- **History option A confirmed** → use the variety-wise resource for backfill and the current resource for the daily run. Both lack arrivals (tonnes).
- The two resources differ in field-name case and price types; the bronze→silver step must normalise both.
- All three date spellings returned the same 13,515 rows, so the filter parses dates leniently; the client will send `dd/mm/yyyy` and assert returned `Arrival_Date` matches.
- **api.data.gov.in stalls with no response on the default `Python-urllib` User-Agent** (60 s read timeout), while `curl` and any custom UA answer in ~0.3 s. Every client must set a User-Agent.
- Latency from Mumbai: 0.3–1.0 s per call.

**History options (pick one — see recommendation):**
- **A. data.gov.in "Variety-wise Daily Market Prices Data of Commodity"** — https://www.data.gov.in/resource/variety-wise-daily-market-prices-data-commodity . Same Agmarknet source, includes `arrival_date`; third-party projects report history back to ~2010 and a date filter. *Unverified* — needs your key. **Recommended first try.**
- **B. Agmarknet 2.0 portal reports** — https://agmarknet.gov.in/ (Prices & Arrivals → date-wise / commodity-wise reports). Has arrivals. No documented API; scraping an ASP.NET/JS app is brittle and slow. Fallback only.
- **C. Third-party bulk mirrors** (CEDA Ashoka https://dca.ceda.ashoka.edu.in , India Data Portal, Kaggle 2023–25). Fast, but licence/provenance must be checked and they aren't "my pipeline ingesting the source".
- **Caveat:** Agmarknet widened market coverage from Nov 2025 (reported in a third-party repo, unverified) → row-count drift checks must account for a structural break.

## 2. APEDA / DGCIS basmati exports — ⚠️

- APEDA AgriExchange monthly export page: https://agriexchange.apeda.gov.in/indexp/monthexport.aspx (has a Basmati Rice product group).
- Country-wise product group report: https://agriexchange.apeda.gov.in/IntTrade/PrdGrpReport.aspx (basmati by country for 2021-22..2023-24, annual).
- Source of truth is DGCI&S; APEDA republishes it. Monthly × country × basmati availability and a download/export button were **not confirmed** — needs a manual check in a browser (ASP.NET postback forms; likely scrapeable with form replay).
- Alternative if monthly×country isn't exposed: TradeStat (Dept. of Commerce, https://tradestat.commerce.gov.in) at HS 10063020 (basmati) monthly by country.
- Not acceptable: Zauba (US customs records, not official).

## 3. Open-Meteo historical weather — ✅

Live call succeeded:
`https://archive-api.open-meteo.com/v1/archive?latitude=29.69&longitude=76.99&start_date=2023-08-01&end_date=2023-08-03&daily=precipitation_sum,temperature_2m_max,temperature_2m_min&timezone=Asia/Kolkata`
→ 2023-08-01..03 precip 0.0 / 10.6 / 3.0 mm, tmax 34.4 / 34.2 / 33.1 °C (Karnal area). Free, no key, non-commercial use. District centroids will be a seed CSV (Punjab, Haryana, western UP basmati districts).

## 4. MEP policy dates — ✅ (2 of 3 official), ⚠️ (Oct 2023 official doc pending)

| Event | Value | Date | Source | Status |
|---|---|---|---|---|
| MEP introduced (RCAC only for contracts ≥ $1,200/t) | $1,200/t | PIB release 27 Aug 2023 | https://www.pib.gov.in/PressReleasePage.aspx?PRID=1952629 | ✅ official |
| Cut | $1,200 → $950/t | Effective 26 Oct 2023 (Commerce Ministry statement) | https://www.deccanherald.com/business/economy/govt-cuts-floor-price-of-basmati-exports-to-950-per-tonne-2743246 ; Reuters via https://www.malaymail.com/news/money/2023/10/26/source-india-cuts-basmati-rice-export-floor-price-to-us950-a-tonne/98581 | ⚠️ news citing ministry; official PIB/APEDA notice not located yet |
| Removed | $950 → none | Order Fri 13 Sep 2024; PIB release 14 Sep 2024 | https://www.pib.gov.in/PressReleasePage.aspx?PRID=2054831 ; https://dawn.com/news/1859129 | ✅ official |

Your recollection (≈$1,200 Aug 2023 → $950 Oct 2023 → removed Sep 2024) matches. A consultancy blog cites "DGFT Notification 30/2023, 42/2023, 30/2024" — **not verified, not used**. Note the MEP was implemented through APEDA RCAC registration rules, not a classic DGFT export-policy notification, so PIB is the right primary source.

## Versions checked on PyPI (2026-11-12)

| Package | Latest | Note |
|---|---|---|
| apache-airflow | 3.4.0 | Python `>=3.11,!=3.15`. Runs in Docker (official image). |
| pyspark | 4.3.0 | Released 2026-10-30 (<2 weeks). Plan: pin latest 4.2.x unless you want 4.3. Spark 4 needs Java 17/21 — your Mac has Java 25, so Spark runs in Docker. |
| dbt-core | 1.12.7 | ✅ meets 1.8+ |
| dbt-duckdb | 1.11.1 | requires dbt-core>=1.8, duckdb>=1.0 |
| duckdb | **2.0.0** | Your spec says DuckDB 1.x. Latest 1.x line is 1.5.x. Decision needed. |
| great-expectations (GX Core) | 1.25.0 | Python `<3.15` |

Local machine: uv installed, Python 3.13 + 3.14 available, Docker 29.9 installed, **disk 25 GB free (95% full)**, 18 GB RAM.
