"""Weekly rice / basmati price forecasts, 1-4 weeks ahead, from the warehouse.

Series:  a rice variety (as reported, e.g. "Rice Common") or a reviewed basmati group and form
         (e.g. "1121 paddy"), per region ("All India" or a state with long history).
Target:  log(price[t+h] / price[t]), the % change from the latest week, so series priced at
         2,500 and 7,000 Rs/quintal can share one model. Weekly price = median of the
         market-day modal prices (robust to the odd mis-keyed report).
Model:   scikit-learn HistGradientBoostingRegressor with quantile loss: one model each for
         the 10th, 50th and 90th percentile, giving a forecast and an 80% band.
Checked: trained only on targets before TEST_START, scored on everything after, against
         two baselines: "no change" and "same change as the same weeks last year".

Every feature uses data up to week t only (no look-ahead).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd

HORIZONS = (1, 2, 3, 4)
QUANTILES = (0.1, 0.5, 0.9)
TEST_START = pd.Timestamp("2024-01-01")
MIN_WEEKS = 300  # a series/region needs this much history to be modelled
MIN_MARKETS = 3  # ...and this many reporting mandis in a typical week: with 1-2, the weekly
# "price" mostly reflects WHICH mandi reported, not a price change
CALIBRATION_WEEKS = 52  # held out to calibrate the 80% band (conformal)
MEP_ON, MEP_OFF = pd.Timestamp("2023-08-27"), pd.Timestamp("2024-09-13")

WEEKLY_SQL = """
select date_trunc('week', f.arrival_date)::date as week, m.state,
       case when v.is_basmati then v.variety_group || ' ' || v.form
            when lower(c.commodity) = 'rice' then 'Rice ' || f.raw_variety end as series,
       f.modal_price, f.market_id
from marts.fct_daily_price f
join marts.dim_commodity c using (commodity_id)
left join marts.dim_variety v using (variety_id)
join marts.dim_market m using (market_version_id)
"""


def weekly_panel(con) -> pd.DataFrame:
    """One row per series x region x week: median modal price and reporting mandis."""
    raw = con.sql(WEEKLY_SQL).df().dropna(subset=["series"])
    by_state = raw.groupby(["series", "state", "week"]).agg(
        price=("modal_price", "median"), markets=("market_id", "nunique")
    )
    india = raw.groupby(["series", "week"]).agg(
        price=("modal_price", "median"), markets=("market_id", "nunique")
    )
    india = india.assign(state="All India").set_index("state", append=True)
    india = india.reorder_levels(["series", "state", "week"])
    panel = pd.concat([by_state, india]).reset_index().rename(columns={"state": "region"})
    panel["week"] = pd.to_datetime(panel["week"])
    g = panel.groupby(["series", "region"])
    keep = (g["week"].transform("nunique") >= MIN_WEEKS) & (
        g["markets"].transform("median") >= MIN_MARKETS
    )
    return panel[keep].sort_values(["series", "region", "week"])


def features(panel: pd.DataFrame) -> pd.DataFrame:
    """Per series/region on a complete weekly calendar; NaN where a lag has no data."""
    out = []
    for (series, region), g in panel.groupby(["series", "region"], sort=False):
        g = g.set_index("week").asfreq("W-MON")  # missing weeks become explicit NaN
        lp = np.log(g["price"])
        f = pd.DataFrame(index=g.index)
        f["series"], f["region"], f["price"] = series, region, g["price"]
        for lag in (1, 2, 4, 8, 13, 26, 52):
            f[f"r{lag}"] = lp - lp.shift(lag)  # change over the last `lag` weeks
        f["dev13"] = lp - lp.rolling(13, min_periods=6).mean()  # vs the 13-week average
        f["vol8"] = lp.diff().rolling(8, min_periods=4).std()  # recent volatility
        f["markets"] = np.log1p(g["markets"])
        f["markets_vs13"] = f["markets"] - f["markets"].rolling(13, min_periods=6).mean()
        # last year's move over the next 1-4 weeks: the seasonal pattern (known at t)
        for h in HORIZONS:
            f[f"ly{h}"] = lp.shift(52 - h) - lp.shift(52)
            f[f"y{h}"] = lp.shift(-h) - lp  # target: change over the next h weeks
        out.append(f)
    df = pd.concat(out).rename_axis("week").reset_index()
    woy = df["week"].dt.isocalendar().week.astype(float)
    df["woy_sin"], df["woy_cos"] = np.sin(2 * np.pi * woy / 52), np.cos(2 * np.pi * woy / 52)
    df["mep_active"] = df["week"].between(MEP_ON, MEP_OFF).astype(int)
    return df[df["price"].notna()]


FEATURES = [
    "h", "r1", "r2", "r4", "r8", "r13", "r26", "r52", "dev13", "vol8", "markets",
    "markets_vs13", "ly", "woy_sin", "woy_cos", "mep_active", "series_code", "region_code",
]  # fmt: skip


def long_form(df: pd.DataFrame) -> pd.DataFrame:
    """One row per (week, series, region, horizon): the horizon is a feature."""
    parts = []
    for h in HORIZONS:
        p = df.drop(columns=[c for c in df if c[:1] in "yl" and c[1:].isdigit()]).copy()
        p["h"], p["ly"], p["y"] = h, df[f"ly{h}"], df[f"y{h}"]
        p["target_week"] = p["week"] + pd.Timedelta(weeks=h)
        parts.append(p)
    out = pd.concat(parts, ignore_index=True)
    out["series_code"] = out["series"].astype("category").cat.codes
    out["region_code"] = out["region"].astype("category").cat.codes
    return out


def _model(q: float):
    from sklearn.ensemble import HistGradientBoostingRegressor

    return HistGradientBoostingRegressor(
        loss="quantile", quantile=q, max_iter=400, learning_rate=0.05,
        max_leaf_nodes=31, min_samples_leaf=50, l2_regularization=1.0,
        categorical_features=[FEATURES.index("series_code"), FEATURES.index("region_code")],
        random_state=0,
    )  # fmt: skip


@dataclass
class Backtest:
    metrics: pd.DataFrame  # MAPE by model and horizon on the test period
    coverage: float  # share of test actuals inside the 80% band
    n_train: int
    n_test: int


def fit_predict(train: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    """Median + an 80% band, conformally calibrated (CQR).

    The 10th/90th-percentile models are fit without the last CALIBRATION_WEEKS of targets;
    on those held-out weeks we measure how far actuals fall outside the raw band, and widen
    the band by the 80th percentile of that miss, so it covers ~80% of new outcomes.
    """
    cut = train["target_week"].max() - pd.Timedelta(weeks=CALIBRATION_WEEKS)
    fit, cal = train[train["target_week"] <= cut], train[train["target_week"] > cut]
    lo, hi = _model(0.1).fit(fit[FEATURES], fit["y"]), _model(0.9).fit(fit[FEATURES], fit["y"])
    miss = np.maximum(lo.predict(cal[FEATURES]) - cal["y"], cal["y"] - hi.predict(cal[FEATURES]))
    widen = float(np.quantile(miss, min(1.0, 0.8 * (1 + 1 / len(cal)))))
    mid = _model(0.5).fit(train[FEATURES], train["y"]).predict(new[FEATURES])
    out = pd.DataFrame(index=new.index)
    out["q50"] = mid
    out["q10"] = np.minimum(lo.predict(new[FEATURES]) - widen, mid)
    out["q90"] = np.maximum(hi.predict(new[FEATURES]) + widen, mid)
    return out


def backtest(rows: pd.DataFrame) -> Backtest:
    data = rows.dropna(subset=["y"])
    train = data[data["target_week"] < TEST_START]
    test = data[data["week"] >= TEST_START].copy()
    pred = fit_predict(train, test)
    actual = test["price"] * np.exp(test["y"])
    candidates = {
        "model": test["price"] * np.exp(pred["q50"]),
        "no change": test["price"],
        "last year's change": test["price"] * np.exp(test["ly"].fillna(0)),
    }
    rows_ = []
    for name, p in candidates.items():
        ape = (p - actual).abs() / actual
        for h, e in ape.groupby(test["h"]):
            rows_.append({"model": name, "h": h, "mape_pct": round(100 * e.mean(), 2)})
        rows_.append({"model": name, "h": "all", "mape_pct": round(100 * ape.mean(), 2)})
    inside = (test["y"] >= pred["q10"]) & (test["y"] <= pred["q90"])
    return Backtest(pd.DataFrame(rows_), round(float(inside.mean()), 3), len(train), len(test))


def forecast(rows: pd.DataFrame) -> pd.DataFrame:
    """Train on all history with a known outcome, predict from each series' latest week."""
    known = rows.dropna(subset=["y"])
    latest = rows[rows["week"] == rows.groupby(["series", "region"])["week"].transform("max")]
    # only series still reporting recently (latest week within 3 weeks of the newest data)
    latest = latest[latest["week"] >= rows["week"].max() - pd.Timedelta(weeks=3)].copy()
    pred = fit_predict(known, latest)
    for q in ("q10", "q50", "q90"):
        latest[q] = latest["price"] * np.exp(pred[q])
    return latest[["series", "region", "week", "h", "target_week", "price", "q10", "q50", "q90"]]


def run(db_path: str, run_date: date | None = None) -> dict:
    """Backtest, forecast, and write ml.* tables into the warehouse file."""
    import duckdb

    con = duckdb.connect(db_path)
    rows = long_form(features(weekly_panel(con)))
    bt = backtest(rows)
    fc = forecast(rows)
    fc["run_date"] = pd.Timestamp(run_date or date.today())
    metrics = bt.metrics.assign(
        coverage_80=bt.coverage, n_train=bt.n_train, n_test=bt.n_test,
        test_start=TEST_START, run_date=fc["run_date"].iloc[0],
    )  # fmt: skip
    con.execute("CREATE SCHEMA IF NOT EXISTS ml")
    con.register("fc", fc)
    con.register("m", metrics.astype({"h": str}))
    con.execute("CREATE OR REPLACE TABLE ml.price_forecast AS SELECT * FROM fc")
    con.execute("CREATE OR REPLACE TABLE ml.backtest_metrics AS SELECT * FROM m")
    # forecast log: every run's predictions, so accuracy can be checked as actuals arrive
    con.execute("CREATE TABLE IF NOT EXISTS ml.forecast_log AS SELECT * FROM fc WHERE false")
    con.execute("DELETE FROM ml.forecast_log WHERE run_date = (SELECT max(run_date) FROM fc)")
    con.execute("INSERT INTO ml.forecast_log SELECT * FROM fc")
    con.close()
    overall = bt.metrics[bt.metrics["h"] == "all"].set_index("model")["mape_pct"]
    return {
        "series": int(fc[["series", "region"]].drop_duplicates().shape[0]),
        "coverage_80": bt.coverage,
        "n_train": bt.n_train,
        "n_test": bt.n_test,
        "mape": overall.to_dict(),
    }


if __name__ == "__main__":
    import json
    import sys

    print(json.dumps(run(sys.argv[1]), indent=2, default=str))
