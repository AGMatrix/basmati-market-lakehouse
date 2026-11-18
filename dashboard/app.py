"""Basmati price dashboard (Streamlit). Run: make dashboard

Reads the *serving copy* of the warehouse (data/warehouse_serving.duckdb), which Airflow
replaces only after dbt and the quality gate pass, so this app never contends with dbt for
DuckDB's single-writer lock and never shows a half-built or failed build.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
SERVING = ROOT / "data" / "warehouse_serving.duckdb"
# Hosted (Streamlit Community Cloud): no lake, so read the basmati extract that
# warehouse_build uploads to a private S3 bucket, using a read-only key in st.secrets["s3"].
# Without secrets, fall back to the snapshot committed in dashboard/demo/ (make demo-db).
DEMO = ROOT / "dashboard" / "demo" / "basmati_demo.duckdb"
HOSTED = Path(tempfile.gettempdir()) / "basmati_hosted.duckdb"


def s3_config() -> dict | None:
    try:
        return dict(st.secrets["s3"])
    except (KeyError, FileNotFoundError, st.errors.StreamlitSecretNotFoundError):
        return None


@st.cache_resource(ttl=3600)  # re-download at most hourly; the pipeline updates it daily
def fetch_hosted(cfg: dict) -> Path:
    import boto3

    s3 = boto3.client(
        "s3",
        region_name=cfg.get("region", "us-east-1"),
        aws_access_key_id=cfg["aws_access_key_id"],
        aws_secret_access_key=cfg["aws_secret_access_key"],
    )
    tmp = HOSTED.with_suffix(".tmp")
    s3.download_file(cfg["bucket"], cfg.get("key", "serving/basmati_demo.duckdb"), str(tmp))
    with duckdb.connect(str(tmp), read_only=True) as con:  # reject a corrupt/partial file
        con.sql("select count(*) from marts.fct_daily_price").fetchone()
    os.replace(tmp, HOSTED)
    return HOSTED


def pick_db() -> Path:
    if os.environ.get("SERVING_DB"):
        return Path(os.environ["SERVING_DB"])
    if SERVING.exists():
        return SERVING
    cfg = s3_config()
    if cfg is None:  # not cached: secrets added later take effect on the next page load
        print("data source: committed snapshot (no [s3] secrets)")
        return DEMO
    try:
        path = fetch_hosted(cfg)
        print(f"data source: s3://{cfg['bucket']}/{cfg.get('key', '')}")
        return path
    except Exception as e:  # noqa: BLE001 (any S3 problem: show the committed snapshot)
        st.warning(f"Could not load the latest data from S3 ({type(e).__name__}); "
                   "showing the committed snapshot.")  # fmt: skip
        return DEMO


DB = pick_db()

# Reference categorical palette, validated (light + dark) with the dataviz validator.
# Each variety group owns a fixed slot, so filtering never repaints the remaining series.
GROUP_ORDER = ["1121", "1509", "1718", "1401", "Traditional", "1885", "1886", "Other Basmati"]
PALETTE = {
    "light": ["#2a78d6", "#eb6834", "#1baf7a", "#eda100",
              "#e87ba4", "#008300", "#6250d6", "#e34948"],
    "dark": ["#3987e5", "#d95926", "#199e70", "#c98500",
             "#d55181", "#0ca30c", "#9085e9", "#e66767"],
}  # fmt: skip
INK = {
    "light": {"muted": "#898781", "grid": "#e1e0d9", "axis": "#c3c2b7", "text": "#52514e"},
    "dark": {"muted": "#898781", "grid": "#2c2c2a", "axis": "#383835", "text": "#c3c2b7"},
}

st.set_page_config(page_title="Basmati prices vs export policy", layout="wide")
mode = "dark" if getattr(getattr(st.context, "theme", None), "type", "light") == "dark" else "light"
ink = INK[mode]
colors = alt.Scale(domain=GROUP_ORDER, range=PALETTE[mode])


def style(chart: alt.Chart) -> alt.Chart:
    """Recessive grid and axes; text in ink tokens, never series colors."""
    return (
        chart.configure_axis(
            gridColor=ink["grid"],
            domainColor=ink["axis"],
            tickColor=ink["axis"],
            labelColor=ink["muted"],
            titleColor=ink["text"],
            labelFontSize=12,
        )  # fmt: skip
        .configure_legend(labelColor=ink["text"], titleColor=ink["text"], orient="top")
        .configure_view(strokeWidth=0)
    )


# Cache keys include the file's modification time, so a newly downloaded extract gets a new
# connection and fresh query results instead of stale cached ones.
VERSION = DB.stat().st_mtime if DB.exists() else 0.0


@st.cache_resource
def connect(db: str, version: float) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(db, read_only=True)


@st.cache_data(ttl=300)
def query(sql: str, db: str = str(DB), version: float = VERSION) -> pd.DataFrame:
    return connect(db, version).sql(sql).df()


if not DB.exists():
    st.title("Basmati prices vs export policy")
    st.warning(
        f"No published warehouse yet ({DB.relative_to(ROOT)}). It appears after the first "
        "warehouse_build run whose quality gate passes; until the basmati variety names are "
        "reviewed (make map-suggest, then make map-merge), the gate holds publication."
    )
    st.stop()

events = query("select event_id, event_date, event_type, mep_usd_per_tonne_after, "
               "verification_status from marts.dim_policy_event order by event_date")  # fmt: skip
prices = query(
    """
    select date_trunc('week', f.arrival_date)::date as week, v.variety_group, v.form,
           m.state, avg(f.modal_price) as modal_price, count(*) as market_days
    from marts.fct_daily_price f
    join marts.dim_variety v using (variety_id)
    join marts.dim_market m using (market_version_id)
    where v.is_basmati
    group by all
    """
)
if prices.empty:
    st.title("Basmati prices vs export policy")
    st.info(
        "The warehouse has no reviewed basmati varieties yet. Run make map-suggest / map-merge."
    )
    st.stop()
prices["variety_group"] = prices["variety_group"].where(
    prices["variety_group"].isin(GROUP_ORDER), "Other Basmati"
)

# --- filters: one row above the charts ------------------------------------------------------
st.title("Basmati prices vs export policy")
st.caption(
    "Agmarknet daily mandi prices (Rs per quintal), weekly average of market-day modal prices. "
    "Vertical lines mark Minimum Export Price (MEP) changes."
)
c1, c2, c3, c4 = st.columns([2, 2, 2, 3])
form = c1.segmented_control("Form", ["paddy", "rice"], default="paddy")
states = sorted(prices["state"].unique())
state = c2.selectbox("State", ["All states", *states])
groups = [g for g in GROUP_ORDER if g in set(prices.loc[prices["form"] == form, "variety_group"])]
variety = c3.selectbox("Variety", ["All varieties", *groups])
prices["week"] = pd.to_datetime(prices["week"])
lo, hi = prices["week"].min().date(), prices["week"].max().date()
start, end = c4.slider("Weeks", min_value=lo, max_value=hi, value=(lo, hi), format="YYYY-MM-DD")


def by_variety(df: pd.DataFrame, col: str = "variety_group") -> pd.DataFrame:
    """Keep rows of the selected variety (groups outside GROUP_ORDER count as Other Basmati)."""
    if variety == "All varieties":
        return df
    return df[df[col].where(df[col].isin(GROUP_ORDER), "Other Basmati") == variety]


sel = prices[
    (prices["form"] == form) & prices["week"].between(pd.Timestamp(start), pd.Timestamp(end))
]
if state != "All states":
    sel = sel[sel["state"] == state]
sel = by_variety(sel)
weekly = (
    sel.assign(w=sel["modal_price"] * sel["market_days"])
    .groupby(["week", "variety_group"], as_index=False)[["w", "market_days"]]
    .sum()
)
weekly["modal_price"] = weekly["w"] / weekly["market_days"]
# Weeks with no reports become explicit gaps (null), so the line breaks instead of drawing a
# smooth curve through months with no data.
if not weekly.empty:
    spine = pd.MultiIndex.from_product(
        [pd.date_range(weekly["week"].min(), weekly["week"].max(), freq="W-MON"),
         weekly["variety_group"].unique()], names=["week", "variety_group"],
    )  # fmt: skip
    weekly = weekly.set_index(["week", "variety_group"]).reindex(spine).reset_index()
    # Each unbroken run of weeks becomes its own line segment (Vega-Lite would otherwise either
    # join across the gap or, with invalid=None, draw missing weeks as zero).
    weekly["segment"] = weekly.groupby("variety_group")["modal_price"].transform(
        lambda p: p.isna().cumsum()
    )
    weekly = weekly.dropna(subset=["modal_price"])

# --- 1. price trend with MEP markers --------------------------------------------------------
st.subheader("Weekly modal price by variety")
hover = alt.selection_point(fields=["week"], nearest=True, on="pointerover", empty=False)
base = alt.Chart(weekly).encode(
    x=alt.X("week:T", title=None),
    y=alt.Y("modal_price:Q", title="Rs per quintal", scale=alt.Scale(zero=False)),
    color=alt.Color("variety_group:N", scale=colors, title="Variety group"),
)
lines = base.mark_line(strokeWidth=2).encode(detail="segment:N")
points = (
    base.mark_point(size=64, filled=True, strokeWidth=2, stroke="white")
    .encode(
        opacity=alt.condition(hover, alt.value(1), alt.value(0)),
        tooltip=[
            alt.Tooltip("week:T", title="Week of"),
            alt.Tooltip("variety_group:N", title="Variety"),
            alt.Tooltip("modal_price:Q", title="Rs/quintal", format=",.0f"),
            alt.Tooltip("market_days:Q", title="Market-days"),
        ],
    )
    .add_params(hover)
)
crosshair = (
    alt.Chart(weekly).mark_rule(color=ink["axis"]).encode(x="week:T").transform_filter(hover)
)
events["event_date"] = pd.to_datetime(events["event_date"])
ev = events[events["event_date"].between(pd.Timestamp(start), pd.Timestamp(end))].copy()
# Events can be weeks apart (Aug and Oct 2023): stagger the labels so they never overlap.
ev["label_y"] = [8 + 14 * (i % 2) for i in range(len(ev))]
rules = alt.Chart(ev).mark_rule(color=ink["muted"], strokeDash=[4, 4], strokeWidth=1).encode(
    x="event_date:T",
    tooltip=[alt.Tooltip("event_id:N", title="Event"), alt.Tooltip("event_date:T", title="Date"),
             alt.Tooltip("mep_usd_per_tonne_after:Q", title="MEP after (USD/t)")],
)  # fmt: skip
labels = (
    alt.Chart(ev)
    .mark_text(align="left", dx=4, dy=0, color=ink["text"], fontSize=11)
    .encode(x="event_date:T", y=alt.Y("label_y:Q", scale=None), text="event_type:N")
)
st.altair_chart(style(alt.layer(lines, points, crosshair, rules, labels).properties(height=380)),
                width="stretch")  # fmt: skip
with st.expander("Table view"):
    st.dataframe(weekly.pivot(index="week", columns="variety_group", values="modal_price").round(0))

# --- 2. before vs after each MEP change -----------------------------------------------------
st.subheader("Before vs after each MEP change")
pv = query("select * from marts.mart_basmati_price_vs_policy")
k1, k2 = st.columns(2)
event = k1.selectbox("Event", events["event_id"], index=len(events) - 1)
window = k2.segmented_control("Window (days)", [30, 60, 90], default=60)
row = pv[(pv["event_id"] == event) & (pv["window_days"] == window)
         & (pv["state"] == (state if state != "All states" else "All India"))]  # fmt: skip
row = by_variety(row)
if row.empty:
    st.info(
        "No prices for this selection around this event. The policy comparison covers 1121, "
        "1509 and Other Basmati; 1718, 1885 and 1886 first appear in the data in June 2026, "
        "after all three MEP changes."
        if variety not in ("All varieties", "1121", "1509", "Other Basmati")
        else "No basmati prices in this window for the selected state."
    )
else:
    long = row.melt(
        id_vars=["variety_group", "pct_change", "n_markets"],
        value_vars=["avg_modal_before", "avg_modal_after"], var_name="side", value_name="price",
    )  # fmt: skip
    long["side"] = long["side"].map({"avg_modal_before": "Before", "avg_modal_after": "After"})
    bars = alt.Chart(long).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
        x=alt.X("variety_group:N", title=None, sort=GROUP_ORDER),
        xOffset=alt.XOffset("side:N", sort=["Before", "After"]),
        y=alt.Y("price:Q", title="Average modal price, Rs per quintal"),
        color=alt.Color("variety_group:N", scale=colors, legend=None),
        opacity=alt.Opacity("side:N", scale=alt.Scale(domain=["Before", "After"], range=[0.45, 1]),
                            legend=alt.Legend(title=None)),
        tooltip=["variety_group", "side", alt.Tooltip("price:Q", format=",.0f"),
                 alt.Tooltip("pct_change:Q", title="% change", format="+.1f"), "n_markets"],
    )  # fmt: skip
    st.altair_chart(style(bars.properties(height=320)), width="stretch")
    st.dataframe(row[["variety_group", "avg_modal_before", "avg_modal_after", "pct_change",
                      "n_market_days_before", "n_market_days_after", "n_markets"]],
                 hide_index=True)  # fmt: skip
    st.caption(
        "Before/after comparison, not a causal estimate: harvest season, global prices and "
        "the overlap of the Aug and Oct 2023 windows also move prices."
    )

# --- 3. where to buy now --------------------------------------------------------------------
st.subheader("Where to buy now: cheapest recent prices by variety")
recent = query(
    f"""
    with last as (select max(arrival_date) as d from marts.fct_daily_price),
    latest as (  -- each market's most recent report per variety in the last 14 days
        select v.variety_group, m.state, m.district, m.market, f.grade,
               f.arrival_date, f.min_price, f.modal_price, f.max_price,
               row_number() over (partition by v.variety_group, f.market_id
                                  order by f.arrival_date desc) as rn
        from marts.fct_daily_price f
        join marts.dim_variety v using (variety_id)
        join marts.dim_market m using (market_version_id), last
        where v.is_basmati and v.form = '{form}' and f.arrival_date > last.d - 14
    )
    select * exclude (rn) from latest where rn = 1
    """  # noqa: S608 (form comes from a fixed two-option control)
)
if state != "All states":
    recent = recent[recent["state"] == state]
recent = by_variety(recent)
if recent.empty:
    st.info("No basmati reports in the last 14 days for this form and state.")
else:
    as_of = pd.to_datetime(recent["arrival_date"]).max().date()
    st.caption(
        f"Latest reported mandi prices, as of {as_of:%d %b %Y} (each market's most recent "
        "report in the last 14 days). Agmarknet publishes daily reports, not live quotes, so "
        "this is the most recent data, not a real-time price. Modal = the most common traded "
        "price. Grade, moisture, transport and mandi fees also change the real cost."
    )
    # A report far below the variety's median is usually another grade, broken grains or a
    # data-entry slip, not a bargain: flag it, and pick "cheapest" among the rest.
    median = recent.groupby("variety_group")["modal_price"].transform("median")
    recent["check"] = (recent["modal_price"] < 0.7 * median).map(
        {True: "verify: >30% below median", False: ""}
    )
    plausible = recent[recent["check"] == ""]
    cheapest = plausible.loc[plausible.groupby("variety_group")["modal_price"].idxmin()]
    summary = (
        recent.groupby("variety_group")
        .agg(markets=("market", "nunique"), median_modal=("modal_price", "median"),
             flagged=("check", lambda c: (c != "").sum()))
        .join(cheapest.set_index("variety_group")[["market", "state", "modal_price",
                                                     "arrival_date"]])
        .rename(columns={"market": "cheapest_market", "modal_price": "cheapest_modal",
                         "arrival_date": "reported_on"})
        .reset_index()
    )  # fmt: skip
    summary["saving_vs_median_%"] = (
        100 * (summary["median_modal"] - summary["cheapest_modal"]) / summary["median_modal"]
    ).round(1)
    st.dataframe(summary.round({"median_modal": 0, "cheapest_modal": 0}), hide_index=True,
                 width="stretch")  # fmt: skip
    st.caption(
        "flagged = markets whose latest price is over 30% below the variety's median; they "
        "are listed below with a 'verify' note but not picked as cheapest."
    )
    pick = (
        st.selectbox("Cheapest markets for variety", sorted(recent["variety_group"].unique()))
        if variety == "All varieties" or recent["variety_group"].nunique() > 1
        else recent["variety_group"].iloc[0]
    )
    st.dataframe(
        recent[recent["variety_group"] == pick]
        .sort_values("modal_price")
        .head(10)[["market", "district", "state", "grade", "arrival_date",
                   "min_price", "modal_price", "max_price", "check"]],
        hide_index=True, width="stretch",
    )  # fmt: skip

# --- 4. mandi prices, portal style ---------------------------------------------------------
st.subheader("Mandi prices by state")
reports = query(
    """
    with last as (select max(arrival_date) as d from marts.fct_daily_price)
    select f.arrival_date, m.state, m.district, m.market, v.commodity, f.raw_variety,
           v.variety_group, f.grade, f.min_price, f.max_price, f.modal_price
    from marts.fct_daily_price f
    join marts.dim_variety v using (variety_id)
    join marts.dim_market m using (market_version_id), last
    where v.is_basmati and f.arrival_date > last.d - 90
    """
)
reports["arrival_date"] = pd.to_datetime(reports["arrival_date"]).dt.date
m1, m2, m3 = st.columns([2, 2, 3])
report_states = sorted(reports["state"].unique())
default_state = state if state in report_states else "Punjab"
mandi_state = m1.selectbox(
    "State", report_states, key="mandi_state",
    index=report_states.index(default_state) if default_state in report_states else 0,
)  # fmt: skip
in_state = reports[reports["state"] == mandi_state]
district = m2.selectbox("District", ["All districts", *sorted(in_state["district"].unique())])
days = m3.segmented_control("Reported in the last (days)", [1, 7, 30, 90], default=30) or 30
as_of = reports["arrival_date"].max()
rows = in_state[in_state["arrival_date"] > as_of - pd.Timedelta(days=days)]
if district != "All districts":
    rows = rows[rows["district"] == district]
rows = by_variety(rows)
# Latest report per mandi x commodity x variety x grade, like the portal's daily listing.
latest = (
    rows.sort_values("arrival_date")
    .groupby(["market", "commodity", "raw_variety", "grade"], as_index=False)
    .last()
    .sort_values(["district", "market", "commodity", "raw_variety"])
)
if latest.empty:
    st.info(
        f"No basmati reports from {mandi_state} in the {days}-day window ending {as_of:%d %b %Y}."
    )
else:
    st.caption(
        f"{latest['market'].nunique()} mandis in {mandi_state}: each one's latest basmati report "
        f"in the {days} days to {as_of:%d %b %Y} (the newest data loaded). Agmarknet publishes "
        "daily reports, not live quotes; the pipeline adds a new day each evening while it runs. "
        "Prices in Rs per quintal."
    )
    st.dataframe(
        latest[["arrival_date", "district", "market", "commodity", "raw_variety", "grade",
                "min_price", "max_price", "modal_price"]].rename(columns={
            "arrival_date": "Price date", "district": "District", "market": "Market",
            "commodity": "Commodity", "raw_variety": "Variety", "grade": "Grade",
            "min_price": "Min price", "max_price": "Max price", "modal_price": "Modal price"}),
        hide_index=True, width="stretch",
    )  # fmt: skip

st.caption(
    "Exports by destination are not shown: the APEDA / DGCI&S monthly-by-country source is "
    "not yet verified (docs/STEP0_SOURCES.md)."
)
