import numpy as np
import pandas as pd

from basmati import forecast as F


def _panel(weeks=320, seed=0):
    rng = np.random.default_rng(seed)
    idx = pd.date_range("2018-01-01", periods=weeks, freq="W-MON")
    season = 1 + 0.05 * np.sin(2 * np.pi * np.arange(weeks) / 52)
    price = 3000 * season * np.exp(np.cumsum(rng.normal(0, 0.01, weeks)))
    return pd.DataFrame({"series": "Rice Common", "region": "All India", "week": idx,
                         "price": price, "markets": 10})  # fmt: skip


def test_features_use_no_future_data():
    p = _panel()
    base = F.features(p)
    # change only the LAST week's price: no feature of any earlier week may change
    p2 = p.copy()
    p2.loc[p2.index[-1], "price"] *= 2
    changed = F.features(p2)
    cols = [c for c in base if c[:1] in "rd" or c.startswith(("vol", "ly", "markets"))]
    early = base["week"] < p["week"].iloc[-1]
    pd.testing.assert_frame_equal(base.loc[early, cols], changed.loc[early, cols])


def test_target_is_future_change():
    f = F.features(_panel())
    lp = np.log(f["price"].to_numpy())
    assert np.allclose(f["y1"].to_numpy()[:-1], lp[1:] - lp[:-1])


def test_band_is_ordered_and_contains_the_median():
    rows = F.long_form(F.features(_panel(weeks=400)))
    known = rows.dropna(subset=["y"])
    new = rows[rows["week"] == rows["week"].max()]
    out = F.fit_predict(known, new)
    assert (out["q10"] <= out["q50"]).all() and (out["q50"] <= out["q90"]).all()
