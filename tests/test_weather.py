from datetime import date

from basmati.ingest.weather import District, _candidates, fetch_daily, geocode, slugify


class Resp:
    def __init__(self, body, status=200, headers=None):
        self.body = body
        self.status_code = status
        self.headers = headers or {}

    def raise_for_status(self):
        pass

    def json(self):
        return self.body


class Session:
    def __init__(self, bodies):
        self.bodies = list(bodies)
        self.params = []

    def get(self, url, params, timeout):
        self.params.append(params)
        b = self.bodies.pop(0)
        return b if isinstance(b, Resp) else Resp(b)


def test_candidates_try_bracketed_alternative():
    assert _candidates("Jalaun (orai)") == ["Jalaun (orai)", "Jalaun", "orai"]
    assert _candidates("Karnal") == ["Karnal"]


def test_slugify_is_stable():
    assert slugify("Uttar Pradesh-Bulandshahar") == "uttar-pradesh-bulandshahar"
    assert District("Haryana", "Karnal").slug == "haryana-karnal"


def test_geocode_requires_state_match_and_falls_back_to_alternative_name():
    s = Session([
        {"results": [{"name": "Jalaun", "admin1": "Bihar", "latitude": 1, "longitude": 2}]},
        {"results": []},
        {"results": [{"name": "Orai", "admin1": "Uttar Pradesh",
                      "latitude": 26.0, "longitude": 79.5}]},
    ])  # fmt: skip
    d = geocode(s, "Uttar Pradesh", "Jalaun (orai)")
    assert (d.status, d.matched_name, d.latitude) == ("resolved", "Orai", 26.0)


def test_geocode_unresolved_when_no_state_match():
    s = Session([{"results": [{"name": "X", "admin1": "Bihar", "latitude": 1, "longitude": 2}]}])
    assert geocode(s, "Punjab", "X").status == "unresolved"


def test_fetch_daily_flattens_arrays():
    body = {"daily": {"time": ["2024-09-13", "2024-09-14"],
                      "precipitation_sum": [0.0, 12.5],
                      "temperature_2m_max": [34.1, 31.0],
                      "temperature_2m_min": [25.0, 24.2]}}  # fmt: skip
    rows = fetch_daily(Session([body]), District("Haryana", "Karnal", 29.7, 77.0, "Karnal",
                       "resolved"), date(2024, 9, 13), date(2024, 9, 14))  # fmt: skip
    assert rows[1] == {"date": "2024-09-14", "precipitation_mm": 12.5,
                       "temperature_max_c": 31.0, "temperature_min_c": 24.2}  # fmt: skip


def test_fetch_daily_waits_and_retries_on_429():
    ok = {"daily": {"time": ["2024-09-13"], "precipitation_sum": [1.0],
                    "temperature_2m_max": [30.0], "temperature_2m_min": [20.0]}}  # fmt: skip
    slept = []
    s = Session([Resp({}, 429, {"Retry-After": "7"}), Resp({}, 429), ok])
    d = District("Haryana", "Karnal", 29.7, 77.0, "Karnal", "resolved")
    rows = fetch_daily(s, d, date(2024, 9, 13), date(2024, 9, 13), sleep=slept.append)
    assert len(rows) == 1 and slept == [7.0, 120.0]
