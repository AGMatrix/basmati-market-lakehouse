from datetime import date

from basmati.transform.silver import RAW_SCHEMAS, build_silver


def hist(**kw):
    row = {
        "State": "Haryana", "District": "Karnal", "Market": "Karnal", "Commodity": "Paddy(Basmati)",
        "Commodity_Code": "414", "Variety": "Pusa 1121", "Grade": "FAQ",
        "Arrival_Date": "01/09/2023",
        "Min_Price": "3500", "Max_Price": "3900", "Modal_Price": "3700",
    }  # fmt: skip
    return {**row, **kw}


def current(**kw):
    row = {k.lower(): v for k, v in hist().items() if k != "Commodity_Code"}
    return {**row, **kw}


def silver(spark, rows, source="history"):
    return build_silver(spark.createDataFrame(rows, RAW_SCHEMAS[source]), source)


def test_history_shape_is_parsed_and_typed(spark):
    df = silver(spark, [hist()])
    r = df.collect()[0]
    assert r.arrival_date == date(2023, 9, 1)
    assert (r.min_price, r.max_price, r.modal_price) == (3500.0, 3900.0, 3700.0)
    assert r.price_unit == "Rs/quintal" and r.source == "history" and r.commodity_code == "414"


def test_current_shape_matches_history_shape(spark):
    h = silver(spark, [hist()])
    c = silver(spark, [current()], "current")
    cols = [x for x in h.columns if x not in ("ingested_at", "source", "commodity_code")]
    assert h.select(cols).collect() == c.select(cols).collect()


def test_names_are_cleaned_and_states_canonicalised(spark):
    df = silver(spark, [hist(State=" ORISSA ", District="cuttack  city", Market="  CUTTACK")])
    r = df.collect()[0]
    assert (r.state, r.district, r.market) == ("Odisha", "Cuttack City", "Cuttack")


def test_bad_prices_become_null(spark):
    df = silver(spark, [hist(Min_Price="NA", Max_Price="0", Modal_Price="3,650")])
    r = df.collect()[0]
    assert (r.min_price, r.max_price, r.modal_price) == (None, None, 3650.0)


def test_rows_without_keys_or_date_are_dropped(spark):
    df = silver(spark, [hist(), hist(Market="  "), hist(Arrival_Date="31/02/2023")])
    assert df.count() == 1


def test_duplicate_keys_keep_highest_modal_deterministically(spark):
    rows = [hist(Modal_Price="3600"), hist(Modal_Price="3700"), hist(Modal_Price="3650")]
    df = silver(spark, rows)
    assert [r.modal_price for r in df.collect()] == [3700.0]


def test_missing_variety_and_grade_get_defaults(spark):
    df = silver(spark, [hist(Variety=None, Grade="")])
    r = df.collect()[0]
    assert (r.variety, r.grade) == ("Other", "FAQ")
