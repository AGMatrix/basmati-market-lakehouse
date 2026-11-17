"""Raw Agmarknet JSON -> silver.daily_price (Iceberg).

The two source resources differ in field-name case and types (see docs/STEP0_SOURCES.md):
current = lower-case, integer prices; history = Title-case, string prices. Everything is
normalised to one schema here.

Units: data.gov.in documents Agmarknet min/max/modal prices as Rs per quintal (100 kg) and
the API has no unit column, so `price_unit` is a constant and per-kg / per-tonne prices are
derived from it. Arrivals (tonnes) are not exposed by either resource.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

TABLE = "lake.silver.daily_price"

# Raw spellings -> canonical state names (renamed states and common misspellings).
STATE_ALIASES = {
    "Orissa": "Odisha",
    "Pondicherry": "Puducherry",
    "Chattisgarh": "Chhattisgarh",
    "Chhatisgarh": "Chhattisgarh",
    "Nct Of Delhi": "Delhi",
    "Uttaranchal": "Uttarakhand",
    "Jammu And Kashmir": "Jammu and Kashmir",
    "Andaman And Nicobar": "Andaman and Nicobar Islands",
    "Dadra And Nagar Haveli": "Dadra and Nagar Haveli and Daman and Diu",
}

KEY = ["arrival_date", "state", "district", "market", "commodity", "variety", "grade"]

SCHEMA_DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    arrival_date   DATE    COMMENT 'Trading day at the market',
    state          STRING  COMMENT 'Canonical state name',
    district       STRING,
    market         STRING  COMMENT 'Mandi (APMC market) name, cleaned',
    commodity      STRING,
    commodity_code STRING  COMMENT 'Agmarknet code; history resource only',
    variety        STRING  COMMENT 'Raw variety name as reported (mapped in dbt)',
    grade          STRING,
    min_price      DOUBLE  COMMENT 'Rs per quintal',
    max_price      DOUBLE  COMMENT 'Rs per quintal',
    modal_price    DOUBLE  COMMENT 'Most frequent traded price, Rs per quintal',
    price_unit     STRING,
    source         STRING  COMMENT 'current | history (data.gov.in resource)',
    ingested_at    TIMESTAMP COMMENT 'When this partition was last rewritten',
    report_count   INT     COMMENT 'Source rows combined into this row (same key, same day)'
)
USING iceberg
PARTITIONED BY (days(arrival_date))
TBLPROPERTIES (
    'format-version' = '2',
    'write.distribution-mode' = 'hash',
    'write.parquet.compression-codec' = 'zstd'
)
"""


def clean_text(c: Column) -> Column:
    """Trim, collapse inner whitespace, no space before commas; empty -> null."""
    t = F.regexp_replace(F.regexp_replace(F.trim(c), r"\s+", " "), r" ,", ",")
    return F.when(t == "", None).otherwise(t)


def clean_name(c: Column) -> Column:
    """Place names: cleaned and Title Case, so 'KARNAL' and 'karnal ' both become 'Karnal'."""
    return F.initcap(F.lower(clean_text(c)))


def to_price(c: Column) -> Column:
    """'3,500' / '3500' / 3500 -> 3500.0; blanks, NA, and non-positive values -> null."""
    p = F.regexp_replace(c.cast("string"), ",", "").try_cast("double")
    return F.when(p > 0, p)


def normalise(raw: DataFrame, source: str) -> DataFrame:
    """Map either raw shape to the silver schema (no dedup)."""
    cols = {c.lower(): c for c in raw.columns}

    def col(name: str) -> Column:
        return F.col(cols[name]) if name in cols else F.lit(None).cast("string")

    state = clean_name(col("state"))
    alias = F.create_map(*[F.lit(x) for kv in STATE_ALIASES.items() for x in kv])
    return raw.select(
        F.try_to_date(clean_text(col("arrival_date")), "dd/MM/yyyy").alias("arrival_date"),
        F.coalesce(alias[state], state).alias("state"),
        clean_name(col("district")).alias("district"),
        clean_name(col("market")).alias("market"),
        clean_text(col("commodity")).alias("commodity"),
        clean_text(col("commodity_code")).alias("commodity_code"),
        clean_text(col("variety")).alias("variety"),
        clean_text(col("grade")).alias("grade"),
        to_price(col("min_price")).alias("min_price"),
        to_price(col("max_price")).alias("max_price"),
        to_price(col("modal_price")).alias("modal_price"),
        F.lit("Rs/quintal").alias("price_unit"),
        F.lit(source).alias("source"),
    )


SILVER_COLUMNS = [
    *KEY[:5], "commodity_code", *KEY[5:], "min_price", "max_price", "modal_price",
    "price_unit", "source", "ingested_at", "report_count",
]  # fmt: skip


def combine_repeats(df: DataFrame) -> DataFrame:
    """One row per market x commodity x variety x grade x day.

    About 10% of keys are reported more than once a day with different prices (measured on
    Nov 2026 data: e.g. separate lots or sessions at the same mandi). Picking one row would
    bias the price, so repeats are combined: lowest min, highest max, mean of the modal
    prices, and `report_count` keeps how many reports went in.
    """
    return df.groupBy(*KEY).agg(
        F.min("min_price").alias("min_price"),
        F.max("max_price").alias("max_price"),
        F.round(F.avg("modal_price"), 2).alias("modal_price"),
        F.max("commodity_code").alias("commodity_code"),
        F.first("price_unit").alias("price_unit"),
        F.first("source").alias("source"),
        F.count(F.lit(1)).cast("int").alias("report_count"),
    )


def build_silver(raw: DataFrame, source: str) -> DataFrame:
    df = normalise(raw, source).filter(
        F.col("arrival_date").isNotNull()
        & F.col("market").isNotNull()
        & F.col("commodity").isNotNull()
    )
    # Variety/grade are part of the key; null would make "unknown" rows collapse oddly.
    df = df.fillna({"variety": "Other", "grade": "FAQ", "district": "Unknown"})
    out = combine_repeats(df).withColumn("ingested_at", F.current_timestamp())
    return out.select(*SILVER_COLUMNS)


# Explicit all-string schemas, one per source shape: no schema inference pass over the
# raw zone, and no clash between "State" and "state" (Spark resolves names case-insensitively).
_HISTORY_FIELDS = ["State", "District", "Market", "Commodity", "Commodity_Code", "Variety",
                   "Grade", "Arrival_Date", "Min_Price", "Max_Price", "Modal_Price"]  # fmt: skip
RAW_SCHEMAS = {
    "history": StructType([StructField(f, StringType()) for f in _HISTORY_FIELDS]),
    "current": StructType(
        [StructField(f.lower(), StringType()) for f in _HISTORY_FIELDS if f != "Commodity_Code"]
    ),
}


def raw_paths(bucket: str, days: list[date]) -> list[str]:
    return [f"s3a://{bucket}/raw/agmarknet/dt={d.isoformat()}/records.json.gz" for d in days]


def read_silver(
    spark: SparkSession, bucket: str, days_by_source: dict[str, list[date]]
) -> DataFrame:
    """Read the raw days (grouped by which resource they came from) as one silver DataFrame."""
    parts = [
        build_silver(
            spark.read.schema(RAW_SCHEMAS[source])
            .option("multiLine", "true")  # each file is a single JSON array
            .json(raw_paths(bucket, days)),
            source,
        )
        for source, days in days_by_source.items()
        if days and source in RAW_SCHEMAS
    ]
    if not parts:
        return spark.createDataFrame([], spark.table(TABLE).schema)
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out


def ensure_table(spark: SparkSession) -> None:
    """Create the table, or evolve an older one in place (Iceberg schema evolution).

    Adding a column is a metadata-only change in Iceberg: no data files are rewritten, and
    rows written before the change read the new column as NULL until their day is rebuilt.
    """
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")
    spark.sql(SCHEMA_DDL)
    if "report_count" not in spark.table(TABLE).columns:
        spark.sql(
            f"ALTER TABLE {TABLE} ADD COLUMN report_count INT "
            "COMMENT 'Source rows combined into this row (same key, same day)'"
        )


def write_days(silver: DataFrame, days: list[date]) -> int:
    """Atomically replace exactly `days` in the table with `silver` (one Iceberg commit).

    overwrite(condition) deletes every row matching the condition and appends the new rows
    in a single snapshot, so readers never see a day half-replaced, a revised day that lost
    rows is fixed, and a day that became empty is cleared. Re-running is a no-op in effect.
    """
    in_days = F.col("arrival_date").isin(days)
    df = silver.filter(in_days).sortWithinPartitions("commodity", "market").cache()
    rows = df.count()
    df.writeTo(TABLE).overwrite(in_days)
    df.unpersist()
    return rows
