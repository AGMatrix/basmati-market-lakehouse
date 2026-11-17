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

from pyspark.sql import Column, DataFrame, SparkSession, Window
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
    ingested_at    TIMESTAMP COMMENT 'When this partition was last rewritten'
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
    """Trim, collapse inner whitespace; empty -> null."""
    t = F.regexp_replace(F.trim(c), r"\s+", " ")
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


def deduplicate(df: DataFrame) -> DataFrame:
    """One row per market x commodity x variety x grade x day.

    If the source sends the same key more than once (the run metrics record how often), we
    keep the row with the highest modal price, ties broken by max/min price, so the choice
    is deterministic across re-runs.
    """
    w = Window.partitionBy(*KEY).orderBy(
        F.col("modal_price").desc_nulls_last(),
        F.col("max_price").desc_nulls_last(),
        F.col("min_price").desc_nulls_last(),
    )
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def build_silver(raw: DataFrame, source: str) -> DataFrame:
    df = normalise(raw, source).filter(
        F.col("arrival_date").isNotNull()
        & F.col("market").isNotNull()
        & F.col("commodity").isNotNull()
    )
    # Variety/grade are part of the key; null would make "unknown" rows collapse oddly.
    df = df.fillna({"variety": "Other", "grade": "FAQ", "district": "Unknown"})
    return deduplicate(df).withColumn("ingested_at", F.current_timestamp())


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
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")
    spark.sql(SCHEMA_DDL)


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
