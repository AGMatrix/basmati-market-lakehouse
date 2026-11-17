"""raw/weather JSON -> lake.silver.weather_daily (Iceberg), plus picking which districts.

Weather is small (tens of districts x ~4,000 days), so the table is rebuilt in full each
run in one atomic commit: simpler than incremental, and still idempotent.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

TABLE = "lake.silver.weather_daily"
# States in the basmati Geographical Indication (GI) region
GI_STATES = ["Punjab", "Haryana", "Uttar Pradesh", "Uttarakhand", "Delhi",
             "Himachal Pradesh", "Jammu and Kashmir"]  # fmt: skip
BASMATI_SIGNAL = "basmati|basumati|1121|1509|1718|1401|1847|1885|1886|1692|pusa|sugandh|taraori"


def basmati_districts(spark: SparkSession, top_n: int = 40) -> list[tuple[str, str]]:
    """The GI-state districts with the most basmati price rows (the ones worth weather for)."""
    rows = (
        spark.table("lake.silver.daily_price")
        .where(F.col("state").isin(GI_STATES))
        .where(
            F.lower("commodity").rlike("basmati")
            | (
                F.lower("commodity").rlike("paddy|rice|dhan")
                & F.lower("variety").rlike(BASMATI_SIGNAL)
            )
        )
        .groupBy("state", "district")
        .count()
        .orderBy(F.desc("count"))
        .limit(top_n)
        .collect()
    )
    return [(r.state, r.district) for r in rows if r.district != "Unknown"]


def read_weather(spark: SparkSession, bucket: str) -> DataFrame:
    raw = spark.read.option("multiLine", "true").json(
        f"s3a://{bucket}/raw/weather/district=*/daily.json.gz"
    )
    return (
        raw.select("district", F.explode("daily").alias("d"))
        .select(
            F.col("district.state").alias("state"),
            F.col("district.district").alias("district"),
            F.to_date("d.date").alias("weather_date"),
            F.col("d.precipitation_mm").cast("double").alias("precipitation_mm"),
            F.col("d.temperature_max_c").cast("double").alias("temperature_max_c"),
            F.col("d.temperature_min_c").cast("double").alias("temperature_min_c"),
            F.col("district.latitude").cast("double").alias("latitude"),
            F.col("district.longitude").cast("double").alias("longitude"),
            F.col("district.matched_name").alias("geocoded_place"),
        )
        .withColumn("ingested_at", F.current_timestamp())
    )


def write_weather(spark: SparkSession, df: DataFrame) -> int:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")
    df = df.cache()
    n = df.count()
    (
        df.sortWithinPartitions("state", "district", "weather_date")
        .writeTo(TABLE)
        .tableProperty("format-version", "2")
        .createOrReplace()
    )
    df.unpersist()
    return n
