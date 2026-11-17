"""Airflow assets shared by the pipeline DAGs (data-aware scheduling, Airflow 3).

Producers declare an asset as a task outlet; consumers use it as their `schedule`, so the
Spark DAG runs because new raw data landed, not at a guessed time.
"""

import os

from airflow.sdk import Asset

_BUCKET = os.environ.get("LAKE_BUCKET", "lake")

RAW_AGMARKNET = Asset(name="raw_agmarknet", uri=f"s3://{_BUCKET}/raw/agmarknet/")
SILVER_PRICES = Asset(name="silver_daily_price", uri=f"s3://{_BUCKET}/warehouse/silver/daily_price")
SILVER_WEATHER = Asset(
    name="silver_weather_daily", uri=f"s3://{_BUCKET}/warehouse/silver/weather_daily"
)
WAREHOUSE = Asset(name="warehouse_serving", uri="file:///opt/project/data/warehouse_serving.duckdb")
