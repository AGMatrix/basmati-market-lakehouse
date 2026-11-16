"""Manual smoke test for the local stack: Airflow -> MinIO, and Spark (Java) inside a task.

Trigger it once from the UI after `make up`. Both tasks green = the platform works.
"""

from __future__ import annotations

import os

from airflow.sdk import dag, task


@dag(schedule=None, catchup=False, tags=["platform"])
def platform_smoke():
    @task
    def lake_bucket_reachable() -> str:
        import boto3

        bucket = os.environ["LAKE_BUCKET"]
        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ["S3_ENDPOINT"],
            aws_access_key_id=os.environ["S3_ACCESS_KEY"],
            aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        )
        s3.head_bucket(Bucket=bucket)  # raises if the bucket is missing or creds are wrong
        return bucket

    @task
    def spark_local_mode() -> int:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.master("local[2]").appName("smoke").getOrCreate()
        try:
            n = spark.range(1_000_000).filter("id % 7 = 0").count()
            print(f"Spark {spark.version}: counted {n} rows")
            return n
        finally:
            spark.stop()

    lake_bucket_reachable() >> spark_local_mode()


platform_smoke()
