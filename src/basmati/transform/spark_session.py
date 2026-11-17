"""SparkSession wired to the lake: Iceberg REST catalog `lake` + S3A for the raw zone."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def build_spark(app: str = "basmati", master: str = "local[*]") -> SparkSession:
    endpoint = os.environ["S3_ENDPOINT"]
    key, secret = os.environ["S3_ACCESS_KEY"], os.environ["S3_SECRET_KEY"]
    bucket = os.environ.get("LAKE_BUCKET", "lake")
    cat = "spark.sql.catalog.lake"
    return (
        SparkSession.builder.master(master)
        .appName(app)
        # Driver = executor in local mode; must be set before the JVM starts.
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
        # Few cores, small data: the default 200 shuffle partitions would mean tiny tasks.
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(cat, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{cat}.type", "rest")
        .config(f"{cat}.uri", os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181"))
        .config(f"{cat}.warehouse", f"s3://{bucket}/warehouse")
        .config(f"{cat}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{cat}.s3.endpoint", endpoint)
        .config(f"{cat}.s3.path-style-access", "true")
        .config(f"{cat}.s3.access-key-id", key)
        .config(f"{cat}.s3.secret-access-key", secret)
        .config(f"{cat}.client.region", "us-east-1")
        # S3A (Hadoop) for reading raw JSON with spark.read
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", key)
        .config("spark.hadoop.fs.s3a.secret.key", secret)
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(endpoint.startswith("https")))
        .getOrCreate()
    )
