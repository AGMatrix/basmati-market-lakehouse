# syntax=docker/dockerfile:1
#
# Airflow 3 + Java + PySpark in one image.
# Spark runs in *local mode* inside the Airflow task process (spec: "Spark 4 in local
# mode"), so there is no separate Spark cluster container to build or keep in sync.
# Extending the official image (instead of building Airflow from scratch) keeps
# Airflow's own tested dependency set.

ARG AIRFLOW_IMAGE=apache/airflow:3.3.2-python3.12

# --- Spark jars, downloaded once and checked against Maven Central's published checksums.
# curl + busybox sha1sum/sha512sum, no package install (keeps the build off apk mirrors)
FROM curlimages/curl:8.11.1 AS jars
ARG MAVEN=https://repo1.maven.org/maven2
ARG ICEBERG=1.12.0
ARG HADOOP=3.4.2
# S3A also needs the SDK's transfer manager and Apache HTTP client (httpclient/httpcore
# themselves already ship with PySpark); same SDK version as inside iceberg-aws-bundle,
# so the two share one AWS SDK instead of adding the ~550 MB full SDK bundle.
ARG AWS_SDK=2.54.17
WORKDIR /tmp/jars
RUN true \
 # Iceberg Spark runtime (Spark 4.1 / Scala 2.13) and its AWS bundle (S3FileIO + AWS SDK v2)
 && curl -fsSLO "$MAVEN/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/$ICEBERG/iceberg-spark-runtime-4.1_2.13-$ICEBERG.jar" \
 && curl -fsSLO "$MAVEN/org/apache/iceberg/iceberg-aws-bundle/$ICEBERG/iceberg-aws-bundle-$ICEBERG.jar" \
 # S3A filesystem so Spark can read the raw zone; must match PySpark's bundled Hadoop
 && curl -fsSLO "$MAVEN/org/apache/hadoop/hadoop-aws/$HADOOP/hadoop-aws-$HADOOP.jar" \
 && curl -fsSLO "$MAVEN/software/amazon/awssdk/s3-transfer-manager/$AWS_SDK/s3-transfer-manager-$AWS_SDK.jar" \
 && curl -fsSLO "$MAVEN/software/amazon/awssdk/apache-client/$AWS_SDK/apache-client-$AWS_SDK.jar" \
 && printf '%s  %s\n' \
      ac1e2cb00970309f0c6bf241fabf334aef373c1f3e3b443181d429d5f2c0903ace47df9cfc1d55c95807a096cf405ae0a8868831b9e9eb5696dfdb6380587f38 "iceberg-spark-runtime-4.1_2.13-$ICEBERG.jar" \
      b01456f80142c8837004e31329a0f40db8a9d44917a4a694cb7d8d6d3a68cb20c61e6f7788fa1bdcb95c0edf724322b49667d6f9ef60fc9cc46da5c24ce5c7de "iceberg-aws-bundle-$ICEBERG.jar" \
    | sha512sum -c - \
 && printf '%s  %s\n' \
      16f9de6da5c7862241d55a13d34843404d0cbb88 "hadoop-aws-$HADOOP.jar" \
      c72805ac1c7d7d2c74e32baee0830868ebf0c59c "s3-transfer-manager-$AWS_SDK.jar" \
      382ee44d0e6f806cf895942098e513c31fbd01cd "apache-client-$AWS_SDK.jar" \
    | sha1sum -c -

FROM ${AIRFLOW_IMAGE}

# System packages need root. Spark 4.1 needs Java 17 or 21; default-jre-headless gives
# whichever the image's Debian release ships. procps provides `ps`, used by Spark scripts.
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless procps \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Back to the unprivileged user for everything else (Python packages, runtime).
USER airflow

# Re-stating apache-airflow==$AIRFLOW_VERSION (set by the base image) stops pip from
# silently upgrading or downgrading Airflow while it resolves our packages.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt

# Jars on Spark's own classpath (pyspark/jars), so no --packages download at runtime.
COPY --from=jars --chown=airflow:root /tmp/jars/ /tmp/jars/
RUN cp /tmp/jars/*.jar "$(python -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')/jars/" \
 && rm -rf /tmp/jars
