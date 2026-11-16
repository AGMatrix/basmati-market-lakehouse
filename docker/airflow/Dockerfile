# syntax=docker/dockerfile:1
#
# Airflow 3 + Java + PySpark in one image.
# Spark runs in *local mode* inside the Airflow task process (spec: "Spark 4 in local
# mode"), so there is no separate Spark cluster container to build or keep in sync.
# Extending the official image (instead of building Airflow from scratch) keeps
# Airflow's own tested dependency set.

ARG AIRFLOW_IMAGE=apache/airflow:3.3.2-python3.12
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
