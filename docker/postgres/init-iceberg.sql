-- Runs once, when the Postgres volume is first created: a separate database for the
-- Iceberg REST catalog's table pointers, next to Airflow's metadata DB.
CREATE DATABASE iceberg;
