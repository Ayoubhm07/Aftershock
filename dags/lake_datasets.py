from __future__ import annotations

from airflow.datasets import Dataset

BRONZE_CATALOG = Dataset("hdfs://namenode:8020/lake/bronze/source=usgs_catalog")
SILVER_EVENTS = Dataset("hdfs://namenode:8020/lake/silver/events")
GOLD_TABLES = Dataset("hdfs://namenode:8020/lake/gold")
