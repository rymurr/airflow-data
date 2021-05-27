#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import pandas
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("testIceberg").getOrCreate()

region_df = spark.createDataFrame(
    pandas.read_csv(
        "https://raw.githubusercontent.com/rymurr/nessie-demos/main/datasets/nba/totals_stats.csv"
    ))
region_df.createTempView("region_parquet")
spark.sql("""CREATE TEMPORARY VIEW parquetTable
USING org.apache.spark.sql.parquet
OPTIONS (
  path "s3a://rymurr.test.bucket/nation.parquet"
)""")
spark.sql(
    "CREATE TABLE nessie.testing.nation USING iceberg AS SELECT * FROM parquetTable"
)
