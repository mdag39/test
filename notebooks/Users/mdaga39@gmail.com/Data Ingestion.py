# Databricks notebook source
# MAGIC %fs
# MAGIC ls

# COMMAND ----------

from pyspark.sql.types import StructField, IntegerType, StringType, StructType

sche=StructType(fileds=StructField('raceid',IntegerType))

# COMMAND ----------

race_df=spark.read.csv("/mnt/formula1mohit/raw/races.csv").options("headers",True)

# COMMAND ----------

