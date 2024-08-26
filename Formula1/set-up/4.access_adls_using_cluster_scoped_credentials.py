# Databricks notebook source
# MAGIC %md
# MAGIC Access Azure Data Lake using cluster Scoped Credentials
# MAGIC - set the spark config fs.azure.account.key in the cluster
# MAGIC - list files from demo container
# MAGIC - read data from circuits.csv file
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlabrehman.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlabrehman.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

