# Databricks notebook source
# MAGIC %md
# MAGIC Access Azure Data Lake using accesss keys 
# MAGIC - set the spark config fs.azure.account.key
# MAGIC - list files from demo container
# MAGIC - read data from circuits.csv file
# MAGIC

# COMMAND ----------

formula1dl_account_key = dbutils.secrets.get(scope='formula1-scope',key='formula1dlaccountkey')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dlabrehman.dfs.core.windows.net",formula1dl_account_key)


# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlabrehman.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlabrehman.dfs.core.windows.net/circuits.csv"))