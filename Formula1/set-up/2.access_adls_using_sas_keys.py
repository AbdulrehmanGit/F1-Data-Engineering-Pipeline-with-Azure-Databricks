# Databricks notebook source
# MAGIC %md
# MAGIC Access Azure Data Lake using SAS keys 
# MAGIC - set the spark config for SAS TOKEN
# MAGIC - list files from demo container
# MAGIC - read data from circuits.csv file
# MAGIC

# COMMAND ----------

formula1df_demo_sas_token = dbutils.secrets.get(scope='formula1-scope',key='formula1df-demo-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlabrehman.dfs.core.windows.net","SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dlabrehman.dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dlabrehman.dfs.core.windows.net",formula1df_demo_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlabrehman.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlabrehman.dfs.core.windows.net/circuits.csv"))