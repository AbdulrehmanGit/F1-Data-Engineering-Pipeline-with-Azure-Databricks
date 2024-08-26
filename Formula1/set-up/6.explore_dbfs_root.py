# Databricks notebook source
# MAGIC %md
# MAGIC EXPLORE DBFS ROOT
# MAGIC - list all the folders in dbfs root
# MAGIC - interact with dbfs file browser
# MAGIC - upload file to 

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

display(spark.read.csv('/FileStore/circuits.csv'))

# COMMAND ----------

