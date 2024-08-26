# Databricks notebook source
# MAGIC %md
# MAGIC Exploring the capabitlities of the sbutils.secrets utility
# MAGIC

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope='formula1-scope')

# COMMAND ----------

dbutils.secrets.get(scope='formula1-scope',key='formula1dlaccountkey')

# COMMAND ----------

