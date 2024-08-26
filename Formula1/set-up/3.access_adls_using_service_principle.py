# Databricks notebook source
# MAGIC %md
# MAGIC Access Azure Data Lake using Service Principle 
# MAGIC - register Azure AD Application / service Principle 
# MAGIC - generate a secret/password for the application
# MAGIC - set spark config with / app/client id, directory/tenant id & service
# MAGIC - Assign Role 'Storage Blob Data contributor' to the data lake
# MAGIC

# COMMAND ----------


client_id =  dbutils.secrets.get(scope='formula1-scope',key='formula1dl-clientID')
tenant_id = dbutils.secrets.get(scope='formula1-scope',key='formula1dl-tenantID')
client_secret = dbutils.secrets.get(scope='formula1-scope',key='formula1dl-clientSECRET')

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.formula1dlabrehman.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dlabrehman.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dlabrehman.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dlabrehman.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dlabrehman.dfs.core.windows.net", "https://login.microsoftonline.com/"+tenant_id+"/oauth2/token")


# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlabrehman.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlabrehman.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

