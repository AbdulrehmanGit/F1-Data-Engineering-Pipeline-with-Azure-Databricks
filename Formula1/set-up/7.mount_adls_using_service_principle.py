# Databricks notebook source
# MAGIC %md
# MAGIC Mount Azure Data Lake using Service Principle 
# MAGIC - get client_id, tenant_id, and client_secret from key vault 
# MAGIC - set spark config with / app/client id, directory/tenant id & service
# MAGIC - call file system utility mount to mount the storage
# MAGIC - explore other files ystem utilities related to mount (list all mounts, unmmount)

# COMMAND ----------


client_id =  dbutils.secrets.get(scope='formula1-scope',key='formula1dl-clientID')
tenant_id = dbutils.secrets.get(scope='formula1-scope',key='formula1dl-tenantID')
client_secret = dbutils.secrets.get(scope='formula1-scope',key='formula1dl-clientSECRET')

# COMMAND ----------


# spark.conf.set("fs.azure.account.auth.type.formula1dlabrehman.dfs.core.windows.net", "OAuth")
# spark.conf.set("fs.azure.account.oauth.provider.type.formula1dlabrehman.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set("fs.azure.account.oauth2.client.id.formula1dlabrehman.dfs.core.windows.net", client_id)
# spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dlabrehman.dfs.core.windows.net", client_secret)
# spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dlabrehman.dfs.core.windows.net", "https://login.microsoftonline.com/"+tenant_id+"/oauth2/token")


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/"+tenant_id+"/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dlabrehman.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dlabrehman/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dlabrehman/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dlabrehman/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# dbutils.fs.unmount("/mnt/formula1dlabrehman/demo")