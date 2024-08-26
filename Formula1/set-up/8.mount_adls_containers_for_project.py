# Databricks notebook source
# MAGIC %md
# MAGIC Mount Azure Data Lake Containers for the Project 

# COMMAND ----------

def mountadls(storage_account_name,container_name):       
    client_id =  dbutils.secrets.get(scope='formula1-scope',key='formula1dl-clientID')
    tenant_id = dbutils.secrets.get(scope='formula1-scope',key='formula1dl-tenantID')
    client_secret = dbutils.secrets.get(scope='formula1-scope',key='formula1dl-clientSECRET')
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/"+tenant_id+"/oauth2/token"}
    #Unmounts the Mount if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}" )
    
    #Mounts the Mount in the given containers in function Parameters
    dbutils.fs.mount(
        source = "abfss://"+container_name+"@"+storage_account_name+".dfs.core.windows.net/",
        mount_point = "/mnt/"+storage_account_name+"/"+container_name,
        extra_configs = configs)

    display(dbutils.fs.mounts())


# COMMAND ----------

mountadls('formula1dlabrehman', 'raw')

# COMMAND ----------

mountadls('formula1dlabrehman', 'processed')

# COMMAND ----------

mountadls('formula1dlabrehman', 'presentation')

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dlabrehman/demo")