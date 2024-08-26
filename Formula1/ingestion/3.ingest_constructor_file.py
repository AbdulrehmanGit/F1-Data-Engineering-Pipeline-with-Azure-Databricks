# Databricks notebook source
# MAGIC %md
# MAGIC Ingest constructor.json File

# COMMAND ----------

# MAGIC %md
# MAGIC Read the json file using spark dataframe reader

# COMMAND ----------

dbutils.widgets.text('p_data_source',"")
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date',"2021-03-21")
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/Configuration" 

# COMMAND ----------

# MAGIC %run "../includes/Common_functions"

# COMMAND ----------

cosntructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
.schema(cosntructor_schema) \
.json(raw_folder_path+"/"+v_file_date+'/constructors.json')

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC Dropping URL Column

# COMMAND ----------

constructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC renamed column and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

constructor__final_df= constructor_dropped_df.withColumnRenamed('constructorId', "constructor_id") \
        .withColumnRenamed('constructorRef', "constructor_ref") \
    .withColumn('data_source', lit(v_data_source)) \
    .withColumn('file_date', lit(v_file_date))


# COMMAND ----------

constructor_final_df =add_ingestion_date(constructor__final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC writing data to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.constructors

# COMMAND ----------

dbutils.notebook.exit("Success")