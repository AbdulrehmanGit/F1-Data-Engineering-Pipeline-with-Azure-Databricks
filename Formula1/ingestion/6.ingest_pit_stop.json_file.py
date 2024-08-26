# Databricks notebook source
dbutils.widgets.text('p_data_source',"")
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date',"")
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/Configuration" 

# COMMAND ----------

# MAGIC %run "../includes/Common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC Ingest pit_stop.json File

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType,FloatType

# COMMAND ----------

pit_stop_schema  = StructType(fields=[
    StructField('raceId',IntegerType(),False),
    StructField('driverId',IntegerType(),True),
    StructField('stop',StringType(),True),
    StructField('lap',IntegerType(),True),
    StructField('time',StringType(),True),
    StructField('duration',StringType(),True),
    StructField('milliseconds',IntegerType(),True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC Read the json file using spark dataframe reader

# COMMAND ----------

pit_stop_df = spark.read.schema(pit_stop_schema) \
.option('multiLine', True) \
.json(raw_folder_path+"/"+v_file_date+'/pit_stops.json')

# COMMAND ----------

# MAGIC %md
# MAGIC renamed column and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

pit__stop_final_df= pit_stop_df.withColumnRenamed('driverId', "driver_id") \
        .withColumnRenamed('raceId', "race_id") \
    .withColumn('data_source', lit(v_data_source))


# COMMAND ----------

pit_stop_final_df = add_ingestion_date(pit__stop_final_df)

# COMMAND ----------

# overwrite_partition(pit_stop_final_df,'f1_processed','pit_stops','race_id')
merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(pit_stop_final_df,'f1_processed','pit_stops',processed_folder_path,merge_condition,'race_id')

# COMMAND ----------

# MAGIC %md
# MAGIC writing data to parquet file

# COMMAND ----------

# pit_stop_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")



# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table f1_processed.pit_stops

# COMMAND ----------

# MAGIC %sql 
# MAGIC select race_id ,count(1)
# MAGIC from f1_processed.pit_stops 
# MAGIC group by race_id 
# MAGIC order by race_id desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from f1_processed.pit_stops