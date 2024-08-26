# Databricks notebook source
# MAGIC %md
# MAGIC Read Csv using Saprk Dataframe reader api
# MAGIC

# COMMAND ----------

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

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType,FloatType

# COMMAND ----------

lap_times_schema  = StructType(fields=[
    StructField('raceId',IntegerType(),False),
    StructField('driverId',IntegerType(),True),
    StructField('lap',IntegerType(),True),
    StructField('position',IntegerType(),True),
    StructField('time',StringType(),True),
    StructField('milliseconds',IntegerType(),True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC Read the json file using spark dataframe reader

# COMMAND ----------

lap_time_df = spark.read \
.schema(lap_times_schema) \
.csv(raw_folder_path+"/"+v_file_date+'/lap_times/lap_times_split*.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC renamed column and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

final__df= lap_time_df.withColumnRenamed('driverId', "driver_id") \
        .withColumnRenamed('raceId', "race_id") \
    .withColumn('data_source', lit(v_data_source))


# COMMAND ----------

final_df= add_ingestion_date(final__df)

# COMMAND ----------

# MAGIC %md
# MAGIC writing data to parquet file

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")



# COMMAND ----------

# overwrite_partition(final_df,'f1_processed','lap_times','race_id')
merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
merge_delta_data(final_df,'f1_processed','lap_times',processed_folder_path,merge_condition,'race_id')


# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select race_id ,count(1)
# MAGIC from f1_processed.lap_times 
# MAGIC group by race_id 
# MAGIC order by race_id desc

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table f1_processed.lap_times