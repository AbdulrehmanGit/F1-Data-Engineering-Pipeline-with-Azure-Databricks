# Databricks notebook source
dbutils.widgets.text('p_data_source',"")
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date',"")
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %run "../includes/Configuration" 

# COMMAND ----------

# MAGIC %run "../includes/Common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC Ingest results.json File

# COMMAND ----------

# MAGIC %md
# MAGIC Read the json file using spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType,FloatType

# COMMAND ----------

results_schema  = StructType(fields=[
    StructField('resultId',IntegerType(),False),
    StructField('raceId',IntegerType(),True),
    StructField('driverId',IntegerType(),True),
    StructField('constructorId',IntegerType(),True),
    StructField('number',IntegerType()),
    StructField('grid',IntegerType(),True),
    StructField('position',IntegerType(),True),
    StructField('positionText',StringType(),True),
    StructField('positionOrder',IntegerType(),True),
    StructField('points',FloatType(),True),
    StructField('laps',IntegerType(),True),
    StructField('time',StringType(),True),
    StructField('milliseconds',IntegerType(),True),
    StructField('fastestLap',IntegerType(),True),
    StructField('rank',IntegerType(),True),
    StructField('fastestLapTime',StringType(),True),
    StructField('fastestLapSpeed',FloatType(),True),
    StructField('statusId',IntegerType(),True)

    

])

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json(raw_folder_path+"/"+v_file_date+'/results.json')

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC Dropping URL Column

# COMMAND ----------

results_dropped_df = results_df.drop('statusId')

# COMMAND ----------

display(results_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC renamed column and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

results__final_df= results_dropped_df.withColumnRenamed('constructorId', "constructor_id") \
        .withColumnRenamed('resultId', "result_id") \
        .withColumnRenamed('raceId', "race_id") \
        .withColumnRenamed('driverId', "driver_id") \
        .withColumnRenamed('constructorId', "constructor_id") \
        .withColumnRenamed('positionText', "position_text") \
        .withColumnRenamed('positionOrder', "position_order") \
        .withColumnRenamed('fastestLap', "fastest_lap") \
        .withColumnRenamed('fastestLapTime', "fastest_lap_time") \
        .withColumnRenamed('fastestLapSpeed', "fastest_lap_speed") \
    .withColumn('data_source', lit(v_data_source)) \
    .withColumn('file_date', lit(v_file_date))


# COMMAND ----------

results_final_df =add_ingestion_date(results__final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC deduping data
# MAGIC

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC writing data to parquet file

# COMMAND ----------

# MAGIC %md Method 1
# MAGIC

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")



# COMMAND ----------

# MAGIC %md Method 2

# COMMAND ----------

# %sql
# drop table f1_processed.results;

# COMMAND ----------

# overwrite_partition(results_final_df,'f1_processed','results','race_id')



# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id and tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df,'f1_processed','results',processed_folder_path,merge_condition,'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.results

# COMMAND ----------

# results_final_df = results_final_df.select('result_id',
# 'driver_id',
# 'constructor_id',
# 'number',
# 'grid',
# 'position',
# 'position_text', 
# 'position_order',
# 'points', 
# 'laps',
# 'time', 
# 'milliseconds',
# 'fastest_lap',
# 'rank',
# 'fastest_lap_time', 
# 'fastest_lap_speed', 
# 'data_source', 
# 'file_date',
# 'ingestion_date',
# 'race_id' )

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select race_id ,count(1)
# MAGIC from f1_processed.results 
# MAGIC group by race_id 
# MAGIC order by race_id desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, driver_id, count(1)
# MAGIC from f1_processed.results
# MAGIC group by race_id, driver_id
# MAGIC having count(1) >1
# MAGIC order by race_id, driver_id desc