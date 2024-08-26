# Databricks notebook source
# MAGIC %md
# MAGIC Read JSON using Saprk Dataframe reader api
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

qualifying_schema  = StructType(fields=[
    StructField('qualifyId',IntegerType(),False),
    StructField('raceId',IntegerType(),True),
    StructField('driverId',IntegerType(),True),
    StructField('constructorId',IntegerType(),True),
    StructField('number',IntegerType(),True),
    StructField('position',IntegerType(),True),
    StructField('q1',StringType(),True),
    StructField('q2',StringType(),True),
    StructField('q3',StringType(),True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC Read the json file using spark dataframe reader

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option('multiLine',True) \
.json(raw_folder_path+"/"+v_file_date+'/qualifying/qualifying_split*.json')

# COMMAND ----------

# MAGIC %md
# MAGIC renamed column and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

final__df= qualifying_df.withColumnRenamed('qualifyId', "qualifying_id") \
        .withColumnRenamed('raceId', "race_id") \
        .withColumnRenamed('driverId', "driver_id") \
        .withColumnRenamed('constructorId', "constructor_id") \
    .withColumn('data_source', lit(v_data_source))


# COMMAND ----------

final_df=add_ingestion_date(final__df)

# COMMAND ----------

# MAGIC %md
# MAGIC writing data to parquet file

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")



# COMMAND ----------

# overwrite_partition(final_df,'f1_processed','qualifying','race_id')
merge_condition = "tgt.race_id = src.race_id AND tgt.qualifying_id = src.qualifying_id"
merge_delta_data(final_df,'f1_processed','qualifying',processed_folder_path,merge_condition,'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select race_id ,count(1)
# MAGIC from f1_processed.qualifying 
# MAGIC group by race_id 
# MAGIC order by race_id desc

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table f1_processed.qualifying