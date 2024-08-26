# Databricks notebook source
# MAGIC %md
# MAGIC Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC Read the csv file using the spark dataframe reader

# COMMAND ----------

dbutils.widgets.text('p_data_source',"")
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------



# COMMAND ----------

dbutils.widgets.text('p_file_date',"2021-03-21")
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/Configuration" 

# COMMAND ----------

# MAGIC %run "../includes/Common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType,DateType


# COMMAND ----------

races_schema = StructType(fields= [StructField("raceId",IntegerType(),False),
                                      StructField("year",IntegerType(),True),
                                      StructField("round",IntegerType(),True),
                                      StructField("circuitId",IntegerType(),True),
                                      StructField("name",StringType(),True),
                                      StructField("date",DateType(),True),
                                      StructField("time",StringType(),True),
                                      StructField("url",StringType(),True)
                                      ])

# COMMAND ----------

races_df = spark.read \
.option("header",True) \
.schema(races_schema) \
.csv(raw_folder_path+"/"+v_file_date+"/races.csv")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col, to_timestamp,concat,lit


# COMMAND ----------

# MAGIC %md
# MAGIC Adding Ingestion Date and race_timestamp Column

# COMMAND ----------

races_with__timestamp = races_df.withColumn('race_timestamp',to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

races_with_timestamp = add_ingestion_date(races_with__timestamp)

# COMMAND ----------

# MAGIC %md
# MAGIC select only the required columns

# COMMAND ----------

races_selected_df= races_with_timestamp.select(col("raceId"),
col("year"),
col("round"),
col("circuitId"),
col("name"),
col("race_timestamp"),
col("ingestion_date"))







# COMMAND ----------

# MAGIC %md
# MAGIC Renaming the Columns in Circuits DF
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("raceId","race_id") \
    .withColumnRenamed("year","race_year") \
    .withColumnRenamed("circuitId","circuit_id") \
    .withColumn("data_source",lit(v_data_source)) \
    .withColumn('file_date', lit(v_file_date))
    




# COMMAND ----------

# MAGIC %md
# MAGIC write data to datalake as a parquet

# COMMAND ----------

races_renamed_df.write.mode("overwrite").partitionBy("race_year").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.races

# COMMAND ----------

dbutils.notebook.exit("Success")