# Databricks notebook source
# MAGIC %md
# MAGIC Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC Read the csv file using the spark dataframe reader

# COMMAND ----------

dbutils.widgets.text('p_data_source',"")
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date',"2021-03-21")
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------



# COMMAND ----------

# MAGIC %run "../includes/Configuration" 

# COMMAND ----------

# MAGIC %run "../includes/Common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType


# COMMAND ----------

circuits_schema = StructType(fields= [StructField("circuitId",IntegerType(),False),
                                      StructField("circuitRef",StringType(),True),
                                      StructField("name",StringType(),True),
                                      StructField("location",StringType(),True),
                                      StructField("country",StringType(),True),
                                      StructField("lat",DoubleType(),True),
                                      StructField("lng",DoubleType(),True),
                                      StructField("alt",IntegerType(),True),
                                      StructField("url",StringType(),True)
                                      ])

# COMMAND ----------

circuits_df = spark.read \
.option("header",True) \
.schema(circuits_schema) \
.csv(raw_folder_path+"/"+v_file_date+"/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC select only the required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df= circuits_df.select(col("circuitId"),
col("circuitRef"),
col("name"),
col("location"),
col("country"),
col("lat"),
col("lng"),
col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC Renaming the Columns in Circuits DF
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId","circuit_id") \
    .withColumnRenamed("circuitRef","circuit_ref") \
    .withColumnRenamed("lat","latitude") \
    .withColumnRenamed("lng","longitude") \
    .withColumnRenamed("alt","altitude") \
    .withColumn('data_source', lit(v_data_source)) \
    .withColumn('file_date', lit(v_file_date))



# COMMAND ----------

# MAGIC %md
# MAGIC Adding Ingestion Date Column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())
circuits_final_df =add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC write data to datalake as a parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits

# COMMAND ----------

circuits_final_df.describe

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------



# COMMAND ----------

