# Databricks notebook source
# MAGIC %md
# MAGIC Read the json file using spark dataframe Reader api

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

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType

# COMMAND ----------

name_schema  = StructType(fields=[
    StructField('forename',StringType(),True),
    StructField('surname',StringType(),True)

])

# COMMAND ----------

drivers_schema  = StructType(fields=[
    StructField('driverId',IntegerType(),False),
    StructField('driverRef',StringType(),True),
    StructField('number',IntegerType(),True),
    StructField('code',StringType(),True),
    StructField('name',name_schema),
    StructField('dob',DateType(),True),
    StructField('nationality',StringType(),True),
    StructField('url',StringType(),True)

])

# COMMAND ----------

drivers_df = spark.read \
    .schema(drivers_schema) \
    .json(raw_folder_path+"/"+v_file_date+'/drivers.json')

# COMMAND ----------

# MAGIC %md
# MAGIC Rename columns and add new Column
# MAGIC
# MAGIC - -driverid renamed to driver_id
# MAGIC - -driverRef  renamed to driver_ref
# MAGIC - -ingestion date added
# MAGIC - -name added with concatenation of forename and surname 

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drivers__with_column_df = drivers_df.withColumnRenamed('driverId', 'driver_id') \
                                  .withColumnRenamed('driverRef', 'driver_ref') \
                                  .withColumn('name', concat(col('name.forename'),lit(' '),col('name.surname'))) \
    .withColumn('data_source', lit(v_data_source)) \
    .withColumn('file_date', lit(v_file_date))
                                      


# COMMAND ----------

drivers_with_column_df = add_ingestion_date(drivers__with_column_df)

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# MAGIC %md 
# MAGIC drop the unwanted columns 
# MAGIC - name.forename
# MAGIC - name.surname 
# MAGIC - url

# COMMAND ----------

driver_final_df = drivers_with_column_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC Writing Data to a parquet file
# MAGIC

# COMMAND ----------

driver_final_df.write.mode('overwrite').format("delta").saveAsTable("f1_processed.drivers")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.drivers

# COMMAND ----------

driver_final_df.describe

# COMMAND ----------

dbutils.notebook.exit("Success")