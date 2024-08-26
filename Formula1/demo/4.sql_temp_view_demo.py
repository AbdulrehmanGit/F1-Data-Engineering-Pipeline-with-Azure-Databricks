# Databricks notebook source
# MAGIC %md 
# MAGIC Access datafrmaes using sql
# MAGIC - 
# MAGIC - create tempraory view on dataframe
# MAGIC - access the view from sql cell
# MAGIC - access the view from python cell
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/Configuration"

# COMMAND ----------

# MAGIC %run "../includes/Common_functions"

# COMMAND ----------

race_results_df = spark.read.parquet(presentation_folder_path+"/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_race_results

# COMMAND ----------

p_race_year = 2019

# COMMAND ----------

race_results_2019 = spark.sql("select * from v_race_results where race_year ="+str(p_race_year))

# COMMAND ----------

display(race_results_2019)

# COMMAND ----------

# MAGIC
# MAGIC %md 
# MAGIC Access datafrmaes using sql
# MAGIC - Create glboal temprary views on dataframe
# MAGIC - access the view from sql cell
# MAGIC - access the view from python cell
# MAGIC - Access the view from another notebook
# MAGIC

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from global_temp.gv_race_results

# COMMAND ----------

spark.sql("select * from global_temp.gv_race_results")

# COMMAND ----------

