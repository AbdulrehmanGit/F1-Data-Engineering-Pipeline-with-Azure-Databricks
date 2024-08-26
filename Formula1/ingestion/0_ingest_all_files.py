# Databricks notebook source
v_results =  dbutils.notebook.run("1.ingest_circuits_file",0,{'p_data_source':'Ergast Api','p_file_date':'2021-04-18'})

# COMMAND ----------

v_results

# COMMAND ----------

v_results =  dbutils.notebook.run("2.ingest_races_file",0,{'p_data_source':'Ergast Api','p_file_date':'2021-04-18'})

# COMMAND ----------

v_results

# COMMAND ----------

v_results =  dbutils.notebook.run("3.ingest_constructor_file",0,{'p_data_source':'Ergast Api','p_file_date':'2021-04-18'})

# COMMAND ----------

v_results

# COMMAND ----------

v_results =  dbutils.notebook.run("4.ingest_drivers_file",0,{'p_data_source':'Ergast Api','p_file_date':'2021-04-18'})

# COMMAND ----------

v_results

# COMMAND ----------

v_results =  dbutils.notebook.run("5.ingest_results_file",0,{'p_data_source':'Ergast Api','p_file_date':'2021-04-18'})

# COMMAND ----------

v_results

# COMMAND ----------

v_results =  dbutils.notebook.run("6.ingest_pit_stop.json_file",0,{'p_data_source':'Ergast Api','p_file_date':'2021-04-18'})

# COMMAND ----------

v_results

# COMMAND ----------

v_results =  dbutils.notebook.run("7.ingest_lap_times_file",0,{'p_data_source':'Ergast Api','p_file_date':'2021-04-18'})

# COMMAND ----------

v_results

# COMMAND ----------

v_results =  dbutils.notebook.run("8.ingest_qualifying_file",0,{'p_data_source':'Ergast Api','p_file_date':'2021-04-18'})

# COMMAND ----------

v_results

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1) from f1_processed.results group by race_id
# MAGIC order by race_id desc

# COMMAND ----------

