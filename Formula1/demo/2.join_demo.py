# Databricks notebook source
# MAGIC %run "../includes/Configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC Inner join

# COMMAND ----------

circuits_df = spark.read.parquet(processed_folder_path + '/circuits') \
    .filter("circuit_id < 70") \
    .withColumnRenamed("name","circuit_name")

# COMMAND ----------

races_df = spark.read.parquet(processed_folder_path + '/races').filter("race_year = 2019") \
    .withColumnRenamed("name","race_name")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"inner") \
    .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC left Outer Join / Left
# MAGIC

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"Left") \
    .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC RIght outer/ RIght
# MAGIC

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"Right") \
    .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC Full Outer/ Full Join

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"Full") \
    .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Semi join
# MAGIC

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"Semi") \
    .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"Semi")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC Anti 

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df,circuits_df.circuit_id == races_df.circuit_id,"Anti")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md Cross Join
# MAGIC

# COMMAND ----------

race_circuits_df = races_df.crossJoin(circuits_df)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

