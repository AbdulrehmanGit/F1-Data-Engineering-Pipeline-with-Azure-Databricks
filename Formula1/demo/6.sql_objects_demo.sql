-- Databricks notebook source
-- MAGIC %md
-- MAGIC Lesson Objective
-- MAGIC
-- MAGIC - spark sql documentation
-- MAGIC - create database dmeo
-- MAGIC - data tab in the ui
-- MAGIC - show command
-- MAGIC - descrie command
-- MAGIC - find the current database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

show databases;

-- COMMAND ----------

describe database demo;


-- COMMAND ----------

describe database extended demo;


-- COMMAND ----------

select current_database()

-- COMMAND ----------

show tables

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

use demo;


-- COMMAND ----------

select current_database();

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC - Create managed table using python
-- MAGIC - create managed table using sql
-- MAGIC - effect of dropping a managed table
-- MAGIC - describe table

-- COMMAND ----------

-- MAGIC %run "../includes/Configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(presentation_folder_path+"/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

show tables;

-- COMMAND ----------

describe extended  race_results_python

-- COMMAND ----------


select *
from demo.race_results_python
where race_year =2020

-- COMMAND ----------

create table  race_results_sql 
as 
select *
from demo.race_results_python
where race_year =2020

-- COMMAND ----------

describe extended race_results_sql

-- COMMAND ----------

drop table race_results_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Learning objective 
-- MAGIC - creat external table using pyton
-- MAGIC - create externaltalbe using sql
-- MAGIC - effect of dropping an extenernal table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path",processed_folder_path+"/race_results_ext_py").saveAsTable("demo.race_results_python_ext_py")

-- COMMAND ----------

describe extended race_results_python_ext_py

-- COMMAND ----------

create table race_results_ext_sql
(
race_year	int,
race_name	string,
race_date	string,
circuit_location	string,
driver_name	string,
driver_number	int,
driver_nationality	string,
team	string,
grid	int,
fastest_lap	int,
race_time	string,
points	float,
position	int,
created_date	timestamp

)
using parquet
location "/mnt/formula1dlabrehman/presentation/race_results_ext_sql"

-- COMMAND ----------

show tables

-- COMMAND ----------

 

-- COMMAND ----------

insert into  race_results_ext_sql
select * from race_results_python_ext_py where race_year = 2020;

-- COMMAND ----------

select * from race_results_ext_sql

-- COMMAND ----------

show tables 

-- COMMAND ----------

drop table race_results_ext_sql

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Views on tables
-- MAGIC
-- MAGIC - create temp view
-- MAGIC - create global temp view
-- MAGIC - create permenent views

-- COMMAND ----------

show tables 

-- COMMAND ----------

drop table  race_results_python_ext_py

-- COMMAND ----------

create or replace temp view v_race_results
as 
select * from race_results_python where race_year = 2018;

-- COMMAND ----------

select * from v_race_results

-- COMMAND ----------

create or replace global temp view gv_race_results
as 
select * from race_results_python where race_year = 2012;

-- COMMAND ----------

select * from global_temp.gv_race_results

-- COMMAND ----------

create or replace view pv_race_results
as 
select * from race_results_python where race_year = 2000;

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

select * from pv_race_results where race_date not null

-- COMMAND ----------

