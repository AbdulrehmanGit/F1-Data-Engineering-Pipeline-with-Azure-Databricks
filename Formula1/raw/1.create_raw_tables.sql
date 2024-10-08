-- Databricks notebook source
create database if not exists f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC creating Circuits Table

-- COMMAND ----------

drop table if exists f1_raw.circuits;
create table if not exists f1_raw.circuits(
circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
using csv
options (path "/mnt/formula1dlabrehman/raw/circuits.csv",header true )


-- COMMAND ----------


select * from f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC races table

-- COMMAND ----------

create table if not exists f1_raw.races(
raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING
)
using csv
options (path "/mnt/formula1dlabrehman/raw/races.csv",header true )

-- COMMAND ----------


select * from f1_raw.races

-- COMMAND ----------

-- MAGIC %md create table for JSON FILE
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC create constructors table
-- MAGIC

-- COMMAND ----------

create table if not exists f1_raw.constructors(
  constructorId INT, 
  constructorRef STRING, 
  name STRING,
   nationality STRING, 
   url STRING
)
using json
options(path "/mnt/formula1dlabrehman/raw/constructors.json")

-- COMMAND ----------

select * from f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md creating the driver table

-- COMMAND ----------

drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers(
  driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING
)
using json
options(path "/mnt/formula1dlabrehman/raw/drivers.json")

-- COMMAND ----------

select * from f1_raw.drivers

-- COMMAND ----------

create table if not exists f1_raw.results(
 resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING
)
using json
options(path "/mnt/formula1dlabrehman/raw/results.json")

-- COMMAND ----------

select * from f1_raw.results

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC pitstops table
-- MAGIC

-- COMMAND ----------

create table if not exists f1_raw.pit_stops(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING
)
using json
options(path "/mnt/formula1dlabrehman/raw/pit_stops.json",multiLine true)

-- COMMAND ----------

select * from  f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md creating table using list of files

-- COMMAND ----------

drop table if exists f1_raw.lap_times;
create table if not exists f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
using csv
options(path "/mnt/formula1dlabrehman/raw/lap_times")

-- COMMAND ----------

select * from f1_raw.lap_times

-- COMMAND ----------

create table if not exists f1_raw.qualifying(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT
)
using json 
options(path "/mnt/formula1dlabrehman/raw/qualifying",multiLine true)

-- COMMAND ----------

select * from f1_raw.qualifying