-- Databricks notebook source
use f1_processed

-- COMMAND ----------

select * from drivers

-- COMMAND ----------

select *,concat(driver_ref, code) as new_driver_ref from drivers

-- COMMAND ----------

select split(name, " ")[0] as forename,split(name, " ")[1] as surname,* from drivers

-- COMMAND ----------

select *,date_format(dob,'dd-MM-yyyy') from drivers 

-- COMMAND ----------

select *,date_add(dob,1) from drivers 

-- COMMAND ----------

select count(*) from drivers

-- COMMAND ----------

select name from drivers where dob = (select max(dob) from drivers)

-- COMMAND ----------

select nationality, count(*) from drivers group by nationality having count(*) > 100

-- COMMAND ----------

select nationality, name , dob, RANK() over(partition by nationality order by dob desc) age_rank
 from drivers

-- COMMAND ----------

