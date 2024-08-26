-- Databricks notebook source
-- MAGIC %md 
-- MAGIC drop all tables

-- COMMAND ----------

Drop database if exists f1_processed cascade;

-- COMMAND ----------

create database if not exists f1_processed
location "/mnt/formula1dlabrehman/processed";

-- COMMAND ----------

Drop database if exists f1_presentation cascade;

-- COMMAND ----------

create database if not exists f1_presentation
location "/mnt/formula1dlabrehman/presentation";