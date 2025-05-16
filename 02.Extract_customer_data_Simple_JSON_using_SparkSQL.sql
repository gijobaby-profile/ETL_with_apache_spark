-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Extract data from customer Simple JSON files using Spark SQL
-- MAGIC 1. Query single JSON file
-- MAGIC 1. Query multiple JSON files using wild charactors
-- MAGIC 2. Query entire files in a folder

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query single JSON file
-- MAGIC

-- COMMAND ----------

SELECT * FROM JSON.`/Volumes/gizmobox/landing/operational_data/customers/customers_2024_10.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query Multiple JSON files
-- MAGIC

-- COMMAND ----------

SELECT * FROM JSON.`/Volumes/gizmobox/landing/operational_data/customers/customers_2024_*.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query all the JSON files in a folder
-- MAGIC

-- COMMAND ----------

SELECT * FROM JSON.`/Volumes/gizmobox/landing/operational_data/customers/`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### select the metadata
-- MAGIC

-- COMMAND ----------

select _metadata.file_path as file_path
, _metadata.file_name as file_name
,* 
from json.`/Volumes/gizmobox/landing/operational_data/customers/customers_2024_*.json`

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Registering files into unity catalog using views

-- COMMAND ----------

CREATE OR REPLACE VIEW gizmobox.bronze.v_customers 
AS
select *
    , _metadata.file_path as file_path
    , _metadata.file_name as file_name
from json.`/Volumes/gizmobox/landing/operational_data/customers/`

-- COMMAND ----------

select * from gizmobox.bronze.v_customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Temporary views
-- MAGIC 1.  temp views are not registered in unity catalog / hive metastore

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW tv_customer 
AS  
     SELECT *, _metadata.file_path as file_path  
     FROM JSON.`/Volumes/gizmobox/landing/operational_data/customers/`

-- COMMAND ----------

select * from tv_customer 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Global Temporary views
-- MAGIC 1. Global temp views are registed under spark schema called Global_temp and not in unity catalog / hive metastore

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW gtv_customer 
AS  
     SELECT *, _metadata.file_path as file_path  
     FROM JSON.`/Volumes/gizmobox/landing/operational_data/customers/`

-- COMMAND ----------

select * from global_temp.gtv_customer;
