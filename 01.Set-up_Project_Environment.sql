-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Set-up project environment for GizmoBox Data Lakehouse project

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Step 1 : Access the container gizmobox using abfss protocol
-- MAGIC

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://gizmobox@gijodatabricksextdl.dfs.core.windows.net/landing/operational_data'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2. create the external location, as we have already done the unity catalog setup so the storage credentials are aleady created before. 

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `databricksextdl_gizmobox`
URL 'abfss://gizmobox@gijodatabricksextdl.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `databrickscourse_ext_sc`)
COMMENT 'External location for gizmobox container in gijodatabricksextdl datalake';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3. Create catalog ( catalog name is same as the container name)

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS gizmobox
MANAGED LOCATION 'abfss://gizmobox@gijodatabricksextdl.dfs.core.windows.net/'
COMMENT 'Catalog for gizmobox DATA LAKEHOUSE'

-- COMMAND ----------

show catalogs;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 4. Create all the 4 schemas
-- MAGIC 1. landing
-- MAGIC 1. bronze
-- MAGIC 1. silver
-- MAGIC 1. gold

-- COMMAND ----------

select current_catalog()

-- COMMAND ----------

use catalog gizmobox;

CREATE SCHEMA IF NOT EXISTS landing
MANAGED LOCATION 'abfss://gizmobox@gijodatabricksextdl.dfs.core.windows.net/landing'
COMMENT 'landing schema for gizmobox' ;

CREATE SCHEMA IF NOT EXISTS bronze
MANAGED LOCATION 'abfss://gizmobox@gijodatabricksextdl.dfs.core.windows.net/bronze'
COMMENT 'bronze schema for gizmobox' ;


CREATE SCHEMA IF NOT EXISTS silver
MANAGED LOCATION 'abfss://gizmobox@gijodatabricksextdl.dfs.core.windows.net/silver'
COMMENT 'silver schema for gizmobox' ;


CREATE SCHEMA IF NOT EXISTS gold
MANAGED LOCATION 'abfss://gizmobox@gijodatabricksextdl.dfs.core.windows.net/gold'
COMMENT 'gold schema for gizmobox' ;




-- COMMAND ----------

show schemas

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 5. create volume

-- COMMAND ----------

use catalog gizmobox;
use schema landing;

CREATE EXTERNAL VOLUME gizmobox.landing.operational_data
LOCATION 'abfss://gizmobox@gijodatabricksextdl.dfs.core.windows.net/landing/operational_data/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Step 6 : Access the container gizmobox using Volume

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/gizmobox/landing/operational_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Read from Customer JSON files in different methods

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Method 1 : Read a single JSON file

-- COMMAND ----------



-- COMMAND ----------

use catalog gizmobox;
use schema landing;
SELECT * FROM JSON.`/Volumes/gizmobox/landing/operational_data/customers/customers_2024_10.json`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC cust_df=spark.read.json("/Volumes/gizmobox/landing/operational_data/customers/customers_2024_10.json")
-- MAGIC display(cust_df)
