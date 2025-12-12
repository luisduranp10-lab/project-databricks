-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()

-- COMMAND ----------

create widget text storageName default "ldpprojectsmartdata";

-- COMMAND ----------

DROP CATALOG IF EXISTS catalog_dev CASCADE;

CREATE CATALOG catalog_dev
-- MANAGED LOCATION 'abfss://metastore@ldpprojectsmartdata.dfs.core.windows.net/';
MANAGED LOCATION 'abfss://raw@ldpprojectsmartdata.dfs.core.windows.net/';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS catalog_dev.bronze;
CREATE SCHEMA IF NOT EXISTS catalog_dev.silver;
CREATE SCHEMA IF NOT EXISTS catalog_dev.golden;
CREATE SCHEMA IF NOT EXISTS catalog_dev.exploratory;

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-raw`
URL 'abfss://raw@${storageName}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL credential_psd)
COMMENT 'Ubicación externa para las tablas en crudo del Data Lake';

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-bronze`
URL 'abfss://bronze@${storageName}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL credential_psd)
COMMENT 'Ubicación externa para las tablas bronze del Data Lake';

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-silver`
URL 'abfss://silver@${storageName}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL credential_psd)
COMMENT 'Ubicación externa para las tablas silver del Data Lake';

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-golden`
URL 'abfss://golden@${storageName}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL credential_psd)
COMMENT 'Ubicación externa para las tablas golden del Data Lake';