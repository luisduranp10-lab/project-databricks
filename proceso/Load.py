# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalogo", "catalog_dev")
dbutils.widgets.text("esquema_source", "silver")
dbutils.widgets.text("esquema_sink", "golden")

# COMMAND ----------

catalogo = dbutils.widgets.get("catalogo")
esquema_source = dbutils.widgets.get("esquema_source")
esquema_sink = dbutils.widgets.get("esquema_sink")

# COMMAND ----------

df_health_centers_transformed = spark.table(f"{catalogo}.{esquema_source}.health_centers_ubigeo")

df_health_centers_transformed.display()

# COMMAND ----------

df_health_centers_transformed.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_sink}.golden_health_centers_peru")