# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalogo", "catalog_dev")
dbutils.widgets.text("esquema_source", "bronze")
dbutils.widgets.text("esquema_sink", "silver")

# COMMAND ----------

catalogo = dbutils.widgets.get("catalogo")
esquema_source = dbutils.widgets.get("esquema_source")
esquema_sink = dbutils.widgets.get("esquema_sink")

# COMMAND ----------

def categoria_ubicacion(provincia):
    if provincia == "LIMA":
        return "LIMA"
    elif provincia == "CALLAO":
        return "LIMA"
    else:
        return "PROVINCIA"

# COMMAND ----------

ubicacion_udf = F.udf(categoria_ubicacion, StringType())

# COMMAND ----------

df_centers = spark.table(f"{catalogo}.{esquema_source}.centers")
df_ubigeo = spark.table(f"{catalogo}.{esquema_source}.ubigeo")

# COMMAND ----------

df_centers = df_centers.dropna(how="all")\
                        .filter((col("nombre_centro").isNotNull()) | (col("ubigeo")).isNotNull())

df_ubigeo = df_ubigeo.dropna(how="all")\
                    .filter((col("ubigeo").isNotNull()) | (col("distrito")).isNotNull())

# COMMAND ----------

df_ubigeo = df_ubigeo.withColumn("Zona", ubicacion_udf("provincia"))

# COMMAND ----------

df_centers2 = df_centers.withColumnRenamed("ubigeo", "ubigeo_center")
df_ubigeo2 = df_ubigeo.withColumnRenamed("ubigeo", "ubigeo_geo")

# COMMAND ----------

df_joined = df_centers2.alias("x").join(df_ubigeo2.alias("y"), col("x.ubigeo_center") == col("y.ubigeo_geo"), "inner")


# COMMAND ----------

df_joined = df_joined.withColumn("fecha_inicio_operacion",to_date(col("hora_inicio"), "dd/MM/yyyy"))

# COMMAND ----------

df_joined = df_joined.select(col('institucion'), 
                                                   col('cod_unico'),
                                                   col('nombre_centro'),
                                                   col('clasificacion_centro'),
                                                   col('tipo_centro'),
                                                   col('ubigeo_center').alias('ubigeo'),
                                                   col('departamento'),
                                                   col('provincia'),
                                                   col('distrito'),
                                                   col('direccion_centro'),
                                                   col('capital').alias('localidad'),
                                                   col('nombre_disa'),
                                                   col('nombre_red'),
                                                   col('nombre_microred'),
                                                   col('categoria'),
                                                   col('telefono'),
                                                   col('horario'),
                                                   col('fecha_inicio_operacion'),
                                                   col('nombre_responsable'),
                                                   col('estado'),
                                                   col('latitud'),
                                                   col('longitud'),
                                                   col('camas'),
                                                   col('ruc'),
                                                   col('region'),
                                                   col('Zona').alias('zona'),
                                                   col('x.ingestion_date').alias('ingestion_date'))

# COMMAND ----------

df_filtered_sorted = df_joined.orderBy("ubigeo")

df_filtered_sorted.display()


# COMMAND ----------

df_filtered_sorted.write.option("mergeSchema", "true").mode("overwrite").saveAsTable(f"{catalogo}.{esquema_sink}.health_centers_ubigeo")