# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text("storage_name", "ldpprojectsmartdata")
dbutils.widgets.text("container", "raw")
dbutils.widgets.text("catalogo", "catalog_dev")
dbutils.widgets.text("esquema", "bronze")

# COMMAND ----------

storage_name = dbutils.widgets.get("storage_name")
container = dbutils.widgets.get("container")
catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")

ruta = f"abfss://{container}@{storage_name}.dfs.core.windows.net/hospitalesopendata.csv"

# COMMAND ----------

df_centers = spark.read.option('header', True)\
                        .option('inferSchema', True)\
                        .csv(ruta)


# COMMAND ----------

centers_schema = StructType(fields=[StructField("institucion", StringType(), False),
                                     StructField("cod_unico", StringType(), True),
                                     StructField("nombre_centro", StringType(), True),
                                     StructField("clasificacion_centro", StringType(), True),
                                     StructField("tipo_centro", StringType(), True),
                                     StructField("departamento_ori", StringType(), True),
                                     StructField("provincia_ori", StringType(), True),
                                     StructField("distrito_ori", StringType(), True),
                                     StructField("ubigeo", StringType(), True),
                                     StructField("direccion_centro", StringType(), True),
                                     StructField("cod_disa", IntegerType(), True),
                                     StructField("cod_red", IntegerType(), True),
                                     StructField("cod_microred", IntegerType(), True),
                                     StructField("nombre_disa", StringType(), True),
                                     StructField("nombre_red", StringType(), True),
                                     StructField("nombre_microred", StringType(), True),
                                     StructField("cod_ue", IntegerType(), True),
                                     StructField("nombre_ue", StringType(), True),
                                     StructField("categoria", StringType(), True),
                                     StructField("telefono", StringType(), True),
                                     StructField("tipodoc_categoria", StringType(), True),
                                     StructField("numdoc_categoria", StringType(), True),
                                     StructField("horario", StringType(), True),
                                     StructField("hora_inicio", StringType(), True),
                                     StructField("nombre_responsable", StringType(), True),
                                     StructField("estado", StringType(), True),
                                     StructField("latitud", FloatType(), True),
                                     StructField("longitud", FloatType(), True),
                                     StructField("cota", StringType(), True),
                                     StructField("camas", IntegerType(), True),
                                     StructField("ruc", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Use user specified schema to load df with correct types
df_centers_final = spark.read\
.option('header', True)\
.schema(centers_schema)\
.csv(ruta)

# COMMAND ----------

# DBTITLE 1,select only specific cols
centers_selected_df = df_centers_final.select(col("institucion"), 
                                                col("cod_unico"), 
                                                col("nombre_centro"), 
                                                col("clasificacion_centro"), 
                                                col("tipo_centro"), 
                                                col("ubigeo"), 
                                                col("direccion_centro"), 
                                                col("cod_disa"), 
                                                col("cod_red"), 
                                                col("cod_microred"), 
                                                col("nombre_disa"), 
                                                col("nombre_red"), 
                                                col("nombre_microred"), 
                                                col("cod_ue"), 
                                                col("nombre_ue"), 
                                                col("categoria"), 
                                                col("telefono"), 
                                                col("tipodoc_categoria"), 
                                                col("numdoc_categoria"), 
                                                col("horario"), 
                                                col("hora_inicio"), 
                                                col("nombre_responsable"), 
                                                col("estado"), 
                                                col("latitud"), 
                                                col("longitud"), 
                                                col("cota"),
                                                col("camas"), 
                                                col("ruc"))


# COMMAND ----------

# DBTITLE 1,Add col with current timestamp 
centers_final_df = centers_selected_df.withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

centers_final_df.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema}.centers")