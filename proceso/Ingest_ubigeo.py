# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

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

ruta = f"abfss://{container}@{storage_name}.dfs.core.windows.net/ubigeo.csv"

# COMMAND ----------

ubigeo_schema = StructType(fields=[StructField("id_ubigeo", StringType(), False),
                                  StructField("nombre_departamento", StringType(), True),
                                  StructField("nombre_provincia", StringType(), True),
                                  StructField("nombre_distrito", StringType(), True),
                                  StructField("nombre_capital", StringType(), True),
                                  StructField("cod_region_natural", StringType(), True),
                                  StructField("nombre_region_natural", StringType(), True)
])

# COMMAND ----------

ubigeo_df = spark.read \
            .option("header", True) \
            .option("delimiter", ";") \
            .schema(ubigeo_schema) \
            .csv(ruta)


# COMMAND ----------

ubigeo_with_timestamp_df = ubigeo_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

ubigeo_selected_df = ubigeo_with_timestamp_df.select(col('id_ubigeo').alias('ubigeo'), 
                                                   col('nombre_departamento').alias('departamento'),
                                                   col('nombre_provincia').alias('provincia'),
                                                   col('nombre_distrito').alias('distrito'),
                                                   col('nombre_capital').alias('capital'),
                                                   col('nombre_region_natural').alias('region'),
                                                   col('ingestion_date'))

# COMMAND ----------

ubigeo_selected_df.write.option("mergeSchema", "true").mode('overwrite').saveAsTable(f'{catalogo}.{esquema}.ubigeo')