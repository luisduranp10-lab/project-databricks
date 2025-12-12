# Databricks notebook source
dbutils.notebook.run("/Workspace/Users/luis.duranp.10_gmail.com#ext#@luisduranp10gmail.onmicrosoft.com/Proyecto_SmartData/Preparacion_Ambiente",3600,{"storage_name":"ldpprojectsmartdata"})

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/luis.duranp.10_gmail.com#ext#@luisduranp10gmail.onmicrosoft.com/Proyecto_SmartData/Ingest_health_centers",3600,{"storage_name":"ldpprojectsmartdata","container":"raw","catalogo":"catalog_dev","esquema":"bronze"})

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/luis.duranp.10_gmail.com#ext#@luisduranp10gmail.onmicrosoft.com/Proyecto_SmartData/Ingest_ubigeo",3600,{"storage_name":"ldpprojectsmartdata","container":"raw","catalogo":"catalog_dev","esquema":"bronze"})

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/luis.duranp.10_gmail.com#ext#@luisduranp10gmail.onmicrosoft.com/Proyecto_SmartData/Transform",3600,{"catalogo":"catalog_dev","esquema_source":"bronze","esquema_sink":"silver"})

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/luis.duranp.10_gmail.com#ext#@luisduranp10gmail.onmicrosoft.com/Proyecto_SmartData/Load",3600,{"catalogo":"catalog_dev","esquema_source":"silver","esquema_sink":"golden"})