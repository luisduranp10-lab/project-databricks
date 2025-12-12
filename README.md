# project-databricks
Proyecto de Azure Databricks Smart Data 2025

Información en base a los datasets de la Plataforma Nacional de Datos Abiertos del Gobierno del Perú

`hospitalesopendata.csv`
https://www.datosabiertos.gob.pe/dataset/directorio-de-establecimientos-de-salud?utm_source=chatgpt.com

`ubigeo.csv`
https://www.datosabiertos.gob.pe/dataset/ubigeos-c%C3%B3digos-de-ubicaci%C3%B3n-geogr%C3%A1fica-instituto-nacional-de-estad%C3%ADstica-e-inform%C3%A1tica-inei?utm_source=chatgpt.com


## 🚀 Arquitectura del ETL

El proyecto implementa un flujo ETL siguiendo buenas prácticas de arquitectura de datos:

### **🔸 Capa Bronze**
- Ingesta cruda desde fuentes externas.
- Archivos almacenados en external locations:  
  `exlt-raw`, `exlt-bronze`
- Tablas generadas:
  - `bronze.centers`
  - `bronze.ubigeo`

### **🔸 Capa Silver**
- Limpieza, normalización y enriquecimiento.
- External location: `exlt-silver`
- Tabla resultante:
  - `silver.health_centers_ubigeo`

### **🔸 Capa Golden**
- Dataset analítico final para los dashboards.
- External location: `exlt-golden`
- Tabla final:
  - `golden.golden_health_centers_peru`

---

## 🧱 Scripts Incluidos

### **📌 /scripts/**
Contiene los archivos SQL necesarios para preparar el ambiente:

- Creación del catálogo `catalog_dev`
- Creación de schemas:  
  `bronze`, `silver`, `golden`, `exploratory`
- Registro de external locations
- Creación inicial de tablas

Estos scripts deben ejecutarse antes de correr cualquier notebook ETL.

---

## 🔐 Seguridad – /seguridad/

---

## ♻️ Rollback – /reversion/
Contiene el archivo:

### **`reversion/revoke.sql`**
Este script elimina:

✔ Tablas lógicas (bronze, silver, golden)  
✔ Schemas  
✔ External locations  
✔ Catálogo completo  

Debe usarse únicamente para revertir despliegues de prueba o restaurar el ambiente desde cero.

---

## 🧩 Proceso ETL – /proceso/
Incluye los notebooks convertidos a `.py`:

- `Ingest_ubigeo.py`
- `Ingest_health_centers.py`
- `Transform.py`
- `Load.py`
- `Orquestador.py`

Cada archivo representa una etapa del ETL:

1. **Ingestión cruda**  
	- Ingest_ubigeo.py
	- Ingest_health_centers.py
2. **Transformación** 
3. **Unión de datasets**  
	- Transform.py
4. **Carga a capa Golden** 
	- Load.py

Estos pueden ser invocados de manera secuencial mediante 
**Orquestación del flujo** mediante `Orquestador.py`

---

## 📊 Dashboards – /dashboard/

- Reporte en PowerBI: Centros_salud_Peru.pbix

El dashboard final consume la tabla:  
`golden.golden_health_centers_peru`.

---

## 🧾 Evidencias – /certificaciones/

---

## 🔧 CI/CD – /.github/workflows/
Flujos propuestos:

- Validación de estructura del repositorio
- Despliegue automático a ambiente de desarrollo
- Opcional: despliegue a producción

(Se activará cuando se configure GitHub Actions)

---

## ▶️ Cómo ejecutar el proyecto

1. **Ejecutar scripts de `/scripts`**  
   - Crear catálogo, schemas, external locations y tablas base.

2. **Ejecutar los notebooks del ETL desde `/proceso`**  

	Se ejecuta Orquestador.py

   Este Orquesadoor ejecuta en el siguiente orden:
   
   1) Ingest_ubigeo.py  
   2) Ingest_health_centers.py  
   3) Transform.py  
   4) Load.py  

3. **Validar output en tabla Golden**  
   - `catalog_dev.golden.golden_health_centers_peru`

4. **Actualizar dashboard en `/dashboard`**

---

## 🔁 Rollback completo

Para limpiar todo el ambiente:

```sql
%sql
RUN ./reversion/revoke.sql
