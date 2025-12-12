# project-databricks
Proyecto de Azure Databricks Smart Data 2025


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
Incluye los archivos SQL para otorgar permisos:

- GRANTS sobre catálogo
- GRANTS sobre schemas
- GRANTS sobre external locations
- GRANTS sobre tablas

Estos permisos están diseñados para roles como:

- `DataEngineers`
- `Analysts`
- `BI_Team`

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
2. **Transformación**  
3. **Unión de datasets**  
4. **Carga a capa Golden**  

Estos pueden ser invocados de manera secuencial mediante 
**Orquestación del flujo**  

---

## 📊 Dashboards – /dashboard/
Aquí se almacenan:

- Archivos `.json` exportados desde Power BI
- Imágenes `.png` de dashboards
- Reportes `.pbix`
- Enlaces guardados en `.txt`

El dashboard final debe consumir la tabla:  
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
