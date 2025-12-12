---------------------------------------------------------------
-- Archivo     : revoke.sql
---------------------------------------------------------------

-- 1. ELIMINACION DE TABLAS

-- Tablas GOLDEN
DROP TABLE IF EXISTS catalog_dev.golden.golden_health_centers_peru;
-- Tablas SILVER
DROP TABLE IF EXISTS catalog_dev.silver.health_centers_ubigeo;
-- Tablas BRONZE
DROP TABLE IF EXISTS catalog_dev.bronze.centers;
DROP TABLE IF EXISTS catalog_dev.bronze.ubigeo;


-- 2. ELIMINACION DE ESQUEMAS 

DROP SCHEMA IF EXISTS catalog_dev.golden      CASCADE;
DROP SCHEMA IF EXISTS catalog_dev.silver      CASCADE;
DROP SCHEMA IF EXISTS catalog_dev.bronze      CASCADE;
DROP SCHEMA IF EXISTS catalog_dev.exploratory CASCADE;


-- 3. ELIMINACION DE EXTERNAL LOCATIONS

DROP EXTERNAL LOCATION IF EXISTS `exlt-raw`;
DROP EXTERNAL LOCATION IF EXISTS `exlt-bronze`;
DROP EXTERNAL LOCATION IF EXISTS `exlt-silver`;
DROP EXTERNAL LOCATION IF EXISTS `exlt-golden`;


-- 4. ELIMINACION DE CATALOGO COMPLETO

DROP CATALOG IF EXISTS catalog_dev CASCADE;

