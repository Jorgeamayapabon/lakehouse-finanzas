# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 00 · Setup del lakehouse de finanzas
# MAGIC
# MAGIC Crea la estructura en **Unity Catalog** (namespace de 3 niveles: `catalogo.schema.objeto`):
# MAGIC
# MAGIC | Objeto | Para qué |
# MAGIC |---|---|
# MAGIC | `<catalogo>` | Un catálogo por **entorno** (`finanzas_dev`, `finanzas_prod`). Así dev y prod nunca se mezclan. |
# MAGIC | `bronze` | Datos crudos, tal como llegan, + columnas de linaje. Nunca se edita a mano. |
# MAGIC | `silver` | Datos limpios, tipados, deduplicados. |
# MAGIC | `gold` | Tablas de negocio listas para dashboards y para el agente (Proyecto 3). |
# MAGIC | `bronze.landing` (Volume) | Zona de aterrizaje de **archivos** (.xlsx). Los archivos aquí son inmutables. |
# MAGIC
# MAGIC Es idempotente: puedes correrlo las veces que quieras (`IF NOT EXISTS`).

# COMMAND ----------

dbutils.widgets.text("catalogo", "finanzas_dev", "Catálogo (entorno)")
catalogo = dbutils.widgets.get("catalogo")
print(f"Usando catálogo: {catalogo}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Catálogo
# MAGIC Si tu workspace no te deja crear catálogos, usa el catálogo `workspace` que trae Free Edition
# MAGIC (cambia el widget de arriba a `workspace`) y sigue igual.

# COMMAND ----------

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalogo} COMMENT 'Lakehouse de finanzas personales (PoC)'")
except Exception as e:
    print(f"No se pudo crear el catálogo '{catalogo}': {e}\n"
          "-> Cambia el widget a 'workspace' y vuelve a ejecutar.")
    raise
spark.sql(f"USE CATALOG {catalogo}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Schemas (capas medallion)

# COMMAND ----------

capas = {
    "bronze": "Crudo: filas tal cual vienen del extracto + linaje (_archivo, _fila, _ingestado_en)",
    "silver": "Limpio: movimientos tipados, deduplicados y categorizados",
    "gold":   "Negocio: agregados para dashboards y consumo analítico",
}
for schema, comentario in capas.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema} COMMENT '{comentario}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Volume de aterrizaje
# MAGIC Un **Volume** es almacenamiento de archivos gobernado por Unity Catalog (permisos, linaje),
# MAGIC a diferencia de una tabla. Ruta: `/Volumes/<catalogo>/bronze/landing/extractos/`

# COMMAND ----------

spark.sql("CREATE VOLUME IF NOT EXISTS bronze.landing COMMENT 'Archivos fuente sin procesar (extractos .xlsx)'")
ruta_landing = f"/Volumes/{catalogo}/bronze/landing/extractos/"
dbutils.fs.mkdirs(ruta_landing)
print(f"Sube tus extractos a: {ruta_landing}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificación

# COMMAND ----------

catalogo = dbutils.widgets.get("catalogo")
ruta_landing = f"/Volumes/{catalogo}/bronze/landing/extractos/"

display(spark.sql(f"SHOW SCHEMAS IN {catalogo}"))
display(dbutils.fs.ls(ruta_landing))