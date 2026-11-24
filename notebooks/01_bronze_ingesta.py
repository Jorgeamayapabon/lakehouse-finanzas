# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze: ingesta incremental de extractos
# MAGIC
# MAGIC ```
# MAGIC Volume landing/extractos/*.xlsx
# MAGIC      │  Paso A · Auto Loader (binaryFile, availableNow)  → exactamente una vez por archivo
# MAGIC      ▼
# MAGIC bronze.extracto_archivos   copia binaria del archivo + metadatos (permite reprocesar sin la fuente)
# MAGIC      │  Paso B · parser (openpyxl) solo de archivos pendientes → idempotente
# MAGIC      ▼
# MAGIC bronze.extracto_raw        cada celda como texto, con número de fila (_fila)
# MAGIC bronze.extracto_resumen    bloque "Información General" + "Resumen" (aún como texto)
# MAGIC ```
# MAGIC
# MAGIC **Reglas de bronze:** copia fiel de la fuente, todo como texto, nada se limpia ni se tipa aquí
# MAGIC (eso es silver). Solo se agregan columnas de **linaje** (`_archivo`, `_archivo_sha256`, `_fila`, `_ingestado_en`).
# MAGIC
# MAGIC **¿Por qué no el lector nativo de Excel?** Databricks lee .xlsx nativamente (`format("excel")`), ideal para
# MAGIC hojas tabulares. Nuestro extracto es un *reporte*: encabezados que se repiten en posiciones variables por página
# MAGIC y el orden de filas importa (cadena de saldos). Por eso guardamos el binario con Auto Loader y parseamos
# MAGIC nosotros, conservando el número de fila exacto. Al final hay una celda para comparar ambos enfoques.

# COMMAND ----------

# MAGIC %pip install openpyxl==3.1.5 -q

# COMMAND ----------

dbutils.widgets.text("catalogo", "finanzas_dev", "Catálogo (entorno)")
catalogo = dbutils.widgets.get("catalogo")

RUTA_LANDING = f"/Volumes/{catalogo}/bronze/landing/extractos/"
RUTA_CHECKPOINT = f"/Volumes/{catalogo}/bronze/checkpoints/extracto_archivos/"
T_ARCHIVOS = f"{catalogo}.bronze.extracto_archivos"
T_RAW = f"{catalogo}.bronze.extracto_raw"
T_RESUMEN = f"{catalogo}.bronze.extracto_resumen"
N_COLS = 8  # el extracto usa las columnas A..H

print(f"Catálogo: {catalogo}\nLanding:  {RUTA_LANDING}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Tablas y checkpoint
# MAGIC Las tablas se crean **explícitamente** con su esquema y comentarios (no se infieren al escribir):
# MAGIC el esquema es parte del contrato y queda documentado en Unity Catalog.

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalogo}.bronze.checkpoints "
          "COMMENT 'Estado de los streams (qué archivos ya procesó Auto Loader)'")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_ARCHIVOS} (
  _archivo        STRING    COMMENT 'Nombre del archivo en landing',
  _ruta           STRING    COMMENT 'Ruta completa en el Volume',
  _archivo_sha256 STRING    COMMENT 'Huella del contenido: identifica el archivo aunque cambie de nombre',
  _tamano_bytes   BIGINT,
  _modificado_en  TIMESTAMP COMMENT 'Fecha de modificación del archivo en el Volume',
  contenido       BINARY    COMMENT 'Bytes originales del .xlsx',
  _ingestado_en   TIMESTAMP
) COMMENT 'Bronze: copia binaria de cada extracto recibido (Auto Loader)'
""")

columnas_celdas = ",\n  ".join(f"col_{i} STRING" for i in range(1, N_COLS + 1))
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_RAW} (
  _archivo        STRING,
  _archivo_sha256 STRING,
  _fila           INT     COMMENT 'Número de fila en el Excel (1 = primera). Conserva el orden original',
  {columnas_celdas},
  _ingestado_en   TIMESTAMP
) COMMENT 'Bronze: todas las celdas del extracto como texto, una fila por fila de Excel'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_RESUMEN} (
  _archivo STRING, _archivo_sha256 STRING,
  desde STRING, hasta STRING, tipo_cuenta STRING, nro_cuenta STRING, sucursal STRING,
  saldo_anterior STRING, total_abonos STRING, total_cargos STRING, saldo_actual STRING,
  saldo_promedio STRING, cupo_sugerido STRING, intereses STRING, retefuente STRING,
  _ingestado_en TIMESTAMP
) COMMENT 'Bronze: encabezado y resumen de cada extracto (texto). Base de la reconciliación'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Paso A · Auto Loader
# MAGIC - `cloudFiles` lista el Volume y guarda en el **checkpoint** qué archivos ya leyó → cada archivo entra **una sola vez**.
# MAGIC - `trigger(availableNow=True)`: procesa todo lo nuevo y se detiene. Es *batch* con la contabilidad de un stream:
# MAGIC   el mismo código servirá en el Proyecto 2 (tiempo real) cambiando solo el trigger.
# MAGIC - `binaryFile`: no interpreta el archivo, solo trae sus bytes.

# COMMAND ----------

from pyspark.sql import functions as F

nuevos = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("pathGlobFilter", "*.xlsx")
    .load(RUTA_LANDING)
    .select(
        F.element_at(F.split("path", "/"), -1).alias("_archivo"),
        F.col("path").alias("_ruta"),
        F.sha2("content", 256).alias("_archivo_sha256"),
        F.col("length").alias("_tamano_bytes"),
        F.col("modificationTime").alias("_modificado_en"),
        F.col("content").alias("contenido"),
        F.current_timestamp().alias("_ingestado_en"),
    )
)

query = (
    nuevos.writeStream
    .option("checkpointLocation", RUTA_CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(T_ARCHIVOS)
)
query.awaitTermination()

n_nuevos = sum(p["numInputRows"] for p in query.recentProgress)
print(f"Archivos nuevos detectados por Auto Loader: {n_nuevos}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Paso B · Parsear archivos pendientes
# MAGIC **Idempotencia:** solo se parsean los archivos cuyo `_archivo_sha256` aún no está en la tabla destino
# MAGIC (*anti-join*). Si esta celda falla a la mitad y la vuelves a correr, no duplica nada.
# MAGIC
# MAGIC **Fail fast:** si un archivo no tiene la estructura del contrato, se detiene con un error claro
# MAGIC en vez de cargar basura.

# COMMAND ----------

import io

import openpyxl
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

CAMPOS_GENERAL = ["desde", "hasta", "tipo_cuenta", "nro_cuenta", "sucursal"]
CAMPOS_RESUMEN = ["saldo_anterior", "total_abonos", "total_cargos", "saldo_actual",
                  "saldo_promedio", "cupo_sugerido", "intereses", "retefuente"]


def parsear_extracto(contenido: bytes) -> list[tuple]:
    """Devuelve [(fila, col_1..col_8)] con todas las celdas como texto (None si está vacía)."""
    wb = openpyxl.load_workbook(io.BytesIO(contenido), read_only=True, data_only=True)
    ws = wb.worksheets[0]
    filas = []
    for n, row in enumerate(ws.iter_rows(max_col=N_COLS, values_only=True), start=1):
        valores = [None if v is None else str(v) for v in row]
        valores += [None] * (N_COLS - len(valores))
        filas.append((n, *valores[:N_COLS]))
    wb.close()
    return filas


def extraer_resumen(filas: list[tuple], archivo: str) -> tuple:
    """Toma la fila siguiente a los encabezados 'DESDE' y 'SALDO ANTERIOR' (primera aparición)."""
    def fila_despues_de(etiqueta: str) -> tuple:
        for k, f in enumerate(filas[:-1]):
            if f[1] == etiqueta:
                return filas[k + 1][1:]
        raise ValueError(f"{archivo}: no encontré el encabezado '{etiqueta}'. ¿Cumple el contrato de la fuente?")

    general = fila_despues_de("DESDE")[: len(CAMPOS_GENERAL)]
    resumen = fila_despues_de("SALDO ANTERIOR")[: len(CAMPOS_RESUMEN)]
    return (*general, *resumen)


SCHEMA_RAW = StructType(
    [StructField("_archivo", StringType()), StructField("_archivo_sha256", StringType()),
     StructField("_fila", IntegerType())]
    + [StructField(f"col_{i}", StringType()) for i in range(1, N_COLS + 1)]
)
SCHEMA_RESUMEN = StructType(
    [StructField(c, StringType()) for c in ["_archivo", "_archivo_sha256", *CAMPOS_GENERAL, *CAMPOS_RESUMEN]]
)


def pendientes(destino: str):
    """Archivos de bronze.extracto_archivos que aún no están en `destino` (por huella de contenido)."""
    archivos = spark.table(T_ARCHIVOS).dropDuplicates(["_archivo_sha256"])
    ya = spark.table(destino).select("_archivo_sha256").distinct()
    return archivos.join(ya, "_archivo_sha256", "left_anti").select("_archivo", "_archivo_sha256", "contenido")


# --- extracto_raw ---
filas_raw = []
for a in pendientes(T_RAW).collect():
    filas = parsear_extracto(bytes(a.contenido))
    filas_raw += [(a._archivo, a._archivo_sha256, *f) for f in filas]
    print(f"raw     <- {a._archivo}: {len(filas)} filas")
if filas_raw:
    (spark.createDataFrame(filas_raw, SCHEMA_RAW)
     .withColumn("_ingestado_en", F.current_timestamp())
     .write.mode("append").saveAsTable(T_RAW))

# --- extracto_resumen ---
filas_res = []
for a in pendientes(T_RESUMEN).collect():
    filas_res.append((a._archivo, a._archivo_sha256, *extraer_resumen(parsear_extracto(bytes(a.contenido)), a._archivo)))
    print(f"resumen <- {a._archivo}")
if filas_res:
    (spark.createDataFrame(filas_res, SCHEMA_RESUMEN)
     .withColumn("_ingestado_en", F.current_timestamp())
     .write.mode("append").saveAsTable(T_RESUMEN))

if not filas_raw and not filas_res:
    print("Nada pendiente: bronze ya está al día ✔")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificación

# COMMAND ----------

display(spark.sql(f"""
SELECT a._archivo,
       a._ingestado_en,
       (SELECT count(*) FROM {T_RAW} r WHERE r._archivo_sha256 = a._archivo_sha256) AS filas_raw,
       s.desde, s.hasta, s.saldo_anterior, s.saldo_actual
FROM {T_ARCHIVOS} a
LEFT JOIN {T_RESUMEN} s USING (_archivo_sha256)
ORDER BY s.desde
"""))

# COMMAND ----------

# Así se ve bronze: el extracto tal cual, con encabezados repetidos y todo como texto
display(spark.sql(f"SELECT * FROM {T_RAW} WHERE _fila BETWEEN 10 AND 22 ORDER BY _archivo, _fila"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Delta Lake: historial de versiones
# MAGIC Cada escritura crea una **versión** nueva de la tabla. Puedes auditar qué pasó y consultar el pasado
# MAGIC (*time travel*): `SELECT count(*) FROM <tabla> VERSION AS OF 1`.

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {T_RAW}").select("version", "timestamp", "operation", "operationMetrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. (Exploración) Lector nativo de Excel
# MAGIC Compara: el lector nativo funciona, pero no conserva el número de fila y trata el reporte como una tabla plana.

# COMMAND ----------

try:
    display(spark.read.format("excel").load(RUTA_LANDING).limit(25))
except Exception as e:
    print(f"El lector nativo no está disponible en este workspace: {e}")
