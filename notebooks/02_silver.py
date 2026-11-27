# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver: movimientos limpios, únicos y categorizados
# MAGIC
# MAGIC ```
# MAGIC bronze.extracto_resumen ──► v_extractos ──────────────────────────────► MERGE ► silver.extractos
# MAGIC bronze.extracto_raw ──────► v_mov_tipados ─► v_mov_silver ──────────► MERGE ► silver.movimientos
# MAGIC                              (filtrar, tipar,   (deduplicar +
# MAGIC                               fecha con año,     categorizar con
# MAGIC                               id_movimiento)     silver.reglas_categoria)
# MAGIC ```
# MAGIC
# MAGIC **Reglas de silver:** tipos correctos, una fila por hecho real (sin duplicados), nombres de negocio y
# MAGIC enriquecimiento (categoría). Las transformaciones se escriben en **SQL** con vistas temporales:
# MAGIC cada paso es pequeño, legible y se puede inspeccionar por separado.
# MAGIC
# MAGIC **Estrategia de carga:** se recalcula silver desde *todo* bronze en cada corrida y se aplica con `MERGE`.
# MAGIC Con este volumen es barato, y tiene una ventaja: si cambias una regla de categoría, la próxima corrida
# MAGIC re-categoriza el histórico completo. El `MERGE` solo inserta lo nuevo y solo actualiza lo que cambió.

# COMMAND ----------

dbutils.widgets.text("catalogo", "finanzas_dev", "Catálogo (entorno)")
catalogo = dbutils.widgets.get("catalogo")

T_RAW = f"{catalogo}.bronze.extracto_raw"
T_RESUMEN = f"{catalogo}.bronze.extracto_resumen"
T_REGLAS = f"{catalogo}.silver.reglas_categoria"
T_EXTRACTOS = f"{catalogo}.silver.extractos"
T_MOV = f"{catalogo}.silver.movimientos"

print(f"Catálogo: {catalogo}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Reglas de categorización (*seed*)
# MAGIC Las reglas viven **en el código** (versionadas en Git) y se cargan a una tabla en cada corrida.
# MAGIC A esto se le llama *seed*: datos de referencia pequeños que mantiene el equipo, no una fuente externa.
# MAGIC
# MAGIC - `patron`: expresión regular sobre la descripción (`(?i)` = sin distinguir mayúsculas).
# MAGIC - `signo`: `+` solo abonos, `-` solo cargos, `*` ambos.
# MAGIC - `naturaleza`: cómo cuenta el movimiento en el flujo de caja:
# MAGIC   `INGRESO`, `GASTO`, `INTERNO` (entre cuentas propias: no es ingreso ni gasto) o `INVERSION`.
# MAGIC - Gana la regla con **menor prioridad** que coincida. Lo que no coincide queda como `Sin categoría`.

# COMMAND ----------

REGLAS = [
    # prioridad, patron,                                         signo, categoria,                         naturaleza
    (10,  r"(?i)^ABONO INTERESES",                                 "+", "Intereses",                       "INGRESO"),
    (20,  r"(?i)^TRANSF (DE|A) TITULAR",                           "*", "Transferencia entre cuentas propias", "INTERNO"),
    (30,  r"(?i)^(PAGO DE PROV|PAGO INMEDIATO DE) ",               "+", "Salario",                         "INGRESO"),
    (40,  r"(?i)^TRANSF DE ",                                      "+", "Transferencias recibidas",        "INGRESO"),
    (50,  r"(?i)^CONSIGNACION",                                    "+", "Consignaciones",                  "INGRESO"),
    (60,  r"(?i)NEQUI",                                            "-", "Transferencias a Nequi",          "GASTO"),
    (70,  r"(?i)^TRANSF A ",                                       "-", "Transferencias enviadas",         "GASTO"),
    (80,  r"(?i)^TRANSFERENCIA CTA SUC VIRTUAL",                   "-", "Transferencias programadas",      "GASTO"),
    (90,  r"(?i)TC (VISA|AMEX)",                                   "-", "Pago tarjeta de crédito",         "GASTO"),
    (100, r"(?i)COMPENSAR",                                        "-", "Salud y seguridad social",        "GASTO"),
    (110, r"(?i)GASES DEL CARIBE|CARIBEMAR|EMPRESA DE SERVICIO",   "-", "Servicios públicos",              "GASTO"),
    (120, r"(?i)PROTEGE TU VIDA",                                  "-", "Seguros",                         "GASTO"),
    (130, r"(?i)^C MANEJO TARJ",                                   "-", "Comisiones bancarias",            "GASTO"),
    (140, r"(?i)^RETIRO CAJERO",                                   "-", "Retiro de efectivo",              "GASTO"),
    (150, r"(?i)UBER",                                             "*", "Transporte",                      "GASTO"),
    (160, r"(?i)^PAGO (QR|LLAVE) ",                                "-", "Compras en comercios",            "GASTO"),
    (170, r"(?i)FIDEICOMISOS|PEXTO CAPITAL",                       "-", "Inversiones",                     "INVERSION"),
    (180, r"(?i)TECNIPAGOS",                                       "-", "Pagos PSE varios",                "GASTO"),
]

(spark.createDataFrame(REGLAS, "prioridad INT, patron STRING, signo STRING, categoria STRING, naturaleza STRING")
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_REGLAS))
spark.sql(f"COMMENT ON TABLE {T_REGLAS} IS 'Seed: reglas de categorización (fuente de verdad: notebook 02_silver en Git)'")
display(spark.table(T_REGLAS).orderBy("prioridad"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extractos: el encabezado, tipado
# MAGIC De texto a tipos reales. `try_cast` devuelve `NULL` en vez de fallar si un valor no es convertible
# MAGIC (con ANSI SQL, un `CAST` normal detendría todo el notebook por un solo valor malo).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW v_extractos AS
SELECT
  _archivo_sha256,
  _archivo,
  nro_cuenta                                                       AS cuenta,
  tipo_cuenta,
  to_date(desde, 'yyyy/MM/dd')                                     AS fecha_desde,
  to_date(hasta, 'yyyy/MM/dd')                                     AS fecha_hasta,
  try_cast(replace(saldo_anterior, ',', '') AS DECIMAL(18,2))      AS saldo_anterior,
  try_cast(replace(total_abonos,   ',', '') AS DECIMAL(18,2))      AS total_abonos,
  try_cast(replace(total_cargos,   ',', '') AS DECIMAL(18,2))      AS total_cargos,
  try_cast(replace(saldo_actual,   ',', '') AS DECIMAL(18,2))      AS saldo_actual,
  try_cast(replace(saldo_promedio, ',', '') AS DECIMAL(18,2))      AS saldo_promedio,
  try_cast(replace(intereses,      ',', '') AS DECIMAL(18,2))      AS intereses,
  try_cast(replace(retefuente,     ',', '') AS DECIMAL(18,2))      AS retefuente
FROM {T_RESUMEN}
""")
display(spark.table("v_extractos"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Movimientos: filtrar, limpiar y tipar
# MAGIC 1. **Filtrar:** solo filas cuya columna A es una fecha `d/MM`. Esto descarta de un golpe los bloques de
# MAGIC    encabezado, los encabezados repetidos por página y `FIN ESTADO DE CUENTA`.
# MAGIC 2. **Año de la fecha:** el extracto no lo trae. Si el mes del movimiento es ≤ al mes de `fecha_hasta`, es el año
# MAGIC    de `fecha_hasta`; si es mayor, es el año anterior. Así un extracto nov→ene asigna 2026 a noviembre y 2027 a enero.
# MAGIC 3. **Limpiar:** espacios dobles en la descripción (`RETIRO CAJERO  ATM` → `RETIRO CAJERO ATM`), números a DECIMAL.
# MAGIC 4. **Llave:** `id_movimiento = sha2(cuenta|fecha|descripcion|valor|saldo)`, la llave natural definida en el contrato.
# MAGIC
# MAGIC **DECIMAL y no DOUBLE para dinero:** `DOUBLE` es binario y no representa exactamente 0.10;
# MAGIC sumar miles de movimientos acumula errores de centavos. `DECIMAL(18,2)` es exacto.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW v_mov_tipados AS
WITH filas_mov AS (
  SELECT r._archivo, r._archivo_sha256, r._fila,
         trim(r.col_1)                                  AS fecha_txt,
         regexp_replace(trim(r.col_2), ' +', ' ')       AS descripcion,
         nullif(trim(r.col_3), '')                      AS sucursal,
         nullif(trim(r.col_4), '')                      AS documento,
         r.col_5                                        AS valor_txt,
         r.col_6                                        AS saldo_txt,
         e.cuenta, e.fecha_desde, e.fecha_hasta
  FROM {T_RAW} r
  JOIN v_extractos e USING (_archivo_sha256)
  WHERE trim(r.col_1) RLIKE '^[0-9]{{1,2}}/[0-9]{{1,2}}$'
),
tipados AS (
  SELECT *,
    CAST(try_to_timestamp(
           concat(fecha_txt, '/',
                  CASE WHEN CAST(split(fecha_txt, '/')[1] AS INT) <= month(fecha_hasta)
                       THEN year(fecha_hasta) ELSE year(fecha_hasta) - 1 END),
           'd/M/yyyy') AS DATE)                             AS fecha,
    try_cast(replace(valor_txt, ',', '') AS DECIMAL(18,2))  AS valor,
    try_cast(replace(saldo_txt, ',', '') AS DECIMAL(18,2))  AS saldo
  FROM filas_mov
)
SELECT *,
  sha2(concat_ws('|', cuenta, CAST(fecha AS STRING), descripcion,
                 CAST(valor AS STRING), CAST(saldo AS STRING)), 256)  AS id_movimiento,
  (fecha IS NOT NULL AND valor IS NOT NULL AND saldo IS NOT NULL)     AS es_valido
FROM tipados
""")

display(spark.sql("""
SELECT _archivo, count(*) AS movimientos, sum(CASE WHEN es_valido THEN 0 ELSE 1 END) AS invalidos,
       min(fecha) AS primera_fecha, max(fecha) AS ultima_fecha
FROM v_mov_tipados GROUP BY _archivo ORDER BY primera_fecha
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filas inválidas
# MAGIC No se cargan a silver, pero **no se pierden en silencio**: aquí se ven. En el Día 3 irán a una tabla de
# MAGIC cuarentena con el motivo, junto con el resto de reglas de calidad del contrato.

# COMMAND ----------

display(spark.sql("""
SELECT _archivo, _fila, fecha_txt, descripcion, valor_txt, saldo_txt
FROM v_mov_tipados WHERE NOT es_valido ORDER BY _archivo, _fila
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Deduplicar y categorizar
# MAGIC - **Deduplicar:** si el mismo `id_movimiento` aparece más de una vez (una fila repetida, o dos extractos con
# MAGIC   periodos que se solapan), se conserva una sola fila. Además el `MERGE` lo **exige**: si dos filas de origen
# MAGIC   coinciden con la misma fila destino, falla.
# MAGIC - **Categorizar:** cada movimiento se cruza con todas las reglas que coinciden (patrón + signo) y se queda con la
# MAGIC   de menor prioridad (`row_number`).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW v_mov_silver AS
WITH unicos AS (
  SELECT * FROM (
    SELECT *, row_number() OVER (PARTITION BY id_movimiento ORDER BY fecha_hasta DESC, _fila) AS rn_dup
    FROM v_mov_tipados WHERE es_valido
  ) WHERE rn_dup = 1
),
con_regla AS (
  SELECT u.*, g.categoria, g.naturaleza,
         row_number() OVER (PARTITION BY u.id_movimiento ORDER BY g.prioridad NULLS LAST) AS rn_regla
  FROM unicos u
  LEFT JOIN {T_REGLAS} g
    ON u.descripcion RLIKE g.patron
   AND (g.signo = '*' OR (g.signo = '+' AND u.valor > 0) OR (g.signo = '-' AND u.valor < 0))
)
SELECT
  id_movimiento, cuenta, fecha, descripcion, sucursal, documento, valor, saldo,
  CASE WHEN valor >= 0 THEN 'ABONO' ELSE 'CARGO' END                         AS tipo,
  coalesce(categoria, 'Sin categoría')                                       AS categoria,
  coalesce(naturaleza, CASE WHEN valor >= 0 THEN 'INGRESO' ELSE 'GASTO' END) AS naturaleza,
  _archivo, _archivo_sha256, _fila
FROM con_regla
WHERE rn_regla = 1
""")

display(spark.sql("""
SELECT (SELECT count(*) FROM v_mov_tipados WHERE es_valido) AS validos,
       (SELECT count(*) FROM v_mov_silver)                  AS unicos
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Tablas silver
# MAGIC - `NOT NULL`: Delta **rechaza** cualquier escritura que lo viole: es una regla de calidad que vive en la tabla.
# MAGIC - `PRIMARY KEY`: en Unity Catalog es **informativa** (no se valida), pero documenta la llave para personas,
# MAGIC   herramientas de BI y el agente del Proyecto 3. La unicidad la garantiza el `MERGE`.
# MAGIC - `CLUSTER BY (fecha)`: *liquid clustering*. Organiza los archivos por fecha para que las consultas por periodo
# MAGIC   lean menos datos. Reemplaza al particionamiento clásico.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_EXTRACTOS} (
  _archivo_sha256 STRING NOT NULL,
  _archivo        STRING,
  cuenta          STRING NOT NULL,
  tipo_cuenta     STRING,
  fecha_desde     DATE   NOT NULL,
  fecha_hasta     DATE   NOT NULL,
  saldo_anterior  DECIMAL(18,2),
  total_abonos    DECIMAL(18,2),
  total_cargos    DECIMAL(18,2),
  saldo_actual    DECIMAL(18,2),
  saldo_promedio  DECIMAL(18,2),
  intereses       DECIMAL(18,2),
  retefuente      DECIMAL(18,2),
  _actualizado_en TIMESTAMP,
  CONSTRAINT pk_extractos PRIMARY KEY (_archivo_sha256)
) COMMENT 'Silver: un registro por extracto recibido, con periodo y saldos tipados'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_MOV} (
  id_movimiento   STRING        NOT NULL COMMENT 'sha2(cuenta|fecha|descripcion|valor|saldo)',
  cuenta          STRING        NOT NULL,
  fecha           DATE          NOT NULL,
  descripcion     STRING        NOT NULL,
  sucursal        STRING,
  documento       STRING,
  valor           DECIMAL(18,2) NOT NULL COMMENT 'Positivo = abono, negativo = cargo',
  saldo           DECIMAL(18,2) NOT NULL COMMENT 'Saldo después del movimiento',
  tipo            STRING        NOT NULL COMMENT 'ABONO | CARGO',
  categoria       STRING        NOT NULL,
  naturaleza      STRING        NOT NULL COMMENT 'INGRESO | GASTO | INTERNO | INVERSION',
  _archivo        STRING,
  _archivo_sha256 STRING,
  _fila           INT,
  _creado_en      TIMESTAMP,
  _actualizado_en TIMESTAMP,
  CONSTRAINT pk_movimientos PRIMARY KEY (id_movimiento)
)
CLUSTER BY (fecha)
COMMENT 'Silver: un registro por movimiento real, tipado, deduplicado y categorizado'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. MERGE (upsert)
# MAGIC `MERGE` compara origen y destino por la llave y decide fila por fila:
# MAGIC
# MAGIC | Caso | Acción |
# MAGIC |---|---|
# MAGIC | La llave no existe en destino | `INSERT` |
# MAGIC | Existe y cambió la categoría | `UPDATE` (solo las columnas que pueden cambiar) |
# MAGIC | Existe y no cambió nada | nada: no se reescribe |
# MAGIC
# MAGIC `<=>` es la igualdad que trata `NULL = NULL` como verdadero (la `=` normal devuelve `NULL`).

# COMMAND ----------

res_ext = spark.sql(f"""
MERGE INTO {T_EXTRACTOS} t
USING v_extractos s
ON t._archivo_sha256 = s._archivo_sha256
WHEN NOT MATCHED THEN INSERT (
  _archivo_sha256, _archivo, cuenta, tipo_cuenta, fecha_desde, fecha_hasta, saldo_anterior, total_abonos,
  total_cargos, saldo_actual, saldo_promedio, intereses, retefuente, _actualizado_en
) VALUES (
  s._archivo_sha256, s._archivo, s.cuenta, s.tipo_cuenta, s.fecha_desde, s.fecha_hasta, s.saldo_anterior, s.total_abonos,
  s.total_cargos, s.saldo_actual, s.saldo_promedio, s.intereses, s.retefuente, current_timestamp()
)
""")
print("silver.extractos")
display(res_ext)

res_mov = spark.sql(f"""
MERGE INTO {T_MOV} t
USING v_mov_silver s
ON t.id_movimiento = s.id_movimiento
WHEN MATCHED AND NOT (t.categoria <=> s.categoria AND t.naturaleza <=> s.naturaleza) THEN UPDATE SET
  t.categoria = s.categoria,
  t.naturaleza = s.naturaleza,
  t._actualizado_en = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
  id_movimiento, cuenta, fecha, descripcion, sucursal, documento, valor, saldo, tipo, categoria, naturaleza,
  _archivo, _archivo_sha256, _fila, _creado_en, _actualizado_en
) VALUES (
  s.id_movimiento, s.cuenta, s.fecha, s.descripcion, s.sucursal, s.documento, s.valor, s.saldo, s.tipo, s.categoria,
  s.naturaleza, s._archivo, s._archivo_sha256, s._fila, current_timestamp(), current_timestamp()
)
""")
print("silver.movimientos")
display(res_mov)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verificación
# MAGIC ### Reconciliación (adelanto del Día 3)
# MAGIC Por cada extracto: `saldo_anterior + Σ valor` debe ser igual a `saldo_actual`.

# COMMAND ----------

display(spark.sql(f"""
SELECT e._archivo, e.fecha_desde, e.fecha_hasta,
       count(m.id_movimiento)                      AS movimientos,
       e.saldo_anterior,
       e.saldo_anterior + coalesce(sum(m.valor),0) AS saldo_calculado,
       e.saldo_actual,
       e.saldo_anterior + coalesce(sum(m.valor),0) = e.saldo_actual AS cuadra
FROM {T_EXTRACTOS} e
LEFT JOIN {T_MOV} m ON m._archivo_sha256 = e._archivo_sha256
GROUP BY ALL
ORDER BY e.fecha_desde
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resumen por categoría

# COMMAND ----------

display(spark.sql(f"""
SELECT naturaleza, categoria, count(*) AS movimientos, sum(valor) AS total
FROM {T_MOV}
GROUP BY ALL
ORDER BY naturaleza, total
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sin categoría
# MAGIC Lo que ninguna regla reconoce. Revisa esta lista y agrega reglas en la celda 1: en la próxima corrida el
# MAGIC `MERGE` re-categoriza el histórico.

# COMMAND ----------

display(spark.sql(f"""
SELECT descripcion, tipo, count(*) AS veces, sum(valor) AS total
FROM {T_MOV} WHERE categoria = 'Sin categoría'
GROUP BY ALL ORDER BY veces DESC
"""))
