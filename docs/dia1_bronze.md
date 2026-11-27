# Día 1 · Ingesta incremental a bronze

> Notebook: [`notebooks/01_bronze_ingesta.py`](../notebooks/01_bronze_ingesta.py)

**Objetivo:** llevar los Excel del Volume a tablas Delta con tres garantías:

1. Cada archivo se procesa **una sola vez**, aunque el notebook se corra muchas veces.
2. Bronze es una **copia fiel** de la fuente: no se limpia nada.
3. Siempre se sabe **de dónde vino cada fila** (linaje).

```
Volume landing/extractos/*.xlsx
     │  PASO A · Auto Loader ──► recuerda qué archivos ya leyó (checkpoint)
     ▼
bronze.extracto_archivos   ← bytes originales de cada .xlsx
     │  PASO B · Parser ──► solo procesa lo que falta (anti-join)
     ▼
bronze.extracto_raw        ← cada celda como texto + número de fila
bronze.extracto_resumen    ← periodo y saldos de cada extracto
```

---

## Paso 0 · Dependencias

```python
%pip install openpyxl==3.1.5 -q
```

- `%pip` instala la librería en la sesión del notebook. `openpyxl` lee archivos .xlsx.
- `==3.1.5` **fija la versión**, la misma de `uv.lock`: reproducibilidad.
- `-q` reduce el texto de salida.

> **Lección del error `Failed to reconnect streaming listener`:** se quitó `dbutils.library.restartPython()`.
> Reiniciar Python crea un cliente de Spark nuevo, y el *listener* del stream anterior quedó huérfano.
> Como `openpyxl` se instala antes de importarse por primera vez, no hacía falta reiniciar.
> Ver [detalle del error](#lo-que-enseñaron-los-errores).

---

## Paso 1 · Parámetros y constantes

```python
dbutils.widgets.text("catalogo", "finanzas_dev", "Catálogo (entorno)")
catalogo = dbutils.widgets.get("catalogo")

RUTA_LANDING    = f"/Volumes/{catalogo}/bronze/landing/extractos/"
RUTA_CHECKPOINT = f"/Volumes/{catalogo}/bronze/checkpoints/extracto_archivos/"
T_ARCHIVOS = f"{catalogo}.bronze.extracto_archivos"
T_RAW      = f"{catalogo}.bronze.extracto_raw"
T_RESUMEN  = f"{catalogo}.bronze.extracto_resumen"
N_COLS = 8
```

- **Sin valores escritos a mano** (*no hardcoding*): todo sale de `catalogo`. Con `finanzas_prod`, el
  mismo notebook escribe en producción.
- **Nombres completos** (`catalogo.schema.tabla`): no se depende de un `USE CATALOG` que se pierde si la
  sesión se reinicia.
- **Constantes en MAYÚSCULAS:** convención de Python para separar configuración de lógica.
- `N_COLS = 8`: el extracto usa las columnas A a H. Un número con nombre es legible y fácil de cambiar.

---

## Paso 2 · Tablas y checkpoint

```python
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalogo}.bronze.checkpoints "
          "COMMENT 'Estado de los streams (qué archivos ya procesó Auto Loader)'")
```

El **checkpoint** es la memoria de Auto Loader: dónde guarda qué archivos ya procesó. Vive en un Volume
propio porque es estado técnico, no datos de negocio. **Nunca se borra a mano**: si se borra, Auto Loader
olvida y vuelve a leer todo.

```python
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
```

| Decisión | Por qué |
|---|---|
| **Crear la tabla explícitamente, antes de escribir** | Si Spark crea la tabla al escribir, infiere el esquema y puede equivocarse. Definirlo hace que **el esquema sea parte del contrato**: si llega algo que no encaja, falla en vez de cambiar la tabla en silencio. |
| **`COMMENT` en tablas y columnas** | Documentación que vive junto a los datos, visible en el explorador de Unity Catalog |
| **Prefijo `_` en columnas de linaje** | Distingue los metadatos agregados por el pipeline de los datos de la fuente |
| **`contenido BINARY`** | Permite **reprocesar sin volver a la fuente**: si hay un bug en el parser, se corrige y se reparsea desde esta tabla |

```python
columnas_celdas = ",\n  ".join(f"col_{i} STRING" for i in range(1, N_COLS + 1))
# → "col_1 STRING,\n  col_2 STRING, ... col_8 STRING"
```

Las columnas se generan en lugar de escribirse a mano: si el banco agrega una columna, se cambia `N_COLS`.

En `extracto_raw`, **`_fila INT` es la columna más importante de bronze**: guarda el número de fila de
Excel y permite reconstruir el orden exacto del extracto. Sin ella, validar la cadena de saldos sería imposible.

---

## Paso 3 · Paso A con Auto Loader

```python
antes = spark.table(T_ARCHIVOS).count()

nuevos = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("pathGlobFilter", "*.xlsx")
    .load(RUTA_LANDING)
```

| Código | Qué hace |
|---|---|
| `readStream` | Lectura como **stream**: permite recordar qué ya se procesó |
| `format("cloudFiles")` | Activa **Auto Loader**: detecta archivos nuevos en un almacenamiento |
| `cloudFiles.format = binaryFile` | No interpreta el archivo: entrega bytes (`content`) + metadatos (`path`, `length`, `modificationTime`) |
| `pathGlobFilter = *.xlsx` | Solo toma .xlsx; un .pdf subido por error se ignora |

```python
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
```

- `F.split("path", "/")` + `element_at(..., -1)` toma el último pedazo de la ruta, que es el nombre del archivo.
- **`F.sha2("content", 256)`** es la huella digital del contenido. Mismos bytes = mismo hash, aunque cambie
  el nombre; un byte distinto = hash completamente distinto. Es la **identidad del archivo**.
- `F.current_timestamp()` se guarda en **UTC** (por eso aparece `04:30` cuando en Bogotá eran las 11:30 PM).
  Estándar de la industria: guardar en UTC y convertir a hora local al mostrar.
- Todo son funciones de Spark (`F.`), así que se ejecuta en el servidor, no en el notebook.

```python
query = (
    nuevos.writeStream
    .option("checkpointLocation", RUTA_CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(T_ARCHIVOS)
)
query.awaitTermination()

n_nuevos = spark.table(T_ARCHIVOS).count() - antes
print(f"Archivos nuevos detectados por Auto Loader: {n_nuevos}")
```

| Código | Qué hace |
|---|---|
| `checkpointLocation` | Dónde guardar la memoria del stream. **Garantiza que cada archivo entre una sola vez.** |
| `trigger(availableNow=True)` | Procesa todo lo nuevo **ahora** y se detiene: batch con contabilidad de stream |
| `toTable(T_ARCHIVOS)` | Escribe en la tabla Delta en modo *append* |
| `awaitTermination()` | Espera a que el stream termine antes de seguir |

- **Exactly-once:** Auto Loader marca un archivo como procesado en el checkpoint **después** de que Delta
  confirma la escritura. Si algo falla en medio, el reintento retoma desde el último punto confirmado.
- **¿Por qué `availableNow` y no un batch normal?** En el Proyecto 2 (tiempo real) basta con cambiar el
  trigger (ej. `processingTime="1 minute"`) y el mismo código queda escuchando.
- **Bug corregido:** la versión original sumaba `numInputRows` de `query.recentProgress`, que en Serverless
  traía `None`. Lección: **medir el resultado en el destino, no las métricas del proceso**.

---

## Paso 4 · Paso B con el parser

### Leer el Excel

```python
def parsear_extracto(contenido: bytes) -> list[tuple]:
    wb = openpyxl.load_workbook(io.BytesIO(contenido), read_only=True, data_only=True)
    ws = wb.worksheets[0]
    filas = []
    for n, row in enumerate(ws.iter_rows(max_col=N_COLS, values_only=True), start=1):
        valores = [None if v is None else str(v) for v in row]
        valores += [None] * (N_COLS - len(valores))
        filas.append((n, *valores[:N_COLS]))
    wb.close()
    return filas
```

| Código | Qué hace |
|---|---|
| `io.BytesIO(contenido)` | Hace que los bytes en memoria "parezcan" un archivo; no se escribe nada a disco |
| `read_only=True` | Lectura en streaming: menos memoria |
| `data_only=True` | Si hay fórmulas, trae el **valor calculado** |
| `wb.worksheets[0]` | La primera hoja, venga con el nombre que venga |
| `enumerate(..., start=1)` | Numera desde 1, igual que Excel: ese es `_fila` |
| `str(v)` | **Todo a texto.** Tipar es tarea de silver |
| `valores += [None] * (...)` | *Padding*: en `read_only` algunas filas vienen más cortas; se completan a 8 columnas |
| `(n, *valores[:N_COLS])` | Tupla `(fila, col_1, ..., col_8)`; `*` desempaca la lista |

### Extraer el resumen y fallar rápido

```python
def extraer_resumen(filas: list[tuple], archivo: str) -> tuple:
    def fila_despues_de(etiqueta: str) -> tuple:
        for k, f in enumerate(filas[:-1]):
            if f[1] == etiqueta:
                return filas[k + 1][1:]
        raise ValueError(f"{archivo}: no encontré el encabezado '{etiqueta}'. ¿Cumple el contrato de la fuente?")

    general = fila_despues_de("DESDE")[: len(CAMPOS_GENERAL)]
    resumen = fila_despues_de("SALDO ANTERIOR")[: len(CAMPOS_RESUMEN)]
    return (*general, *resumen)
```

- Busca la fila del encabezado (`DESDE`, `SALDO ANTERIOR`) y toma **la siguiente**, que tiene los valores.
  Funciona aunque el bloque cambie de posición.
- `f[1]` es `col_1` (`f[0]` es el número de fila). `filas[:-1]` evita revisar la última fila, que no tiene siguiente.
- **Fail fast:** si un archivo no tiene `DESDE`, se detiene con un error claro en lugar de guardar un resumen
  vacío. Un pipeline detenido es mejor que uno que carga basura en silencio, porque la basura se propaga a
  silver y gold y se descubre semanas después.

### Esquemas explícitos

```python
SCHEMA_RAW = StructType(
    [StructField("_archivo", StringType()), StructField("_archivo_sha256", StringType()),
     StructField("_fila", IntegerType())]
    + [StructField(f"col_{i}", StringType()) for i in range(1, N_COLS + 1)]
)
```

Al convertir listas de Python en DataFrame, Spark tendría que adivinar los tipos; si una columna es toda
`None`, no puede y falla. El esquema explícito elimina la ambigüedad.

### Idempotencia con anti-join

```python
def pendientes(destino: str):
    archivos = spark.table(T_ARCHIVOS).dropDuplicates(["_archivo_sha256"])
    ya = spark.table(destino).select("_archivo_sha256").distinct()
    return archivos.join(ya, "_archivo_sha256", "left_anti").select("_archivo", "_archivo_sha256", "contenido")
```

Un **left anti join** devuelve las filas de la izquierda **sin pareja** en la derecha: los archivos que
**aún no** están en la tabla destino.

```
archivos (izquierda)        ya en destino (derecha)      left_anti =
SEP2026 (sha 2cb0...)       SEP2026 (sha 2cb0...)        DIC2026 (sha ...)
DIC2026 (sha ...)
```

- `dropDuplicates(["_archivo_sha256"])`: si el mismo extracto se sube dos veces con nombres distintos, se
  procesa una sola vez.
- Se compara por **sha256**, no por nombre.
- **¿Por qué `pendientes(T_RAW)` y `pendientes(T_RESUMEN)` por separado?** Cada escritura es una transacción
  independiente. Si el notebook fallara entre escribir `raw` y `resumen`, al reintentar `raw` no tendría
  pendientes y `resumen` sí. Cada tabla se autorrepara: **idempotencia por destino**.

### Escribir

```python
filas_raw = []
for a in pendientes(T_RAW).collect():
    filas = parsear_extracto(bytes(a.contenido))
    filas_raw += [(a._archivo, a._archivo_sha256, *f) for f in filas]
    print(f"raw     <- {a._archivo}: {len(filas)} filas")
if filas_raw:
    (spark.createDataFrame(filas_raw, SCHEMA_RAW)
     .withColumn("_ingestado_en", F.current_timestamp())
     .write.mode("append").saveAsTable(T_RAW))
```

- `.collect()` trae los archivos pendientes al notebook (el cliente) para parsearlos con openpyxl.
- `bytes(a.contenido)`: Spark entrega el binario como `bytearray`.
- A cada fila se le agrega el linaje (`_archivo`, `_archivo_sha256`).
- `if filas_raw:` evita escribir un DataFrame vacío, que crearía una versión de Delta inútil. Por eso al
  re-ejecutar sin archivos nuevos **no aparece una versión nueva** en el historial.
- `mode("append")`: agrega sin tocar lo existente.

> **Limitación de escala:** `.collect()` lleva los datos a un solo proceso. Con archivos de 10 KB es
> perfecto; con miles de archivos grandes habría que parsear en paralelo dentro de Spark (pandas UDF o
> `foreachBatch`). Saber cuándo un diseño deja de escalar es parte del oficio.

---

## Paso 5 · Verificación

```sql
SELECT a._archivo,
       a._ingestado_en,
       (SELECT count(*) FROM extracto_raw r WHERE r._archivo_sha256 = a._archivo_sha256) AS filas_raw,
       s.desde, s.hasta, s.saldo_anterior, s.saldo_actual
FROM extracto_archivos a
LEFT JOIN extracto_resumen s USING (_archivo_sha256)
ORDER BY s.desde
```

Une las tres tablas por la huella del archivo. Es un **chequeo de completitud**: un archivo con
`filas_raw = 0` o resumen `NULL` indicaría una falla en el Paso B.

| _archivo | filas_raw | desde | hasta | saldo_anterior | saldo_actual |
|---|---|---|---|---|---|
| EXTRACTO_0882_SEP2026.xlsx | 199 | 2026/06/30 | 2026/09/30 | 7,142,081.20 | **5,157,443.80** |
| EXTRACTO_0882_DIC2026.xlsx | 194 | 2026/10/01 | 2026/12/31 | **5,157,443.80** | 6,361,296.91 |

**Continuidad:** el saldo anterior de DIC es igual al saldo actual de SEP. No falta ningún periodo.

---

## Paso 6 · Delta Lake: historial y time travel

```python
display(spark.sql(f"DESCRIBE HISTORY {T_RAW}"))
```

| Versión | Operación | Filas |
|---|---|---|
| 0 | CREATE TABLE | – |
| 1 | WRITE (SEP) | 199 |
| 2 | WRITE (DIC) | 194 |

```sql
SELECT 'versión 1', count(*) FROM extracto_raw VERSION AS OF 1   -- 199
UNION ALL
SELECT 'versión 2', count(*) FROM extracto_raw VERSION AS OF 2   -- 393
```

**Cómo funciona:** Delta no sobrescribe archivos. Cada escritura agrega archivos Parquet nuevos y una
entrada en el **transaction log** (`_delta_log`) que dice qué archivos componen cada versión. Consultar
una versión vieja es leer la lista de archivos de esa versión.

**Usos:** auditoría ("¿qué datos tenía el reporte del lunes?"), recuperación
(`RESTORE TABLE ... TO VERSION AS OF n`) y depuración (comparar antes y después de una carga).

---

## Paso 7 · Lector nativo de Excel

```python
spark.read.format("excel").load(RUTA_LANDING)
```

Funcionó bien (columnas `_c0` a `_c7`, respetó las filas), pero **no trae el número de fila de Excel** y
no garantiza el orden cuando Spark procesa en paralelo. Para un reporte donde el orden importa (la cadena
de saldos), conservar `_fila` explícitamente es lo seguro. Para archivos tabulares normales, el lector
nativo es la mejor opción.

**Lección:** no hay herramienta "correcta" en abstracto; se elige según la forma de los datos y lo que
hay que garantizar.

---

## Lo que demostraron los experimentos

| Experimento | Resultado | Propiedad |
|---|---|---|
| Correr dos veces sin archivos nuevos | 0 nuevos, "Nada pendiente", sin versión nueva | **Idempotencia** |
| Subir DIC y correr | Solo entró DIC (194 filas); SEP no se tocó | **Carga incremental** |
| Comparar saldos SEP → DIC | `5,157,443.80` en ambos | **Continuidad** |
| Time travel v1 vs v2 | 199 → 393 | **Versionado y auditoría** |

## Lo que enseñaron los errores

| Error | Causa | Lección |
|---|---|---|
| `TypeError: int + NoneType` | Métricas del stream incompletas en Serverless | Medir el resultado en el destino, no las métricas del proceso |
| `Failed to reconnect streaming listener` | `restartPython()` después de un stream | El estado de la sesión es frágil; no agregar pasos "por si acaso" |

**Detalle del segundo error.** En Serverless, el notebook (cliente) y Spark (servidor) son procesos
separados que se comunican por red (*Spark Connect*). Al arrancar un stream, Databricks registra un
*listener* atado al cliente de Spark de ese momento. `restartPython()` mata ese cliente y crea uno nuevo,
pero el listener viejo siguió vivo y no pudo reconectarse:

```
1ª corrida:  %pip → restartPython() → cliente A → stream → listener atado a A ✓
2ª corrida:  %pip → restartPython() → muere A, nace B; listener sigue atado a A ✗
             → stream en B → "Listener thread still active, was not able to reconnect" 💥
```

Se resolvió con *Reset session* y quitando el reinicio innecesario. En un Job no ocurre, porque cada
ejecución arranca con una sesión limpia.

En ningún caso se dañaron los datos: el checkpoint y las escrituras atómicas de Delta dan
**tolerancia a fallos** por diseño. El error fue en la capa de monitoreo, no en la de datos.

---

## Glosario

| Concepto | En una frase |
|---|---|
| **Auto Loader** | Detecta archivos nuevos en un almacenamiento y los procesa una sola vez |
| **Checkpoint** | La memoria del stream: qué ya se procesó |
| **availableNow** | Procesa todo lo pendiente y se detiene: batch con contabilidad de stream |
| **Exactly-once** | Cada dato se procesa exactamente una vez, aun con fallos |
| **SHA-256** | Huella digital del contenido; identidad que no depende del nombre |
| **Left anti join** | "Lo que está aquí y no está allá": base de la idempotencia |
| **Linaje** | Saber de qué archivo y fila viene cada dato |
| **Fail fast** | Detenerse con un error claro antes que cargar datos inválidos |
| **Transaction log** | Registro de Delta que hace posibles las versiones y el time travel |
| **Spark Connect** | Arquitectura cliente–servidor de Serverless: el notebook envía instrucciones, Spark las ejecuta |

---

**Siguiente:** Día 2 · silver: de texto a movimientos limpios (fechas con año, valores numéricos, ID único,
categorías y `MERGE` sin duplicados).