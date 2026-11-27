# Día 2 · Silver: movimientos limpios, únicos y categorizados

> Notebook: [`notebooks/02_silver.py`](../notebooks/02_silver.py)

**Objetivo:** convertir el "Excel en tabla" de bronze (todo texto, con encabezados repetidos) en
**una fila por movimiento real**, con tipos correctos, sin duplicados y con una categoría de negocio.

```
bronze.extracto_resumen ──► v_extractos ─────────────────────────────► MERGE ► silver.extractos
bronze.extracto_raw ──────► v_mov_tipados ─► v_mov_silver ───────────► MERGE ► silver.movimientos
                             (filtrar, tipar,   (deduplicar +
                              fecha con año,     categorizar con
                              id_movimiento)     silver.reglas_categoria)
```

| Capa | Pregunta que responde |
|---|---|
| Bronze | ¿Qué llegó exactamente? |
| **Silver** | **¿Qué pasó realmente?** (cada movimiento, una vez, con su significado) |
| Gold | ¿Qué significa para el negocio? (totales, tendencias) |

**Estilo:** las transformaciones están en **SQL** con vistas temporales (`CREATE OR REPLACE TEMP VIEW`).
Cada paso es pequeño, se puede inspeccionar por separado y cualquier analista lo puede leer.

**Estrategia de carga:** en cada corrida se recalcula silver desde *todo* bronze y se aplica con `MERGE`.
Con este volumen es barato y permite que un cambio de regla re-categorice el histórico completo.
El `MERGE` solo inserta lo nuevo y solo actualiza lo que cambió.

---

## Paso 1 · Parámetros

```python
T_RAW       = f"{catalogo}.bronze.extracto_raw"
T_RESUMEN   = f"{catalogo}.bronze.extracto_resumen"
T_REGLAS    = f"{catalogo}.silver.reglas_categoria"
T_EXTRACTOS = f"{catalogo}.silver.extractos"
T_MOV       = f"{catalogo}.silver.movimientos"
```

Igual que en el Día 1: nombres completos armados desde el parámetro `catalogo`, sin depender del estado
de la sesión.

---

## Paso 2 · Reglas de categorización (*seed*)

```python
REGLAS = [
    # prioridad, patron,                                signo, categoria,                         naturaleza
    (10,  r"(?i)^ABONO INTERESES",                        "+", "Intereses",                       "INGRESO"),
    (20,  r"(?i)^TRANSF (DE|A) TITULAR",                  "*", "Transferencia entre cuentas propias", "INTERNO"),
    (30,  r"(?i)^(PAGO DE PROV|PAGO INMEDIATO DE) ",      "+", "Salario",                         "INGRESO"),
    (40,  r"(?i)^TRANSF DE ",                             "+", "Transferencias recibidas",        "INGRESO"),
    ...
    (170, r"(?i)FIDEICOMISOS|PEXTO CAPITAL",              "-", "Inversiones",                     "INVERSION"),
]

(spark.createDataFrame(REGLAS, "prioridad INT, patron STRING, signo STRING, categoria STRING, naturaleza STRING")
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_REGLAS))
```

**¿Qué es un *seed*?** Datos de referencia pequeños que mantiene el equipo (no vienen de una fuente
externa). La **fuente de verdad es el código en Git**, y cada corrida los carga a una tabla. Así cada cambio
de regla queda versionado, revisado y con autor. El término viene de *dbt*, la herramienta de
transformación más usada del mercado.

| Campo | Significado |
|---|---|
| `prioridad` | Si varias reglas coinciden, gana la de **menor** número. Por eso `TRANSF DE TITULAR` (20) va antes que `TRANSF DE` (40): la más específica primero |
| `patron` | Expresión regular sobre la descripción. `(?i)` = sin distinguir mayúsculas (hay `Supra SAS` y `EMPRESA DE SERVICIO`). `^` = "empieza por" |
| `signo` | `+` solo abonos, `-` solo cargos, `*` ambos. `UBER` usa `*` porque hubo un reembolso positivo |
| `naturaleza` | Cómo cuenta en el flujo de caja (ver abajo) |

**La naturaleza es la decisión de negocio más importante del día:**

| Naturaleza | Ejemplo | ¿Por qué separarla? |
|---|---|---|
| `INGRESO` | Salario, intereses | Dinero que entra de verdad |
| `GASTO` | Tarjeta de crédito, servicios | Dinero que sale de verdad |
| `INTERNO` | `TRANSF DE TITULAR` | Moverte plata a ti mismo **no es ingreso ni gasto**. Sin esto, los ingresos se inflarían ~24 millones en la muestra |
| `INVERSION` | Fideicomisos | Sale de la cuenta, pero sigue siendo tu patrimonio |

**`overwrite` + `overwriteSchema`:** la tabla de reglas se reemplaza completa en cada corrida (es pequeña y
el código es la verdad). `overwriteSchema` permite agregar columnas a las reglas en el futuro.

---

## Paso 3 · Extractos tipados

```sql
CREATE OR REPLACE TEMP VIEW v_extractos AS
SELECT
  _archivo_sha256, _archivo,
  nro_cuenta                                                   AS cuenta,
  to_date(desde, 'yyyy/MM/dd')                                 AS fecha_desde,
  to_date(hasta, 'yyyy/MM/dd')                                 AS fecha_hasta,
  try_cast(replace(saldo_anterior, ',', '') AS DECIMAL(18,2))  AS saldo_anterior,
  ...
FROM bronze.extracto_resumen
```

| Técnica | Por qué |
|---|---|
| `to_date(texto, 'yyyy/MM/dd')` | Convierte `'2026/09/30'` en una fecha real, con la que se puede comparar, restar y agrupar por mes |
| `replace(x, ',', '')` | `'7,142,081.20'` → `'7142081.20'`: quita el separador de miles |
| **`try_cast` en vez de `CAST`** | Serverless usa **ANSI SQL**: un `CAST('N/A' AS DECIMAL)` **detiene todo el notebook**. `try_cast` devuelve `NULL` y deja decidir qué hacer con el valor malo |
| **`DECIMAL(18,2)` y no `DOUBLE`** | `DOUBLE` es binario y no representa exactamente 0.10. Sumando miles de movimientos aparecen errores de centavos y la reconciliación fallaría. `DECIMAL` es exacto: **dinero siempre en DECIMAL** |

`DECIMAL(18,2)` = hasta 18 dígitos en total, 2 de ellos decimales (hasta 9.999 billones con centavos).

---

## Paso 4 · Movimientos: filtrar, limpiar y tipar

Está escrita con **CTEs** (`WITH ... AS`): subconsultas con nombre que se leen de arriba abajo, como pasos.

### 4.1 Filtrar las filas de movimientos

```sql
WITH filas_mov AS (
  SELECT r._archivo, r._archivo_sha256, r._fila,
         trim(r.col_1)                               AS fecha_txt,
         regexp_replace(trim(r.col_2), ' +', ' ')    AS descripcion,
         nullif(trim(r.col_3), '')                   AS sucursal,
         nullif(trim(r.col_4), '')                   AS documento,
         r.col_5 AS valor_txt, r.col_6 AS saldo_txt,
         e.cuenta, e.fecha_desde, e.fecha_hasta
  FROM bronze.extracto_raw r
  JOIN v_extractos e USING (_archivo_sha256)
  WHERE trim(r.col_1) RLIKE '^[0-9]{1,2}/[0-9]{1,2}$'
),
```

- **Un solo filtro descarta toda la basura estructural:** si la columna A tiene forma de fecha `d/MM`, es un
  movimiento. Eso elimina de un golpe los bloques de cliente y resumen, los encabezados repetidos por página
  y `FIN ESTADO DE CUENTA`. Es el mismo regex que usaste en Colab el primer día, ahora en SQL.
- **`JOIN v_extractos`:** cada movimiento hereda la cuenta y el periodo de su extracto, que se necesitan para
  calcular el año.
- **`regexp_replace(..., ' +', ' ')`:** colapsa espacios repetidos (`RETIRO CAJERO  ATM` → `RETIRO CAJERO ATM`).
  Sin esto, la misma descripción escrita con 1 o 2 espacios daría `id_movimiento` distintos.
- **`nullif(x, '')`:** una celda vacía (`''`) se convierte en `NULL`. Así "no hay dato" se representa de una
  sola forma.

> **Detalle de Python:** en el notebook el regex aparece como `{{1,2}}`. Dentro de un f-string, `{` y `}`
> son para variables; `{{` produce un `{` literal. Spark recibe `{1,2}`.

### 4.2 Tipar y calcular el año

```sql
tipados AS (
  SELECT *,
    CAST(try_to_timestamp(
           concat(fecha_txt, '/',
                  CASE WHEN CAST(split(fecha_txt, '/')[1] AS INT) <= month(fecha_hasta)
                       THEN year(fecha_hasta) ELSE year(fecha_hasta) - 1 END),
           'd/M/yyyy') AS DATE)                              AS fecha,
    try_cast(replace(valor_txt, ',', '') AS DECIMAL(18,2))   AS valor,
    try_cast(replace(saldo_txt, ',', '') AS DECIMAL(18,2))   AS saldo
  FROM filas_mov
)
```

**El año que el extracto no trae.** Regla: si el mes del movimiento es ≤ al mes de `fecha_hasta`, el año es el
de `fecha_hasta`; si es mayor, es el año anterior.

| Extracto (hasta) | Movimiento | Mes ≤ mes hasta | Año |
|---|---|---|---|
| 2026/09/30 | `1/07` | 7 ≤ 9 ✔ | 2026 |
| 2027/01/31 | `15/11` | 11 ≤ 1 ✘ | 2026 |
| 2027/01/31 | `3/01` | 1 ≤ 1 ✔ | 2027 |

Así un extracto que cruza diciembre → enero asigna bien los dos años (el riesgo que se documentó en el
contrato del Día 0).

- `split('1/07', '/')[1]` → `'07'` (los arrays en SQL empiezan en 0).
- **`try_to_timestamp`** devuelve `NULL` para fechas imposibles como `31/02/2027`, en lugar de fallar.
- `'d/M/yyyy'`: `d` y `M` aceptan 1 o 2 dígitos (`1/07` y `15/11`).

### 4.3 Llave y validez

```sql
SELECT *,
  sha2(concat_ws('|', cuenta, CAST(fecha AS STRING), descripcion,
                 CAST(valor AS STRING), CAST(saldo AS STRING)), 256)  AS id_movimiento,
  (fecha IS NOT NULL AND valor IS NOT NULL AND saldo IS NOT NULL)     AS es_valido
FROM tipados
```

- **`id_movimiento`:** la llave natural definida en el contrato. `concat_ws('|', ...)` une los campos con un
  separador (`XXXXXXX0882|2026-07-01|ABONO INTERESES AHORROS|1.96|7142083.16`) y `sha2` lo convierte en un
  identificador fijo de 64 caracteres. El **saldo** hace que dos movimientos idénticos el mismo día tengan
  llaves distintas.
- **El separador importa:** sin `|`, `('AB', 'C')` y `('A', 'BC')` producirían el mismo texto `ABC`.
- **`es_valido`:** una bandera, no un filtro. Las filas inválidas **se marcan, no se borran**.

---

## Paso 5 · Filas inválidas

```sql
SELECT _archivo, _fila, fecha_txt, descripcion, valor_txt, saldo_txt
FROM v_mov_tipados WHERE NOT es_valido
```

No entran a silver, pero **no se pierden en silencio**: se muestran con su archivo y fila de origen (el linaje
de bronze permite ir directo a la celda del Excel). En el Día 3 irán a una tabla de cuarentena con el motivo.

Con el archivo de errores, la prueba local detectó exactamente lo inyectado:

| _fila | fecha_txt | valor_txt | Problema |
|---|---|---|---|
| 23 | `2/04` | `N/A` | Valor no numérico |
| 46 | `31/02` | `2.34` | Fecha imposible |

---

## Paso 6 · Deduplicar y categorizar

```sql
WITH unicos AS (
  SELECT * FROM (
    SELECT *, row_number() OVER (PARTITION BY id_movimiento ORDER BY fecha_hasta DESC, _fila) AS rn_dup
    FROM v_mov_tipados WHERE es_valido
  ) WHERE rn_dup = 1
),
```

**Funciones de ventana:** `row_number() OVER (PARTITION BY id_movimiento ORDER BY ...)` numera las filas
**dentro de cada grupo** con el mismo `id_movimiento`. Quedarse con `rn_dup = 1` deja una sola por grupo.

¿De dónde salen duplicados?
- Una fila repetida dentro del mismo archivo (el caso inyectado: 624 válidos → 623 únicos).
- **Dos extractos con periodos que se solapan** (el banco reenvía un extracto corregido). El mismo movimiento
  aparece en ambos, con el mismo `id_movimiento`. `ORDER BY fecha_hasta DESC` conserva el del extracto más reciente.

Además, **el `MERGE` lo exige**: si dos filas de origen coinciden con la misma fila destino, falla con
`multiple source rows matched`.

```sql
con_regla AS (
  SELECT u.*, g.categoria, g.naturaleza,
         row_number() OVER (PARTITION BY u.id_movimiento ORDER BY g.prioridad NULLS LAST) AS rn_regla
  FROM unicos u
  LEFT JOIN silver.reglas_categoria g
    ON u.descripcion RLIKE g.patron
   AND (g.signo = '*' OR (g.signo = '+' AND u.valor > 0) OR (g.signo = '-' AND u.valor < 0))
)
SELECT ...,
  CASE WHEN valor >= 0 THEN 'ABONO' ELSE 'CARGO' END                         AS tipo,
  coalesce(categoria, 'Sin categoría')                                       AS categoria,
  coalesce(naturaleza, CASE WHEN valor >= 0 THEN 'INGRESO' ELSE 'GASTO' END) AS naturaleza
FROM con_regla WHERE rn_regla = 1
```

- **Join con condición de regex:** no es un join por igualdad. Cada movimiento se cruza con **todas** las
  reglas cuyo patrón y signo coinciden, y la segunda ventana se queda con la de menor prioridad.
- **`LEFT JOIN`:** un movimiento sin regla no desaparece; llega con `categoria = NULL`.
- **`coalesce`:** devuelve el primer valor no nulo. Sin regla → `'Sin categoría'` y una naturaleza por defecto
  según el signo.
- **`NULLS LAST`:** si no hubo coincidencia, la fila con `NULL` queda de última.

---

## Paso 7 · Tablas silver

```sql
CREATE TABLE IF NOT EXISTS silver.movimientos (
  id_movimiento   STRING        NOT NULL COMMENT 'sha2(cuenta|fecha|descripcion|valor|saldo)',
  fecha           DATE          NOT NULL,
  valor           DECIMAL(18,2) NOT NULL COMMENT 'Positivo = abono, negativo = cargo',
  ...
  CONSTRAINT pk_movimientos PRIMARY KEY (id_movimiento)
)
CLUSTER BY (fecha)
COMMENT 'Silver: un registro por movimiento real, tipado, deduplicado y categorizado'
```

| Elemento | Qué hace |
|---|---|
| **`NOT NULL`** | Delta **rechaza** cualquier escritura que lo viole. Es una regla de calidad que vive en la tabla, no en el código: aunque alguien escriba desde otro notebook, no puede meter un movimiento sin fecha |
| **`PRIMARY KEY`** | En Unity Catalog es **informativa**: no se valida. Documenta la llave para personas, herramientas de BI y el agente del Proyecto 3. La unicidad la garantiza el `MERGE` |
| **`CLUSTER BY (fecha)`** | *Liquid clustering*: organiza los archivos por fecha para que una consulta como "movimientos de marzo" lea solo los archivos de marzo. Reemplaza al particionamiento clásico (`PARTITIONED BY`) y se puede cambiar sin reescribir la tabla |
| **`_creado_en` / `_actualizado_en`** | Cuándo entró la fila y cuándo cambió por última vez. Permiten auditar y hacer cargas incrementales hacia gold |

---

## Paso 8 · MERGE (*upsert*)

```sql
MERGE INTO silver.movimientos t
USING v_mov_silver s
ON t.id_movimiento = s.id_movimiento
WHEN MATCHED AND NOT (t.categoria <=> s.categoria AND t.naturaleza <=> s.naturaleza) THEN UPDATE SET
  t.categoria = s.categoria,
  t.naturaleza = s.naturaleza,
  t._actualizado_en = current_timestamp()
WHEN NOT MATCHED THEN INSERT (...) VALUES (..., current_timestamp(), current_timestamp())
```

`MERGE` compara origen (`s`) y destino (`t`) por la llave y decide **fila por fila**:

| Caso | Acción |
|---|---|
| La llave no existe en destino | `INSERT` |
| Existe y cambió la categoría o la naturaleza | `UPDATE` solo de esas columnas |
| Existe y no cambió nada | **Nada**: no se reescribe |

- **`AND NOT (...)` en `WHEN MATCHED`:** sin esa condición, el `MERGE` actualizaría las 301 filas en cada
  corrida aunque no cambiaran, y cada corrida crearía una versión de Delta con 301 filas reescritas.
- **`<=>`** es la igualdad **segura con nulos**: `NULL <=> NULL` es verdadero. Con `=`, `NULL = NULL` da `NULL`
  y la comparación fallaría en silencio.
- **Solo se actualiza lo que puede cambiar.** Fecha, valor, saldo y descripción forman parte de la llave: si
  cambiaran, sería otro `id_movimiento`, es decir, otro movimiento.
- **`INSERT` con columnas explícitas:** no depende del orden de las columnas; agregar una columna a la vista no
  rompe la carga.

**¿Por qué recalcular todo y no solo lo nuevo?**

| Enfoque | Ventaja | Costo |
|---|---|---|
| **Recalcular todo + `MERGE`** (el elegido) | Un cambio de regla corrige el histórico automáticamente; lógica simple | Lee todo bronze en cada corrida |
| Incremental (solo archivos nuevos) | Escala a millones de filas | Cambiar una regla exige un reproceso aparte |

Con cientos de filas, recalcular cuesta segundos. Con millones se pasaría a incremental (por ejemplo, con
*Change Data Feed* de bronze). **Elegir la estrategia según el volumen** es una decisión de diseño, no una regla fija.

**El resultado del `MERGE`** (`num_inserted_rows`, `num_updated_rows`...) es la evidencia de qué hizo cada corrida.

---

## Paso 9 · Verificación

### Reconciliación

```sql
SELECT e._archivo,
       count(m.id_movimiento)                         AS movimientos,
       e.saldo_anterior + coalesce(sum(m.valor), 0)   AS saldo_calculado,
       e.saldo_actual,
       e.saldo_anterior + coalesce(sum(m.valor), 0) = e.saldo_actual AS cuadra
FROM silver.extractos e
LEFT JOIN silver.movimientos m ON m._archivo_sha256 = e._archivo_sha256
GROUP BY ALL
```

| Extracto | Movimientos | Saldo calculado | Saldo actual | Cuadra |
|---|---|---|---|---|
| SEP2026 (real) | 153 | 5.157.443,80 | 5.157.443,80 | ✅ |
| DIC2026 | 148 | 6.361.296,91 | 6.361.296,91 | ✅ |
| MAR2027 | 177 | 10.933.789,23 | 10.933.789,23 | ✅ |

**Es la prueba de calidad más fuerte del pipeline**, porque compara contra un número que **el banco** calculó
de forma independiente. Si se hubiera perdido una fila, un signo se hubiera invertido, un número se hubiera
convertido mal o un duplicado se hubiera colado, no cuadraría al centavo. (Y solo cuadra al centavo porque
se usó `DECIMAL`.)

- `GROUP BY ALL`: agrupa por todas las columnas que no son agregaciones, sin tener que listarlas.
- `coalesce(sum(...), 0)`: un extracto sin movimientos suma `NULL`; así da 0.

### Resumen por categoría y "Sin categoría"

Sirven para revisar que la categorización tenga sentido y para encontrar descripciones que ninguna regla reconoce.

---

## Lo que demostraron los experimentos

| Experimento | Resultado del `MERGE` en movimientos | Propiedad |
|---|---|---|
| Primera carga (SEP + DIC) | 301 insertados (153 + 148) | Carga inicial |
| Correr de nuevo sin cambios | **0 / 0 / 0 / 0** | **Idempotencia**: no reescribe lo que no cambió |
| Regla de Nequi `GASTO` → `INTERNO` | **72 actualizados, 0 insertados** | **Re-categorización del histórico** sin reprocesar a mano |
| Subir MAR y correr 01 → 02 | 177 insertados | **Carga incremental** extremo a extremo |

En todos los casos la reconciliación siguió en ✅: cambiar una categoría no altera ningún valor.

---

## Hallazgo: las reglas de negocio también detectan errores en los datos

Al categorizar apareció `PAGO DE PROV PEXTO COLOMBIA` como **cargo sin categoría**. En el extracto real es un
**abono de ~5 millones** (el salario de julio y agosto). El generador sintético lo creaba con el signo al revés.

- Se corrigió el generador (y de paso la convención de la fecha `DESDE`: es el último día del periodo
  anterior, como en el banco).
- **No se regeneraron** los archivos ya cargados: esas filas quedan en "Sin categoría" como caso conocido.
- **Lección:** la lista "Sin categoría" no es solo trabajo pendiente de reglas; es un **detector de anomalías**.
  Un movimiento que no encaja en ningún patrón conocido merece una mirada.

## Cómo se probó antes de llevarlo a Databricks

Las transformaciones se probaron en un **Spark local** (con ANSI activado, como Serverless) con los 4 extractos,
reutilizando el parser del notebook 01 para construir bronze. Así se verificaron fechas, tipos, deduplicación,
categorías y reconciliación antes de tocar el workspace. Lo exclusivo de Delta (`MERGE`, `PRIMARY KEY`,
`CLUSTER BY`) se validó en Databricks.

**Lección:** probar la lógica fuera del entorno productivo es más rápido y barato. Lo que depende de la
plataforma se prueba en la plataforma.

## Decisiones de negocio pendientes

Los supuestos de categorización se confirman con quien conoce el negocio (aquí, el titular de la cuenta):

| Movimiento | Supuesto actual | Alternativa |
|---|---|---|
| `TRANSFERENCIAS A NEQUI` | `GASTO` | `INTERNO` si el Nequi es propio |
| `TRANSFERENCIA CTA SUC VIRTUAL` (771.750 dos veces al mes) | Transferencias programadas, `GASTO` | Arriendo, ahorro o crédito |
| `FIDEICOMISOS SOCIED` / `Pexto Capital` | `INVERSION` | Por confirmar |

Cambiar cualquiera es editar una línea de `REGLAS`; la siguiente corrida corrige todo el histórico.

---

## Glosario

| Concepto | En una frase |
|---|---|
| **Seed** | Datos de referencia mantenidos en el código y cargados a una tabla en cada corrida |
| **Naturaleza** | Cómo cuenta un movimiento en el flujo de caja: ingreso, gasto, interno o inversión |
| **ANSI SQL** | Modo estricto: una conversión inválida es un error, no un `NULL` silencioso |
| **`try_cast` / `try_to_timestamp`** | Versiones que devuelven `NULL` en vez de fallar |
| **`DECIMAL`** | Tipo numérico exacto; el único adecuado para dinero |
| **CTE (`WITH`)** | Subconsulta con nombre; permite escribir SQL como una secuencia de pasos |
| **Función de ventana** | Cálculo sobre un grupo de filas sin colapsarlas (`row_number() OVER (PARTITION BY ...)`) |
| **`MERGE` / upsert** | Inserta lo nuevo, actualiza lo que cambió y deja intacto lo demás, en una sola operación atómica |
| **`<=>`** | Igualdad segura con nulos |
| **`NOT NULL` en Delta** | Restricción que la tabla hace cumplir en cada escritura |
| **PK informativa** | Llave documentada en Unity Catalog, no validada por el motor |
| **Liquid clustering** | Organización física de los datos por columnas de consulta frecuente; reemplaza al particionamiento |
| **Reconciliación** | Comparar el resultado contra un total calculado de forma independiente por la fuente |

---

**Siguiente:** Día 3 · calidad de datos y gold: cuarentena con motivo, reglas del contrato (R1–R6), bloqueo de
archivos que no reconcilian y las primeras tablas de negocio (flujo mensual, gasto por categoría, saldo diario).
