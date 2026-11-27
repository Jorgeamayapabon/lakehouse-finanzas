# Día 0 · Setup del proyecto

> Antes de escribir una línea del pipeline, un equipo de datos prepara **los datos de prueba**,
> **el repositorio**, **la documentación de la fuente** y **la infraestructura** (*bootstrapping*).
> Es la parte poco vistosa, pero si se hace mal, todo lo demás se complica.

```
TU PC                                   GITHUB                    DATABRICKS
─────────────────────────               ──────────────            ──────────────────────────
1. Anonimizar extracto real  ┐          3. Repo con código  ───▶  4. Git folder (sincronizado)
2. Generar extractos falsos  ┘                                    5. Catálogo + schemas + Volume
          │                                                       6. Subir el archivo al Volume
          └──────────────── (archivos .xlsx, nunca a Git) ──────────────────▲
```

---

## 1. Anonimización · `tools/anonimizar_extracto.py`

**Qué hace:** reemplaza los datos personales del extracto real sin tocar ningún número.

| Original | Anonimizado |
|---|---|
| Nombre del cliente | `TITULAR DEMO` |
| Dirección | `NO DISPONIBLE` |
| Número de cuenta | `XXXXXXX0882` |
| `TRANSF DE <otra persona>` | `TRANSF DE PERSONA_3D8A2A` |
| `TRANSF DE <el titular>` | `TRANSF DE TITULAR` |
| Dígitos de tarjeta débito | `XXXX` |
| Nombre del archivo (traía la cuenta) | `EXTRACTO_0882_SEP2026.xlsx` |

```bash
uv run tools/anonimizar_extracto.py <extracto.xlsx> --sal "<tu-sal>" --salida data/landing
```

**Conceptos**

- **PII (Personally Identifiable Information):** dato que identifica a una persona. Regla de la
  industria: protegerla **lo más cerca posible de la fuente**. Por eso el script corre en tu PC: los
  datos personales nunca llegan a la nube. En Colombia lo exige además la Ley 1581 de protección de datos.
- **Seudonimización:** en lugar de borrar los nombres, se reemplazan por un código:
  `SHA-256(sal + nombre)`.
  - **Determinístico:** la misma persona siempre da el mismo código, así que puedes analizar
    "a PERSONA_3D8A2A le transfiero cada mes" sin saber quién es.
  - **Irreversible sin la sal:** sin ella, alguien podría calcular el hash de nombres comunes hasta
    encontrar coincidencias. La sal se guarda como una contraseña, fuera del repo.
- **Preservar la utilidad analítica:** marcar como `TITULAR` las transferencias desde tus otras cuentas
  importa porque **no son ingresos reales**. Si se contaran como ingreso, inflarían los ingresos.

---

## 2. Datos sintéticos · `tools/generar_extractos.py`

**Qué hace:** crea extractos falsos que imitan **exactamente** el formato desordenado del banco
(encabezados repetidos por página, números como texto, fechas sin año, fila `FIN ESTADO DE CUENTA`)
y también los patrones de gasto: pago de la tarjeta Visa el día 1, Compensar el día 2, manejo de la
tarjeta débito el día 9, salario el último día hábil, Nequi e intereses casi diarios.

```bash
# Continúa desde el saldo final del extracto real
uv run tools/generar_extractos.py --continuar-de data/landing/EXTRACTO_0882_SEP2026.xlsx --trimestres 2 --salida data/landing
# Archivo con errores a propósito
uv run tools/generar_extractos.py --desde 2027-04-01 --saldo-inicial 3000000 --errores --salida data/pruebas --semilla 7
```

| Archivo | Periodo | Uso |
|---|---|---|
| `EXTRACTO_0882_SEP2026.xlsx` | jul–sep 2026 (real, anonimizado) | Primera carga |
| `EXTRACTO_0882_DIC2026.xlsx` | oct–dic 2026 (sintético) | Carga incremental |
| `EXTRACTO_0882_MAR2027.xlsx` | ene–mar 2027 (sintético) | Carga incremental + cambio de año |
| `EXTRACTO_0882_JUN2027_ERRORES.xlsx` | abr–jun 2027 | Probar reglas de calidad |

**Conceptos**

- **¿Por qué sintéticos?** Con un solo archivo no se puede probar lo más importante de un pipeline
  batch: **la carga incremental**. ¿Qué pasa cuando llega el segundo archivo? ¿Y si llega dos veces?
- **Fidelidad con la fuente:** un generador que produzca datos limpios no sirve si la fuente real es
  desordenada. El pipeline debe enfrentar los mismos problemas que la realidad.
- **Reproducibilidad (`--semilla`):** con la misma semilla siempre salen los mismos archivos, así que
  una prueba que falla se puede repetir exactamente.
- **Consistencia interna:** en todos los archivos, saldo anterior + Σ movimientos = saldo actual. Si el
  generador produjera datos incoherentes, no sabrías si el error está en el pipeline o en los datos de prueba.
- **Pruebas negativas:** el archivo con errores (valor `N/A`, fecha `31/02`, fila duplicada, saldo que
  no cuadra) sirve para comprobar que las validaciones **detectan** problemas. Un validador que nunca
  falla podría estar roto sin que lo notes.

---

## 3. Repositorio

```
tools/       → corre en TU PC (toca datos sensibles)
notebooks/   → corre en Databricks
docs/        → documentación
data/        → datos locales (ignorados por Git)
```

| Decisión | Por qué |
|---|---|
| **uv + `pyproject.toml` + `uv.lock`** | `pyproject.toml` declara qué necesitas; `uv.lock` fija la versión **exacta** de cada paquete. `uv sync` da un entorno idéntico en cualquier máquina. Evita el "en mi máquina funciona". |
| **`ruff` como dependencia de desarrollo** | Ayuda a escribir el código, pero no se necesita para ejecutarlo |
| **`.gitignore` con `data/`, `*.xlsx`, `*.sal`** | El repo es **público**. Un `git add .` descuidado publicaría los extractos, y aunque se borren después quedan en el historial. Regla: **los datos nunca van a Git**, ni anonimizados. |
| **Conventional commits** (`feat:`, `fix:`, `docs:`, `chore:`) | Historial legible y automatizable (changelogs, versiones) |

---

## 4. Contrato de datos · `docs/contrato_fuente_extracto.md`

Documenta qué "promete" la fuente: estructura, campos (formato de origen y tipo destino), llave natural
y reglas de calidad.

- **Se escribe antes del pipeline.** Muchos pipelines fallan porque se construyeron asumiendo cosas de
  la fuente que nadie verificó.
- **Llave natural:** el banco no da ID por transacción, así que se define
  `sha2(cuenta | fecha | descripción | valor | saldo)`. Incluir el **saldo** es clave: dos transferencias
  a Nequi de 10.000 el mismo día tienen igual fecha, descripción y valor, pero distinto saldo.
- **Reglas con severidad:** una fila mala va a **cuarentena** (se aparta y el resto sigue); un archivo
  que no reconcilia se **bloquea** completo, porque si los totales no cuadran no se puede confiar en sus filas.
- **Riesgos documentados:** la fecha no trae año, y un extracto de noviembre a enero cruza de año.

---

## 5. Infraestructura en Databricks · `notebooks/00_setup.py`

```
finanzas_dev                ← catálogo (entorno de desarrollo)
├── bronze                  ← datos crudos
│   ├── landing (Volume)    ← zona de aterrizaje de archivos
│   └── checkpoints (Volume)← estado de Auto Loader (Día 1)
├── silver                  ← datos limpios
└── gold                    ← datos de negocio
```

**Conceptos**

- **Unity Catalog, namespace de 3 niveles:** `catalogo.schema.objeto` (ej. `finanzas_dev.silver.movimientos`).
  Sin ambigüedad sobre de dónde viene un dato. Además maneja permisos y linaje.
- **Un catálogo por entorno:** `finanzas_dev` para experimentar y `finanzas_prod` para producción.
  Mismo código, distinto catálogo: un error de pruebas nunca daña producción.
- **Arquitectura medallion:**
  - **Bronze:** copia fiel de la fuente. Si hay un error en la limpieza, se reprocesa desde aquí sin
    volver a pedir los archivos al banco.
  - **Silver:** datos limpios, tipados y sin duplicados: la versión confiable.
  - **Gold:** tablas para responder preguntas de negocio; las consumen dashboards y el agente (Proyecto 3).
- **Volume vs. tabla:** una tabla guarda datos estructurados (Delta); un Volume guarda **archivos** de
  cualquier tipo. Ambos con permisos de Unity Catalog. `landing` es inmutable: los archivos llegan y no se modifican.
- **Idempotencia (`IF NOT EXISTS`):** correr el notebook 1 o 10 veces da el mismo resultado. Los
  pipelines fallan y se reintentan; un reintento nunca debe duplicar ni romper nada.
- **Widgets (parámetros):** el catálogo se lee de `dbutils.widgets.get("catalogo")`, no se escribe en el
  código. En el Día 4, un Job le pasará `finanzas_dev` o `finanzas_prod`.

---

## 6. Lecciones que salieron sobre la marcha

| Problema | Causa | Lección |
|---|---|---|
| `NameError: ruta_landing` y `SHOW SCHEMAS` sin `bronze/silver/gold` | Serverless liberó la sesión inactiva y se perdieron las variables y el `USE CATALOG` | **No depender del estado de la sesión:** cada celda (y cada tarea de un Job) lee sus parámetros y usa nombres completos |
| `403 GITHUB_APP_CREDENTIALS_NO_ACCESS` | La app de Databricks estaba *autorizada* pero no *instalada* en el repo | Autorizar ≠ instalar. "Only select repositories" = **mínimo privilegio** |

Flujo de **Git folders**: se edita en Databricks, se hace commit y push, y el código queda versionado en
GitHub. Si Databricks borrara la cuenta por inactividad, no se perdería nada.

---

## Resultado

Datos realistas sin riesgo de privacidad, repo reproducible y seguro, fuente documentada e
infraestructura con entornos y capas separadas. Todavía no se procesó ningún dato: eso empieza en el
[Día 1](dia1_bronze.md).