# Lakehouse de finanzas personales (PoC)

Pipeline batch que convierte extractos bancarios en Excel —tal como los entrega el banco— en tablas
limpias y un dashboard de flujo de caja, con arquitectura **medallion** sobre **Databricks + Delta Lake + Unity Catalog**.

```
 tu PC                              Databricks (Unity Catalog: finanzas_dev)
┌──────────────────────┐   upload  ┌─────────────────────────────────────────────────────────┐
│ extracto real .xlsx  │──────────▶│ Volume bronze.landing/extractos/*.xlsx                  │
│  └▶ anonimizar  ─────┤           │   ▼                                                     │
│ generar_extractos ───┘           │ BRONZE  extracto_raw, extracto_resumen  (texto + linaje)│
└──────────────────────┘           │   ▼                                                     │
                                   │ SILVER  movimientos (MERGE idempotente), cuarentena     │
                                   │   ▼                                                     │
                                   │ GOLD    flujo_mensual, gasto_categoria_mes, saldo_diario│
                                   │   ▼                                                     │
                                   │ Job (bronze→silver→gold) + Dashboard AI/BI              │
                                   └─────────────────────────────────────────────────────────┘
```

## Estructura del repo
```
tools/                       # corre en TU PC (los datos con PII nunca salen sin anonimizar)
  anonimizar_extracto.py
  generar_extractos.py
notebooks/                   # corre en Databricks (sincronizado vía Git folder)
  00_setup.py
docs/
  contrato_fuente_extracto.md
data/                        # ignorado por git
  landing/                   # archivos listos para subir al Volume
  pruebas/                   # archivos con errores a propósito
```

## Día 0 · Setup

### 1. Preparar los datos (local)
```bash
pip install -r requirements.txt

# Anonimizar el extracto real. Guarda la sal fuera del repo y reutilízala siempre.
python tools/anonimizar_extracto.py ruta/19778070882_SEP2026.xlsx --sal "<tu-sal>" --salida data/landing

# Generar 2 trimestres sintéticos que continúan desde el saldo final del real
python tools/generar_extractos.py --continuar-de data/landing/EXTRACTO_0882_SEP2026.xlsx --trimestres 2 --salida data/landing

# Archivo con errores para probar calidad (Día 3)
python tools/generar_extractos.py --desde 2027-04-01 --saldo-inicial 3000000 --errores --salida data/pruebas --semilla 7
```

### 2. Databricks Free Edition
1. Crea la cuenta en Databricks Free Edition.
2. Sube este repo a GitHub y en Databricks: **Workspace → Create → Git folder** con la URL del repo.
   Así los notebooks se versionan en Git, no viven solo en el workspace.
3. Abre `notebooks/00_setup.py`, conéctalo a **Serverless** y ejecútalo.
4. Sube **solo** `EXTRACTO_0882_SEP2026.xlsx` al Volume (Catalog → `finanzas_dev` → `bronze` → `landing` → `extractos` → *Upload*).
   Los otros trimestres los subiremos después, para simular que van llegando con el tiempo y probar la carga incremental.

### Criterios de terminado del Día 0
- [ ] `SHOW SCHEMAS` en `finanzas_dev` muestra `bronze`, `silver`, `gold`
- [ ] El Volume `bronze.landing` tiene `extractos/EXTRACTO_0882_SEP2026.xlsx`
- [ ] El repo está en GitHub y conectado como Git folder, **sin** archivos de `data/`
