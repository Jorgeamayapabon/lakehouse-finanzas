# Contrato de datos · Fuente: extracto de cuenta de ahorros (.xlsx)

> Un **contrato de datos** documenta qué promete la fuente (estructura, tipos, reglas) para que
> el pipeline sepa qué esperar y qué hacer cuando no se cumple. Aquí lo escribimos *antes* de
> construir bronze/silver, a partir del archivo real.

## Entrega
| Atributo | Valor |
|---|---|
| Formato | .xlsx, una hoja (`Table1`), **todas las celdas como texto** |
| Periodicidad | Trimestral (`DESDE`–`HASTA`), ej. 2026/06/30 – 2026/09/30 |
| Nombre de archivo | `EXTRACTO_<últimos4cuenta>_<MES><AAAA>.xlsx` (tras anonimizar) |
| Aterrizaje | `/Volumes/<catalogo>/bronze/landing/extractos/` |
| PII | Se anonimiza **antes** de subir (`tools/anonimizar_extracto.py`) |

## Estructura (no es una tabla plana)
1. Fila vacía
2. Bloque **Información Cliente**: CLIENTE, DIRECCIÓN, CIUDAD
3. Bloque **Información General**: DESDE, HASTA (`AAAA/MM/DD`), TIPO CUENTA, NRO CUENTA, SUCURSAL
4. Bloque **Resumen** (solo en la primera página): SALDO ANTERIOR, TOTAL ABONOS, TOTAL CARGOS,
   SALDO ACTUAL, SALDO PROMEDIO, CUPO SUGERIDO, INTERESES, RETEFUENTE
5. **Movimientos**: FECHA, DESCRIPCIÓN, SUCURSAL, DCTO., VALOR, SALDO
6. Los bloques 2, 3 y 5 (encabezado) **se repiten en cada página** en medio de los movimientos
7. Última fila: `FIN ESTADO DE CUENTA` en la columna B

## Campos de movimientos
| Campo | Formato origen | Tipo destino | Notas |
|---|---|---|---|
| FECHA | `d/MM` (ej. `1/07`) | DATE | **Sin año**: se deriva del periodo DESDE–HASTA. Cuidado con periodos que cruzan diciembre→enero |
| DESCRIPCIÓN | texto | STRING | Contraparte embebida (`TRANSF DE X`, `PAGO PSE X`) |
| SUCURSAL | texto, casi siempre vacío | STRING | |
| DCTO. | casi siempre vacío | STRING | |
| VALOR | `-4,444,055.00`, `.96` | DECIMAL(18,2) | Negativo = cargo, positivo = abono |
| SALDO | igual que VALOR | DECIMAL(18,2) | Saldo después del movimiento |

**Llave natural:** no existe ID de transacción. Se propone `sha2(cuenta|fecha|descripcion|valor|saldo)`;
el saldo acumulado hace que dos movimientos idénticos el mismo día tengan llaves distintas.

## Reglas de calidad (se implementan en el Día 3)
| # | Regla | Severidad |
|---|---|---|
| R1 | FECHA es una fecha válida dentro de [DESDE, HASTA] | Error → cuarentena |
| R2 | VALOR y SALDO son numéricos | Error → cuarentena |
| R3 | Cadena de saldos: `saldo[i-1] + valor[i] = saldo[i]` | Error → cuarentena |
| R4 | Reconciliación: SALDO ANTERIOR + Σ VALOR = SALDO ACTUAL | Error → bloquea el archivo |
| R5 | Σ positivos = TOTAL ABONOS y Σ negativos = TOTAL CARGOS | Error → bloquea el archivo |
| R6 | Sin `id_movimiento` duplicados | Error → se deduplica |

`data/pruebas/EXTRACTO_0882_JUN2027_ERRORES.xlsx` viola R1, R2, R3 y R6 a propósito.
