"""
Anonimiza un extracto bancario (.xlsx) ANTES de que salga de tu computador.

Qué hace (sin cambiar ningún valor numérico, así la reconciliación sigue cuadrando):
  - Nombre del cliente  -> "TITULAR DEMO"
  - Dirección           -> "NO DISPONIBLE"
  - Número de cuenta    -> "XXXXXXX" + últimos 4 dígitos
  - "TRANSF DE/A <nombre>":
        si el nombre coincide con el titular -> "TITULAR"   (transferencias entre cuentas propias)
        si no                               -> "PERSONA_<hash>" (seudónimo estable con sal)
  - Últimos dígitos de tarjeta débito en "C MANEJO TARJ DEB ####" -> "XXXX"

Concepto: esto es SEUDONIMIZACIÓN. Con la misma --sal, el mismo nombre siempre da el mismo
seudónimo, así que puedes seguir analizando "a quién le transfiero más" sin saber quién es.
Guarda la sal fuera del repo (es como una contraseña).

Uso:
  python anonimizar_extracto.py entrada.xlsx --sal "mi-sal-secreta" [--salida carpeta/]
"""
import argparse
import hashlib
import re
import secrets
import sys
from pathlib import Path

import openpyxl

RE_FECHA = re.compile(r"^\d{1,2}/\d{2}$")
RE_TRANSF = re.compile(r"^(TRANSF (?:DE|A))\s+(.+)$", re.IGNORECASE)
RE_TARJETA = re.compile(r"(C MANEJO TARJ DEB )\d{4}")
MESES = ["ENE", "FEB", "MAR", "ABR", "MAY", "JUN", "JUL", "AGO", "SEP", "OCT", "NOV", "DIC"]


def seudonimo(nombre: str, sal: str) -> str:
    h = hashlib.sha256(f"{sal}|{nombre.strip().upper()}".encode()).hexdigest()[:6].upper()
    return f"PERSONA_{h}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("entrada")
    ap.add_argument("--sal", help="Sal secreta para los seudónimos (reutilízala en todos tus extractos)")
    ap.add_argument("--salida", default=".", help="Carpeta de salida")
    args = ap.parse_args()

    sal = args.sal
    if not sal:
        sal = secrets.token_hex(16)
        print(f"[aviso] No pasaste --sal. Generé esta; guárdala y reutilízala:\n  {sal}", file=sys.stderr)

    wb = openpyxl.load_workbook(args.entrada)
    ws = wb.active
    rows = list(ws.iter_rows())

    # 1) Encontrar datos del titular y la cuenta (aparecen en cada "página")
    titular_tokens: set[str] = set()
    cuenta = None
    hasta = None
    for i, row in enumerate(rows):
        v0 = row[0].value
        if v0 == "CLIENTE":
            fila = rows[i + 1]
            titular_tokens |= {t for t in str(fila[0].value).upper().split() if len(t) >= 4}
        if v0 == "DESDE":
            fila = rows[i + 1]
            hasta = str(fila[1].value)
            cuenta = str(fila[3].value)
    if not cuenta:
        sys.exit("No encontré el bloque 'Información General' (DESDE/HASTA/NRO CUENTA).")
    cuenta_mask = "XXXXXXX" + cuenta[-4:]

    # 2) Reemplazos
    cambios = 0
    for i, row in enumerate(rows):
        v0 = row[0].value
        if v0 == "CLIENTE":
            fila = rows[i + 1]
            fila[0].value, fila[1].value = "TITULAR DEMO", "NO DISPONIBLE"
            cambios += 2
        elif v0 == "DESDE":
            rows[i + 1][3].value = cuenta_mask
            cambios += 1
        elif v0 and RE_FECHA.match(str(v0)):
            desc = str(row[1].value or "")
            m = RE_TRANSF.match(desc)
            if m:
                prefijo, nombre = m.group(1).upper(), m.group(2)
                es_titular = bool(titular_tokens & set(nombre.upper().split()))
                row[1].value = f"{prefijo} {'TITULAR' if es_titular else seudonimo(nombre, sal)}"
                cambios += 1
            elif RE_TARJETA.search(desc):
                row[1].value = RE_TARJETA.sub(r"\1XXXX", desc)
                cambios += 1

    # 3) El nombre del archivo original también trae el número de cuenta
    yyyy, mm = hasta.split("/")[0], int(hasta.split("/")[1])
    out = Path(args.salida) / f"EXTRACTO_{cuenta[-4:]}_{MESES[mm - 1]}{yyyy}.xlsx"
    out.parent.mkdir(parents=True, exist_ok=True)
    wb.save(out)
    print(f"OK -> {out}  ({cambios} celdas anonimizadas)")


if __name__ == "__main__":
    main()
