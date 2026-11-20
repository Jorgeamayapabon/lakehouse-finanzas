"""
Genera extractos bancarios SINTÉTICOS con el mismo formato "sucio" del banco:
  - bloques de encabezado (cliente, información general, resumen) arriba
  - el encabezado se repite en cada "página" en medio de los movimientos
  - todo como texto: '-4,444,055.00', '.96', fechas '1/07' sin año
  - fila final 'FIN ESTADO DE CUENTA'

Los saldos son consistentes (saldo anterior + Σ valores = saldo actual), igual que en el real.
Con --errores se inyectan problemas a propósito para probar las reglas de calidad (Día 3).

Concepto: datos sintéticos que imitan la fuente real -> puedes probar cargas incrementales,
reprocesos y validaciones sin exponer datos reales, y de forma reproducible (--semilla).

Uso:
  # Continúa justo donde termina un extracto existente (toma su saldo final y fecha HASTA):
  python generar_extractos.py --continuar-de EXTRACTO_0882_SEP2026.xlsx --trimestres 2
  # O desde cero:
  python generar_extractos.py --desde 2026-10-01 --trimestres 1 --saldo-inicial 5157443.80
"""
import argparse
import random
from datetime import date, timedelta
from pathlib import Path

import openpyxl

MESES = ["ENE", "FEB", "MAR", "ABR", "MAY", "JUN", "JUL", "AGO", "SEP", "OCT", "NOV", "DIC"]
FILAS_PRIMERA_PAGINA = 33
FILAS_POR_PAGINA = 56
ENCABEZADO_MOV = ["FECHA", "DESCRIPCIÓN", "SUCURSAL", "DCTO.", "VALOR", "SALDO"]

CLIENTE = ("TITULAR DEMO", "NO DISPONIBLE", "VALLEDUPAR  CESAR")
TIPO_CUENTA, SUCURSAL = "CUENTA DE AHORROS", "SUCURSAL GUATAPURI"


def fmt(v: float) -> str:
    """Formato del banco: '1,234.50', '.96', '-.50', '.00'."""
    s = f"{v:,.2f}"
    if s.startswith("0."):
        s = s[1:]
    elif s.startswith("-0."):
        s = "-" + s[2:]
    return s


def parse(s: str) -> float:
    return float(str(s).replace(",", ""))


def fin_de_mes(d: date) -> date:
    return (d.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)


def ultimo_habil(d: date) -> date:
    f = fin_de_mes(d)
    while f.weekday() >= 5:
        f -= timedelta(days=1)
    return f


class Generador:
    def __init__(self, rng: random.Random):
        self.r = rng
        self.personas = [f"PERSONA_{rng.getrandbits(24):06X}" for _ in range(6)]
        self.comercios_qr = ["PAGO QR LA FUENTE", "PAGO LLAVE LA FUENTE", "PAGO LLAVE DELICIAS C",
                             "PAGO QR DON ALBERTO", "PAGO QR LA TREINTA CR", "PAGO LLAVE QUEEN VPAR"]

    def eventos_del_dia(self, d: date) -> list[tuple]:
        """Devuelve [(descripcion, sucursal, valor)] planeados para el día d."""
        r, ev = self.r, []
        dia = d.day
        # --- Ingresos ---
        if d == ultimo_habil(d):
            ev.append(("PAGO INMEDIATO DE Supra SAS", None, round(r.uniform(5.20e6, 5.30e6))))
        if r.random() < 0.10:
            ev.append(("TRANSF DE TITULAR", None, r.choice([200, 300, 500, 800, 1000, 1500]) * 1000))
        if r.random() < 0.04:
            ev.append((f"TRANSF DE {r.choice(self.personas)}", None, r.randint(3, 20) * 10000))
        if r.random() < 0.01:
            ev.append(("CONSIGNACION CORRESPONSAL CB", "CANAL CORRESPONSA", r.randint(5, 30) * 10000))
        # --- Pagos fijos ---
        if dia == 1:
            ev += [("PAGO SUC VIRT TC VISA", None, -round(r.uniform(2.5e6, 4.6e6))),
                   ("PAGO SUC VIRT TC AMEX PESOS", None, -round(r.uniform(1.5e5, 4.0e5)))]
            if r.random() < 0.7:
                ev.append(("PAGO PSE FIDEICOMISOS SOCIED", None, -190550))
        if dia in (1, 15):
            ev.append(("TRANSFERENCIA CTA SUC VIRTUAL", None, -771750))
        if dia == 2:
            ev += [("PAGO PSE COMPENSAR OI", None, -508300),
                   ("PAGO PSE Empresa de Servicio", None, -r.randint(45000, 56000))]
        if dia == 6:
            ev.append(("PAGO PLAN PROTEGE TU VIDA", "GUATAPURI", -29695))
        if dia == 9:
            ev.append((f"C MANEJO TARJ DEB XXXX {d:%m} {d:%y}", None, -11175))
        if dia == 12:
            ev.append(("PAGO PSE GASES DEL CARIBE S", None, -r.randint(28000, 65000)))
        if dia == 18:
            ev.append(("PAGO PSE CARIBEMAR DE LA COS", None, -r.randint(150000, 320000)))
        if dia == 20 and r.random() < 0.6:
            ev.append(("PAGO DE PROV PEXTO COLOMBIA", None, -r.randint(10, 40) * 10000))
        # --- Gasto variable ---
        for _ in range(r.choices([0, 1, 2], [0.65, 0.28, 0.07])[0]):
            ev.append(("TRANSFERENCIAS A NEQUI", None, -r.choice([5, 6, 10, 13, 20, 27, 30, 40, 50, 75, 100]) * 1000))
        if r.random() < 0.15:
            ev.append((r.choice(self.comercios_qr), None, -r.randint(80, 900) * 100))
        if r.random() < 0.03:
            ev.append((f"TRANSF A {r.choice(self.personas)}", None, -r.randint(5, 50) * 10000))
        if r.random() < 0.02:
            ev.append(("PAGO INTERBANC UBER BV", None, -r.randint(80, 400) * 100))
        if r.random() < 0.03:
            ev.append(("RETIRO CAJERO  ATM EDS CALLEJA", None, -r.choice([200, 300, 500, 1000]) * 1000))
        return ev


def generar_trimestre(g: Generador, desde: date, hasta: date, saldo_ini: float, errores: bool):
    r = g.r
    movs, saldo, d = [], round(saldo_ini, 2), desde
    saldos_dia = []
    while d <= hasta:
        ev = g.eventos_del_dia(d)
        entradas = [e for e in ev if e[2] > 0]
        salidas = [e for e in ev if e[2] < 0]
        r.shuffle(salidas)
        orden = entradas[:]
        if r.random() < 0.55:  # abono de intereses casi diario, como en el real
            interes = round(saldo * r.uniform(1.1e-6, 1.6e-6), 2)
            if interes >= 0.5:
                orden.append(("ABONO INTERESES AHORROS", None, interes))
        orden += salidas
        for desc, suc, valor in orden:
            if saldo + valor < 0:  # el banco rechaza el débito: no aparece en el extracto
                continue
            saldo = round(saldo + valor, 2)
            movs.append([f"{d.day}/{d:%m}", desc, suc, None, fmt(valor), fmt(saldo)])
        saldos_dia.append(saldo)
        d += timedelta(days=1)

    if errores and len(movs) > 20:
        movs[7][4] = "N/A"                                   # valor no numérico
        movs[15][5] = fmt(parse(movs[15][5]) + 1000)         # saldo que no cuadra con la cadena
        movs.insert(25, list(movs[24]))                      # fila duplicada
        movs[30][0] = "31/02"                                # fecha imposible

    valores = [parse(m[4]) for m in movs if m[4] != "N/A"]
    resumen = {
        "SALDO ANTERIOR": saldo_ini,
        "TOTAL ABONOS": sum(v for v in valores if v > 0),
        "TOTAL CARGOS": -sum(v for v in valores if v < 0),
        "SALDO ACTUAL": saldo,
        "SALDO PROMEDIO": round(sum(saldos_dia) / len(saldos_dia)),
        "INTERESES": sum(parse(m[4]) for m in movs if m[1] == "ABONO INTERESES AHORROS"),
    }
    return movs, resumen


def escribir_xlsx(path: Path, desde: date, hasta: date, cuenta: str, movs, res) -> None:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Table1"
    info_general = [f"{desde:%Y/%m/%d}", f"{hasta:%Y/%m/%d}", TIPO_CUENTA, cuenta, SUCURSAL]

    def bloque(con_resumen: bool):
        ws.append(["Información Cliente:"])
        ws.append(["CLIENTE", "DIRECCIÓN", "CIUDAD"])
        ws.append(list(CLIENTE))
        ws.append([])
        ws.append(["Información General:"])
        ws.append(["DESDE", "HASTA", "TIPO CUENTA", "NRO CUENTA", "SUCURSAL"])
        ws.append(info_general)
        ws.append([])
        if con_resumen:
            ws.append(["Resumen:"])
            ws.append(["SALDO ANTERIOR", "TOTAL ABONOS", "TOTAL CARGOS", "SALDO ACTUAL",
                       "SALDO PROMEDIO", "CUPO SUGERIDO", "INTERESES", "RETEFUENTE"])
            ws.append([fmt(res["SALDO ANTERIOR"]), fmt(res["TOTAL ABONOS"]), fmt(res["TOTAL CARGOS"]),
                       fmt(res["SALDO ACTUAL"]), f"{res['SALDO PROMEDIO']:,}", ".00",
                       fmt(res["INTERESES"]), ".00"])
            ws.append([])
        ws.append(["Movimientos:"])
        ws.append(ENCABEZADO_MOV)

    ws.append([])  # el real empieza con una fila vacía
    bloque(con_resumen=True)
    i, cupo = 0, FILAS_PRIMERA_PAGINA
    while i < len(movs):
        for m in movs[i:i + cupo]:
            ws.append(m)
        i += cupo
        cupo = FILAS_POR_PAGINA
        if i < len(movs):
            bloque(con_resumen=False)
    ws.append([None, "FIN ESTADO DE CUENTA"])
    path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(path)


def leer_cierre(path: str) -> tuple[date, float, str]:
    """Lee HASTA, SALDO ACTUAL y NRO CUENTA de un extracto existente."""
    rows = list(openpyxl.load_workbook(path, read_only=True).active.iter_rows(values_only=True))
    hasta = saldo = cuenta = None
    for i, row in enumerate(rows):
        if row[0] == "DESDE" and hasta is None:
            y, m, d = map(int, str(rows[i + 1][1]).split("/"))
            hasta, cuenta = date(y, m, d), str(rows[i + 1][3])
        if row[0] == "SALDO ANTERIOR" and saldo is None:
            saldo = parse(rows[i + 1][3])
    return hasta, saldo, cuenta


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--continuar-de", help="Extracto existente desde el que continuar")
    ap.add_argument("--desde", type=date.fromisoformat)
    ap.add_argument("--saldo-inicial", type=float)
    ap.add_argument("--cuenta", default="XXXXXXX0882")
    ap.add_argument("--trimestres", type=int, default=1)
    ap.add_argument("--semilla", type=int, default=42)
    ap.add_argument("--errores", action="store_true", help="Inyecta errores de calidad a propósito")
    ap.add_argument("--salida", default=".")
    a = ap.parse_args()

    if a.continuar_de:
        hasta_prev, saldo, cuenta = leer_cierre(a.continuar_de)
        desde = hasta_prev + timedelta(days=1)
    else:
        if a.desde is None or a.saldo_inicial is None:
            ap.error("usa --continuar-de, o bien --desde y --saldo-inicial")
        desde, saldo, cuenta = a.desde, a.saldo_inicial, a.cuenta

    g = Generador(random.Random(a.semilla))
    for _ in range(a.trimestres):
        fin_mes3 = fin_de_mes((desde.replace(day=1) + timedelta(days=62)))
        movs, res = generar_trimestre(g, desde, fin_mes3, saldo, a.errores)
        sufijo = "_ERRORES" if a.errores else ""
        out = Path(a.salida) / f"EXTRACTO_{cuenta[-4:]}_{MESES[fin_mes3.month - 1]}{fin_mes3.year}{sufijo}.xlsx"
        escribir_xlsx(out, desde, fin_mes3, cuenta, movs, res)
        print(f"OK -> {out}  {desde} a {fin_mes3}  {len(movs)} movs  "
              f"saldo {fmt(res['SALDO ANTERIOR'])} -> {fmt(res['SALDO ACTUAL'])}")
        desde, saldo = fin_mes3 + timedelta(days=1), res["SALDO ACTUAL"]


if __name__ == "__main__":
    main()
