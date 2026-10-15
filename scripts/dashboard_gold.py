"""Genera docs/dashboard/dashboard-gold.html: un dashboard autocontenido con los datos de gold.

Consulta con Athena (solo SELECT, centavos) los agregados de flights_gold y los incrusta en
docs/dashboard/plantilla.html. El HTML resultante no necesita internet ni servidor.

Uso:  python scripts/dashboard_gold.py            (con el perfil `flights` de la CLI ya logueado)
"""
import datetime
import json
import subprocess
import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
PLANTILLA = RAIZ / "docs" / "dashboard" / "plantilla.html"
SALIDA = RAIZ / "docs" / "dashboard" / "dashboard-gold.html"
PERFIL, REGION, WORKGROUP = "flights", "us-east-2", "flights-wg"

FACT = "flights_gold.fact_flights"
DEMORADO = "SUM(CASE WHEN is_operated = 1 AND arr_del15 = 1 THEN 1 ELSE 0 END)"
SUMA = "SUM(CASE WHEN is_operated = 1 THEN arr_delay END)"
CANTIDAD = "COUNT(CASE WHEN is_operated = 1 THEN arr_delay END)"

# Medidas aditivas (suma y cantidad): el dashboard calcula los promedios, nunca promedia promedios.
CONSULTAS = {
    "daily": """SELECT date_key, SUM(flights) AS flights, SUM(operated) AS operated, SUM(cancelled) AS cancelled,
        SUM(delayed_15) AS delayed15, SUM(arr_delay_sum) AS delay_sum, SUM(arr_delay_n) AS delay_n
        FROM flights_gold.agg_daily_airport GROUP BY date_key ORDER BY date_key""",
    "airports": """SELECT month, airport_code AS code, city_name AS city, SUM(flights) AS flights, SUM(operated) AS operated,
        SUM(cancelled) AS cancelled, SUM(delayed_15) AS delayed15, SUM(arr_delay_sum) AS delay_sum, SUM(arr_delay_n) AS delay_n
        FROM flights_gold.agg_daily_airport GROUP BY month, airport_code, city_name""",
    "carriers": f"""SELECT f.month AS month, f.carrier_code AS code, c.carrier_name AS name, COUNT(*) AS flights,
        SUM(f.is_operated) AS operated, SUM(f.cancelled) AS cancelled,
        SUM(CASE WHEN f.is_operated = 1 AND f.arr_del15 = 1 THEN 1 ELSE 0 END) AS delayed15,
        SUM(CASE WHEN f.is_operated = 1 THEN f.arr_delay END) AS delay_sum,
        COUNT(CASE WHEN f.is_operated = 1 THEN f.arr_delay END) AS delay_n
        FROM {FACT} f JOIN flights_gold.dim_carrier c ON f.carrier_code = c.carrier_code
        GROUP BY f.month, f.carrier_code, c.carrier_name""",
    "causes": f"""SELECT month, SUM(carrier_delay) AS carrier, SUM(weather_delay) AS weather, SUM(nas_delay) AS nas,
        SUM(security_delay) AS security, SUM(late_aircraft_delay) AS late_aircraft FROM {FACT} GROUP BY month""",
    "blocks": f"""SELECT month, dep_time_blk AS blk, SUM(is_operated) AS operated, {DEMORADO} AS delayed15,
        {SUMA} AS delay_sum, {CANTIDAD} AS delay_n FROM {FACT} GROUP BY month, dep_time_blk""",
}
TEXTO = ("code", "city", "name", "blk")  # el resto son numeros


def aws(*args):
    r = subprocess.run(["aws", *args, "--profile", PERFIL, "--region", REGION], capture_output=True, text=True)
    if r.returncode:
        sys.exit(f"aws {' '.join(args[:2])} fallo: {r.stderr.strip()[:300]}")
    return r.stdout


def correr(sql):
    q = aws("athena", "start-query-execution", "--work-group", WORKGROUP, "--query-string", " ".join(sql.split()),
            "--query", "QueryExecutionId", "--output", "text").strip()
    for _ in range(90):
        estado = aws("athena", "get-query-execution", "--query-execution-id", q, "--query",
                     "QueryExecution.[Status.State,Statistics.DataScannedInBytes]", "--output", "text").split()
        if estado[0] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)
    if estado[0] != "SUCCEEDED":
        sys.exit(f"la consulta {q} termino en {estado[0]}")
    filas = json.loads(aws("athena", "get-query-results", "--query-execution-id", q, "--output", "json"))["ResultSet"]["Rows"]
    cabecera = [c["VarCharValue"] for c in filas[0]["Data"]]
    return [dict(zip(cabecera, [c.get("VarCharValue") for c in f["Data"]])) for f in filas[1:]], int(estado[1])


def numero(x):
    if x in (None, ""):
        return None
    f = float(x)
    return int(f) if f == int(f) else round(f, 4)


def main():
    datos, escaneado = {}, 0
    for nombre, sql in CONSULTAS.items():
        filas, bytes_ = correr(sql)
        escaneado += bytes_
        datos[nombre] = [{k: (v if k in TEXTO else numero(v)) for k, v in f.items()} for f in filas]
        print(f"{nombre:9} {len(datos[nombre]):5} filas  {bytes_ / 1e6:6.2f} MB escaneados")
    datos["generado"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    # conciliacion: gold tiene que cerrar con lo medido en silver y en el plan
    total = sum(r["flights"] for r in datos["daily"])
    operados = sum(r["operated"] for r in datos["daily"])
    por_aerolinea = sum(r["operated"] for r in datos["carriers"])
    if operados != por_aerolinea:
        sys.exit(f"no cierra: operados en el agregado {operados} contra {por_aerolinea} en el hecho")
    print(f"vuelos {total:,} | operados {operados:,} (agregado y hecho coinciden) | escaneado {escaneado / 1e6:.1f} MB")

    html = PLANTILLA.read_text(encoding="utf-8")
    if "/*DATA*/null" not in html:
        sys.exit("la plantilla no tiene el marcador /*DATA*/null")
    incrustado = json.dumps(datos, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")
    SALIDA.write_text(html.replace("/*DATA*/null", incrustado), encoding="utf-8", newline="\n")  # LF también en Windows
    print(f"escrito {SALIDA.relative_to(RAIZ)} ({SALIDA.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
