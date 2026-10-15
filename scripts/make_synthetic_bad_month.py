#!/usr/bin/env python3
"""Genera un mes SINTETICO (abril de 2024) que pasa el job 1 pero NO pasa la calidad.

NO es un dato real de BTS. Se arma con las primeras filas de enero, cambiando las fechas a abril
para que el job 1 no rechace nada (fecha dentro del mes, year y month coherentes). Con solo 5.000
filas, la regla `RowCount > 400000` de dq/silver.dqdl tiene que fallar: es un archivo truncado.

El resultado va a data/synthetic/, NUNCA a data/landing/, para que `aws s3 sync data/landing`
no lo suba por accidente. El nombre empieza con SINTETICO_ para reconocerlo en S3.

Uso (desde la raiz del repo):
    python -I scripts/make_synthetic_bad_month.py
"""
import csv
import sys
from pathlib import Path

ROWS = 5000
JAN_DIR = Path("data/landing/flights/year=2024/month=01")
OUT_DIR = Path("data/synthetic/flights/year=2024/month=04")
OUT_NAME = "SINTETICO_On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2024_4.csv"


def main() -> int:
    sources = sorted(JAN_DIR.glob("*.csv"))
    if len(sources) != 1:
        print(f"No encuentro el CSV de enero en {JAN_DIR}. Corre antes scripts/download_data.py")
        return 1
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / OUT_NAME

    with open(sources[0], newline="", encoding="utf-8-sig") as src, \
            open(out_path, "w", newline="", encoding="utf-8") as dst:
        reader = csv.reader(src)
        writer = csv.writer(dst, lineterminator="\n")
        header = next(reader)
        writer.writerow(header)
        i_year, i_quarter, i_month = header.index("Year"), header.index("Quarter"), header.index("Month")
        i_day, i_date = header.index("DayofMonth"), header.index("FlightDate")
        written = 0
        for row in reader:
            day = min(int(row[i_day]), 30)  # abril tiene 30 dias
            row[i_year], row[i_quarter], row[i_month] = "2024", "2", "4"
            row[i_day], row[i_date] = str(day), f"2024-04-{day:02d}"
            writer.writerow(row)
            written += 1
            if written == ROWS:
                break

    print(f"Archivo sintetico: {out_path}")
    print(f"  {written:,} filas (un mes real tiene entre 519.221 y 591.767)")
    print(f"  {out_path.stat().st_size / 1_000_000:.1f} MB")
    print("Subirlo a bronce lo vuelve a disparar el pipeline: la regla RowCount deberia FALLAR.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
