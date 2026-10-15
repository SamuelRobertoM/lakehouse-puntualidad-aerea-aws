#!/usr/bin/env python3
"""Baja los datos de BTS On-Time Performance, los descomprime y los valida.

Deja en data/landing/ la misma estructura que va a tener el bucket de bronce, para subirla
tal cual con `aws s3 sync`:

    flights/year=2024/month=01/<csv original>
    lookups/airlines/lookup_airlines.csv
    lookups/airports/lookup_airports.csv
    lookups/cancellation_causes/lookup_cancellation_causes.csv

Los zips quedan en data/downloads/ (fuera de landing, no suben a S3). No toca AWS.
Si un archivo ya existe lo saltea, así que se puede volver a correr sin bajar todo de nuevo.

Uso (desde la raíz del repo):
    python -I scripts/download_data.py                    # enero y febrero de 2024
    python -I scripts/download_data.py --months 2024-03   # otro mes
    python -I scripts/download_data.py --validate-only    # solo valida lo ya descargado
"""
import argparse
import csv
import io
import shutil
import sys
import urllib.request
import zipfile
from pathlib import Path

FLIGHTS_URL = (
    "https://transtats.bts.gov/PREZIP/"
    "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"
)
# nombre del archivo en landing, URL, filas esperadas según docs/PLAN.md
LOOKUPS = {
    "airlines": (
        "lookup_airlines.csv",
        "https://www.transtats.bts.gov/Download_Lookup.asp?Y11x72=Y_haVdhR_PNeeVRef",
        1779,
    ),
    "airports": (
        "lookup_airports.csv",
        "https://www.transtats.bts.gov/Download_Lookup.asp?Y11x72=Y_NVecbeg_VQ",
        6915,
    ),
    "cancellation_causes": (
        "lookup_cancellation_causes.csv",
        "https://www.transtats.bts.gov/Download_Lookup.asp?Y11x72=Y_PNaPRYYNgVba",
        4,
    ),
}
EXPECTED_ROWS = {(2024, 1): 547271, (2024, 2): 519221}  # medidas en los archivos reales
EXPECTED_COLUMNS = 110  # la última no tiene nombre (coma final del CSV)
REQUIRED_COLUMNS = ["FlightDate", "Tail_Number", "ArrDelay", "Cancelled"]

OK, AVISO, ERROR = "OK", "AVISO", "ERROR"


def download(url: str, dest: Path, force: bool) -> None:
    if dest.exists() and not force:
        print(f"  ya existe, lo salteo: {dest}")
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + ".part")
    print(f"  bajando {url}")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (practice-02)"})
    with urllib.request.urlopen(req, timeout=120) as resp, open(part, "wb") as out:
        shutil.copyfileobj(resp, out)
    part.rename(dest)
    print(f"  listo: {dest} ({dest.stat().st_size / 1_000_000:.1f} MB)")


def extract_single_csv(zip_path: Path, out_dir: Path, force: bool) -> Path:
    """Saca el único CSV del zip. Usa solo el nombre base del miembro: nunca confía en rutas del zip."""
    with zipfile.ZipFile(zip_path) as zf:
        bad = zf.testzip()
        if bad:
            raise RuntimeError(f"zip dañado, falla en {bad}")
        csvs = [m for m in zf.infolist() if m.filename.lower().endswith(".csv")]
        if len(csvs) != 1:
            raise RuntimeError(f"se esperaba 1 CSV en el zip y hay {len(csvs)}")
        member = csvs[0]
        dest = out_dir / Path(member.filename).name
        if dest.exists() and not force:
            print(f"  ya existe, lo salteo: {dest}")
            return dest
        out_dir.mkdir(parents=True, exist_ok=True)
        print(f"  descomprimiendo {dest.name}")
        with zf.open(member) as src, open(dest, "wb") as out:
            shutil.copyfileobj(src, out)
        return dest


def validate_flights(csv_path: Path, year: int, month: int) -> list[tuple[str, str, str]]:
    checks: list[tuple[str, str, str]] = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader)
        n_cols = len(header)
        checks.append((
            "columnas", OK if n_cols == EXPECTED_COLUMNS else ERROR,
            f"{n_cols} (esperadas {EXPECTED_COLUMNS})",
        ))
        checks.append((
            "última columna sin nombre", OK if header[-1] == "" else AVISO,
            f"nombre = {header[-1]!r}",
        ))
        missing = [c for c in REQUIRED_COLUMNS if c not in header]
        checks.append((
            "columnas clave presentes", OK if not missing else ERROR,
            "todas" if not missing else f"faltan {missing}",
        ))
        if missing:
            return checks
        i_date = header.index("FlightDate")
        prefix = f"{year}-{month:02d}"
        rows = bad_len = out_of_month = 0
        for row in reader:
            rows += 1
            if len(row) != n_cols:
                bad_len += 1
            if not row[i_date].startswith(prefix):
                out_of_month += 1
    expected = EXPECTED_ROWS.get((year, month))
    if expected is None:
        checks.append(("filas", OK, f"{rows:,} (sin referencia en el plan)"))
    else:
        checks.append((
            "filas", OK if rows == expected else AVISO,
            f"{rows:,} (el plan midió {expected:,})",
        ))
    checks.append((
        "filas con otro largo", OK if bad_len == 0 else AVISO, f"{bad_len:,}",
    ))
    checks.append((
        f"FlightDate dentro de {prefix}", OK if out_of_month == 0 else AVISO,
        f"{out_of_month:,} filas fuera del mes",
    ))
    return checks


def validate_lookup(csv_path: Path, expected: int) -> list[tuple[str, str, str]]:
    raw = csv_path.read_bytes()
    try:
        text, enc_status, enc_detail = raw.decode("utf-8-sig"), OK, "UTF-8"
    except UnicodeDecodeError:
        # Bronce guarda el archivo tal cual: el job tendrá que leerlo con esta codificación.
        text, enc_status = raw.decode("cp1252", errors="replace"), AVISO
        enc_detail = "NO es UTF-8 (probable Latin-1/cp1252); el job debe indicar la codificación al leerlo"
    reader = csv.reader(io.StringIO(text, newline=""))
    next(reader, None)  # encabezado
    rows = sum(1 for _ in reader)
    return [
        ("codificación", enc_status, enc_detail),
        ("filas (sin encabezado)", OK if rows == expected else AVISO,
         f"{rows:,} (el plan midió {expected:,})"),
    ]


def parse_month(text: str) -> tuple[int, int]:
    year, month = text.split("-")
    return int(year), int(month)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--months", nargs="+", default=["2024-01", "2024-02"], metavar="AAAA-MM")
    ap.add_argument("--landing", type=Path, default=Path("data/landing"))
    ap.add_argument("--downloads", type=Path, default=Path("data/downloads"))
    ap.add_argument("--force", action="store_true", help="vuelve a bajar y descomprimir")
    ap.add_argument("--validate-only", action="store_true", help="no baja nada, solo valida")
    args = ap.parse_args()

    results: list[tuple[str, list[tuple[str, str, str]]]] = []

    for text in args.months:
        year, month = parse_month(text)
        print(f"\n[vuelos {year}-{month:02d}]")
        zip_path = args.downloads / f"flights-{year}-{month:02d}" / f"flights_{year}_{month}.zip"
        out_dir = args.landing / "flights" / f"year={year}" / f"month={month:02d}"
        if args.validate_only:
            found = sorted(out_dir.glob("*.csv"))
            if len(found) != 1:
                results.append((f"vuelos {text}", [("archivo", ERROR, f"{len(found)} CSV en {out_dir}")]))
                continue
            csv_path = found[0]
        else:
            download(FLIGHTS_URL.format(year=year, month=month), zip_path, args.force)
            csv_path = extract_single_csv(zip_path, out_dir, args.force)
        print("  validando...")
        results.append((f"vuelos {text}", validate_flights(csv_path, year, month)))

    for name, (filename, url, expected) in LOOKUPS.items():
        print(f"\n[lookup {name}]")
        dest = args.landing / "lookups" / name / filename
        if not args.validate_only:
            download(url, dest, args.force)
        if not dest.exists():
            results.append((f"lookup {name}", [("archivo", ERROR, f"no existe {dest}")]))
            continue
        results.append((f"lookup {name}", validate_lookup(dest, expected)))

    print("\n" + "=" * 70 + "\nRESULTADO DE LA VALIDACIÓN\n" + "=" * 70)
    errors = 0
    for title, checks in results:
        print(f"\n{title}")
        for label, status, detail in checks:
            errors += status == ERROR
            print(f"  [{status:<5}] {label}: {detail}")
    print()
    if errors:
        print(f"{errors} error(es). No subas esto a bronce hasta entender por qué.")
        return 1
    print("Sin errores. Los AVISO son diferencias respecto del plan para revisar, no fallas.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
