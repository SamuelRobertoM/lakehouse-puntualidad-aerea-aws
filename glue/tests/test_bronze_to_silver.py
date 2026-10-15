"""Pruebas de la lógica del job 1. Corren en el contenedor de Glue 5.0 (ver PROGRESO.md)."""
import csv
import datetime
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

import bronze_to_silver as b2s

REPO = Path(__file__).resolve().parents[2]
JAN_DIR = REPO / "data" / "landing" / "flights" / "year=2024" / "month=01"

# Un vuelo normal, con los nombres originales de BTS y todo como texto (así llega del CSV).
BASE = {
    "Year": "2024", "Month": "1", "DayofMonth": "8", "DayOfWeek": "1", "FlightDate": "2024-01-08",
    "Reporting_Airline": "9E", "Tail_Number": "N485PX", "OriginAirportID": "12953", "DestAirportID": "14122",
    "CRSDepTime": "0856", "DepTime": "0851", "DepDelay": "-5.00", "DepDel15": "0.00",
    "CRSArrTime": "1135", "ArrTime": "1124", "ArrDelay": "-11.00", "ArrDel15": "0.00",
    "Cancelled": "0.00", "CancellationCode": "", "Diverted": "0.00", "Distance": "1148.00",
    "CarrierDelay": "", "WeatherDelay": "", "NASDelay": "", "SecurityDelay": "", "LateAircraftDelay": "",
    "_c109": "",
}


def make_df(spark, *overrides):
    rows = [{**BASE, **o} for o in (overrides or [{}])]
    schema = StructType([StructField(k, StringType(), True) for k in BASE])
    return spark.createDataFrame([tuple(r[k] for k in BASE) for r in rows], schema)


def run(spark, *overrides, year=2024, month=1):
    silver, quarantine, unique = b2s.transform(make_df(spark, *overrides), year, month)
    return silver.collect(), quarantine.collect(), unique.count()


# ------------------------------------------------------------------ nombres

@pytest.mark.parametrize("raw,expected", [
    ("FlightDate", "flight_date"), ("DayofMonth", "day_of_month"), ("DayOfWeek", "day_of_week"),
    ("CRSDepTime", "crs_dep_time"), ("DOT_ID_Reporting_Airline", "dot_id_reporting_airline"),
    ("OriginAirportID", "origin_airport_id"), ("NASDelay", "nas_delay"), ("DepDel15", "dep_del15"),
    ("Div1TotalGTime", "div1_total_g_time"), ("Div5TailNum", "div5_tail_num"),
    ("CRSElapsedTime", "crs_elapsed_time"), ("TotalAddGTime", "total_add_g_time"),
])
def test_snake_case(raw, expected):
    assert b2s.to_snake_case(raw) == expected


def test_descarta_la_columna_sin_nombre(spark):
    df = b2s.rename_to_snake_case(make_df(spark))
    assert "_c109" not in df.columns and "flight_date" in df.columns


def test_el_lector_de_spark_nombra_la_columna_vacia_del_csv(spark, tmp_path):
    """Lock del comportamiento que asume rename_to_snake_case con la coma final de BTS."""
    p = tmp_path / "t.csv"
    p.write_text("A,B,\n1,2,\n")
    df = spark.read.option("header", "true").csv(str(p))
    assert df.columns == ["A", "B", "_c2"]
    assert b2s.rename_to_snake_case(df).columns == ["a", "b"]


@pytest.mark.skipif(not JAN_DIR.exists(), reason="no hay datos descargados")
def test_los_110_nombres_reales_quedan_unicos_y_validos():
    csv_path = next(JAN_DIR.glob("*.csv"))
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        header = next(csv.reader(f))
    names = [b2s.to_snake_case(h) for h in header if h]
    assert len(names) == 109 and len(set(names)) == 109
    assert all(n.replace("_", "").isalnum() and n == n.lower() for n in names)


# ------------------------------------------------------------------ silver: lo que se conserva y se tipa

def test_vuelo_normal_va_a_silver_con_tipos(spark):
    silver, quarantine, _ = run(spark)
    assert len(silver) == 1 and not quarantine
    r = silver[0]
    assert r["flight_date"] == datetime.date(2024, 1, 8)
    assert r["arr_delay"] == -11.0 and r["distance"] == 1148.0
    assert r["cancelled"] == 0 and r["year"] == 2024 and r["month"] == 1
    assert r["crs_dep_time"] == "0856"  # la hora queda como texto HHMM


def test_demora_negativa_es_valida(spark):
    silver, quarantine, _ = run(spark, {"ArrDelay": "-28.00"})
    assert len(silver) == 1 and not quarantine
    assert silver[0]["arr_delay"] == -28.0


def test_cancelado_con_horas_y_demora_vacias_es_valido(spark):
    silver, quarantine, _ = run(spark, {"Cancelled": "1.00", "CancellationCode": "B", "DepTime": "",
                                        "ArrTime": "", "ArrDelay": "", "ArrDel15": ""})
    assert len(silver) == 1 and not quarantine
    r = silver[0]
    assert r["cancelled"] == 1 and r["cancellation_code"] == "B"
    assert r["arr_delay"] is None and r["dep_time"] is None


def test_desviado_sin_arr_delay_es_valido(spark):
    silver, quarantine, _ = run(spark, {"Diverted": "1.00", "ArrDelay": "", "ArrDel15": ""})
    assert len(silver) == 1 and not quarantine


def test_causas_vacias_en_vuelo_a_tiempo_son_validas(spark):
    silver, _, _ = run(spark)
    assert silver[0]["carrier_delay"] is None and silver[0]["late_aircraft_delay"] is None


def test_causas_informadas_en_vuelo_demorado(spark):
    silver, quarantine, _ = run(spark, {"ArrDelay": "27.00", "ArrDel15": "1.00", "CarrierDelay": "0.00",
                                        "WeatherDelay": "0.00", "NASDelay": "4.00",
                                        "SecurityDelay": "0.00", "LateAircraftDelay": "23.00"})
    assert not quarantine
    r = silver[0]
    assert r["nas_delay"] + r["late_aircraft_delay"] == r["arr_delay"] == 27.0


def test_hora_2400_se_normaliza_a_0000(spark):
    silver, quarantine, _ = run(spark, {"ArrTime": "2400"})
    assert not quarantine and silver[0]["arr_time"] == "0000"


# ------------------------------------------------------------------ quarantine: lo que se rechaza y por qué

def test_hora_invalida_va_a_quarantine(spark):
    silver, quarantine, _ = run(spark, {"ArrTime": "2575"})
    assert not silver and len(quarantine) == 1
    assert quarantine[0]["rejection_reason"] == "hora_invalida:arr_time"


def test_fecha_fuera_del_mes(spark):
    silver, quarantine, _ = run(spark, {"FlightDate": "2024-02-03"})
    assert not silver and quarantine[0]["rejection_reason"] == "fecha_fuera_de_mes"


def test_distancia_no_positiva(spark):
    _, quarantine, _ = run(spark, {"Distance": "0.00"})
    assert quarantine[0]["rejection_reason"] == "distancia_no_positiva"


def test_cancelado_sin_codigo(spark):
    _, quarantine, _ = run(spark, {"Cancelled": "1.00", "CancellationCode": ""})
    assert quarantine[0]["rejection_reason"] == "cancelado_sin_codigo"


def test_numero_ilegible(spark):
    _, quarantine, _ = run(spark, {"ArrDelay": "abc"})
    assert quarantine[0]["rejection_reason"] == "tipo_invalido:arr_delay"


def test_bandera_fuera_de_0_y_1(spark):
    _, quarantine, _ = run(spark, {"Cancelled": "2.00"})
    assert quarantine[0]["rejection_reason"] == "flag_invalido:cancelled"


def test_campo_obligatorio_vacio(spark):
    _, quarantine, _ = run(spark, {"Reporting_Airline": ""})
    assert quarantine[0]["rejection_reason"] == "campo_obligatorio_vacio:reporting_airline"


def test_varios_motivos_se_acumulan(spark):
    _, quarantine, _ = run(spark, {"Distance": "-5.00", "FlightDate": "2024-03-01"})
    assert set(quarantine[0]["rejection_reason"].split("|")) == {"distancia_no_positiva", "fecha_fuera_de_mes"}


def test_quarantine_guarda_el_dato_crudo_y_el_mes_de_origen(spark):
    _, quarantine, _ = run(spark, {"ArrDelay": "abc"})
    r = quarantine[0]
    assert r["arr_delay"] == "abc"  # texto tal cual, sin castear
    assert r["source_year"] == 2024 and r["source_month"] == 1


# ------------------------------------------------------------------ duplicados, conciliación y esquema

def test_duplicado_exacto_se_elimina(spark):
    silver, quarantine, n_unique = run(spark, {}, {})
    assert len(silver) == 1 and not quarantine and n_unique == 1


def test_las_filas_cierran(spark):
    """filas leídas = silver + quarantine + duplicados eliminados."""
    rows = [{}, {}, {"FlightDate": "2024-01-09"}, {"Distance": "0.00"}, {"ArrTime": "2575"},
            {"Cancelled": "1.00", "CancellationCode": "A"}]
    silver, quarantine, n_unique = run(spark, *rows)
    duplicates = len(rows) - n_unique
    assert len(silver) + len(quarantine) + duplicates == len(rows)
    assert (len(silver), len(quarantine), duplicates) == (3, 2, 1)


def test_falta_una_columna_obligatoria(spark):
    df = make_df(spark).drop("Distance")
    with pytest.raises(ValueError, match="faltan columnas obligatorias"):
        b2s.transform(df, 2024, 1)


# ------------------------------------------------------------------ datos reales (se saltea si no están)

@pytest.mark.skipif(not JAN_DIR.exists(), reason="no hay datos descargados")
def test_muestra_real_de_enero_cierra_y_queda_casi_limpia(spark):
    src = str(next(JAN_DIR.glob("*.csv")))
    df = spark.read.option("header", "true").csv(src).limit(5000).cache()
    n_raw = df.count()
    silver, quarantine, unique = b2s.transform(df, 2024, 1)
    n_unique, n_silver, n_quarantine = unique.count(), silver.count(), quarantine.count()
    assert n_unique == n_silver + n_quarantine
    assert n_raw == n_unique  # el plan midió cero duplicados exactos
    assert n_quarantine == 0, quarantine.select("rejection_reason").distinct().collect()
    assert len(silver.columns) == 109
    types = dict(silver.dtypes)
    assert types["flight_date"] == "date" and types["arr_delay"] == "double" and types["cancelled"] == "int"
    assert silver.filter(F.col("arr_delay") < 0).count() > 0  # las demoras negativas siguen ahí


# ------------------------------------------------------------------ escritura (carpetas e idempotencia)

FEB = {"Month": "2", "FlightDate": "2024-02-10"}


def write_month(spark, tmp_path, rows, month):
    silver, quarantine, _ = b2s.transform(make_df(spark, *rows), 2024, month)
    b2s.write_outputs(silver, quarantine, str(tmp_path / "silver"), str(tmp_path / "quarantine"), 2024, month)


def parquet_rows(spark, folder):
    """Filas de una carpeta; 0 si no tiene archivos (Spark puede no escribir ninguno si no hay filas)."""
    return spark.read.parquet(str(folder)).count() if list(folder.glob("*.parquet")) else 0


def test_carpetas_con_mes_de_dos_digitos(spark, tmp_path):
    write_month(spark, tmp_path, [{}], 1)
    assert (tmp_path / "silver" / "year=2024" / "month=01").is_dir()
    assert not (tmp_path / "silver" / "year=2024" / "month=1").exists()


def test_un_solo_archivo_parquet_por_mes(spark, tmp_path):
    write_month(spark, tmp_path, [{"FlightDate": f"2024-01-{d:02d}"} for d in range(1, 21)], 1)
    assert len(list((tmp_path / "silver" / "year=2024" / "month=01").glob("*.parquet"))) == 1


def test_year_y_month_viajan_en_la_ruta_y_spark_los_reconstruye(spark, tmp_path):
    write_month(spark, tmp_path, [{}], 1)
    r = spark.read.parquet(str(tmp_path / "silver")).collect()[0]
    assert r["year"] == 2024 and r["month"] == 1 and r["arr_delay"] == -11.0


def test_reprocesar_un_mes_no_toca_los_otros(spark, tmp_path):
    write_month(spark, tmp_path, [{}, {"FlightDate": "2024-01-09"}], 1)
    write_month(spark, tmp_path, [FEB], 2)
    write_month(spark, tmp_path, [{}], 1)  # reproceso de enero, ahora con una fila menos
    df = spark.read.parquet(str(tmp_path / "silver"))
    counts = {r["month"]: r["n"] for r in df.groupBy("month").agg(F.count("*").alias("n")).collect()}
    assert counts == {1: 1, 2: 1}


def test_reprocesar_limpia_quarantine_si_ya_no_hay_rechazos(spark, tmp_path):
    q_dir = tmp_path / "quarantine" / "source_year=2024" / "source_month=01"
    write_month(spark, tmp_path, [{"Distance": "0.00"}], 1)
    assert parquet_rows(spark, q_dir) == 1
    write_month(spark, tmp_path, [{}], 1)  # se arregló la causa: ya no se rechaza nada
    assert parquet_rows(spark, q_dir) == 0
