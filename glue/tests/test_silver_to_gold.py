"""Pruebas de la lógica del job 2. Corren en el contenedor de Glue 5.0 (ver PROGRESO.md)."""
import datetime
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType

import silver_to_gold as s2g

REPO = Path(__file__).resolve().parents[2]
JAN_DIR = REPO / "data" / "landing" / "flights" / "year=2024" / "month=01"
LOOKUPS = REPO / "data" / "landing" / "lookups"

# Un vuelo normal de silver (ya tipado, nombres en snake_case).
SILVER_TYPES = {
    "flight_date": DateType(), "reporting_airline": StringType(), "origin_airport_id": IntegerType(),
    "dest_airport_id": IntegerType(), "origin": StringType(), "dest": StringType(),
    "cancellation_code": StringType(), "flight_number_reporting_airline": IntegerType(),
    "tail_number": StringType(), "dep_time_blk": StringType(), "arr_time_blk": StringType(),
    "dep_delay": DoubleType(), "dep_delay_minutes": DoubleType(), "arr_delay": DoubleType(),
    "arr_delay_minutes": DoubleType(), "dep_del15": IntegerType(), "arr_del15": IntegerType(),
    "taxi_out": DoubleType(), "taxi_in": DoubleType(), "air_time": DoubleType(), "distance": DoubleType(),
    "carrier_delay": DoubleType(), "weather_delay": DoubleType(), "nas_delay": DoubleType(),
    "security_delay": DoubleType(), "late_aircraft_delay": DoubleType(),
    "cancelled": IntegerType(), "diverted": IntegerType(),
}
BASE = {
    "flight_date": datetime.date(2024, 1, 8), "reporting_airline": "9E", "origin_airport_id": 12953,
    "dest_airport_id": 14122, "origin": "LGA", "dest": "PIT", "cancellation_code": None,
    "flight_number_reporting_airline": 3333, "tail_number": "N485PX", "dep_time_blk": "0800-0859",
    "arr_time_blk": "1100-1159", "dep_delay": -5.0, "dep_delay_minutes": 0.0, "arr_delay": -11.0,
    "arr_delay_minutes": 0.0, "dep_del15": 0, "arr_del15": 0, "taxi_out": 12.0, "taxi_in": 4.0,
    "air_time": 70.0, "distance": 335.0, "carrier_delay": None, "weather_delay": None, "nas_delay": None,
    "security_delay": None, "late_aircraft_delay": None, "cancelled": 0, "diverted": 0,
}
CANCELLED = {"cancelled": 1, "cancellation_code": "B", "arr_delay": None, "arr_del15": None, "air_time": None}
DIVERTED = {"diverted": 1, "arr_delay": None, "arr_del15": None}


def silver_df(spark, *overrides):
    rows = [{**BASE, **o} for o in (overrides or [{}])]
    schema = StructType([StructField(k, t, True) for k, t in SILVER_TYPES.items()])
    return spark.createDataFrame([tuple(r[k] for k in SILVER_TYPES) for r in rows], schema)


def lookup_df(spark, *rows):
    return spark.createDataFrame(list(rows), ["Code", "Description"])


def codes_df(spark, *rows):
    """(airport_id, airport_code, n_codes)"""
    return spark.createDataFrame(list(rows), "airport_id int, airport_code string, n_codes long")


def airports_lookup(spark):
    return lookup_df(
        spark,
        ("12953", "New York, NY: LaGuardia"),
        ("14122", "Pittsburgh, PA: Pittsburgh International"),
        ("10397", "Atlanta, GA: Hartsfield-Jackson Atlanta International"),
    )


def dim_airport(spark):
    codes = codes_df(spark, (12953, "LGA", 1), (14122, "PIT", 1), (10397, "ATL", 1))
    return s2g.build_dim_airport(airports_lookup(spark), codes)


# ------------------------------------------------------------------ dimensiones desde los lookups

def test_dim_cancellation(spark):
    raw = lookup_df(spark, ("A", "Carrier"), ("B", "Weather"), ("C", "National Air System"), ("D", "Security"))
    df = s2g.build_dim_cancellation(raw)
    assert df.columns == ["cancellation_code", "cancellation_reason"]
    assert dict(df.collect()) == {"A": "Carrier", "B": "Weather", "C": "National Air System", "D": "Security"}


def test_dim_carrier_recorta_espacios(spark):
    df = s2g.build_dim_carrier(lookup_df(spark, (" DL ", " Delta Air Lines Inc. ")))
    assert df.columns == ["carrier_code", "carrier_name"]
    assert df.collect()[0].asDict() == {"carrier_code": "DL", "carrier_name": "Delta Air Lines Inc."}


@pytest.mark.parametrize("description,city,state,name", [
    ("Atlanta, GA: Hartsfield-Jackson Atlanta International", "Atlanta, GA", "GA",
     "Hartsfield-Jackson Atlanta International"),
    ("Anaa, French Polynesia: Anaa Airport", "Anaa, French Polynesia", None, "Anaa Airport"),  # extranjero
    ("Foo, TX: Bar: Terminal 2", "Foo, TX", "TX", "Bar: Terminal 2"),  # parte solo en el primer ': '
    ("Sin separador", None, None, "Sin separador"),
])
def test_dim_airport_separa_ciudad_estado_y_nombre(spark, description, city, state, name):
    df = s2g.build_dim_airport(lookup_df(spark, ("10001", description)), codes_df(spark))
    row = df.collect()[0]
    assert (row.airport_id, row.city_name, row.state, row.airport_name) == (10001, city, state, name)


def test_dim_airport_conserva_todos_y_trae_el_codigo_cuando_hay(spark):
    raw = lookup_df(spark, ("10397", "Atlanta, GA: Hartsfield"), ("10001", "Afognak Lake, AK: Afognak Lake Airport"))
    df = s2g.build_dim_airport(raw, codes_df(spark, (10397, "ATL", 1)))
    assert df.columns == ["airport_id", "airport_code", "airport_name", "city_name", "state"]
    assert {r.airport_id: r.airport_code for r in df.collect()} == {10397: "ATL", 10001: None}


def test_codigos_de_aeropuerto_salen_de_origen_y_destino(spark):
    silver = silver_df(spark, {}, {"origin_airport_id": 14122, "origin": "PIT", "dest_airport_id": 12953, "dest": "LGA"})
    codes = {r.airport_id: (r.airport_code, r.n_codes) for r in s2g.airport_codes(silver).collect()}
    assert codes == {12953: ("LGA", 1), 14122: ("PIT", 1)}


def test_detecta_un_aeropuerto_con_dos_codigos(spark):
    silver = silver_df(spark, {}, {"origin": "LGX"})  # el mismo ID con otro código
    ambiguous = s2g.ambiguous_airport_codes(s2g.airport_codes(silver)).collect()
    assert [r.airport_id for r in ambiguous] == [12953]


def test_dim_date_dias_y_dia_de_la_semana_como_bts(spark):
    df = s2g.build_dim_date(spark, "2024-01-01", "2024-03-01")
    assert df.count() == 31 + 29 + 1  # 2024 es bisiesto
    rows = {r.date_key: r for r in df.collect()}
    assert (rows[20240101].day_of_week, rows[20240101].day_name, rows[20240101].is_weekend) == (1, "Monday", False)
    assert (rows[20240106].day_of_week, rows[20240106].is_weekend) == (6, True)  # sábado
    assert (rows[20240107].day_of_week, rows[20240107].is_weekend) == (7, True)  # domingo
    assert 20240229 in rows and rows[20240229].month_name == "February" and rows[20240229].quarter == 1


def test_dim_date_completa_tiene_clave_unica(spark):
    df = s2g.build_dim_date(spark)
    s2g.assert_unique_key(df, "date_key", "dim_date")
    assert df.count() == 4748  # 2018-01-01 a 2030-12-31


def test_clave_repetida_o_nula_se_rechaza(spark):
    dup = lookup_df(spark, ("DL", "Delta"), ("DL", "Delta bis"))
    with pytest.raises(ValueError, match="repetida"):
        s2g.assert_unique_key(s2g.build_dim_carrier(dup), "carrier_code", "dim_carrier")
    with pytest.raises(ValueError, match="nulas"):
        s2g.assert_unique_key(spark.createDataFrame([(None,), (1,)], "k int"), "k", "x")


# ------------------------------------------------------------------ hecho

def test_fact_una_fila_por_vuelo_con_claves_y_medidas(spark):
    fact = s2g.build_fact_flights(silver_df(spark, {}, {"flight_date": datetime.date(2024, 1, 9)}))
    assert fact.count() == 2
    row = fact.orderBy("date_key").collect()[0]
    assert (row.date_key, row.carrier_code, row.origin_airport_id, row.dest_airport_id) == (20240108, "9E", 12953, 14122)
    assert (row.flight_number, row.dep_time_blk, row.arr_delay, row.distance) == (3333, "0800-0859", -11.0, 335.0)
    assert {"year", "month", "flight_date", "origin", "dest"}.isdisjoint(fact.columns)  # lo que no va a gold


def test_is_operated_excluye_cancelados_y_desviados(spark):
    fact = s2g.build_fact_flights(silver_df(spark, {}, CANCELLED, DIVERTED))
    by_case = {(r.cancelled, r.diverted): r.is_operated for r in fact.collect()}
    assert by_case == {(0, 0): 1, (1, 0): 0, (0, 1): 0}


def test_demora_negativa_se_conserva(spark):
    """Un vuelo adelantado no es un error: la demora negativa sigue en gold."""
    assert s2g.build_fact_flights(silver_df(spark)).collect()[0].arr_delay == -11.0


# ------------------------------------------------------------------ claves huérfanas y broadcast join

def test_vuelo_con_aeropuerto_que_no_existe_es_huerfano(spark):
    fact = s2g.build_fact_flights(silver_df(spark, {}, {"origin_airport_id": 99999}))
    orphans = s2g.orphan_rows(fact, dim_airport(spark), "origin_airport_id", "airport_id").collect()
    assert [r.origin_airport_id for r in orphans] == [99999]


def test_causa_de_cancelacion_nula_no_es_huerfana_pero_una_inexistente_si(spark):
    dim = s2g.build_dim_cancellation(lookup_df(spark, ("A", "Carrier"), ("B", "Weather")))
    fact = s2g.build_fact_flights(silver_df(spark, {}, CANCELLED, {**CANCELLED, "cancellation_code": "Z"}))
    orphans = s2g.orphan_rows(fact, dim, "cancellation_code", "cancellation_code").collect()
    assert [r.cancellation_code for r in orphans] == ["Z"]


def test_el_chequeo_de_huerfanos_es_un_broadcast_join(spark):
    fact = s2g.build_fact_flights(silver_df(spark))
    plan = s2g.orphan_rows(fact, dim_airport(spark), "origin_airport_id", "airport_id")._jdf.queryExecution() \
        .executedPlan().toString()
    assert "BroadcastHashJoin" in plan and "LeftAnti" in plan, plan


# ------------------------------------------------------------------ agregado

def day_of_flights(spark):
    """4 vuelos del mismo día y aeropuerto: normal, demorado, cancelado (con una demora que no debe contar) y desviado."""
    return s2g.build_fact_flights(silver_df(
        spark,
        {"arr_delay": 10.0, "arr_del15": 0},
        {"arr_delay": 30.0, "arr_del15": 1},
        {**CANCELLED, "arr_delay": 500.0},
        DIVERTED,
    ))


def test_agg_cuenta_y_promedia_solo_vuelos_operados(spark):
    agg = s2g.build_agg_daily_airport(day_of_flights(spark), dim_airport(spark))
    assert agg.count() == 1
    r = agg.collect()[0]
    assert (r.flights, r.cancelled, r.diverted, r.operated, r.delayed_15) == (4, 1, 1, 2, 1)
    assert (r.arr_delay_sum, r.arr_delay_n, r.avg_arr_delay) == (40.0, 2, 20.0)  # el 500 del cancelado no entra
    assert r.pct_delayed == 0.5


def test_agg_trae_los_datos_del_aeropuerto(spark):
    r = s2g.build_agg_daily_airport(day_of_flights(spark), dim_airport(spark)).collect()[0]
    assert (r.date_key, r.airport_id, r.airport_code, r.airport_name, r.city_name, r.state) == \
           (20240108, 12953, "LGA", "LaGuardia", "New York, NY", "NY")


def test_agg_no_pierde_aeropuertos_que_no_estan_en_la_dimension(spark):
    fact = s2g.build_fact_flights(silver_df(spark, {"origin_airport_id": 99999}))
    r = s2g.build_agg_daily_airport(fact, dim_airport(spark)).collect()[0]
    assert r.airport_id == 99999 and r.airport_name is None and r.flights == 1


def test_agg_un_dia_sin_vuelos_operados_no_divide_por_cero(spark):
    fact = s2g.build_fact_flights(silver_df(spark, CANCELLED, CANCELLED))
    r = s2g.build_agg_daily_airport(fact, dim_airport(spark)).collect()[0]
    assert (r.flights, r.operated, r.pct_delayed, r.avg_arr_delay) == (2, 0, None, None)


def test_sumar_dias_con_suma_y_cantidad_da_el_promedio_correcto(spark):
    """Lección de medidas aditivas: el promedio de los promedios diarios NO es el promedio real."""
    day1 = [{"arr_delay": 10.0}]  # 1 vuelo, promedio 10
    day2 = [{"flight_date": datetime.date(2024, 1, 9), "arr_delay": float(d)} for d in (40, 40, 40)]  # 3 vuelos, promedio 40
    fact = s2g.build_fact_flights(silver_df(spark, *day1, *day2))
    agg = s2g.build_agg_daily_airport(fact, dim_airport(spark))
    direct = fact.filter("is_operated = 1").agg(F.avg("arr_delay")).collect()[0][0]
    total = agg.agg((F.sum("arr_delay_sum") / F.sum("arr_delay_n")).alias("v")).collect()[0].v
    average_of_averages = agg.agg(F.avg("avg_arr_delay")).collect()[0][0]
    assert total == pytest.approx(direct) == pytest.approx(32.5)
    assert average_of_averages == pytest.approx(25.0)  # el error que se evita


# ------------------------------------------------------------------ escritura (carpetas e idempotencia)

def test_hecho_en_carpeta_con_mes_de_dos_digitos_y_un_solo_archivo(spark, tmp_path):
    fact = s2g.build_fact_flights(silver_df(spark, {}, {"flight_date": datetime.date(2024, 1, 9)}))
    s2g.write_month(fact, str(tmp_path), "fact_flights", 2024, 1)
    folder = tmp_path / "fact_flights" / "year=2024" / "month=01"
    assert folder.is_dir() and not (tmp_path / "fact_flights" / "year=2024" / "month=1").exists()
    assert len(list(folder.glob("*.parquet"))) == 1


def test_reprocesar_un_mes_no_duplica_y_no_toca_los_otros(spark, tmp_path):
    jan = s2g.build_fact_flights(silver_df(spark, {}, {}))
    feb = s2g.build_fact_flights(silver_df(spark, {"flight_date": datetime.date(2024, 2, 5)}))
    s2g.write_month(jan, str(tmp_path), "fact_flights", 2024, 1)
    s2g.write_month(feb, str(tmp_path), "fact_flights", 2024, 2)
    s2g.write_month(jan, str(tmp_path), "fact_flights", 2024, 1)  # el mismo mes dos veces
    df = spark.read.parquet(str(tmp_path / "fact_flights"))
    assert df.count() == 3 and df.filter("month = 1").count() == 2 and df.filter("month = 2").count() == 1


def test_dimension_se_reemplaza_entera_en_cada_corrida(spark, tmp_path):
    for _ in range(2):
        s2g.write_dimension(dim_airport(spark), str(tmp_path), "dim_airport")
    assert spark.read.parquet(str(tmp_path / "dim_airport")).count() == 3
    assert len(list((tmp_path / "dim_airport").glob("*.parquet"))) == 1


# ------------------------------------------------------------------ datos reales (se saltean si no están)

def read_lookup(spark, name, encoding="UTF-8"):
    return spark.read.option("header", "true").option("encoding", encoding).csv(str(LOOKUPS / name))


@pytest.mark.skipif(not LOOKUPS.exists(), reason="no hay lookups descargados")
def test_lookups_reales_dan_dimensiones_con_clave_unica_y_sin_texto_roto(spark):
    airports = s2g.build_dim_airport(read_lookup(spark, "airports", "ISO-8859-1"), codes_df(spark))
    carriers = s2g.build_dim_carrier(read_lookup(spark, "airlines"))
    causes = s2g.build_dim_cancellation(read_lookup(spark, "cancellation_causes"))
    assert (airports.count(), carriers.count(), causes.count()) == (6915, 1779, 4)
    s2g.assert_unique_key(airports, "airport_id", "dim_airport")
    s2g.assert_unique_key(carriers, "carrier_code", "dim_carrier")
    broken = airports.filter(F.col("airport_name").contains("�") | F.col("city_name").contains("�")).count()
    assert broken == 0  # con la codificación equivocada aparecen caracteres de reemplazo
    assert airports.filter(F.col("state").isNull()).count() > 3000  # los extranjeros quedan sin estado


@pytest.mark.skipif(not (JAN_DIR.exists() and LOOKUPS.exists()), reason="no hay datos descargados")
def test_muestra_real_de_enero_silver_y_gold_dan_lo_mismo(spark):
    import bronze_to_silver as b2s  # el job 1 produce el silver real; si no compila, esta prueba falla

    src = str(next(JAN_DIR.glob("*.csv")))
    raw = spark.read.option("header", "true").csv(src).limit(5000).cache()
    silver, quarantine, _ = b2s.transform(raw, 2024, 1)
    silver = silver.cache()
    assert quarantine.count() == 0

    dim = s2g.build_dim_airport(read_lookup(spark, "airports", "ISO-8859-1"), s2g.airport_codes(silver))
    carriers = s2g.build_dim_carrier(read_lookup(spark, "airlines"))
    causes = s2g.build_dim_cancellation(read_lookup(spark, "cancellation_causes"))
    fact = s2g.build_fact_flights(silver).cache()
    agg = s2g.build_agg_daily_airport(fact, dim).cache()

    assert fact.count() == silver.count()
    assert agg.agg(F.sum("flights")).collect()[0][0] == fact.count()
    for dim_df, fact_key, dim_key in ((dim, "origin_airport_id", "airport_id"), (dim, "dest_airport_id", "airport_id"),
                                      (carriers, "carrier_code", "carrier_code"),
                                      (causes, "cancellation_code", "cancellation_code")):
        assert s2g.orphan_rows(fact, dim_df, fact_key, dim_key).count() == 0, fact_key
    assert s2g.ambiguous_airport_codes(s2g.airport_codes(silver)).count() == 0

    # la pregunta de negocio, calculada desde silver y desde gold, debe coincidir
    from_silver = (silver.filter((F.col("cancelled") == 0) & (F.col("diverted") == 0))
                   .agg(F.avg("arr_delay")).collect()[0][0])
    from_gold = agg.agg((F.sum("arr_delay_sum") / F.sum("arr_delay_n")).alias("v")).collect()[0].v
    assert from_gold == pytest.approx(from_silver)
