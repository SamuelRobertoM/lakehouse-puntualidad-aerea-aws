"""Job 1 de Glue: bronze (CSV de BTS) a silver (Parquet) y quarantine.

La lógica son funciones puras de PySpark (reciben y devuelven DataFrames): se prueban en local
con pytest y el mismo código corre en Glue. Lo propio de Glue vive solo en main().

Flujo de transform():
    CSV crudo (todo texto) -> snake_case y sin columna sin nombre -> vacíos a null
    -> quitar duplicados exactos -> evaluar reglas fila por fila
    -> sin motivo: silver (tipado, horas 2400 normalizadas)   con motivo: quarantine (crudo + motivo)
"""
import re
import sys

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# ---------------------------------------------------------------- nombres de columnas

IRREGULAR_NAMES = {"DayofMonth": "day_of_month", "DayOfWeek": "day_of_week"}


def to_snake_case(name: str) -> str:
    if name in IRREGULAR_NAMES:
        return IRREGULAR_NAMES[name]
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)  # CRSDepTime -> CRS_DepTime
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)  # DepTime -> Dep_Time
    return s.lower()


def rename_to_snake_case(df: DataFrame) -> DataFrame:
    """Pasa los nombres a snake_case y descarta la columna sin nombre (la coma final del CSV)."""
    keep = [c for c in df.columns if c.strip() and not re.fullmatch(r"_c\d+", c)]
    new = [to_snake_case(c) for c in keep]
    if len(set(new)) != len(new):
        dup = sorted({n for n in new if new.count(n) > 1})
        raise ValueError(f"nombres repetidos después de pasar a snake_case: {dup}")
    return df.select(*[F.col(f"`{c}`").alias(n) for c, n in zip(keep, new)])


def blank_to_null(df: DataFrame) -> DataFrame:
    """En el CSV lo vacío significa 'sin dato': se guarda como null."""
    return df.select(*[
        F.when(F.trim(F.col(f"`{c}`")) == "", F.lit(None)).otherwise(F.trim(F.col(f"`{c}`"))).alias(c)
        for c in df.columns
    ])


def drop_exact_duplicates(df: DataFrame) -> DataFrame:
    return df.dropDuplicates()


# ---------------------------------------------------------------- tipos de columnas

_DIV = range(1, 6)  # BTS informa hasta 5 aeropuertos de desvío

TIME_COLS = (
    ["crs_dep_time", "dep_time", "wheels_off", "wheels_on", "crs_arr_time", "arr_time", "first_dep_time"]
    + [f"div{n}_wheels_{w}" for n in _DIV for w in ("on", "off")]
)  # texto HHMM
INT_COLS = (
    ["year", "quarter", "month", "day_of_month", "day_of_week", "dot_id_reporting_airline",
     "flight_number_reporting_airline", "origin_airport_id", "origin_airport_seq_id",
     "origin_city_market_id", "origin_state_fips", "origin_wac", "dest_airport_id",
     "dest_airport_seq_id", "dest_city_market_id", "dest_state_fips", "dest_wac",
     "departure_delay_groups", "arrival_delay_groups", "distance_group", "div_airport_landings", "flights"]
    + [f"div{n}_airport_{k}" for n in _DIV for k in ("id", "seq_id")]
)
FLAG_COLS = ["dep_del15", "arr_del15", "cancelled", "diverted", "div_reached_dest"]  # 0.00 / 1.00
DOUBLE_COLS = (
    ["dep_delay", "dep_delay_minutes", "taxi_out", "taxi_in", "arr_delay", "arr_delay_minutes",
     "crs_elapsed_time", "actual_elapsed_time", "air_time", "distance", "carrier_delay", "weather_delay",
     "nas_delay", "security_delay", "late_aircraft_delay", "total_add_g_time", "longest_add_g_time",
     "div_actual_elapsed_time", "div_arr_delay", "div_distance"]
    + [f"div{n}_{k}_g_time" for n in _DIV for k in ("total", "longest")]
)
DATE_COL = "flight_date"
DATE_FORMAT = "yyyy-MM-dd"
TIME_PATTERN = r"^([01][0-9]|2[0-3])[0-5][0-9]$"

# Sin estas columnas el job no puede decidir nada: si falta alguna, el esquema cambió.
REQUIRED_COLS = ["flight_date", "year", "month", "reporting_airline", "origin_airport_id",
                 "dest_airport_id", "distance", "cancelled", "cancellation_code"]
# Y estas no pueden venir vacías (cancellation_code sí: solo existe en vuelos cancelados).
REQUIRED_NOT_NULL = ["flight_date", "year", "month", "reporting_airline", "origin_airport_id",
                     "dest_airport_id", "distance", "cancelled"]


def _num(c: str) -> Column:
    return F.col(c).cast("double")


def _int(c: str) -> Column:
    return F.col(c).cast("double").cast("int")  # "5.00" -> 5


def _date() -> Column:
    return F.to_date(F.col(DATE_COL), DATE_FORMAT)


def _midnight(c: str) -> Column:
    """BTS escribe 2400 para la medianoche; no es una hora válida y pasa a 0000."""
    return F.when(F.col(c) == "2400", F.lit("0000")).otherwise(F.col(c))


# ---------------------------------------------------------------- reglas de rechazo

def add_rejection_reason(df: DataFrame, year: int, month: int) -> DataFrame:
    """Agrega rejection_reason: null si la fila está bien, o los motivos separados por '|'.

    Lo que NO se rechaza, a propósito: demoras negativas (vuelo adelantado), ArrDelay vacío en
    cancelados y desviados, y las 5 columnas de causa vacías en vuelos con menos de 15 minutos.
    """
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"faltan columnas obligatorias, ¿cambió el esquema?: {missing}")
    cols = set(df.columns)
    checks: list[Column] = []

    for c in REQUIRED_NOT_NULL:
        checks.append(F.when(F.col(c).isNull(), F.lit(f"campo_obligatorio_vacio:{c}")))

    for c in INT_COLS + DOUBLE_COLS + FLAG_COLS:
        if c in cols:
            checks.append(F.when(F.col(c).isNotNull() & _num(c).isNull(), F.lit(f"tipo_invalido:{c}")))
    checks.append(F.when(F.col(DATE_COL).isNotNull() & _date().isNull(), F.lit(f"tipo_invalido:{DATE_COL}")))

    for c in FLAG_COLS:
        if c in cols:
            checks.append(F.when(_num(c).isNotNull() & ~_num(c).isin(0.0, 1.0), F.lit(f"flag_invalido:{c}")))

    for c in TIME_COLS:
        if c in cols:
            checks.append(F.when(F.col(c).isNotNull() & ~_midnight(c).rlike(TIME_PATTERN),
                                 F.lit(f"hora_invalida:{c}")))

    in_month = ((_int("year") == year) & (_int("month") == month)
                & (F.year(_date()) == year) & (F.month(_date()) == month))
    checks.append(F.when(~in_month, F.lit("fecha_fuera_de_mes")))
    checks.append(F.when(_num("distance") <= 0, F.lit("distancia_no_positiva")))
    checks.append(F.when((_num("cancelled") == 1) & F.col("cancellation_code").isNull(),
                         F.lit("cancelado_sin_codigo")))

    reasons = F.array_compact(F.array(*checks))
    return df.withColumn("rejection_reason", F.when(F.size(reasons) > 0, F.concat_ws("|", reasons)))


# ---------------------------------------------------------------- silver

def cast_types(df: DataFrame) -> DataFrame:
    """Pone los tipos reales. Solo se llama con filas que ya pasaron las reglas."""
    out = []
    for c in df.columns:
        if c == DATE_COL:
            expr = _date()
        elif c in INT_COLS or c in FLAG_COLS:
            expr = _int(c)
        elif c in DOUBLE_COLS:
            expr = _num(c)
        elif c in TIME_COLS:
            expr = _midnight(c)
        else:
            expr = F.col(c)
        out.append(expr.alias(c))
    return df.select(*out)


def transform(df_raw: DataFrame, year: int, month: int) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Devuelve (silver, quarantine, unicas). `unicas` sirve para conciliar las filas:
    filas leídas = silver + quarantine + duplicados eliminados."""
    unique = drop_exact_duplicates(blank_to_null(rename_to_snake_case(df_raw)))
    classified = add_rejection_reason(unique, year, month)
    silver = cast_types(classified.filter(F.col("rejection_reason").isNull()).drop("rejection_reason"))
    quarantine = (classified.filter(F.col("rejection_reason").isNotNull())
                  .withColumn("source_year", F.lit(year))
                  .withColumn("source_month", F.lit(month)))
    return silver, quarantine, unique


# ---------------------------------------------------------------- escritura

def write_outputs(silver: DataFrame, quarantine: DataFrame, silver_path: str, quarantine_path: str,
                  year: int, month: int) -> None:
    """Escribe cada mes en su propia carpeta (year=2024/month=01), igual que bronce.

    Idempotente: reprocesar un mes reemplaza esa carpeta entera, incluso si ahora no hay filas
    (con partitionOverwriteMode dinámico una carpeta que quedó vacía no se limpiaría).
    year y month viajan en la ruta, no dentro de los archivos. repartition(1): ~25 MB por mes
    es un solo archivo; sin esto Spark escribe uno por tarea (small files problem).
    """
    silver_dir = f"{silver_path.rstrip('/')}/year={year}/month={month:02d}/"
    quarantine_dir = f"{quarantine_path.rstrip('/')}/source_year={year}/source_month={month:02d}/"
    # mode("overwrite") es lo que hace idempotente al job: correrlo dos veces deja lo mismo, no el doble
    silver.drop("year", "month").repartition(1).write.mode("overwrite").parquet(silver_dir)
    (quarantine.drop("source_year", "source_month").repartition(1)
     .write.mode("overwrite").parquet(quarantine_dir))


# ---------------------------------------------------------------- envoltorio de Glue

def main() -> None:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "bronze_path", "silver_path", "quarantine_path",
                                         "year", "month"])
    year, month = int(args["year"]), int(args["month"])
    glue = GlueContext(SparkContext.getOrCreate())
    spark = glue.spark_session
    job = Job(glue)
    job.init(args["JOB_NAME"], args)

    src = f"{args['bronze_path'].rstrip('/')}/flights/year={year}/month={month:02d}/"
    df_raw = spark.read.option("header", "true").csv(src)  # todo texto: los tipos los pone silver
    silver, quarantine, unique = transform(df_raw, year, month)
    n_raw, n_unique = df_raw.count(), unique.cache().count()
    n_silver, n_quarantine = silver.count(), quarantine.count()
    print(f"filas leídas={n_raw} duplicados eliminados={n_raw - n_unique} "
          f"silver={n_silver} quarantine={n_quarantine}")
    if n_unique != n_silver + n_quarantine:
        raise RuntimeError("las filas no cierran: se perdió o se duplicó alguna")

    write_outputs(silver, quarantine, f"{args['silver_path'].rstrip('/')}/flights/",
                  f"{args['quarantine_path'].rstrip('/')}/flights/", year, month)
    job.commit()


if __name__ == "__main__":
    main()
