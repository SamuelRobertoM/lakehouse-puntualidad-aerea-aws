"""Job 2 de Glue: silver (Parquet) a gold (esquema estrella).

Igual que el job 1: la lógica son funciones puras de PySpark (reciben y devuelven DataFrames), se
prueban en local con pytest y el mismo código corre en Glue. Lo propio de Glue vive solo en main().

Flujo:
    lookups de bronce (CSV)  -> dim_airport, dim_carrier, dim_cancellation      (completas)
    calendario generado      -> dim_date                                        (completa)
    silver del mes           -> fact_flights -> chequeo de claves huérfanas -> agg_daily_airport
Se valida todo en memoria y recién después se escribe: si algo no cierra, gold no se toca.
"""
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Rango fijo del calendario: no depende del mes que se procesa, así dim_date siempre da lo mismo.
DATE_START, DATE_END = "2018-01-01", "2030-12-31"

# Medidas del hecho, con los nombres que dejó silver.
FACT_MEASURES = [
    "dep_delay", "dep_delay_minutes", "arr_delay", "arr_delay_minutes", "dep_del15", "arr_del15",
    "taxi_out", "taxi_in", "air_time", "distance",
    "carrier_delay", "weather_delay", "nas_delay", "security_delay", "late_aircraft_delay",
    "cancelled", "diverted",
]

# "Atlanta, GA: Hartsfield-Jackson Atlanta International" -> ubicación y nombre, partiendo en el primer ": ".
# Los extranjeros ("Anaa, French Polynesia: Anaa Airport") parten igual, pero su ubicación no termina en ", ST".
_SPLIT_RE = r"^(.+?): (.+)$"
_STATE_RE = r", ([A-Z]{2})$"


# ---------------------------------------------------------------- dimensiones desde los lookups

def _lookup(raw: DataFrame) -> DataFrame:
    """Los 3 lookups de BTS tienen las mismas columnas: Code y Description."""
    return raw.select(F.trim(F.col("Code")).alias("code"), F.trim(F.col("Description")).alias("description"))


def build_dim_cancellation(raw: DataFrame) -> DataFrame:
    return _lookup(raw).select(F.col("code").alias("cancellation_code"),
                               F.col("description").alias("cancellation_reason"))


def build_dim_carrier(raw: DataFrame) -> DataFrame:
    return _lookup(raw).select(F.col("code").alias("carrier_code"), F.col("description").alias("carrier_name"))


def airport_codes(silver: DataFrame) -> DataFrame:
    """Código IATA (ATL) de cada aeropuerto, sacado de los vuelos: el lookup no lo trae.

    Devuelve también cuántos códigos distintos vio por aeropuerto (n_codes): debería ser 1.
    """
    origin = silver.select(F.col("origin_airport_id").alias("airport_id"), F.col("origin").alias("airport_code"))
    dest = silver.select(F.col("dest_airport_id").alias("airport_id"), F.col("dest").alias("airport_code"))
    return (origin.union(dest)
            .filter(F.col("airport_id").isNotNull() & F.col("airport_code").isNotNull())
            .groupBy("airport_id")
            .agg(F.min("airport_code").alias("airport_code"), F.countDistinct("airport_code").alias("n_codes")))


def ambiguous_airport_codes(codes: DataFrame) -> DataFrame:
    """Aeropuertos con más de un código: si hay alguno, `min` elegiría uno en silencio."""
    return codes.filter(F.col("n_codes") > 1)


def build_dim_airport(raw: DataFrame, codes: DataFrame) -> DataFrame:
    """Los 6.915 aeropuertos del lookup, también los extranjeros (state queda null).

    city_name conserva el formato de BTS ("Atlanta, GA"), el mismo de origin_city_name en silver.
    airport_code es null para los aeropuertos que no aparecen en ningún vuelo.
    """
    desc = F.col("description")
    has_split = desc.rlike(_SPLIT_RE)
    location = F.when(has_split, F.regexp_extract(desc, _SPLIT_RE, 1))
    base = _lookup(raw).select(
        F.col("code").cast("int").alias("airport_id"),
        location.alias("city_name"),
        F.when(location.rlike(_STATE_RE), F.regexp_extract(location, _STATE_RE, 1)).alias("state"),
        F.when(has_split, F.regexp_extract(desc, _SPLIT_RE, 2)).otherwise(desc).alias("airport_name"),
    )
    return (base.join(F.broadcast(codes.select("airport_id", "airport_code")), "airport_id", "left")
            .select("airport_id", "airport_code", "airport_name", "city_name", "state"))


def build_dim_date(spark: SparkSession, start: str = DATE_START, end: str = DATE_END) -> DataFrame:
    """Un día por fila. day_of_week como BTS: 1 = lunes ... 7 = domingo."""
    days = spark.sql(f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS date")
    dow = ((F.dayofweek("date") + 5) % 7) + 1  # dayofweek de Spark: 1 = domingo
    return days.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("date"),
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.dayofmonth("date").alias("day_of_month"),
        dow.alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
        (dow >= 6).alias("is_weekend"),
    )


def assert_unique_key(df: DataFrame, key: str, name: str) -> None:
    """Una dimensión con la clave repetida o nula duplicaría o perdería vuelos en el join."""
    if df.filter(F.col(key).isNull()).limit(1).count() > 0:
        raise ValueError(f"{name}: hay claves {key} nulas")
    if df.groupBy(key).count().filter(F.col("count") > 1).limit(1).count() > 0:
        raise ValueError(f"{name}: la clave {key} está repetida, el join duplicaría vuelos")


# ---------------------------------------------------------------- hecho y control de claves

def build_fact_flights(silver: DataFrame) -> DataFrame:
    """Una fila por vuelo, con claves a las dimensiones y las medidas.

    is_operated = ni cancelado ni desviado: los promedios de demora se calculan solo sobre estos.
    Si cancelled o diverted vinieran null, is_operated queda null (no se inventa el dato).
    """
    return silver.select(
        F.date_format("flight_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("reporting_airline").alias("carrier_code"),
        "origin_airport_id",
        "dest_airport_id",
        "cancellation_code",
        F.col("flight_number_reporting_airline").alias("flight_number"),
        "tail_number",
        "dep_time_blk",
        "arr_time_blk",
        *FACT_MEASURES,
        ((F.col("cancelled") == 0) & (F.col("diverted") == 0)).cast("int").alias("is_operated"),
    )


def orphan_rows(fact: DataFrame, dim: DataFrame, fact_key: str, dim_key: str) -> DataFrame:
    """Vuelos cuya clave no existe en la dimensión (integridad referencial).

    Es un left_anti entre el hecho (cientos de miles de filas) y una dimensión chica: la dimensión
    se copia entera a cada worker (broadcast join) y el hecho no se mueve (sin shuffle).
    Las claves nulas no cuentan: cancellation_code es null en los vuelos que no se cancelaron.
    """
    keys = dim.select(F.col(dim_key).alias("_dim_key"))
    return (fact.filter(F.col(fact_key).isNotNull())
            .join(F.broadcast(keys), F.col(fact_key) == F.col("_dim_key"), "left_anti"))


# ---------------------------------------------------------------- agregado para el dashboard

def build_agg_daily_airport(fact: DataFrame, dim_airport: DataFrame) -> DataFrame:
    """Un día × aeropuerto de origen. Demorado = llegada con 15 minutos o más, solo en vuelos operados.

    Guarda la suma y la cantidad de la demora, además del promedio: un promedio de promedios es
    incorrecto al juntar días o aeropuertos, la suma y la cantidad sí se pueden sumar.
    """
    operated = F.col("is_operated") == 1
    daily = fact.groupBy("date_key", F.col("origin_airport_id").alias("airport_id")).agg(
        F.count(F.lit(1)).alias("flights"),
        F.sum("cancelled").alias("cancelled"),
        F.sum("diverted").alias("diverted"),
        F.sum("is_operated").alias("operated"),
        F.sum(F.when(operated & (F.col("arr_del15") == 1), 1).otherwise(0)).alias("delayed_15"),
        F.sum(F.when(operated, F.col("arr_delay"))).alias("arr_delay_sum"),
        F.count(F.when(operated, F.col("arr_delay"))).alias("arr_delay_n"),
    )
    daily = (daily
             .withColumn("pct_delayed", F.when(F.col("operated") > 0, F.col("delayed_15") / F.col("operated")))
             .withColumn("avg_arr_delay", F.when(F.col("arr_delay_n") > 0,
                                                 F.col("arr_delay_sum") / F.col("arr_delay_n"))))
    airports = F.broadcast(dim_airport.select("airport_id", "airport_code", "airport_name", "city_name", "state"))
    return (daily.join(airports, "airport_id", "left")
            .select("date_key", "airport_id", "airport_code", "airport_name", "city_name", "state",
                    "flights", "cancelled", "diverted", "operated", "delayed_15", "pct_delayed",
                    "arr_delay_sum", "arr_delay_n", "avg_arr_delay"))


# ---------------------------------------------------------------- escritura

def write_dimension(df: DataFrame, gold_path: str, table: str) -> None:
    """Dimensión completa: se reemplaza entera en cada corrida. Pesa KB: un solo archivo y sin particiones."""
    df.repartition(1).write.mode("overwrite").parquet(f"{gold_path.rstrip('/')}/{table}/")


def write_month(df: DataFrame, gold_path: str, table: str, year: int, month: int) -> None:
    """Hecho y agregado: una carpeta por mes (year=2024/month=01), como silver. Reprocesar un mes
    reemplaza solo esa carpeta (idempotente). year y month viajan en la ruta, no en los archivos."""
    df.repartition(1).write.mode("overwrite").parquet(f"{gold_path.rstrip('/')}/{table}/year={year}/month={month:02d}/")


# ---------------------------------------------------------------- envoltorio de Glue

def main() -> None:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "bronze_path", "silver_path", "gold_path", "year", "month"])
    year, month = int(args["year"]), int(args["month"])
    bronze, silver_path, gold = (args[k].rstrip("/") for k in ("bronze_path", "silver_path", "gold_path"))
    glue = GlueContext(SparkContext.getOrCreate())
    spark = glue.spark_session
    job = Job(glue)
    job.init(args["JOB_NAME"], args)

    def lookup(name: str, encoding: str = "UTF-8") -> DataFrame:
        return (spark.read.option("header", "true").option("encoding", encoding)
                .csv(f"{bronze}/lookups/{name}/"))

    # El lookup de aeropuertos no está en UTF-8 (Latin-1): sin indicarlo se rompen los acentos.
    raw_airports, raw_airlines, raw_causes = lookup("airports", "ISO-8859-1"), lookup("airlines"), lookup("cancellation_causes")

    # Los códigos IATA salen de TODO silver (no solo del mes) para que dim_airport no dependa del mes.
    codes = airport_codes(spark.read.parquet(f"{silver_path}/flights/year=*/month=*/")).cache()
    if ambiguous_airport_codes(codes).count() > 0:
        raise RuntimeError("hay aeropuertos con más de un código IATA en silver")

    dim_airport = build_dim_airport(raw_airports, codes)
    dim_carrier = build_dim_carrier(raw_airlines)
    dim_cancellation = build_dim_cancellation(raw_causes)
    dim_date = build_dim_date(spark)
    for df, key, name in ((dim_airport, "airport_id", "dim_airport"), (dim_carrier, "carrier_code", "dim_carrier"),
                          (dim_cancellation, "cancellation_code", "dim_cancellation"), (dim_date, "date_key", "dim_date")):
        assert_unique_key(df, key, name)

    silver = spark.read.parquet(f"{silver_path}/flights/year={year}/month={month:02d}/")
    fact = build_fact_flights(silver).cache()
    n_silver, n_fact = silver.count(), fact.count()
    orphans = {
        "origen": orphan_rows(fact, dim_airport, "origin_airport_id", "airport_id").count(),
        "destino": orphan_rows(fact, dim_airport, "dest_airport_id", "airport_id").count(),
        "aerolinea": orphan_rows(fact, dim_carrier, "carrier_code", "carrier_code").count(),
        "causa": orphan_rows(fact, dim_cancellation, "cancellation_code", "cancellation_code").count(),
    }
    agg = build_agg_daily_airport(fact, dim_airport).cache()
    n_agg = agg.count()
    n_agg_flights = agg.agg(F.sum("flights")).collect()[0][0]
    print(f"silver={n_silver} fact={n_fact} agg={n_agg} vuelos en agg={n_agg_flights} huérfanos={orphans} "
          f"aeropuertos={dim_airport.count()} aerolíneas={dim_carrier.count()}")
    if n_fact != n_silver or n_agg_flights != n_fact:
        raise RuntimeError("las filas no cierran entre silver, fact y agg")
    if any(orphans.values()):
        raise RuntimeError(f"hay claves del hecho que no existen en las dimensiones: {orphans}")

    write_dimension(dim_airport, gold, "dim_airport")
    write_dimension(dim_carrier, gold, "dim_carrier")
    write_dimension(dim_cancellation, gold, "dim_cancellation")
    write_dimension(dim_date, gold, "dim_date")
    write_month(fact, gold, "fact_flights", year, month)
    write_month(agg, gold, "agg_daily_airport", year, month)
    job.commit()


if __name__ == "__main__":
    main()
