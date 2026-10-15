-- Consultas clave del proyecto, tal como se corrieron en Athena (workgroup flights-wg).
-- Las claves de partición year y month son texto: se comparan con comillas ('2024', '01').

-- 1. Filas por mes en gold (dan 547.271, 519.221 y 591.767)
SELECT month, COUNT(*) AS filas_fact
FROM flights_gold.fact_flights
GROUP BY month
ORDER BY month;

-- 2. La pregunta de negocio desde SILVER: demora media de llegada de los vuelos operados, por aeropuerto
SELECT origin AS airport_code, COUNT(*) AS n, ROUND(AVG(arr_delay), 2) AS demora
FROM flights_silver.flights
WHERE year = '2024' AND month = '01' AND cancelled = 0 AND diverted = 0
GROUP BY origin
HAVING COUNT(*) >= 5000
ORDER BY demora DESC
LIMIT 5;

-- 3a. La misma pregunta desde GOLD, forma INCORRECTA: promedio de promedios diarios (da ORD 23,16 y no 19,39)
SELECT airport_code, SUM(operated) AS n, ROUND(AVG(avg_arr_delay), 2) AS demora
FROM flights_gold.agg_daily_airport
WHERE year = '2024' AND month = '01'
GROUP BY airport_code
HAVING SUM(operated) >= 5000
ORDER BY demora DESC
LIMIT 5;

-- 3b. La misma pregunta desde GOLD, forma CORRECTA: suma ÷ cantidad (medidas aditivas). Coincide con silver
SELECT airport_code, SUM(operated) AS n, ROUND(SUM(arr_delay_sum) / SUM(arr_delay_n), 2) AS demora
FROM flights_gold.agg_daily_airport
WHERE year = '2024' AND month = '01'
GROUP BY airport_code
HAVING SUM(operated) >= 5000
ORDER BY demora DESC
LIMIT 5;

-- 4. Join del hecho con una dimensión: las tres aerolíneas con más demora en febrero
SELECT c.carrier_name, COUNT(*) AS vuelos, ROUND(AVG(f.arr_delay), 2) AS demora
FROM flights_gold.fact_flights f
JOIN flights_gold.dim_carrier c ON f.carrier_code = c.carrier_code
WHERE f.year = '2024' AND f.month = '02' AND f.is_operated = 1
GROUP BY c.carrier_name
ORDER BY demora DESC
LIMIT 3;

-- 5. Seguridad con Lake Formation: la MISMA consulta con tres identidades (ver aws/scripts/demo-analistas.sh)
--    Admin: 15 aerolíneas. Analista sin matrícula: 15. Analista de una aerolínea: 1 (DL).
SELECT carrier_code, COUNT(*) AS vuelos
FROM flights_gold.fact_flights
WHERE year = '2024' AND month = '01'
GROUP BY carrier_code
ORDER BY vuelos DESC;

--    El analista sin matrícula recibe COLUMN_NOT_FOUND (la columna "no existe" para él)
SELECT tail_number FROM flights_gold.fact_flights LIMIT 5;

--    El analista de una aerolínea recibe TABLE_NOT_FOUND: no tiene SELECT sobre el agregado de todas las aerolíneas
SELECT date_key, airport_code, flights
FROM flights_gold.agg_daily_airport
ORDER BY date_key, airport_code
LIMIT 2;

-- 6. Una partición nueva que el catálogo todavía no conoce devuelve 0 filas SIN error.
--    Se registra con el crawler, con esta sentencia (solo carpetas clave=valor) o con ALTER TABLE ... ADD PARTITION
MSCK REPAIR TABLE flights_silver.flights;
