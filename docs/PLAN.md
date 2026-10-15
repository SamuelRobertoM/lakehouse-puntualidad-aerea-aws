# Plan del lakehouse de puntualidad aérea de EE. UU. en AWS (plan gratuito)

Este es el plan de construcción. Las reglas de cómo trabajar están en `CLAUDE.md`.

## Qué vamos a construir

Un lakehouse con arquitectura medallion sobre los vuelos domésticos de EE. UU. Los archivos mensuales entran a S3, dos jobs de Glue los llevan por tres capas, y el resultado se consulta con Athena, se gobierna con Lake Formation y se ve en un dashboard. Después se le agrega orquestación, calidad de datos y monitoreo.

Todo usa servicios que entran en el **plan gratuito** de AWS. No hay Redshift, Kinesis ni Firehose: esa versión completa vive en `practice-01` y queda para cuando se pase al plan de pago.

El diagrama está en `docs/arquitectura-gratis.svg`.

```
BTS On-Time (zip mensual) ──> script ──> S3 bronce ──> Glue job 1 ──> S3 plata ──> Glue job 2 ──> S3 oro
 + 3 tablas de referencia      unzip       (CSV)           │           (Parquet)                  (estrella)
                              + sync                       └─> cuarentena     │                      │
                                                                         Glue Crawler ──> Data Catalog <── Athena ──> Quick (opcional)

Orquestación:  EventBridge ──> Step Functions ──> job 1 ─> Glue Data Quality ─> job 2 ─> crawler
Monitoreo:     CloudWatch (logs + alarma) ──> SNS ──> mail        AWS Budgets
Seguridad:     KMS key (SSE-KMS en los 3 buckets) · IAM role de Glue · Lake Formation (columna y fila) · CloudTrail
IaC:           CloudFormation, un stack por etapa (base, orquestación)
```

### El dataset

**BTS On-Time Performance**: el registro oficial de puntualidad de los vuelos domésticos que publica el Bureau of Transportation Statistics (Departamento de Transporte de EE. UU.). Un archivo por mes.

| Archivo | Qué es | Tamaño | Filas |
|---|---|---|---|
| `..._1987_present_2024_1.zip` | Vuelos de enero 2024 | 27,6 MB zip · 246,8 MB CSV | 547.271 |
| `..._1987_present_2024_2.zip` | Vuelos de febrero 2024 | 25,1 MB zip · 234,4 MB CSV | 519.221 |
| Aerolíneas (lookup) | Código y nombre | 54 KB | 1.779 |
| Aeropuertos (lookup) | ID y descripción `Ciudad, ST: Aeropuerto` | 333 KB | 6.915 |
| Causas de cancelación (lookup) | A, B, C, D | 91 B | 4 |

Las URLs se verificaron con la descarga real (los archivos mensuales llevan el prefijo `On_Time_Reporting_Carrier_On_Time_Performance_`):

```
https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2024_1.zip
https://www.transtats.bts.gov/Download_Lookup.asp?Y11x72=Y_haVdhR_PNeeVRef     (aerolíneas)
https://www.transtats.bts.gov/Download_Lookup.asp?Y11x72=Y_NVecbeg_VQ          (aeropuertos)
https://www.transtats.bts.gov/Download_Lookup.asp?Y11x72=Y_PNaPRYYNgVba        (cancelaciones)
```

Marzo de 2024 (29,3 MB, verificado) queda sin bajar a propósito: se usa en la fase 4 para probar que el pipeline procesa solo un mes nuevo.

**Lo que se midió en los archivos reales** (leídos en enero y febrero de 2024, con scripts de lectura sin modificar nada):

- 110 columnas, y la última no tiene nombre (coma final del CSV). Todo viaja como texto: los tipos los pone silver.
- **El esquema es estable**: enero y febrero de 2024, junio de 2025 y marzo de 2026 tienen las mismas 110 columnas en el mismo orden. No hay deriva natural de nombres como en los taxis. La prueba de esquema de la fase 1 usa una copia modificada a propósito y se declara como sintética.
- Sin filas duplicadas exactas, sin fechas fuera del mes, distancia siempre mayor que cero. **Los datos reales están casi limpios**, así que la cuarentena puede quedar casi vacía. Para ver el flujo completo (fase 4) se genera un archivo de prueba con defectos inyectados, marcado como sintético.
- Horas con valor `2400` (en enero: 239 llegadas, 201 aterrizajes, 58 despegues de pista y 40 salidas). No es una hora válida; hay que normalizarla.
- 5.293 vuelos sin matrícula (`Tail_Number`), cerca del 1%.
- Cancelados: 3,73% en enero (20.389) y 0,58% en febrero. Los cancelados y los 1.512 desviados no tienen `ArrDelay`.
- **Las demoras negativas son válidas**: 306.999 vuelos de enero (56%) llegaron antes de la hora. Si se copia la regla "importe no negativo" de los taxis, se descartaría más de la mitad del mes.
- Las 5 columnas de causa de demora (`CarrierDelay`, `WeatherDelay`, `NASDelay`, `SecurityDelay`, `LateAircraftDelay`) solo vienen informadas en los 126.410 vuelos con `ArrDel15 = 1` (23%). En el resto están vacías.
- 67 vuelos con demora de más de 24 horas: valores extremos que hay que decidir si se marcan.
- Zip: Spark no lee `.zip`, así que el script lo descomprime antes de subir a bronce.
- **Licencia**: no pude verificar un texto de licencia explícito en la página de BTS. Son datos del gobierno de EE. UU.; revisá los términos en transportation.gov antes de publicar nada. Los datos no van al repo.

### Preguntas de negocio (propuesta)

El analista es el equipo de operaciones de una aerolínea ficticia, con datos públicos reales.

| # | Pregunta | Decisión que habilita | Qué exige a gold |
|---|---|---|---|
| 1 | ¿Qué aeropuertos y aerolíneas concentran más demoras y cancelaciones? | Dónde reforzar la operación y los márgenes de programación | `agg_daily_airport`, `dim_airport`, `dim_carrier` |
| 2 | ¿Qué causas explican las demoras y cambian por mes y aeropuerto? | Invertir en tripulación y mantenimiento (causa aerolínea) o coordinar con el control aéreo (sistema nacional) | Las 5 medidas de causa en `fact_flights` |
| 3 | ¿Qué rutas y franjas horarias tienen peor puntualidad? | Reprogramar vuelos | Origen, destino y bloque horario en `fact_flights` |

Una pregunta transversal: ¿cuántos vuelos se corrigen o se rechazan, y por qué? Se responde con la cuarentena.

Tres trampas de interpretación, equivalentes a las propinas en efectivo de los taxis: los promedios de demora se calculan solo sobre vuelos operados (sin cancelados ni desviados), las causas solo existen para vuelos con 15 minutos o más de demora, y una demora negativa es un vuelo adelantado, no un error.

### Las capas

| Capa | Qué guarda | Formato | Regla |
|---|---|---|---|
| **Bronze** | Los archivos tal como salen del zip, más las 3 tablas de referencia | CSV originales | Inmutable. Nunca se edita ni se limpia acá. Prefijo por fuente y por mes |
| **Silver** | Los mismos vuelos, limpios y tipados | Parquet con Snappy, particionado por `year` y `month` | Nombres en snake_case, tipos reales, columna vacía descartada, horas `2400` normalizadas, sin duplicados, solo vuelos del mes que corresponde |
| **Cuarentena** | Las filas que silver rechaza, con el motivo | Parquet | Nada se descarta en silencio: cada fila rechazada queda guardada con su causa |
| **Gold** | Modelo de negocio | Parquet | Esquema estrella y una tabla agregada para el dashboard |

Se arranca con **Parquet plano**, porque crawlers, particiones y `MSCK REPAIR` son lo que más pregunta el examen. En la fase 1 explicame qué gano y qué pierdo respecto de Delta. Como extra de la fase 2, se puede pasar gold a **Apache Iceberg**.

### El modelo de gold (definido en la fase 2, con lo medido en los lookups)

Un solo job (`glue/silver_to_gold.py`) con parámetros `year` y `month`. Claves **naturales** (`DL`, `10397`, `20240131`): los lookups son una foto sin historia, o sea dimensiones SCD tipo 1 (se sobrescriben), y no hace falta clave sustituta.

| Tabla | Una fila por | Clave | Origen | Escritura |
|---|---|---|---|---|
| `fact_flights` | vuelo | `date_key`, `carrier_code`, `origin_airport_id`, `dest_airport_id`, `cancellation_code` | silver | carpeta `year=/month=` del mes, `overwrite` |
| `agg_daily_airport` | día × aeropuerto de origen | `date_key`, `airport_id` | `fact_flights` | carpeta del mes, `overwrite` |
| `dim_airport` | aeropuerto (6.915) | `airport_id` | lookup + código IATA de silver | completa, `overwrite` |
| `dim_carrier` | aerolínea (1.779) | `carrier_code` | lookup | completa, `overwrite` |
| `dim_cancellation` | causa (4: A carrier, B weather, C national air system, D security) | `cancellation_code` | lookup | completa, `overwrite` |
| `dim_date` | día (2018 a 2030) | `date_key` | generada con `sequence()` | completa, `overwrite` |

- `fact_flights`: vuelo y matrícula, bloque horario de salida, demoras de salida y llegada, distancia, tiempo en el aire, taxi, las 5 causas, las marcas `cancelled`, `diverted`, `dep_del15`, `arr_del15` y `is_operated` (`cancelled = 0 y diverted = 0`, para dejar escrita una vez la trampa de los promedios).
- `agg_daily_airport`: vuelos, cancelados, desviados, operados, demorados de 15 minutos o más (llegada, solo operados), `pct_delayed` y demora de llegada promedio de operados. Guarda también la **suma** y la **cantidad** de la demora, porque un promedio de promedios es incorrecto al sumar días o aeropuertos (medida aditiva). Trae nombre, ciudad y estado del aeropuerto para el dashboard.
- Las dimensiones **no dependen del mes**: se reconstruyen enteras en cada corrida y dan siempre lo mismo (idempotencia). `dim_airport` conserva los 6.915 aeropuertos: 3.941 son extranjeros (`Ciudad, País: Nombre`) y quedan con `state` nulo. `airport_code` (el IATA, `ATL`) sale de silver porque el lookup no lo trae.
- **Broadcast join** en dos lugares: el chequeo de huérfanos (`left_anti` del hecho contra `dim_airport`, para origen y destino, y contra `dim_carrier`) y el agregado, que trae los datos del aeropuerto. Con `F.broadcast()` explícito y `explain()` para ver `BroadcastHashJoin`.

## Estructura del repo

```
(raíz del repo)
├── CLAUDE.md                  reglas de trabajo para Claude Code
├── PROGRESO.md                bitácora: fase actual, recursos que existen, decisiones
├── README.md
├── docs/
│   ├── PLAN.md                este archivo
│   ├── DECISIONES.md          para qué se construyó y qué se decidió, con sus números
│   ├── ERRORES.md             lo que salió mal y cómo se diagnosticó
│   ├── arquitectura-gratis.svg y .png
│   └── dashboard/             dashboard HTML de gold: plantilla y resultado
├── data/                      zips y CSV descargados, y el mes sintético (fuera de git)
├── scripts/                   download_data.py, make_synthetic_bad_month.py, dashboard_gold.py
├── infra/
│   └── orquestacion.yaml      CloudFormation: EventBridge, Step Functions, alarma
│                              (la base se creó a mano; infra/base.yaml queda como mejora pendiente)
├── glue/
│   ├── bronze_to_silver.py    Glue job 1
│   ├── silver_to_gold.py      Glue job 2
│   ├── run_tests.sh           corre las pruebas en el contenedor de Glue 5.0
│   └── tests/                 pruebas locales (68)
├── dq/                        silver.dqdl (reglas de Glue Data Quality) y run_eval.sh
├── sql/athena/                consultas clave (consultas-clave.sql)
├── statemachine/pipeline.asl.json
└── aws/scripts/               up.sh, vivos.sh, lf-permisos.sh, demo-analistas.sh
```

No existe un `down.sh`: el apagado final del proyecto (borrar recursos) se hace a mano, con confirmación en cada borrado. `vivos.sh` lista lo que está encendido.

Los jobs de Glue se escriben como funciones de PySpark puras (reciben y devuelven DataFrames) más un envoltorio fino con lo propio de Glue. Así la lógica se prueba en un contenedor de Spark local y el mismo código corre en Glue.

Nombres: cinco buckets en el *account regional namespace* de la cuenta, `flights-<capa>-<account-id>-us-east-2-an` (la consola agrega el sufijo; solo se escribe `flights-<capa>`), con capa `bronze`, `silver`, `gold`, `quarantine` y `work`; databases del catálogo `flights_bronze`, `flights_silver`, `flights_gold`; jobs `flights-bronze-to-silver` y `flights-silver-to-gold`; tag `Project=flights`. Decidido el 2026-10-09: la cuarentena tiene bucket propio (retención y acceso distintos, y no se mezcla con silver) y `work` guarda los scripts de Glue y los resultados de Athena en los prefijos `glue-scripts/`, `glue-temp/` y `athena-results/`.

## Las fases

Trabajamos en este orden. No empieces una fase sin cerrar la anterior. Los dominios son los de la guía oficial del examen: D1 Ingestion and Transformation (34%), D2 Data Store Management (26%), D3 Data Operations and Support (22%), D4 Data Security and Governance (18%).

### Fase 0 · Base

**Se arma:** la conexión de la CLI (la cuenta no tiene root ni IAM Identity Center; verificar en la documentación cómo se conecta), la confirmación de la región del proyecto, el presupuesto en Budgets, y la base: la customer managed key de KMS (primero, porque el cifrado de los buckets apunta a ella), los cinco buckets (versioning, cifrado por defecto SSE-KMS con bucket key, bloqueo de acceso público, lifecycle rules: en bronze (pasar a Glacier Instant Retrieval a los 90 días y limpiar versiones), en silver, gold, quarantine y work (limpieza: versiones no actuales a los 30 días y multipart incompletos a los 7) y en `athena-results/` (expirar a los 7 días). La limpieza se agregó en la fase 1, tras comprobar que cada reproceso deja versiones no actuales), el IAM role de Glue con least privilege y un topic de SNS con mi mail suscrito. **Por ahora se crea a mano en la consola**, para ver cada opción; pasarlo a CloudFormation (`flights-base`) queda pendiente.

**Hecho cuando:**
- Llega el mail de confirmación de SNS y el presupuesto aparece en la consola.
- `aws s3api get-bucket-encryption` muestra `aws:kms` con la key del proyecto en los cinco buckets.
- Puedo explicar cada statement de la policy del role y de la key policy.

**🎯 Examen (D4, D2):** SSE-S3 vs SSE-KMS vs SSE-C, key policy vs IAM policy, bucket key, least privilege, IAM role para un servicio, versioning y lifecycle rules, storage classes, CloudFormation y `DependsOn` (se practican cuando se pase la base a un stack).

### Fase 1 · Bronce a plata

**Se arma:** `scripts/download_data.py` y la subida a bronce con `aws s3 sync`; el job 1 en PySpark (snake_case, tipos, columna vacía, horas `2400`, duplicados, cuarentena, Parquet particionado por `year` y `month`); el job desplegado en Glue; un crawler sobre silver; el workgroup de Athena y las primeras consultas.

**Hecho cuando:**
- La lógica pasa las pruebas en local con una muestra y el job corre en Glue con los dos meses completos.
- Silver tiene carpetas `year=2024/month=01/` y `month=02/`, y nada más.
- Filas de bronce = filas de silver + filas de cuarentena + duplicados eliminados, y los números cierran.
- Medí el dato escaneado de la misma consulta en CSV (copia hecha con CTAS), en Parquet sin filtro, en Parquet filtrando por partición y en Parquet pidiendo pocas columnas frente a todas. Con 110 columnas, la diferencia por columnas se ve clarísima.

**🎯 Examen (D1, D2, D3):** Glue ETL, DynamicFrame vs DataFrame, job bookmarks, workers y DPU, formatos columnares, particionado y partition pruning, crawlers y Data Catalog, schema evolution, schema-on-read, Athena cobra por dato escaneado, CTAS, `MSCK REPAIR TABLE` vs crawler vs partition projection.

### Fase 2 · Plata a oro

**Se arma:** el job 2, que construye `dim_airport`, `dim_carrier`, `dim_date`, `dim_cancellation`, `fact_flights` y `agg_daily_airport`; el crawler sobre gold; consultas que siguen un mismo vuelo por las tres capas. Extra opcional: gold en Iceberg.

**Hecho cuando:**
- Gold tiene la tabla de hechos, las dimensiones y el agregado, registrados en `flights_gold`.
- Una consulta de negocio (demora promedio de vuelos operados por aeropuerto y mes) da el mismo resultado calculada desde silver y desde gold.
- Puedo explicar por qué el join con aeropuertos es un broadcast join, y qué pasaría si corro el job dos veces (idempotencia y modo de escritura).

**🎯 Examen (D1, D2):** modelado dimensional, star schema, hechos y dimensiones, slowly changing dimensions, modos de escritura (append vs overwrite), broadcast join, shuffle y data skew, Iceberg y formatos de tabla abiertos.

### Fase 3 · Consumo y gobierno

**Se arma:** Athena como capa de servicio (vistas para el analista, CTAS, límite de datos escaneados por workgroup) y Lake Formation: un analista que no ve la columna de matrícula y otro que solo ve los vuelos de una aerolínea. Opcional: el dashboard en Quick (versión Free o Plus; confirmar el costo antes de activarlo).

**Hecho cuando:**
- Dos analistas corren la misma consulta y ven resultados distintos.
- Leí el error real de una consulta sin permiso y puedo explicar qué lo causó.
- Puedo decir cuándo conviene una vista, un CTAS o una tabla del catálogo.

**🎯 Examen (D2, D3, D4):** Lake Formation (permisos por columna y por fila), Athena workgroups y límites, vistas, CTAS, SPICE, diferencia entre permisos de IAM y de Lake Formation.

### Fase 4 · Operación

**Se arma:** las reglas de Glue Data Quality sobre silver (fecha de vuelo completa y dentro del mes, distancia positiva, aeropuertos que existen en la tabla de aeropuertos, aerolínea conocida, cancelado implica código de cancelación, cantidad mínima de filas; **sin** exigir demora no negativa); la state machine que encadena job 1, calidad, job 2 y crawler con `Retry` y `Catch`, y que recibe el mes a procesar como parámetro; la regla de EventBridge (por horario y por objeto nuevo en bronce); los logs en CloudWatch, una alarma por job fallido y el aviso por SNS. Opcional: la misma orquestación con Glue Workflows para comparar, y CloudTrail con data events sobre gold (avisar el costo).

**Hecho cuando:**
- Bajo marzo de 2024, lo subo a bronce y el pipeline procesa solo ese mes hasta el final, sin que yo toque nada.
- Subo el archivo sintético que rompe una regla de calidad, la corrida se frena antes de oro y me llega el mail.
- Puedo leer la ejecución en la consola de Step Functions y decir qué hizo cada estado.

**🎯 Examen (D1, D3):** orquestación con Step Functions vs Glue Workflows vs MWAA, estados `Task`, `Choice`, `Retry` y `Catch`, integraciones `.sync`, EventBridge (schedule vs event pattern), notificaciones de S3, carga incremental (job bookmarks vs parámetro), Glue Data Quality y DQDL, logs, metric filters y alarmas de CloudWatch, CloudTrail (management events vs data events).

### Fase 5 · Futura (plan de pago)

Redshift Serverless (`COPY`, Spectrum, distribution styles, materialized view) y streaming con Kinesis Data Streams y Firehose. Requieren pasar al plan de pago; según el FAQ de AWS, los créditos que queden se siguen aplicando a las facturas futuras. El diseño completo está en `practice-01`.

## Servicios y cobertura del examen

Verificados contra la [lista oficial](https://docs.aws.amazon.com/accounts/latest/reference/supported-services-sign-up-new.html) del plan gratuito.

| Dentro del plan gratuito | Fuera (plan de pago o administrados por AWS) |
|---|---|
| S3, KMS, IAM, Glue (ETL, crawlers, Catalog), Athena, Lake Formation, Step Functions, EventBridge, CloudWatch, SNS, CloudFormation, CloudTrail, Budgets, Quick (Free o Plus) | Redshift, Kinesis Data Streams, Data Firehose, MSK, EMR · IAM Identity Center y Organizations (los administra AWS en esta cuenta) |

| Dominio | Se practica | Queda en teoría |
|---|---|---|
| D1 Ingesta y transformación | Glue, Step Functions, EventBridge | Kinesis, Firehose, MSK, EMR |
| D2 Almacenamiento | S3, formatos, Catalog, Lake Formation | Redshift |
| D3 Operación | Athena, CloudWatch, Data Quality | Redshift Data API |
| D4 Seguridad | KMS, IAM, Lake Formation, CloudTrail | Identity Center, Organizations |

## Pendientes por verificar

Estado al 2026-10-14.

| Pendiente | Estado |
|---|---|
| Región del proyecto | **Resuelto**: `us-east-2`. AWS la asigna según el país de contacto y no se puede cambiar |
| Cómo se conecta la CLI en una cuenta sin root ni Identity Center | **Resuelto**: `aws login --profile flights`, con credenciales temporales de la sesión de consola (duran hasta 12 horas) |
| Versión de Glue y del runtime de Spark | **Resuelto**: Glue 5.0 (Spark 3.5.4, Python 3.11) |
| Glue Data Quality, Iceberg y workers `G.1X` bajo el plan gratuito | Data Quality y `G.1X` **funcionan**; Iceberg **no se probó** |
| Términos de uso de los datos de BTS | **Sin resolver**: no se encontró un texto de licencia explícito. Los datos no van al repo |
| Nombre vigente de QuickSight | **Resuelto**: Amazon Quick. Para crear dashboards necesita el plan Enterprise, que no entra en la cuenta gratuita; se reemplazó por un dashboard HTML local |
| Políticas de control de servicios (SCP) | **Vigente**: las administra AWS en esta cuenta y un `AccessDenied` puede venir de ahí y no de tu IAM |
