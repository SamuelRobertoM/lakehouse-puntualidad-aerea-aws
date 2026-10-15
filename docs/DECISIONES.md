# Decisiones de diseño

Para qué se construyó este lakehouse, qué se decidió en el camino, por qué, y qué se descartó. Cada decisión lleva el número que la respalda. El detalle de las fases está en [PLAN.md](PLAN.md); lo que salió mal y cómo se diagnosticó, en [ERRORES.md](ERRORES.md); el registro cronológico, en [../PROGRESO.md](../PROGRESO.md).

## El caso

Un caso ficticio con datos públicos reales: el **equipo de operaciones de una aerolínea** necesita decidir con datos, y los datos son los vuelos domésticos de EE. UU. que publica el gobierno (**BTS On-Time Performance**), de **enero, febrero y marzo de 2024: 1.658.259 vuelos**. La aerolínea es ficticia; los datos son reales.

La pregunta de fondo es *¿qué tan puntuales son los vuelos, y quién es más o menos puntual?* No hay un solo número que la conteste: depende de dónde, de qué aerolínea, a qué hora y en qué mes. El analista quiere contestar tres preguntas:

| # | Pregunta | Decisión que habilita |
|---|---|---|
| 1 | ¿Qué aeropuertos y aerolíneas son más y menos puntuales, y cuáles cancelan más? | Dónde reforzar la operación |
| 2 | ¿Qué causas explican las demoras, y cambian de un mes a otro? | Invertir en tripulación y mantenimiento, o coordinar con el control aéreo |
| 3 | ¿Qué franjas horarias (y rutas) tienen peor puntualidad? | Reprogramar vuelos |

**Por qué es ingeniería de datos y no solo una consulta.** Los archivos crudos son pesados y requieren interpretación: 110 columnas, ~247 MB por mes, horas `2400`, columnas vacías según el tipo de vuelo, y reglas como que una demora negativa es un vuelo adelantado. Consultados tal cual darían números equivocados, lentos y caros. El pipeline hace que la respuesta sea correcta, rápida y barata, y que cada persona vea solo lo que le corresponde.

## Lo que el analista descubre

Resultados calculados de la capa gold (se ven en [dashboard/dashboard-gold.html](dashboard/dashboard-gold.html)). "Más puntual" se mide con la **demora media de llegada** sobre vuelos operados: un valor negativo significa que, en promedio, llegan antes de lo programado.

| Pregunta | Enero a marzo de 2024 |
|---|---|
| Mes | **Enero el peor**: 24,1 % de vuelos demorados 15 minutos o más y 3,7 % cancelados. **Febrero el mejor**: 15,9 % y 0,6 %. Marzo: 20,8 % |
| Aeropuertos más puntuales (entre los 86 con al menos 3.000 vuelos operados) | **Newark (EWR) y LaGuardia (LGA)**: llegan 2,4 min antes. Les siguen Syracuse, Kahului y Pittsburgh |
| Aeropuertos menos puntuales | **San Juan (SJU), Miami (MIA) y Fort Lauderdale (FLL)**: 16 a 19 min de demora media y ~30 % de vuelos demorados |
| Aerolíneas | Más puntuales: **Republic Airline** (6,5 min antes), Endeavor y Delta. Menos: **Frontier, American y JetBlue** (13 a 14 min) |
| Hora de salida | **6 h**: 10,9 % demorados. **19 h**: 27,6 %. La demora se acumula durante el día |
| Causas | **Llegada tardía de la aeronave (40 %)** y **la aerolínea (33 %)**: casi tres de cada cuatro minutos. Sistema aéreo nacional 20 %, clima 7 % |
| Un día especialmente malo | **15 de enero**: 50,5 min de demora media y 15,9 % de cancelaciones (2.962 vuelos). Los datos muestran qué pasó, no por qué |

El dashboard cubre aeropuertos, aerolíneas, días, causas y franjas horarias. **No muestra rutas** (origen y destino), aunque la tabla de hechos tiene el dato: es una ampliación posible.

## Las decisiones

| # | Decisión | Por qué | Lo que se descartó | El número |
|---|---|---|---|---|
| 1 | **Un bucket por capa** (bronce, plata, cuarentena, oro, trabajo), todos con **SSE-KMS** y una key propia | Permisos y ciclo de vida distintos por capa; bronce inmutable | Un bucket con prefijos: no permite un permiso por capa con una política simple | 5 buckets de datos y trabajo + 1 de auditoría; la key cuesta ~USD 1 al mes |
| 2 | **Parquet particionado por `year` y `month`**, no por aeropuerto ni aerolínea | Cada mes llega como un archivo y el reporte filtra por mes (*partition pruning*) | Particionar por aeropuerto: cientos de archivos diminutos (*small files problem*) | CSV de 247 MB a **23 MB** de Parquet por mes, en un solo archivo |
| 3 | **Idempotencia por *overwrite* de la carpeta del mes** | Reprocesar un mes reemplaza esa carpeta en lugar de sumarse | *Job bookmarks*: se saltearían los archivos en un reproceso | Enero reprocesado: **547.271 filas antes y después** |
| 4 | **Calidad en dos niveles**: el job 1 rechaza filas (con el motivo) y Glue Data Quality valida el conjunto | Lo fila por fila y lo agregado son problemas distintos | Descartar filas en silencio | 5 reglas; la cuarentena guarda cada fila rechazada con su causa |
| 5 | **Step Functions y no Airflow** | Flujo lineal, mensual y disparado por un archivo; MWAA es infraestructura que queda encendida | MWAA: figura en la lista del plan gratuito, pero cobra por hora encendido (el ejemplo de su página de precios, entorno chico en N. Virginia, es USD 0,49 por hora, ~USD 365 al mes) | Ejecución completa: ~9 minutos y ~USD 0,09 en total; la Step Function sola, ~USD 0,001 |
| 6 | **Esquema estrella, claves naturales y medidas aditivas** | Las dimensiones son una foto sin historia (SCD tipo 1); el agregado guarda suma y cantidad | Un promedio de promedios | Chicago: **19,39** correcto contra **23,16** del error. Gold escanea **17 veces menos** que silver |
| 7 | **Lake Formation en modo híbrido**, con un workgroup de Athena por analista | El pipeline sigue con IAM; solo los analistas pasan por Lake Formation; resultados aislados | Dar a los analistas acceso al bucket de gold | Mismo SQL: 15 aerolíneas, 1 aerolínea, o la columna matrícula que "no existe" |
| 8 | **CloudTrail con eventos de gestión y validación de logs** | Auditar quién leyó qué: Lake Formation registra `GetDataAccess` por analista | Eventos de datos y CloudTrail Lake: cobran | Primera copia de eventos de gestión gratis |
| 9 | **Versionado en los buckets de datos, con reglas de lifecycle de limpieza** | El versionado protege de borrados y pisados por error; pero cada `overwrite` deja versiones viejas y *delete markers* que cobran almacenamiento | Versionado sin lifecycle (el almacenamiento crece solo); sin versionado (sin red de seguridad) | No actuales a los **30 días**, multipart a **7**, resultados de Athena a **7**, bronce a **Glacier Instant Retrieval a los 90** |
| 10 | **Dashboard local en HTML en lugar de Quick** | Quick solo crea dashboards en su plan Enterprise, que no entra en la cuenta gratuita y cobra USD 250 al mes de infraestructura | Quick (verificado en la página de precios y en la lista de servicios del plan) | HTML de 191 KB, sin internet, alimentado por 5 consultas de Athena que escanean **9,1 MB** (~USD 0,00005) |

## Los errores que más enseñaron

El detalle de cada uno, con su diagnóstico, está en [ERRORES.md](ERRORES.md).

1. **La primera ejecución real falló.** La calidad de datos lee por el catálogo y el crawler que registra la partición corría *después*. La regla frenó, que era lo correcto. Lección: probar el caso real y no solo el repetido.
2. **Los analistas no podían arrancar una consulta.** Una condición de IAM por prefijo en una acción que no envía prefijo. El error hablaba del bucket de resultados y no de las tablas. Lección: leer el mensaje antes de tocar nada.
3. **Se veía la matrícula "como analista".** Se estaba cambiando solo el workgroup y la identidad seguía siendo la del administrador. Lake Formation mira **quién** consulta. Lección: probar con la identidad real.

## Números de referencia

| Dato | Valor |
|---|---|
| Vuelos procesados | 1.658.259 (enero 547.271, febrero 519.221, marzo 591.767) |
| Pruebas automáticas | 68 (pytest en el contenedor de Glue 5.0) |
| ETL en Glue, medido | Job 1 USD 0,335 en 10 corridas; Job 2 USD 0,097 en 4 corridas. Data Quality y crawlers, estimados |
| Costo por mes procesado | ~USD 0,09 (job 1 + calidad + job 2) |
| Costo en reposo | ~USD 0,04 por día, casi todo la key de KMS |
| Datos escaneados en Athena (consulta de la fase 1) | CSV comprimido 28,9 MB; Parquet con todas las columnas 23,1 MB; pocas columnas 1,4 MB; con filtro de partición 0,77 MB |

## Qué mejoraría

- **Escribir primero a una zona de *staging* y publicar solo si pasa la calidad** (*write-audit-publish*). Hoy la partición queda visible ~2 minutos antes de validarse y los archivos de plata quedan en S3 aunque la calidad falle.
- **La base (buckets, KMS, roles) en CloudFormation.** Se creó a mano a propósito, para entender cada servicio; la orquestación sí está como código.
- **Permisos por etiquetas (*LF-Tags*)** en lugar de por tabla, cuando haya muchas.
- Un dashboard en un servicio gestionado (Quick Enterprise) y gold en un formato de tabla abierto (Iceberg).
