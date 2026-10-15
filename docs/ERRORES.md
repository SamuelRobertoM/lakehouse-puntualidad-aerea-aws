# Errores y aprendizajes

Este proyecto se hizo con ayuda de IA, y los errores sirven para ver cómo se razonó. Acá quedan los que más enseñaron, el principal con su análisis completo.

## El error principal: la primera ejecución real falló, y la calidad funcionó

### Qué se había armado

Un pipeline automático. Al llegar un CSV mensual al bucket de bronce, EventBridge dispara una state machine de Step Functions que encadena cuatro pasos: el job de Glue que limpia los datos, una evaluación de calidad con Glue Data Quality, el crawler que registra la partición en el catálogo, y un aviso por mail con SNS.

### Qué pasó

Se probó primero a mano con febrero y salió todo bien. Después se subió **marzo**, un mes nuevo, para ver el disparo automático. La ejecución falló.

| Hora | Qué ocurrió |
|---|---|
| 21:10 | Llega el CSV de marzo a bronce y EventBridge arranca la state machine |
| ~21:14 | El job de Glue termina bien (136 s) y deja el Parquet de marzo en silver |
| 21:14 | La evaluación de calidad da **0 filas** y falla las 5 reglas, con puntaje 0,0 |
| 21:14 | La state machine termina en `CalidadNoPaso`: manda un mail con el detalle y la alarma de CloudWatch manda otro |

### Cómo se diagnosticó

Los mensajes de Glue Data Quality eran extraños: `Value: 0 does not meet the constraint requirement!` e `Input data does not include column tail_number!`. Eso no es "datos malos": es **no ver datos**. Con datos malos habría aparecido el número de filas y los valores que no cumplen.

Cuatro chequeos de solo lectura lo confirmaron:

| Dónde se miró | Qué había de marzo |
|---|---|
| Bronce (S3) | El CSV, de 267 MB |
| Silver (S3) | El Parquet de marzo, de 24,3 MB |
| Data Catalog | **Solo las particiones de enero y febrero** |
| Crawler | Su última corrida fue antes de que arrancara marzo |

### La causa

Un error de **orden** en el diseño. Glue Data Quality lee la tabla a través del Data Catalog, y es el catálogo quien decide qué particiones existen. Marzo estaba en S3 pero **no registrado**, así que el filtro por mes no encontró nada. El crawler, que es quien registra la partición, estaba puesto **después** de la evaluación de calidad. Era una dependencia circular: la calidad necesitaba lo que el crawler hacía, y el crawler esperaba a la calidad.

Con febrero había funcionado porque esa partición ya estaba registrada. La prueba manual probaba el caso repetido, y no el caso real de un mes nuevo.

Es el mismo mecanismo de una trampa clásica del examen: una partición no registrada hace que Athena devuelva cero filas **sin ningún error**. Acá lo mismo le pasó a Data Quality.

### Qué se corrigió

El orden quedó así: job de Glue, crawler, calidad, aviso. Y si la calidad falla, un nuevo estado (`RetirarParticion`) saca la partición del catálogo para que nadie consulte datos que no pasaron el control. Los archivos quedan en S3.

**El trade-off:** durante unos dos minutos la partición es visible en el catálogo antes de validarse. Una alternativa más estricta es el patrón *write-audit-publish*: correr la calidad en un job de Glue que lea la ruta de S3 directamente, y registrar en el catálogo solo si pasa. Es más correcta y más compleja, y queda como mejora.

### El resultado: funcionó

Se desplegó la corrección y se relanzó marzo. La ejecución terminó en `SUCCEEDED`, pasó por el crawler **antes** de la calidad y dio puntaje 1,0 en las cinco reglas:

| Regla | Medido en marzo |
|---|---|
| Cantidad de filas | 591.767 |
| Matrícula informada | 99,86% |
| Demora de llegada informada | 98,93% |
| Distancia (mínima y máxima) | 31 y 5.095 millas |
| Demora promedio | 6,50 minutos |

Y la prueba que cierra el círculo: Athena ve las tres particiones (enero 547.271 filas, febrero 519.221 y marzo 591.767) **sin que nadie haya corrido `MSCK REPAIR TABLE` ni el crawler a mano**. El crawler lo hizo la state machine.

### La prueba inversa: que falle a propósito

Con el orden corregido faltaba probar el otro camino: ¿frena de verdad cuando los datos son malos? Se subió un **abril sintético** (5.000 filas, una fracción de un mes real) y se dejó que EventBridge lo disparara solo.

| Qué se vio | Resultado |
|---|---|
| Job 1 | Aceptó las 5.000 filas: el sintético es limpio a propósito, para que falle la calidad y no la limpieza |
| Calidad | **Puntaje 0,8**: falla solo `RowCount` (`Value: 5000 does not meet the constraint requirement!`) y pasan las otras cuatro |
| State machine | `RetirarParticion` → mail → `CalidadNoPaso` → `FAILED` (más el mail de la alarma de CloudWatch) |
| Catálogo y Athena | Quedan `01`, `02` y `03`; abril no existe para quien consulta (0 filas) |

La diferencia con el fallo de marzo es lo que enseña a leer los mensajes. En marzo la calidad decía **0 filas** y que faltaban columnas: no veía datos. En abril dice **5000**: ve los datos y no alcanzan. Mismo estado final, causas opuestas.

### Qué funcionó bien

- **La regla de calidad hizo su trabajo.** Vio que no había datos y frenó, en vez de publicar algo vacío.
- **Los avisos llegaron:** el detalle de la state machine y la alarma.
- **El diagnóstico fue rápido**, porque Step Functions guarda el historial completo de cada ejecución.
- Antes de relanzar, se comprobó con el CSV que **marzo real pasa las cinco reglas** (matrícula 99,86%, demora informada 98,93%, media de demora 6,50 minutos).

### Lecciones

1. **Probar el caso real, no solo el repetido.** Un mes nuevo se comporta distinto de uno ya procesado.
2. **El catálogo es la fuente de verdad de qué particiones existen**, no S3. Quien lee por catálogo no ve lo que no está registrado.
3. **Un control de calidad que falla fuerte es mejor que uno que deja pasar.** Acá frenó por la razón equivocada, pero frenó.
4. **El estado de una corrida no es el resultado de las reglas.** Una evaluación puede terminar en `SUCCEEDED` aunque las reglas fallen: hay que leer el puntaje.

## Otros errores y qué enseñaron

| Qué pasó | Qué se aprendió |
|---|---|
| El límite de datos por consulta del workgroup de Athena quedó en **1 PB** en vez de 1 GB: la unidad quedó mal elegida | Un valor mal cargado no da error y deja el control inservible. Verificar la configuración real (`get-work-group`) y no confiar en lo que se ve en pantalla |
| En el ejercicio de la partición sin registrar, la consulta devolvió 0, pero **el archivo de prueba nunca se había copiado**: el 0 no probaba nada | Comprobar la precondición antes de sacar conclusiones |
| El simulador de políticas de IAM devolvió `AllowedByOrganizations: false` para S3 y Glue, pero las llamadas reales funcionaban | No confiar ciegamente en una herramienta de simulación: contrastar con la llamada real |
| El job escribía `month=1` en vez de `month=01`, y dejaba 8 archivos de ~3 MB por mes | Lo que se escribe define cómo se consulta. Un solo archivo por mes y carpetas con dos dígitos como en bronce |
| Reprocesar un mes con `partitionOverwriteMode` dinámico no limpia la carpeta si ahora queda vacía | Escribir directamente en la carpeta del mes con `overwrite` hace el job idempotente de verdad |
| Cada reproceso dejaba versiones no actuales y delete markers acumulándose (versioning activado) | Cuando hay versioning, hace falta una regla de lifecycle de limpieza |
| El primer CTAS de la comparación falló por correrse antes de crear la base, **escaneó 23 MB y dejó 27 MB huérfanos** | Una consulta fallida igual puede costar y dejar basura |
| `ReferentialIntegrity` de Data Quality solo admite columnas numéricas | Leer los tipos soportados de una regla antes de diseñar con ella |
| El lookup de aeropuertos de BTS **no está en UTF-8** (Latin-1) | Medir los datos reales antes de asumir: bronce los guarda tal cual y el job debe indicar la codificación |
| Creí que `AVG` sobre el promedio diario era "más óptimo" que calcular suma ÷ cantidad. Daba **23,16 min** en Chicago contra **19,39** reales, y hasta cambiaba cuáles eran los 5 peores aeropuertos | Athena cobra por bytes escaneados, no por la cuenta; y un promedio de promedios da el mismo peso a un día con 3 vuelos que a uno con 100. El agregado guarda **suma y cantidad** (medidas aditivas) |
| El script local del job 1 no compilaba: una nota de una línea con una sangría de más (`IndentationError`). El job desplegado en S3 era la versión vieja, por eso seguía andando | Lo que está en el repo y lo que está desplegado pueden divergir en silencio. Las pruebas lo cazaron al importar el módulo |
| Creé la base de datos del catálogo como `flights_golden` (con "en") en vez de `flights_gold`, sin tag | Una errata de nombre se arrastra a crawlers, consultas y permisos. Se detectó verificando con la CLI, no a ojo |
| Etiquetas de costo con un **espacio al final** (`Project `): me pasó dos veces, en la key de KMS y en un role | Para el presupuesto y los filtros, `Project ` y `Project` son claves distintas. Verificar las tags escribiéndolas a mano y leyéndolas con la CLI |
| Pegué una política de **permisos** en el editor de la política de **confianza** de un role: `Has prohibited field Resource` | Una trust policy responde "quién puede asumir el role" y nunca lleva `Resource`; la de permisos responde "qué puede hacer". El guardado falló y la confianza quedó intacta |
| Probé el analista sin matrícula **cambiando solo el workgroup** en la consola y veía la matrícula | Lake Formation mira **quién consulta**, no el workgroup. Yo seguía siendo el admin, que no está en el opt-in. Para probar hay que asumir el role del analista (perfiles de CLI) |
| El asistente de CloudTrail creó el trail **multi-región** y con el cifrado **SSE-KMS activado por defecto** (que crea otra key de KMS) | Revisar cada casilla de un asistente: lo que viene "por defecto" puede cobrar o chocar con las reglas del proyecto |

## El segundo error: una condición que nunca se cumplía

### Qué pasó

Para la fase de gobierno armé dos roles de analista con una policy de mínimo privilegio: Athena solo en su workgroup, lectura del catálogo, ningún permiso de S3 sobre gold, y S3 solo sobre su propia carpeta de resultados. Al probarlos, **ninguno de los dos pudo ni arrancar una consulta**:

```
StartQueryExecution: Unable to verify/create output bucket … INSUFFICIENT_PERMISSIONS
```

### Cómo se leyó

- Falló en `StartQueryExecution`, **antes de tocar un solo dato**: Lake Formation todavía no había entrado en juego.
- Nombraba el **bucket de resultados**: el problema era de S3 y no de las tablas.

### La causa

Puse `s3:GetBucketLocation` en la misma sentencia que `s3:ListBucket`, con la condición `s3:prefix` para limitar a la carpeta propia. Pero **`GetBucketLocation` no envía esa clave de contexto** (pregunta en qué región está el bucket, no lista nada), así que la condición nunca se cumplía y la acción quedaba negada. Athena la necesita para verificar el bucket. El simulador de IAM por principal no sirvió (en esta cuenta marca todo como denegado por la capa de organización); lo que lo confirmó fue evaluar solo la policy del role.

### El arreglo

`GetBucketLocation` en su propia sentencia, sin condición, y `ListBucket` con el prefijo. Después se verificó que cada role puede escribir en su carpeta y **no** en la del otro.

### Lo que enseñó

**Una condición de IAM solo se cumple si la acción envía esa clave de contexto.** Y el mensaje de error contó la historia correcta: antes de datos, antes de Lake Formation, era un permiso de S3.

### El resultado: la seguridad funcionó

| Consulta | Admin | Analista sin matrícula | Analista de una aerolínea |
|---|---|---|---|
| Vuelos por aerolínea (enero) | 15 aerolíneas | 15 | **1 (`DL`, 74.384)** |
| `SELECT tail_number` | matrículas | **`COLUMN_NOT_FOUND`** | matrículas de Delta |
| `SELECT *` | 29 columnas | **28, sin `tail_number`** | 29 |
| Tabla `agg_daily_airport` | la ve | la ve | **`TABLE_NOT_FOUND`** |
| Parquet de gold directo de S3 | lo lee | `AccessDenied` | `AccessDenied` |

Lake Formation dice **"no existe"** (no confirma que haya algo escondido), mientras que IAM y S3 dicen **"acceso denegado"** con el permiso exacto que falta.

## Para contarlo en el video

> "En la fase de gobierno me pasó otra del mismo tipo: los analistas no podían ni arrancar una consulta. El error hablaba del bucket de resultados, no de las tablas, y eso ya me decía que era S3 y no Lake Formation. Había escrito una condición de IAM por prefijo en una acción que no envía prefijo, así que nunca se cumplía. Lo que me llevo: leer el mensaje antes de tocar nada, y probar con la identidad real, no con la mía de administrador."

> "En la primera ejecución real, la state machine falló. Y me sirvió: la regla de calidad vio que no había datos y frenó. La causa era un error de orden mío: el control de calidad lee por el catálogo, y el crawler que registra la partición corría después. Lo detecté por los mensajes de Glue, que hablaban de cero filas y no de valores malos, lo confirmé con cuatro chequeos y lo corregí. Lo que me llevo: probar siempre el caso real y no solo el repetido."
