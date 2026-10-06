# Pipeline de Movimientos Financieros — Tyba

Pipeline que ingiere los cortes diarios de movimientos (`movimientos_dia_T.parquet`, `movimientos_dia_T1.parquet`) y mantiene en DuckDB una base consultable y con trazabilidad de los cambios entre cortes.

> Los insights y la analítica están en [INSIGHTS.md](INSIGHTS.md).

## Estructura

```
.
├── data/
│   ├── raw/          ← parquet entregados por Tyba
│   └── output/       ← base de datos destino (tyba.duckdb)
├── src/
│   ├── pipeline.py         ← script principal: extract -> validate -> load_scd4
│   ├── schema.py           ← DDL de las tablas
│   ├── dq_rules.py         ← reglas de calidad de datos
│   ├── utils.py            ← funciones de normalización y hash
│   ├── data_profiling.py   ← consultas de perfilamiento de los datos
│   └── insights.py         ← consultas que respaldan INSIGHTS.md
├── Dockerfile
├── docker-compose.yml
├── .dockerignore
├── requirements.txt
├── INSIGHTS.md         ← insights y analítica
└── README.md
```

## Cómo correrlo

### Con Docker

Requisito: Docker con Docker Compose (por ejemplo, Docker Desktop).

```bash
docker compose up --build
```

No requiere configuración adicional. El contenedor:

1. Instala Python 3.14 y las dependencias de `requirements.txt` (versiones fijas).
2. Lee los parquet de `data/raw/` (montada en solo lectura).
3. Carga los cortes pendientes y escribe la base en `data/output/tyba.duckdb` (montada como volumen, así que la base queda en el equipo del usuario).
4. Termina con código `0` si todo fue bien, o `1` si alguna carga falló.

Al ejecutarlo de nuevo, los cortes ya cargados se omiten. Para repetir la carga desde cero:

```bash
rm -f data/output/tyba.duckdb data/output/tyba.duckdb.wal
docker compose up --build
```

Resultado esperado:

```
Cargando movimientos_dia_T.parquet (corte 2024-10-15)
Carga 1 OK: {'read': 50000, 'inserted': 50000, 'updated': 0, 'deleted': 0, 'unchanged': 0}
Cargando movimientos_dia_T1.parquet (corte 2024-10-16)
Carga 2 OK: {'read': 49000, 'inserted': 9996, 'updated': 3845, 'deleted': 10996, 'unchanged': 35159}
```

### En local

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python src/pipeline.py
```

Para volver a generar el perfilamiento de los datos (solo lectura, no modifica la base de datos):

```bash
python src/data_profiling.py
```

Para volver a generar las cifras de [INSIGHTS.md](INSIGHTS.md) (requiere haber ejecutado el pipeline):

```bash
python src/insights.py
```

### Consultar la base

Con el pipeline detenido (DuckDB no permite abrir la base mientras otro proceso escribe en ella):

```python
import duckdb

con = duckdb.connect("data/output/tyba.duckdb", read_only=True)
con.sql("SELECT * FROM load_audit").show()
```

También se puede abrir con el CLI de DuckDB (`duckdb -readonly data/output/tyba.duckdb`) o con DBeaver.

## Decisiones de diseño

### Dos tablas, sin tablas de dimensiones

No se conoce la arquitectura de datos de Tyba: qué modelos, dimensiones o catálogos existen ya, ni qué herramientas los consumen. Por eso, la solución que planteo se limita a lo que pide el documento de prueba: ingerir los cortes y mantener una base consultable y trazable. Para no añadir complejidad que no está en el alcance, el modelo tiene **solo dos tablas** (`transaction_c` y `transaction_h`) y no se crearon tablas de dimensiones.

Pensando en que la solución debe funcionar con millones de filas, un modelo de hechos y dimensiones tampoco aportaría ventajas:

- **No hay atributos propios para las dimensiones.** Una dimensión tiene sentido cuando describe una entidad con información adicional (por ejemplo, un cliente con segmento o ciudad). En los datos solo llega el valor de cada campo categórico: 8 productos, 7 fondos, 2 tipos, 10 nombres comerciales, 11 descripciones y 3.000 clientes, sin ningún atributo más. Cada dimensión sería una tabla con un id y el mismo texto.
- **No se ahorra almacenamiento.** En una base por filas, reemplazar textos repetidos por ids reduce mucho el espacio. DuckDB es columnar y aplica compresión por diccionario automáticamente: guarda cada valor distinto una sola vez y en cada fila solo un código. Es decir, ya hace por dentro lo que haría una dimensión, y la columna ocupa casi lo mismo con 1 millón que con 100 millones de filas.
- **Se encarece la carga diaria.** Antes de cargar los hechos habría que actualizar cada dimensión y buscar el id de cada fila: varios joins extra sobre todo el corte, cada día, sin ningún beneficio.
- **Se complica la trazabilidad del SCD 4.** La detección de cambios compara el contenido de cada transacción (`row_hash`). Si la tabla guarda ids en lugar de valores, un cambio de id (por ejemplo, al reconstruir una dimensión) cambiaría los hashes y rompería la historia.

La limpieza de los valores categóricos (mayúsculas, espacios, sinónimos) se resuelve en la normalización, sin tablas de catálogo.

### Procesamiento en SQL, no en memoria

El documento pide que la solución funcione si el volumen crece a millones de filas. Por eso las transformaciones no se hacen con pandas, que carga todo el archivo en memoria, sino con **SQL ejecutado en DuckDB**:

- Las funciones de normalización de `utils.py` no transforman datos: devuelven **expresiones SQL** que se insertan en las consultas.
- DuckDB lee los parquet directamente (`read_parquet(...)`), procesa los datos por bloques y puede usar el disco cuando no alcanza la memoria.
- La comparación entre cortes (nuevos, corregidos, eliminados, sin cambios) también se hace con SQL, en joins sobre `row_hash` dentro de la base de datos.
- Las reglas de calidad se evalúan todas en una sola consulta (`count(*) FILTER (WHERE ...)` por regla), con una sola pasada sobre el corte.
- El checksum de cada archivo (sha256) se calcula leyéndolo por bloques. Se usa sha256 y no md5 porque, además de no tener las debilidades conocidas de md5, en procesadores modernos (Apple Silicon, Intel/AMD con SHA-NI) está acelerado por hardware: en la máquina de desarrollo fue unas 3,5 veces más rápido que md5.

### Orquestación

No se incluye un orquestador (Airflow, Dagster, Prefect):

- El documento no lo pide. El requisito concreto es que el pipeline corra con `docker compose up --build` sin configuración adicional, y un orquestador añadiría varios servicios (scheduler, webserver, base de metadata) que complican justo eso.
- El flujo es lineal: unos pocos pasos secuenciales por corte. Un orquestador aporta valor con muchas dependencias entre tareas, reintentos por tarea o varios pipelines.

En cambio, el pipeline está **preparado para orquestarse**:

| Característica | Cómo se cumple |
|---|---|
| Idempotente | Cada archivo se identifica por su checksum. Si ya está en `load_audit` con `SUCCESS`, se omite. Ejecutarlo dos veces no duplica nada. |
| Procesa lo pendiente en orden | Busca `data/raw/movimientos_dia_*.parquet`, salta los ya cargados y carga el resto ordenado por fecha de corte. |
| Pasos separados | `extract` → `validate` → `load_scd4`. Cada uno podría ser una tarea de un DAG. |
| Falla de forma clara | Si algo falla: rollback, la carga queda `FAILED` en `load_audit` con el motivo, y el proceso termina con código de salida `1`. Los cortes siguientes no se cargan, porque dependen del anterior. |
| Logs | Con `logging`, con fecha y hora en cada paso y cada advertencia de calidad. |

En producción bastaría con un cron o un DAG que ejecute el contenedor una vez al día, cuando llegue el archivo, sin cambiar el código.

## Modelo de datos: SCD tipo 4

Cada transacción se guarda en dos tablas:

| Tabla | Contenido |
|---|---|
| `transaction_c` | **Current**. Una fila por transacción vigente. Es la tabla que se consulta en el día a día. |
| `transaction_h` | **History**. Todas las versiones de cada transacción y la operación que las generó (`INSERT`, `UPDATE`, `DELETE`). |

Por qué SCD 4:

- **Trazabilidad:** ningún cambio se pierde. Las correcciones y eliminaciones quedan registradas en `transaction_h`, con su fecha de vigencia.
- **Escala:** `transaction_c` solo contiene lo vigente, así que sigue siendo pequeña y rápida de consultar aunque el historial crezca a millones de filas.

SCD es un patrón pensado para dimensiones. Aquí se aplica a una tabla de hechos (transacciones): ambas tablas son de hechos y lo que las distingue es **vigente vs. histórico**.

### Relación con SCD 2

`transaction_h` es, por sí sola, un SCD 2 completo: guarda todas las versiones con su periodo de vigencia. De hecho, `transaction_c` se puede obtener de ella:

```sql
SELECT * FROM transaction_h WHERE valid_to IS NULL AND operation <> 'DELETE'
```

Tras la carga de T y T1, esta consulta devuelve exactamente las mismas filas que `transaction_c`. Es decir, `transaction_c` es la versión vigente del historial, **materializada** para rendimiento.

Se materializa por el volumen de cambios. Entre T y T1 cambió el 30 % del corte, así que el historial crece cada día en torno a la mitad del tamaño del corte: tras dos cargas, `transaction_h` ya tiene 74.837 filas frente a 49.000 vigentes. Con un millón de filas diarias, en un año serían del orden de 180 millones de filas en el historial frente a un millón vigentes. Las consultas habituales se hacen sobre el estado vigente, y leer un millón de filas es mucho más rápido que filtrar 180 millones.

El costo de SCD 4 frente a SCD 2 es escribir en dos tablas y mantenerlas sincronizadas. Se controla con dos mecanismos: cada carga es una transacción (se aplica completa o no se aplica) y al final se concilia que `transaction_c` tenga exactamente las filas del corte.

Alternativas descartadas:

- **Guardar cada corte completo** (una copia por día): duplica información, que es lo que el documento pide evitar. El 70 % de las filas se repetiría cada día.
- **Solo la tabla vigente** (SCD 1): pierde la trazabilidad.

### Cómo se trata cada situación entre cortes

| Situación | Efecto en `transaction_c` | Registro en `transaction_h` |
|---|---|---|
| Registro nuevo | Se inserta | `INSERT` |
| Registro corregido | Se reemplaza la versión anterior | `UPDATE`, con `previous_row_hash` apuntando a la versión anterior |
| Registro eliminado | Se borra | `DELETE`, con los valores de la última versión |
| Sin cambios | Nada | Nada |

En `transaction_h`, cada versión tiene un periodo `[valid_from, valid_to)`. Cuando una versión deja de estar vigente (por corrección o eliminación), se cierra con `valid_to` = fecha del corte. Cada `row_hash` tiene como máximo una fila abierta (`valid_to IS NULL`). Una fila `DELETE` abierta indica que la transacción está eliminada desde ese corte; si reapareciera más adelante, ese `DELETE` se cierra y se registra un `INSERT`.

Esto permite reconstruir el estado a cualquier fecha:

```sql
SELECT * FROM transaction_h
WHERE operation <> 'DELETE'
  AND valid_from <= DATE '2024-10-15'
  AND (valid_to IS NULL OR valid_to > DATE '2024-10-15')
```

### Fecha de corte

Los archivos no traen la fecha del corte: el sufijo `T` / `T1` indica el orden, pero no el día. El pipeline necesita una fecha para `valid_from` / `valid_to`, así que toma como fecha de corte **la fecha de movimiento más reciente del archivo** (`snapshot_date_of` en `pipeline.py`):

| Archivo | Rango de `date` | Fecha de corte |
|---|---|---|
| `movimientos_dia_T` | 2024-09-15 a 2024-10-15 | 2024-10-15 |
| `movimientos_dia_T1` | 2024-09-15 a 2024-10-16 | 2024-10-16 |

Por qué este criterio:

- Un corte diario contiene los movimientos hasta el día del corte, así que el movimiento más reciente indica a qué día corresponde.
- Las dos fechas quedan separadas por un día, como indica el documento (T1 es el corte del día siguiente).
- Es reproducible: reprocesar el archivo en cualquier momento da la misma fecha.

Alternativas descartadas:

- **Fecha de ejecución del pipeline:** si T y T1 se cargan en la misma ejecución, tendrían la misma fecha y el historial no podría ordenarse. Además, cambiaría al reprocesar.
- **Sufijo del nombre** (`T` = día 0, `T1` = día 1): da el orden, pero no una fecha real.

Limitación: si un día no hubiera ningún movimiento con la fecha del corte, la fecha deducida sería anterior. En ese caso, la regla `snapshot_after_last_load` rechazaría el archivo en lugar de cargarlo con una fecha incorrecta. Lo ideal sería que la fuente enviara la fecha del corte en el nombre del archivo (por ejemplo `movimientos_2024-10-16.parquet`) o en sus metadatos.

### Resultado de la carga de T y T1

| Corte | Leídas | Nuevas | Corregidas | Eliminadas | Sin cambios |
|---|---|---|---|---|---|
| T (2024-10-15) | 50.000 | 50.000 | 0 | 0 | 0 |
| T1 (2024-10-16) | 49.000 | 9.996 | 3.845 | 10.996 | 35.159 |

Al terminar cada carga se concilian los conteos: leídas = nuevas + corregidas + sin cambios, y `transaction_c` tiene exactamente las filas del corte. Si no cuadra, la carga se deshace.

### Identificación de la transacción (sin id en la fuente)

El glosario menciona un campo `id` de transacción, pero los archivos no lo traen: solo traen `id_cliente` (3.000 clientes para 50.000 filas), y ninguna combinación de columnas es única.

Por eso cada transacción se identifica así:

1. **`row_hash`**: `md5` de todos los campos, calculado **después** de normalizarlos, para que un cambio de formato (por ejemplo `IN` → `entrada`) no se tome como un cambio real. Los campos se serializan como JSON antes del hash, para que un `NULL` no se confunda con un texto ni se desplacen los campos. Si un corte trajera filas idénticas, se numeran para que cada una tenga su propio hash. En los cortes recibidos no hay colisiones (50.000 y 49.000 hashes únicos). Con el hash se detectan de forma exacta los registros nuevos, eliminados y sin cambios.
2. **Detección de correcciones**: entre los registros que desaparecen y los que aparecen se buscan parejas con la misma clave de negocio `(client_id, date, product, fund, type)`. Si la pareja es 1 a 1, se registra como `UPDATE`. Si hay varias candidatas o ninguna, queda como `DELETE` + `INSERT`.

### Por qué la clave de negocio no sirve como identificador

Varias transacciones legítimas comparten clave: el mismo cliente hace dos movimientos el mismo día, en el mismo producto, fondo y tipo. Después de normalizar, en T hay **116 claves repetidas que agrupan 232 filas**, siempre dos por clave. Por ejemplo:

| client_id | date | product | amount | description | commercial_name |
|---|---|---|---|---|---|
| CLI000023 | 2024-10-06 | Fondo de Pensión | 14.011.467,22 | Ajuste por valoración | Valores Bancolombia |
| CLI000023 | 2024-10-06 | Fondo de Pensión | 27.027.036,62 | Traslado entre fondos | BBVA |

Si la clave fuera el identificador, una de las dos se perdería. Por eso la transacción se identifica con `row_hash`, que incluye todos los campos, y la clave solo se usa para emparejar correcciones cuando la pareja es 1 a 1.

Lo que pasó en T1 con esas 116 claves:

| Caso | Claves | Resultado |
|---|---|---|
| Las dos transacciones siguen igual | 50 | Sin cambios |
| Una sigue igual y la otra desaparece | 46 | `DELETE` |
| Una sigue igual y la otra se corrige | 10 | `UPDATE` (la pareja es 1 a 1, porque la que no cambió no compite) |
| Las dos desaparecen | 9 | Dos `DELETE` |
| Las dos desaparecen y aparece una nueva | 1 | Ambiguo: dos `DELETE` + un `INSERT` |

En total, la carga de T1 tuvo 3.855 parejas candidatas: 3.845 eran 1 a 1 y se registraron como `UPDATE`. Las otras 10 correspondían a 5 claves ambiguas (más de una candidata en algún lado) y quedaron como `DELETE` + `INSERT`. No se pierde ninguna versión; solo falta el enlace `previous_row_hash`, porque no se puede saber con certeza a cuál apunta.

En los casos ambiguos, otros campos (monto, descripción, entidad) suelen dejar ver cuál es la corrección. Desempatar por similitud sería posible, pero es una regla de negocio que define qué cuenta como "la misma transacción", así que queda como mejora a validar con Tyba.

### Elección de la clave de negocio

La clave debe contener campos que identifican la transacción y que no cambian cuando se corrige. El documento dice que una corrección cambia "amount, description u otro campo", así que `amount` y `description` quedan fuera. Los demás campos se eligieron midiendo, sobre la carga de T1, cuántas correcciones detecta cada clave y cuántos casos ambiguos deja:

| Clave | Correcciones detectadas | Candidatas ambiguas |
|---|---|---|
| client_id, date, product | 3.855 | 248 |
| + fund | 3.846 | 26 |
| **+ type (elegida)** | **3.845** | **10** |
| + description | 2.125 | 0 |
| + commercial_name | 3.189 | 0 |

- `fund` y `type` casi nunca cambian en una corrección (añadirlos pierde 10 correcciones) y eliminan la mayor parte de la ambigüedad (de 248 a 10).
- Añadir `description` eliminaría los 10 casos ambiguos, pero dejaría de detectar 1.720 correcciones: las 1.508 que cambian la descripción y unas 200 más con descripción nula, porque en SQL `NULL = NULL` no es verdadero.
- `commercial_name` tiene un 17 % de nulos y, en la clave, perdería más de 600 correcciones.

Limitaciones:

- La detección de correcciones es una inferencia. Si cambia un campo de la clave de negocio (fecha, fondo, producto o tipo), la corrección se registra como `DELETE` + `INSERT`. No se pierde información, solo cambia cómo se clasifica.
- Si cambian las reglas de normalización, cambian los hashes, y hay que reprocesar los cortes desde el inicio.
- Lo ideal sería que la fuente incluyera el `id` de transacción que menciona el glosario.

## Nomenclatura

- **Tablas:** en inglés, singular, snake_case, con un sufijo de una letra para el rol de la tabla: `_c` (current) y `_h` (history).
- **Columnas:** en inglés y snake_case. Los identificadores llevan el sufijo `_id`.
- **Funciones:** en inglés, snake_case. Las de calidad de datos llevan el prefijo `dq_`.
- **Comentarios y docstrings:** en español.

### Diccionario de campos

| Campo origen | Campo destino | Tipo | Descripción |
|---|---|---|---|
| — | `row_hash` | VARCHAR | Hash del contenido normalizado. Identifica la versión de la transacción. |
| `id_cliente` | `client_id` | VARCHAR | Identificador del cliente. Se renombra por consistencia (sufijo `_id`). |
| `date` | `date` | DATE | Fecha del movimiento. En origen llega como texto en dos formatos. |
| `product` | `product` | VARCHAR | Producto asociado. |
| `type` | `type` | VARCHAR | `entrada` / `salida`. |
| `fund` | `fund` | VARCHAR | Fondo, en minúsculas. |
| `amount` | `amount` | DECIMAL(18, 2) | Monto. Tipo exacto en lugar de `DOUBLE` por ser dinero. |
| `description` | `description` | VARCHAR | Información del movimiento. |
| `commercial_name` | `commercial_name` | VARCHAR | Nombre comercial asociado. |
| — | `valid_from` | DATE | Fecha del corte desde el que la versión está vigente. |
| — | `valid_to` | DATE | Fin de la vigencia (`NULL` si sigue vigente). Solo en `transaction_h`. |
| — | `operation` | VARCHAR | `INSERT`, `UPDATE` o `DELETE`. Solo en `transaction_h`. |
| — | `previous_row_hash` | VARCHAR | Versión a la que reemplaza una corrección. Solo en `transaction_h`. |
| — | `load_id` | INTEGER | Carga de `load_audit` que generó la fila. Solo en `transaction_h`. |

### Tablas de control

Además del modelo de datos, el pipeline mantiene dos tablas de **metadata operativa**. No guardan transacciones, sino información sobre las cargas, así que no cambian la decisión de un modelo de dos tablas.

| Tabla | Una fila por | Para qué |
|---|---|---|
| `load_audit` | Intento de carga de un archivo | Idempotencia (checksum), estado (`RUNNING` / `SUCCESS` / `FAILED`), fecha de corte, duración, conteos de la carga y mensaje de error. |
| `dq_result` | Regla de calidad en cada carga | Filas que incumplen cada regla, su porcentaje y si pasó. Permite ver la evolución de la calidad entre cortes. |

## Funciones (`src/utils.py`)

Las funciones de normalización devuelven **expresiones SQL** que se ejecutan en DuckDB (ver *Procesamiento en SQL, no en memoria*).

| Función | Qué hace |
|---|---|
| `read_parquet_file(file_name)` | Lee un parquet de `data/raw/` en un DataFrame de pandas. Se usa solo para exploración: el pipeline debe leer con DuckDB para no cargar todo en memoria. |
| `clean_text(column)` | Quita los espacios de los extremos, colapsa los internos (por ejemplo `"mercado  monetario"`) y convierte `''` en `NULL`. Las demás funciones la usan. |
| `dq_normalize_type(column)` | Mapea las variantes de `type` con `TYPE_MAP`: `entrada`/`in` → `entrada`, `salida`/`out` → `salida`, sin importar mayúsculas. Los valores que no están en el mapa quedan en `NULL` para poder contarlos y reportarlos. |
| `dq_normalize_fund(column)` | Aplica `clean_text` y pasa a minúsculas. Une las 7 categorías de fondo que llegan con distintas mayúsculas y espacios. |
| `dq_normalize_date(column)` | Convierte `date` de texto a `DATE` probando, en orden, los formatos de `DATE_FORMATS` (`yyyy-mm-dd` y `dd/mm/yyyy`). Usa `TRY_STRPTIME`, así que un valor que no encaja en ningún formato queda en `NULL` en vez de detener el pipeline. |
| `content_hash(columns)` | Devuelve el `md5` del contenido de las columnas, serializadas como JSON. Es la base de `row_hash`. |
| `file_checksum(path)` | Calcula el sha256 de un archivo, leyéndolo por bloques de 1 MB. Se guarda en `load_audit` para saber si un archivo ya se cargó. |

Ejemplo de uso:

```python
con.sql(f"""
    SELECT {dq_normalize_type()} AS type, {dq_normalize_fund()} AS fund
    FROM read_parquet('data/raw/movimientos_dia_T.parquet')
""")
```

## Calidad de datos encontrada

Las inconsistencias se encontraron con las consultas de [`src/data_profiling.py`](src/data_profiling.py). Cada una se puede reproducir ejecutando ese script.

### Criterio de tratamiento

El pipeline **corrige el formato, pero no el contenido**:

- **Se normaliza** lo que tiene una única interpretación posible: mayúsculas, espacios, sinónimos (`IN` = `entrada`) y formatos de fecha. No cambia el significado del dato.
- **No se imputa, no se corrige y no se elimina** lo que requeriría decidir qué valor es el correcto: montos nulos o negativos, descripciones que contradicen el tipo, entidades sin informar. Esas son decisiones de negocio. El pipeline conserva el dato tal como llega y documenta el problema, para que lo resuelva quien conoce el negocio o la fuente.

Conservar los `NULL` mantiene la diferencia entre "la fuente no lo informó" y un valor real, y permite registrar como `UPDATE` los casos en que la fuente completa el dato en un corte posterior. En T1, por ejemplo, 72 montos que eran nulos en T llegaron con valor. En las consultas y los reportes, los nulos se tratan de forma explícita: `SUM`/`AVG` los ignoran (y se informa cuántos quedan fuera) y en los textos se puede mostrar `COALESCE(description, 'Sin descripción')`.

### Inconsistencias

| # | Problema | Detalle | Tratamiento |
|---|---|---|---|
| 1 | Sin id de transacción | El glosario menciona `id`, pero los archivos solo traen `id_cliente` (3.000 clientes para 50.000 filas). Ninguna combinación de columnas es única: `(id_cliente, date, product, fund, type)` repite 67 filas en T con los datos crudos, y 116 claves (232 filas) después de normalizar. | `row_hash` + clave de negocio (ver *Identificación de la transacción*) |
| 2 | `type` inconsistente | 10 variantes: `entrada`, `Entrada`, `ENTRADA`, `IN`, `in`, `salida`, `Salida`, `SALIDA`, `OUT`, `out`. | `dq_normalize_type` |
| 3 | `fund` inconsistente | 23 variantes de 7 fondos: mayúsculas y minúsculas, espacios en los extremos y espacios dobles (`"mercado  monetario"`). | `dq_normalize_fund` |
| 4 | `date` como texto en dos formatos | `yyyy-mm-dd` (92.085 filas entre T y T1) y `dd/mm/yyyy` (6.915). El orden día/mes se dedujo de los datos: la segunda parte nunca pasa de 10 y, leídas como `dd/mm`, todas las fechas caen en el rango de los cortes (2024-09-15 a 2024-10-16); como `mm/dd`, solo 254. | `dq_normalize_date` |
| 5 | `amount` nulo | 1.543 filas en T (3 %) y 1.440 en T1. Entre cortes, 72 se completan y 22 se vacían. | Se conserva `NULL`. No se imputa ni se usa 0, porque hay montos que son 0 de verdad (ver #7). |
| 6 | `amount` negativo | 1.034 filas en T, **todas con `type = entrada`**. Ninguna `salida` es negativa, así que no parece una convención de signo. Pueden ser reversos o errores. | Se conserva. Por validar con la fuente. |
| 7 | `amount` en 0 | 940 filas en T. | Se conserva. Por validar con la fuente. |
| 8 | `description` nulo | 4.563 filas en T (9 %) y 4.485 en T1. No se puede deducir de otros campos. | Se conserva `NULL`. |
| 9 | `description` contradice `type` | Cada descripción aparece casi en igual proporción como `entrada` y como `salida`. Por ejemplo, 2.027 "Depósito inicial" son `salida` y 2.527 "Retiro parcial" son `entrada`. | Se conserva. No se sabe cuál de los dos campos es el correcto. |
| 10 | `commercial_name` nulo | 8.307 filas en T (17 %) y 8.167 en T1. No se puede deducir: las 10 entidades ofrecen los 8 productos, los nulos se reparten igual entre productos (16–17,5 %) y la mayoría de clientes opera con 6 a 9 entidades. | Se conserva `NULL`. |

### Reglas de calidad en cada carga

Las reglas están en [`src/dq_rules.py`](src/dq_rules.py), cada una con su condición SQL y su severidad. Se evalúan en cada carga y el resultado queda en `dq_result`.

- **ERROR**: impide cargar el corte con integridad. La carga se marca `FAILED`, no se toca `transaction_c` ni `transaction_h`, y el pipeline termina con código `1`.
- **WARNING**: dato sospechoso que requiere una decisión de negocio. Se carga tal cual y se registra cuántas filas incumplen la regla (ver *Criterio de tratamiento*).

| Regla | Severidad | Falla cuando |
|---|---|---|
| Columnas requeridas | ERROR | Al archivo le falta alguna de las 8 columnas del glosario |
| `file_not_empty` | ERROR | El archivo no tiene filas |
| `snapshot_after_last_load` | ERROR | La fecha del corte no es posterior a la del último corte cargado (archivo fuera de orden, o un corte ya cargado que llega con otro contenido) |
| `client_id_not_null` | ERROR | `client_id` es nulo |
| `date_parsed` | ERROR | `date` no tiene un formato reconocido |
| `type_mapped` | ERROR | `type` no está en `TYPE_MAP` |
| `product_not_null`, `fund_not_null` | WARNING | El campo es nulo |
| `amount_not_null`, `amount_not_negative`, `amount_not_zero` | WARNING | `amount` es nulo, negativo o 0 |
| `description_not_null`, `commercial_name_not_null` | WARNING | El campo es nulo |

Las reglas ERROR se probaron con archivos alterados: un corte con 5 valores de `type` desconocidos y un corte con fecha anterior al último cargado. En ambos casos la carga quedó `FAILED` con el motivo y las tablas no cambiaron.

Mejoras posibles: alertas cuando el porcentaje de una regla WARNING cambia mucho respecto al corte anterior, una tabla de cuarentena para rechazar solo las filas inválidas en lugar del corte completo, y herramientas como Great Expectations o Soda si las reglas crecen.
