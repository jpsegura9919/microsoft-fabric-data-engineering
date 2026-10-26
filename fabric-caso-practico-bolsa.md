# Caso práctico en Microsoft Fabric: medallion, seguridad y alertas con datos bursátiles reales

Proyecto de preparación de la certificación **DP-700 (Fabric Data Engineer Associate)**. En lugar de limitarme a los ejercicios guiados, apliqué los conceptos del temario sobre datos de mi propio proyecto de ETL bursátil: ~9.000 filas de precios diarios de unos 100 tickers internacionales (marzo-septiembre de 2026), originalmente cargadas desde PostgreSQL/Neon.

## Resumen

| Bloque | Qué hice | Qué demostró |
|---|---|---|
| Calidad de datos | Perfilado con Spark, KQL y T-SQL | Un mismo CSV se interpreta distinto según el motor |
| Medallion | Bronze, silver y gold en un Lakehouse | Limpieza sin perder filas y con monedas recuperadas |
| Orquestación | Pipeline con parámetro `process_date` | Idempotencia comprobada con `DESCRIBE HISTORY` |
| Seguridad | Roles por columna y política por filas en un Warehouse | `GRANT`/`DENY` y predicados con `USER_NAME()` |
| Alertas | Regla de Activador sobre una consulta KQL | Cuándo usar Activador y cuándo el Centro de supervisión |
| Supervisión | Filtros por estado y hora en el Centro de supervisión | Diagnóstico antes de optimizar |

## 1. Hallazgos de calidad de datos

**160 filas sin moneda.** Al perfilar con `df.describe()` en PySpark, `currency` tenía 8.814 valores no nulos de 8.974. Agrupando por fecha, todo se concentraba en los tres primeros días del histórico (6, 77 y 77 filas el 25, 26 y 27 de marzo): un fallo de arranque del pipeline, no un problema de tickers concretos.

**El mismo dato, tres interpretaciones.** Al cargar el mismo CSV en cada motor:

| Motor | Valores ausentes de `currency` | Consulta que los detecta |
|---|---|---|
| Spark (Lakehouse) | `NULL` | `isNull()` |
| T-SQL (Warehouse) | `NULL` | `IS NULL` |
| KQL (Eventhouse) | Cadena vacía | `isempty()`. Con `isnull()` devuelve 0 filas |

**11 duplicados exactos.** Bronze tenía 8.974 filas y silver 8.963. Las 11 filas eliminadas eran el mismo registro repetido (mismo `id`, mismo precio) para 11 tickers el 14 de agosto de 2026. Lo verifiqué comparando `closes_distintos` por grupo y con un `distinct` sobre todas las columnas, que dio 8.963.

**`GBp` no es `GBP`.** Las acciones de Londres cotizan en peniques. Añadí una columna `price_divisor` (100 para `GBp`, 1 para el resto) para poder comparar precios absolutos con otras monedas. Los retornos porcentuales no se ven afectados.

## 2. Arquitectura medallion en el Lakehouse

```
dbo.stock_prices (aterrizaje, sin tocar)
   → dbo.bronze_stock_prices   copia fiel + marca de ingesta
   → dbo.silver_stock_prices   deduplicada, con moneda recuperada y divisor de precio
   → dbo.gold_daily_returns    retorno diario por ticker
```

Silver elimina duplicados de forma determinista (conserva la fila de mayor `id`) y recupera la moneda de cada ticker a partir de sus otros días:

```python
w_dup = Window.partitionBy("ticker", "date").orderBy(F.col("id").desc())
silver = (dia.withColumn("_rn", F.row_number().over(w_dup))
             .filter("_rn = 1").drop("_rn")
             .join(monedas, "ticker", "left")
             .withColumn("currency", F.coalesce("currency", "currency_ref"))
             .withColumn("price_divisor", F.when(F.col("currency") == "GBp", 100).otherwise(1)))
```

Recuentos finales: bronze 8.974, silver 8.963 y gold 8.963, con 0 monedas nulas en silver. Se conservan las 160 filas que en un primer análisis había descartado.

## 3. Pipeline idempotente con parámetro

Un pipeline de Data Factory ejecuta tres cuadernos en cadena (`nb_bronze` → `nb_silver` → `nb_gold`). Cada actividad recibe el mismo valor con `@pipeline().parameters.process_date`, y cada cuaderno tiene su primera celda marcada como celda de parámetros. Cada capa procesa un solo día y lo reescribe con `replaceWhere`:

```python
(df.write.format("delta").mode("overwrite")
   .option("replaceWhere", f"`date` = '{process_date}'")
   .saveAsTable("dbo.bronze_stock_prices"))
```

**Cómo comprobé que funciona**
- **Misma fecha dos veces:** los recuentos no cambian (77, 66 y 66 filas el 14 de agosto) y la marca de ingesta de ese día avanza, mientras la del día anterior no se toca.
- **Fecha distinta:** al lanzar el 13 de agosto, `DESCRIBE HISTORY` de silver y gold muestra como última escritura un predicado con `2026-08-13`. Así se demuestra que el parámetro llega a las tres capas, y no solo que el valor por defecto coincide.

## 4. Comparativa de motores

| Concepto | PySpark / Spark SQL | T-SQL (Warehouse) | KQL (Eventhouse) |
|---|---|---|---|
| Función de ventana | `Window.partitionBy().orderBy()` + `lag()` o `LAG() OVER` | `LAG() OVER` | `partition by` + `prev()` |
| Desviación estándar | `stddev()` | `STDEV()` | `stdev()` |
| Nombres reservados | `` `date` `` | `[open]`, `[close]` | `['date']` |
| Límite de particiones | Sin límite explícito | n/a | 64 por defecto. Solución: `partition hint.strategy=native` |

Con ~100 tickers, la consulta KQL con `partition by ticker` falló con `exceeded amount of maximum partitions allowed (64)` hasta añadir el hint. Los tres motores dieron el mismo ranking de volatilidad en el primer análisis (sin las tres primeras fechas): SFTBY (6,06 %), INTC (5,60 %), AMD (4,82 %), QCOM (4,33 %) y SHOP (4,05 %).

## 5. Seguridad en el Warehouse

La tabla `stocks.daily_returns` se creó con CTAS desde la capa gold (8.963 filas).

**Por columnas.** El rol gestor ve toda la tabla. El rol analista solo ve `ticker`, `price_date`, `retorno_diario_pct` y `currency`, con `DENY` explícito sobre `close` y `close_anterior`. Un `DENY` prevalece siempre sobre cualquier `GRANT`.

```sql
GRANT SELECT ON stocks.daily_returns TO rol_gestor;
GRANT SELECT ON stocks.daily_returns (ticker, price_date, retorno_diario_pct, currency) TO rol_analista;
DENY  SELECT ON stocks.daily_returns ([close], close_anterior) TO rol_analista;
```

**Por filas.** Una función en línea con `SCHEMABINDING` compara `USER_NAME()` con una tabla de asignaciones usuario-moneda, y una política de seguridad la aplica como predicado de filtro. Probé la lógica de la función cruzándola con la tabla: con la asignación `EUR`, devolvió únicamente esa moneda (1.309 filas).

```sql
CREATE SECURITY POLICY seguridad.politica_moneda
ADD FILTER PREDICATE seguridad.fn_filtro_moneda(currency)
ON stocks.daily_returns
WITH (STATE = ON);
```

Limitación: no lo probé con una segunda cuenta. Los permisos granulares limitan a los usuarios con acceso de solo lectura, pero los roles Admin, Member y Contributor del workspace tienen control total sobre el Warehouse.

## 6. Alertas y supervisión

**Activador.** Cargué la capa gold en un Eventhouse y creé una consulta en un KQL Queryset que devuelve solo los tickers con retorno absoluto superior al umbral en la última fecha disponible. Con umbral 2,0 devolvió una fila (SIE.DE, +2,01 % el 11 de septiembre). Sobre esa consulta creé una regla de Activador con ejecución programada y acción de correo electrónico. Después eliminé la regla y el Eventhouse para liberar capacidad.

**Centro de supervisión.** Con la vista de ejecuciones de trabajos filtré por estado y hora de inicio. Tras cancelar una ejecución del pipeline, el filtro por estado `Cancelado` aísla ese elemento y su tasa de éxito pasa del 100 % al 75 %.

| Necesidad | Herramienta |
|---|---|
| Acción automática cuando se cumple una condición | Activador |
| Consultar el histórico y diagnosticar ejecuciones | Centro de supervisión (filtros por estado y hora) |
| Auditar las escrituras de una tabla Delta | `DESCRIBE HISTORY` |

## 7. Lecciones aprendidas

1. Los valores ausentes no son iguales en todos los motores. Conviene perfilar con cada uno antes de comparar resultados.
2. Un duplicado exacto y uno contradictorio se resuelven distinto. Primero hay que comprobar cuál es.
3. Para que un pipeline sea idempotente, cada paso debe reescribir solo su partición (`replaceWhere`), y la prueba definitiva es repetir con la misma y con otra fecha.
4. Verificar que un parámetro llega a su destino exige un valor distinto del que ya tenía por defecto.
5. Los recursos que más capacidad consumen (Eventhouse, sesiones de Spark abiertas) hay que cerrarlos al terminar.

## 8. Pendientes

- Añadir una restricción única sobre `(ticker, date)` y `INSERT ... ON CONFLICT DO NOTHING` en la carga de PostgreSQL. No pude determinar si los 11 duplicados del 14 de agosto estaban en la tabla de origen o se introdujeron al exportar, porque el proyecto de Neon se recreó.
- Probar la seguridad por filas y por columnas con una segunda cuenta de solo lectura.
- Repetir el ejercicio de streaming con `spark.readStream` sobre la tabla Delta.

---
*Ejercicio realizado como parte de la preparación de la certificación Microsoft DP-700 (Fabric Data Engineer Associate).*
