# Microsoft Fabric Data Engineering

Casos prácticos y notas de estudio mientras preparo la certificación **DP-700 (Fabric Data Engineer Associate)**. En vez de limitarme a los ejercicios guiados, aplico los conceptos del temario sobre datos de mi propio proyecto: [stock-pipeline-portfolio](https://github.com/jpsegura9919/stock-pipeline-portfolio), un ETL de precios diarios de ~100 tickers internacionales.

## Contenido

| Documento | Qué cubre |
|---|---|
| [Caso práctico: medallion, seguridad y alertas](fabric-caso-practico-bolsa.md) | Calidad de datos, arquitectura medallion en Lakehouse, pipeline idempotente con parámetro, seguridad por filas y columnas en Warehouse, Activator y Centro de supervisión |

## Temas que se tocan

- **Lakehouse:** PySpark, Delta Lake, `replaceWhere`, `DESCRIBE HISTORY`
- **Warehouse:** T-SQL, CTAS, `GRANT`/`DENY` por columna, seguridad por filas con `SECURITY POLICY`
- **Eventhouse:** KQL, funciones de ventana, diferencias de manejo de nulos
- **Orquestación:** pipelines de Data Factory con parámetros y notebooks encadenados
- **Supervisión:** Activator frente a Centro de supervisión

## Hallazgo principal

El mismo CSV se interpretó de tres formas distintas: los valores ausentes llegaron como `NULL` en Spark y en el Warehouse, pero como cadena vacía en el Eventhouse. Conviene perfilar los datos en cada motor antes de comparar resultados.

## Contexto

Trabajo como desarrollador de datos freelance (Power BI, Python, PostgreSQL, Azure) y tengo la certificación PL-300. Este repositorio documenta el camino hacia el DP-700.

---
*Contenido de elaboración propia sobre datos propios. No incluye preguntas de examen.*
