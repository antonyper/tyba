# Insights — Movimientos financieros

Análisis de los dos cortes cargados por el pipeline (T: 2024-10-15, T1: 2024-10-16). Todas las cifras salen de las consultas de [`src/insights.py`](src/insights.py), que lee `data/output/tyba.duckdb` en solo lectura:

```bash
python src/insights.py
```

Los montos están en **miles de millones** en pesos colombianos. El **flujo neto** es entradas − salidas.

## Resumen

Lo que más me llamó la atención es cuánto cambia el histórico de un corte a otro. Entre T y T1 cambió cerca del 30 % de los movimientos, y no solo los recientes: un movimiento de mediados de septiembre tiene la misma probabilidad de ser corregido o eliminado que uno del día anterior. Ningún día del histórico queda cerrado.

Aun así, el flujo neto total apenas se mueve (+1,8 %), porque las correcciones de monto suben y bajan en proporciones parecidas. El problema está en el detalle: la corrección típica es de unos 15 millones sobre montos de unos 24, así que lo que ve un cliente en particular sí puede cambiar bastante de un día a otro.

Casi todas las correcciones tocan un solo campo: el monto en el 61 % de los casos y la descripción en el 39 %.

Del lado del negocio, el mes cierra con una entrada neta de 76,8 mil millones y todos los productos, fondos y entidades tienen más entradas que salidas. Las diferencias entre segmentos son pequeñas; CDT y Divisas son los que más captan.

Antes de usar estas cifras para decidir algo, hay tres temas de calidad que conviene resolver con la fuente. Los montos negativos son el más importante: si resultan ser errores de signo, el flujo neto sube un 65 %. Los montos nulos dejan fuera unos 35 mil millones, y los movimientos sin entidad suman más volumen que cualquier entidad individual.

## 1. Cada corte reescribe el pasado

Entre T y T1:

| | Filas | % de T |
|---|---|---|
| Sin cambios | 35.159 | 70,3 % |
| Eliminadas | 10.996 | 22,0 % |
| Corregidas | 3.845 | 7,7 % |
| Nuevas | 9.996 | — |

Lo relevante no es solo el volumen, sino dónde ocurren los cambios:

- Las eliminaciones y correcciones se reparten de forma uniforme por todo el histórico. Cada día, desde el 2024-09-15 hasta el 2024-10-15, pierde unos 350 movimientos y corrige unos 125. Un movimiento de hace un mes tiene la misma probabilidad de cambiar que uno de ayer.
- El 96,9 % de los movimientos "nuevos" tiene fecha anterior al corte. Solo 312 de las 9.996 filas nuevas son del 2024-10-16; el resto son movimientos de días anteriores que llegan tarde, unos 320 por día.
- Afecta a prácticamente todos los clientes: 2.997 de 3.000 tienen al menos un cambio en su historial.
- No depende del producto: todos los productos pierden entre el 21,4 % y el 22,6 % y corrigen entre el 7,2 % y el 8,3 %.

**Implicaciones:**

- Un reporte de "septiembre" generado hoy y otro generado mañana van a dar cifras distintas. Todo reporte debería indicar **con qué corte** se generó.
- Esto justifica el modelo SCD 4: `transaction_h` permite reconstruir el estado a cualquier fecha de corte y explicar por qué cambió una cifra.
- Conviene preguntarle a la fuente qué significa una eliminación: ¿una cancelación real o una corrección de error? Hoy no se pueden distinguir, y una tasa del 22 % diaria es alta para cualquiera de las dos.

## 2. El total parece estable, pero el detalle no

| Corte | Filas | Entradas | Salidas | Neto |
|---|---|---|---|---|
| T | 50.000 | 604,45 | 528,96 | 75,48 |
| T1 | 49.000 | 595,06 | 518,25 | 76,81 |

- Aunque cambian casi 25.000 filas, el flujo neto solo varía 1,33 mil millones (+1,8 %).
- Las correcciones de monto **no tienen sesgo**: 1.138 suben y 1.114 bajan. Por eso se compensan en el agregado.
- Pero cada corrección es grande: la mediana del cambio es de **15 millones**, cuando la mediana de un monto es de 24 millones. Además, 138 correcciones cambian el signo del monto.

**Implicación:** un indicador global (el flujo neto del mes) es relativamente confiable entre cortes, pero cualquier cifra por cliente o por movimiento puede cambiar mucho de un día a otro. Para el cliente, que ve su propio saldo, la volatilidad es real.

## 3. Qué se corrige

De las 3.845 correcciones:

| Campo que cambia | Correcciones | % |
|---|---|---|
| `amount` | 2.346 | 61,0 % |
| `description` | 1.508 | 39,2 % |
| `commercial_name` | 8 | 0,2 % |
| `amount` y `description` a la vez | 9 | 0,2 % |

- Las correcciones casi siempre cambian **un solo campo**. Eso encaja con correcciones puntuales de la fuente, más que con un reproceso de registros completos.
- **72 montos que eran nulos en T llegan con valor en T1**, y 22 hacen lo contrario. La fuente completa datos con retraso, lo que confirma la decisión de conservar los nulos en lugar de imputarlos: el valor real puede llegar en un corte posterior.

## 4. Panorama del negocio - Estado Actual: T1

- 49.000 movimientos de 3.000 clientes entre el 2024-09-15 y el 2024-10-16.
- 55,3 % son entradas.
- El flujo neto del periodo es de +76,81 mil millones: entran 595,06 y salen 518,25.
- 1.746 clientes (58 %) tienen flujo neto positivo y 1.254 negativo.

### Por producto

| Producto | Entradas | Salidas | Neto |
|---|---|---|---|
| CDT | 76,4 | 63,7 | 12,7 |
| Divisas | 75,1 | 62,9 | 12,2 |
| Bonos | 75,9 | 64,9 | 11,0 |
| ETF | 73,9 | 63,2 | 10,7 |
| Fondo de Pensión | 72,8 | 64,1 | 8,7 |
| Acciones | 72,7 | 64,7 | 8,0 |
| Fondo de Inversión | 74,3 | 67,4 | 6,8 |
| Cuenta de Ahorro | 73,9 | 67,3 | 6,6 |

Todos los productos tienen más entradas que salidas. CDT y Divisas captan casi el doble que Cuenta de Ahorro y Fondo de Inversión, y la diferencia no está en lo que entra sino en lo que sale: Cuenta de Ahorro y Fondo de Inversión tienen las salidas más altas. Tiene sentido, porque son los productos más líquidos.

### Por fondo y entidad

Por fondo pasa lo mismo: todos tienen entrada neta, desde 9,8 mil millones en internacional hasta 12,4 en mercado monetario, una diferencia pequeña.

Las diez entidades mueven volúmenes muy parecidos, entre 94 y 101 mil millones cada una. Donde sí hay diferencia es en el neto: Skandia (8,4) y Scotiabank (7,7) son las que más captan, y Davivienda (4,2) y BTG Pactual (4,3) las que menos.

### Por día

Los dos extremos del rango no son comparables con el resto. El 16 de octubre solo tiene 312 movimientos, lo que hace pensar que el corte de T1 se tomó a mitad del día (y explica en parte que T1 tenga menos filas que T). El 15 de septiembre tiene 1.272: en T tenía 1.613, en T1 perdió 341 y no recibió ninguno nuevo, como si fuera el borde de una ventana móvil de un mes.

Sin contar el 16 de octubre, solo el 19 de septiembre y el 12 de octubre cierran con flujo neto negativo.

## 5. Problemas de calidad que cambian las conclusiones

El pipeline conserva estos datos tal como llegan (ver *Criterio de tratamiento* en el README), pero conviene saber cuánto pesan antes de sacar conclusiones.

### Montos negativos

Hay 989 entradas con monto negativo que suman −25,11 mil millones, y ninguna salida negativa. Si son reversos legítimos, el flujo neto es el que se reporta, 76,81 mil millones. Si en cambio el signo es un error y deberían ser positivas, el flujo neto sería de 127,02, un 65 % más.

De todos los problemas de calidad, este es el que más mueve las cifras, así que lo resolvería con la fuente o con negocio antes de reportar cualquier flujo.

### Montos nulos

1.440 movimientos (2,9 %) no tienen monto y quedan fuera de los totales. Con el monto medio, eso equivale a unos 35 mil millones de volumen, casi la mitad del flujo neto del periodo. No hay forma de saber si son entradas o salidas.

Parte de estos nulos se completa más adelante: como se vio en la sección 3, 72 montos que estaban vacíos en T llegaron con valor en T1.

### Entidad no informada

El 16,7 % de los movimientos no tiene `commercial_name`. Juntos suman 194,7 mil millones de volumen, el doble que cualquier entidad. Por eso cualquier ranking por entidad está incompleto: si se conocieran esas filas, el orden podría cambiar.

### Descripción incoherente con el tipo

Cada descripción aparece casi por igual como entrada y como salida. En T1, por ejemplo, hay 2.443 "Retiro parcial" registrados como entrada y 2.015 "Depósito inicial" como salida. Mientras la fuente no aclare cuál de los dos campos es el correcto, `description` no sirve para clasificar movimientos.


## Recomendaciones

A la fuente le pediría tres cosas: 
- Un `id` de transacción, para que las correcciones dejen de ser una inferencia.
- La fecha del corte dentro del archivo. 
- Una explicación de qué significan los montos negativos y las eliminaciones.

En los reportes, indicaría siempre con qué corte se generaron, y mostraría aparte los movimientos sin monto y sin entidad en lugar de dejarlos fuera sin avisar.

Para el monitoreo, seguiría en `dq_result` y `load_audit` la tasa diaria de eliminaciones y correcciones. Hoy rondan el 22 % y el 8 %; un salto respecto a esos valores sería la primera señal de un problema en la fuente.
