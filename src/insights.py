"""Consultas que respaldan los insights de INSIGHTS.md.

Lee la base generada por el pipeline en modo solo lectura e imprime cada resultado.
Los montos se muestran en miles de millones.

Uso:
    python src/insights.py
"""
from pathlib import Path
import duckdb

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "output" / "tyba.duckdb"

# Flujo neto: entradas suman, salidas restan
NET = "CASE type WHEN 'entrada' THEN amount ELSE -amount END"

QUERIES = {
    "1. Cambios entre cortes por fecha del movimiento": """
        SELECT date,
               count(*) FILTER (WHERE operation = 'DELETE') AS eliminadas,
               count(*) FILTER (WHERE operation = 'UPDATE') AS corregidas,
               count(*) FILTER (WHERE operation = 'INSERT') AS nuevas
        FROM transaction_h WHERE load_id = 2
        GROUP BY 1 ORDER BY 1
    """,
    "1. Nuevas con fecha anterior al corte y clientes afectados": """
        SELECT round(100.0 * count(*) FILTER (WHERE operation = 'INSERT' AND date < '2024-10-16')
                     / count(*) FILTER (WHERE operation = 'INSERT'), 1) AS pct_nuevas_retroactivas,
               count(DISTINCT client_id) AS clientes_con_cambios,
               (SELECT count(DISTINCT client_id) FROM transaction_h WHERE load_id = 1) AS clientes_totales
        FROM transaction_h WHERE load_id = 2
    """,
    "1. Tasa de cambio por producto (sobre las filas de T)": """
        WITH t AS (SELECT * FROM transaction_h WHERE load_id = 1),
             ch AS (SELECT * FROM transaction_h WHERE load_id = 2)
        SELECT t.product, count(*) AS filas_t,
               round(100.0 * count(*) FILTER (WHERE t.valid_to IS NULL) / count(*), 1) AS pct_sin_cambio,
               round(100.0 * count(*) FILTER (WHERE t.row_hash IN
                     (SELECT previous_row_hash FROM ch WHERE operation = 'UPDATE')) / count(*), 1) AS pct_corregido,
               round(100.0 * count(*) FILTER (WHERE t.row_hash IN
                     (SELECT row_hash FROM ch WHERE operation = 'DELETE')) / count(*), 1) AS pct_eliminado
        FROM t GROUP BY 1 ORDER BY 1
    """,
    "2. Totales en T y en T1": f"""
        SELECT 'T' AS corte, count(*) AS filas,
               round(sum(amount) FILTER (WHERE type = 'entrada') / 1e9, 2) AS entradas,
               round(sum(amount) FILTER (WHERE type = 'salida') / 1e9, 2) AS salidas,
               round(sum({NET}) / 1e9, 2) AS neto
        FROM transaction_h WHERE load_id = 1
        UNION ALL
        SELECT 'T1', count(*),
               round(sum(amount) FILTER (WHERE type = 'entrada') / 1e9, 2),
               round(sum(amount) FILTER (WHERE type = 'salida') / 1e9, 2),
               round(sum({NET}) / 1e9, 2)
        FROM transaction_c
    """,
    "2. Magnitud de las correcciones de monto": f"""
        SELECT count(*) AS correcciones,
               round(median(abs(u.amount - o.amount)) / 1e6, 1) AS mediana_cambio_millones,
               round(median(abs(o.amount)) / 1e6, 1) AS mediana_monto_millones,
               count(*) FILTER (WHERE u.amount > o.amount) AS suben,
               count(*) FILTER (WHERE u.amount < o.amount) AS bajan,
               count(*) FILTER (WHERE sign(u.amount) <> sign(o.amount)) AS cambian_signo
        FROM transaction_h u JOIN transaction_h o ON o.row_hash = u.previous_row_hash
        WHERE u.operation = 'UPDATE' AND u.amount <> o.amount
    """,
    "3. Campos que cambian en las correcciones": """
        SELECT count(*) AS correcciones,
               count(*) FILTER (WHERE o.amount IS DISTINCT FROM u.amount) AS amount,
               count(*) FILTER (WHERE o.description IS DISTINCT FROM u.description) AS description,
               count(*) FILTER (WHERE o.commercial_name IS DISTINCT FROM u.commercial_name) AS commercial_name,
               count(*) FILTER (WHERE o.amount IS DISTINCT FROM u.amount
                                AND o.description IS DISTINCT FROM u.description) AS amount_y_description,
               count(*) FILTER (WHERE o.amount IS NULL AND u.amount IS NOT NULL) AS amount_completado,
               count(*) FILTER (WHERE o.amount IS NOT NULL AND u.amount IS NULL) AS amount_vaciado
        FROM transaction_h u JOIN transaction_h o ON o.row_hash = u.previous_row_hash
        WHERE u.operation = 'UPDATE'
    """,
    "4. Panorama vigente": f"""
        SELECT count(*) AS filas, count(DISTINCT client_id) AS clientes, min(date) AS desde, max(date) AS hasta,
               round(100.0 * avg((type = 'entrada')::INT), 1) AS pct_entradas,
               round(sum({NET}) / 1e9, 2) AS neto
        FROM transaction_c
    """,
    "4. Flujo neto por producto": f"""
        SELECT product, count(*) AS filas,
               round(sum(amount) FILTER (WHERE type = 'entrada') / 1e9, 1) AS entradas,
               round(sum(amount) FILTER (WHERE type = 'salida') / 1e9, 1) AS salidas,
               round(sum({NET}) / 1e9, 1) AS neto
        FROM transaction_c GROUP BY 1 ORDER BY neto DESC
    """,
    "4. Flujo neto por fondo": f"""
        SELECT fund, count(*) AS filas, round(sum({NET}) / 1e9, 1) AS neto
        FROM transaction_c GROUP BY 1 ORDER BY neto DESC
    """,
    "4. Volumen por entidad": f"""
        SELECT coalesce(commercial_name, '(no informado)') AS entidad, count(*) AS filas,
               round(sum(abs(amount)) / 1e9, 1) AS volumen, round(sum({NET}) / 1e9, 1) AS neto
        FROM transaction_c GROUP BY 1 ORDER BY volumen DESC
    """,
    "4. Clientes con flujo neto positivo y negativo": f"""
        SELECT count(*) FILTER (WHERE neto > 0) AS positivo, count(*) FILTER (WHERE neto < 0) AS negativo
        FROM (SELECT client_id, sum({NET}) AS neto FROM transaction_c GROUP BY 1)
    """,
    "4. Movimientos por día": f"""
        SELECT date, dayname(date) AS dia, count(*) AS filas, round(sum({NET}) / 1e9, 1) AS neto
        FROM transaction_c GROUP BY 1, 2 ORDER BY 1
    """,
    "5. Impacto de los problemas de calidad": f"""
        SELECT count(*) FILTER (WHERE amount < 0) AS entradas_negativas,
               round(sum(amount) FILTER (WHERE amount < 0) / 1e9, 2) AS suma_negativas,
               round(sum({NET}) / 1e9, 2) AS neto_actual,
               round((sum({NET}) - 2 * sum(amount) FILTER (WHERE amount < 0)) / 1e9, 2) AS neto_si_signo_es_error,
               count(*) FILTER (WHERE amount IS NULL) AS sin_monto,
               round(count(*) FILTER (WHERE amount IS NULL) * avg(abs(amount)) / 1e9, 1) AS volumen_estimado_sin_monto,
               round(100.0 * avg((commercial_name IS NULL)::INT), 1) AS pct_sin_entidad
        FROM transaction_c
    """,
    "6. Distribución de los montos (tramos de 5 millones)": """
        SELECT floor(amount / 5e6) * 5 AS desde_millones, count(*) AS filas
        FROM transaction_c WHERE amount IS NOT NULL GROUP BY 1 ORDER BY 1
    """,
}


if __name__ == "__main__":
    con = duckdb.connect(str(DB_PATH), read_only=True)
    for title, sql in QUERIES.items():
        print(f"\n=== {title} ===")
        con.sql(sql).show(max_rows=50, max_width=200)
    con.close()
