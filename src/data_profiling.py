"""Perfilamiento de los cortes en data/raw.

Reúne las consultas usadas para encontrar las inconsistencias documentadas en el README
(sección "Calidad de datos encontrada"). No escribe en la base de datos: solo lee los
parquet con DuckDB e imprime los resultados.

Uso:
    python src/data_profiling.py
"""
from pathlib import Path
import duckdb
from utils import dq_normalize_type, dq_normalize_fund, dq_normalize_date

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
SNAPSHOTS = {
    "t": RAW_DIR / "movimientos_dia_T.parquet",
    "t1": RAW_DIR / "movimientos_dia_T1.parquet",
}
BUSINESS_KEY = "id_cliente, date, product, fund, type"


def section(title: str) -> None:
    print(f"\n=== {title} ===")


def load_snapshots(con: duckdb.DuckDBPyConnection) -> None:
    """Crea una vista cruda y otra normalizada por cada corte."""
    for name, path in SNAPSHOTS.items():
        con.execute(f"CREATE VIEW {name}_raw AS SELECT * FROM read_parquet('{path}')")
        con.execute(f"""
            CREATE VIEW {name} AS
            SELECT id_cliente,
                   {dq_normalize_date()} AS date,
                   product,
                   {dq_normalize_type()} AS type,
                   {dq_normalize_fund()} AS fund,
                   amount,
                   description,
                   commercial_name
            FROM {name}_raw
        """)


def profile_keys(con: duckdb.DuckDBPyConnection) -> None:
    """Comprueba si existe un identificador único de transacción."""
    section("Filas, clientes y filas duplicadas")
    for name in SNAPSHOTS:
        con.sql(f"""
            SELECT '{name}' AS corte, count(*) AS filas,
                   count(DISTINCT id_cliente) AS clientes,
                   count(*) - count(DISTINCT {name}_raw) AS filas_duplicadas
            FROM {name}_raw
        """).show()

    section("Claves candidatas (filas repetidas en T)")
    for key in ["id_cliente, date", "id_cliente, date, product",
                "id_cliente, date, product, fund", BUSINESS_KEY]:
        con.sql(f"""
            SELECT '{key}' AS clave, count(*) - count(DISTINCT ({key})) AS filas_repetidas
            FROM t_raw
        """).show()


def profile_categories(con: duckdb.DuckDBPyConnection) -> None:
    """raw data de cada campo categórico."""
    for column in ["type", "fund", "product", "description", "commercial_name"]:
        section(f"Valores crudos de {column} (T y T1)")
        con.sql(f"""
            SELECT {column}, count(*) AS filas
            FROM (SELECT {column} FROM t_raw UNION ALL SELECT {column} FROM t1_raw)
            GROUP BY 1 ORDER BY 2 DESC
        """).show(max_rows=50)


def profile_dates(con: duckdb.DuckDBPyConnection) -> None:
    """Identifica los formatos de fecha y si el formato con '/' es dd/mm o mm/dd."""
    both = "(SELECT date FROM t_raw UNION ALL SELECT date FROM t1_raw)"

    section("Formatos de date")
    con.sql(f"""
        SELECT regexp_replace(date, '[0-9]', '9', 'g') AS patron, count(*) AS filas,
               min(date) AS ejemplo
        FROM {both} GROUP BY 1 ORDER BY 2 DESC
    """).show()

    section("Fechas normalizadas")
    con.sql("""
        SELECT count(*) FILTER (WHERE date IS NULL) AS no_reconocidas,
               min(date) AS minima, max(date) AS maxima
        FROM (SELECT date FROM t UNION ALL SELECT date FROM t1)
    """).show()


def profile_nulls(con: duckdb.DuckDBPyConnection) -> None:
    """Cuenta los nulos por campo y revisa si se pueden deducir de otros campos."""
    section("Nulos por campo")
    for name in SNAPSHOTS:
        con.sql(f"""
            SELECT '{name}' AS corte,
                   count(*) FILTER (WHERE amount IS NULL) AS amount,
                   count(*) FILTER (WHERE description IS NULL) AS description,
                   count(*) FILTER (WHERE commercial_name IS NULL) AS commercial_name
            FROM {name}
        """).show()

    section("% de commercial_name nulo por producto (T)")
    con.sql("""
        SELECT product, round(100 * avg((commercial_name IS NULL)::INT), 1) AS pct_nulo
        FROM t GROUP BY 1 ORDER BY 2 DESC
    """).show()


def profile_amount(con: duckdb.DuckDBPyConnection) -> None:
    """Revisa el signo de amount según el type."""
    section("Signo de amount por type (T)")
    con.sql("""
        SELECT type,
               count(*) FILTER (WHERE amount < 0) AS negativos,
               count(*) FILTER (WHERE amount = 0) AS ceros,
               count(*) FILTER (WHERE amount > 0) AS positivos,
               count(*) FILTER (WHERE amount IS NULL) AS nulos
        FROM t GROUP BY 1 ORDER BY 1
    """).show()


def profile_description_vs_type(con: duckdb.DuckDBPyConnection) -> None:
    """Cruza description con type para ver si son coherentes."""
    section("description vs type (T)")
    con.sql("PIVOT (SELECT description, type FROM t) ON type USING count(*) ORDER BY 1").show()




if __name__ == "__main__":
    con = duckdb.connect()
    load_snapshots(con)

    profile_keys(con)
    profile_categories(con)
    profile_dates(con)
    profile_nulls(con)
    profile_amount(con)
    profile_description_vs_type(con)

    con.close()
