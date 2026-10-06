"""Pipeline de cortes diarios de movimientos -> DuckDB (SCD tipo 4).

Busca los parquet de data/raw, salta los que ya se cargaron con éxito y carga los pendientes
en orden de corte. Cada carga: extract -> validate (calidad de datos) -> load_scd4.

Uso:
    python src/pipeline.py
"""
import hashlib
import logging
import sys
from datetime import datetime
from pathlib import Path

import duckdb

from dq_rules import REQUIRED_COLUMNS, ROW_RULES
from schema import ALL_DDL
from utils import clean_text, content_hash, dq_normalize_date, dq_normalize_fund, dq_normalize_type

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
OUTPUT_DIR = BASE_DIR / "data" / "output"
DB_PATH = OUTPUT_DIR / "tyba.duckdb"
FILE_PATTERN = "movimientos_dia_*.parquet"

DATA_COLUMNS = ["client_id", "date", "product", "type", "fund", "amount", "description", "commercial_name"]
BUSINESS_KEY = "client_id, date, product, fund, type"

log = logging.getLogger("pipeline")


class DataQualityError(Exception):
    """Una regla de severidad ERROR no se cumplió."""


def file_checksum(path: Path) -> str:
    """sha256 del archivo, leído por bloques para no cargarlo entero en memoria."""
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def snapshot_date_of(con: duckdb.DuckDBPyConnection, path: Path):
    """Fecha del corte: la fecha de movimiento más reciente del archivo."""
    return con.execute(f"SELECT max({dq_normalize_date()}) FROM read_parquet('{path}')").fetchone()[0]


def pending_files(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Archivos de data/raw que no se han cargado con éxito, ordenados por fecha de corte."""
    loaded = {row[0] for row in con.execute(
        "SELECT file_checksum FROM load_audit WHERE status = 'SUCCESS'").fetchall()}

    pending = []
    for path in sorted(RAW_DIR.glob(FILE_PATTERN)):
        checksum = file_checksum(path)
        if checksum in loaded:
            log.info("%s ya fue cargado, se omite", path.name)
            continue
        pending.append({"path": path, "checksum": checksum, "snapshot_date": snapshot_date_of(con, path)})

    return sorted(pending, key=lambda f: f["snapshot_date"])


def start_load(con: duckdb.DuckDBPyConnection, file: dict) -> int:
    """Registra la carga en load_audit con estado RUNNING y devuelve su load_id."""
    return con.execute("""
        INSERT INTO load_audit (snapshot_date, file_name, file_checksum, started_at, status)
        VALUES (?, ?, ?, ?, 'RUNNING')
        RETURNING load_id
    """, [file["snapshot_date"], file["path"].name, file["checksum"], datetime.now()]).fetchone()[0]


def finish_load(con: duckdb.DuckDBPyConnection, load_id: int, status: str,
                counts: dict | None = None, error: str | None = None) -> None:
    counts = counts or {}
    con.execute("""
        UPDATE load_audit
        SET finished_at = ?, status = ?, rows_read = ?, rows_inserted = ?, rows_updated = ?,
            rows_deleted = ?, rows_unchanged = ?, error_message = ?
        WHERE load_id = ?
    """, [datetime.now(), status, counts.get("read"), counts.get("inserted"), counts.get("updated"),
          counts.get("deleted"), counts.get("unchanged"), error, load_id])


def extract(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    """Lee el parquet, normaliza los campos y calcula row_hash en la tabla temporal stg."""
    columns = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()}
    missing = [c for c in REQUIRED_COLUMNS if c not in columns]
    if missing:
        raise DataQualityError(f"Faltan columnas en {path.name}: {missing}")

    # Filas idénticas dentro del mismo corte se numeran (n) para que cada una tenga su propio row_hash
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE stg AS
        WITH normalized AS (
            SELECT {clean_text('id_cliente')} AS client_id,
                   {dq_normalize_date()} AS date,
                   {clean_text('product')} AS product,
                   {dq_normalize_type()} AS type,
                   {dq_normalize_fund()} AS fund,
                   CAST(amount AS DECIMAL(18, 2)) AS amount,
                   {clean_text('description')} AS description,
                   {clean_text('commercial_name')} AS commercial_name
            FROM read_parquet('{path}')
        ),
        hashed AS (
            SELECT *, {content_hash(DATA_COLUMNS)} AS content_hash FROM normalized
        )
        SELECT * EXCLUDE (content_hash, n),
               CASE WHEN n = 1 THEN content_hash ELSE md5(content_hash || '#' || n) END AS row_hash
        FROM (SELECT *, row_number() OVER (PARTITION BY content_hash) AS n FROM hashed)
    """)


def validate(con: duckdb.DuckDBPyConnection, load_id: int, snapshot_date) -> None:
    """Evalúa las reglas de calidad en una sola consulta, guarda los resultados y falla si alguna ERROR no se cumple."""
    total = con.execute("SELECT count(*) FROM stg").fetchone()[0]
    last_snapshot = con.execute(
        "SELECT max(snapshot_date) FROM load_audit WHERE status = 'SUCCESS'").fetchone()[0]

    # Reglas a nivel de archivo
    results = [
        ("file_not_empty", "ERROR", 0 if total > 0 else 1, 1),
        ("snapshot_after_last_load", "ERROR",
         0 if last_snapshot is None or snapshot_date > last_snapshot else 1, 1),
    ]

    # Reglas por fila
    if total > 0:
        filters = ", ".join(f"count(*) FILTER (WHERE {rule['fails_when']})" for rule in ROW_RULES)
        failed = con.execute(f"SELECT {filters} FROM stg").fetchone()
        results += [(rule["name"], rule["severity"], failed_rows, total)
                    for rule, failed_rows in zip(ROW_RULES, failed)]

    con.executemany("""
        INSERT INTO dq_result VALUES (?, ?, ?, ?, ?, round(100.0 * ? / ?, 2), ? = 0)
    """, [[load_id, name, severity, failed_rows, rows, failed_rows, rows, failed_rows]
          for name, severity, failed_rows, rows in results])

    for name, severity, failed_rows, rows in results:
        if failed_rows:
            log.log(logging.ERROR if severity == "ERROR" else logging.WARNING,
                    "DQ %s %s: %s de %s", severity, name, failed_rows, rows)

    errors = [name for name, severity, failed_rows, _ in results if severity == "ERROR" and failed_rows]
    if errors:
        raise DataQualityError(f"Reglas ERROR incumplidas: {errors}")


def load_scd4(con: duckdb.DuckDBPyConnection, load_id: int, snapshot_date) -> dict:
    """Compara stg con transaction_c y aplica INSERT / UPDATE / DELETE en transaction_c y transaction_h."""
    fields = ", ".join(DATA_COLUMNS)
    s = snapshot_date

    # Filas que aparecen (no están en la tabla vigente) y que desaparecen (no están en el corte)
    con.execute("CREATE OR REPLACE TEMP TABLE new_rows AS SELECT * FROM stg ANTI JOIN transaction_c USING (row_hash)")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE gone_rows AS
        SELECT * EXCLUDE (valid_from) FROM transaction_c ANTI JOIN stg USING (row_hash)
    """)

    # Correcciones: una fila que desaparece y otra que aparece con la misma clave de negocio, 1 a 1
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE pairs AS
        SELECT g.row_hash AS old_hash, n.row_hash AS new_hash
        FROM (SELECT *, count(*) OVER (PARTITION BY {BUSINESS_KEY}) AS k FROM gone_rows) g
        JOIN (SELECT *, count(*) OVER (PARTITION BY {BUSINESS_KEY}) AS k FROM new_rows) n
          USING ({BUSINESS_KEY})
        WHERE g.k = 1 AND n.k = 1
    """)

    # Historial: cerrar las versiones que dejan de estar vigentes y los DELETE de filas que reaparecen
    con.execute("""
        UPDATE transaction_h SET valid_to = ?
        WHERE valid_to IS NULL
          AND (row_hash IN (SELECT row_hash FROM gone_rows)
               OR (operation = 'DELETE' AND row_hash IN (SELECT row_hash FROM new_rows)))
    """, [s])

    con.execute(f"""
        INSERT INTO transaction_h
        SELECT n.row_hash, p.old_hash, 'UPDATE', {fields}, ?, NULL, ?
        FROM new_rows n JOIN pairs p ON n.row_hash = p.new_hash
    """, [s, load_id])
    con.execute(f"""
        INSERT INTO transaction_h
        SELECT row_hash, NULL, 'INSERT', {fields}, ?, NULL, ?
        FROM new_rows WHERE row_hash NOT IN (SELECT new_hash FROM pairs)
    """, [s, load_id])
    con.execute(f"""
        INSERT INTO transaction_h
        SELECT row_hash, NULL, 'DELETE', {fields}, ?, NULL, ?
        FROM gone_rows WHERE row_hash NOT IN (SELECT old_hash FROM pairs)
    """, [s, load_id])

    # Tabla vigente
    con.execute("DELETE FROM transaction_c WHERE row_hash IN (SELECT row_hash FROM gone_rows)")
    con.execute(f"INSERT INTO transaction_c SELECT row_hash, {fields}, ? FROM new_rows", [s])

    counts = dict(zip(
        ["read", "new", "gone", "updated", "current"],
        con.execute("""
            SELECT (SELECT count(*) FROM stg), (SELECT count(*) FROM new_rows), (SELECT count(*) FROM gone_rows),
                   (SELECT count(*) FROM pairs), (SELECT count(*) FROM transaction_c)
        """).fetchone(),
    ))
    result = {
        "read": counts["read"],
        "inserted": counts["new"] - counts["updated"],
        "updated": counts["updated"],
        "deleted": counts["gone"] - counts["updated"],
        "unchanged": counts["read"] - counts["new"],
    }

    # Conciliación: cada fila del corte es nueva, corregida o sin cambios, y la tabla vigente es igual al corte
    if result["read"] != result["inserted"] + result["updated"] + result["unchanged"] or counts["current"] != counts["read"]:
        raise RuntimeError(f"La conciliación no cuadra: {result}, filas vigentes={counts['current']}")

    return result


def process_file(con: duckdb.DuckDBPyConnection, file: dict) -> None:
    """Carga un corte. Si algo falla, deshace los cambios y deja la carga en FAILED."""
    log.info("Cargando %s (corte %s)", file["path"].name, file["snapshot_date"])
    load_id = start_load(con, file)
    in_transaction = False
    try:
        extract(con, file["path"])
        validate(con, load_id, file["snapshot_date"])

        con.begin()
        in_transaction = True
        counts = load_scd4(con, load_id, file["snapshot_date"])
        finish_load(con, load_id, "SUCCESS", counts)
        con.commit()
        log.info("Carga %s OK: %s", load_id, counts)
    except Exception as error:
        if in_transaction:
            con.rollback()
        finish_load(con, load_id, "FAILED", error=str(error))
        log.error("Carga %s FAILED: %s", load_id, error)
        raise


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    try:
        for ddl in ALL_DDL:
            con.execute(ddl)

        files = pending_files(con)
        if not files:
            log.info("No hay cortes pendientes")

        # Los cortes dependen del anterior: si uno falla, no se cargan los siguientes
        for file in files:
            process_file(con, file)
        return 0
    except DataQualityError:
        # Ya quedó registrado en load_audit y dq_result
        return 1
    except Exception:
        log.exception("Pipeline detenido")
        return 1
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
