"""DDL de las tablas del pipeline."""

# Versión vigente de cada transacción (una fila por row_hash)
CREATE_TRANSACTION_C = """
CREATE TABLE IF NOT EXISTS transaction_c (
    row_hash        VARCHAR PRIMARY KEY,
    client_id       VARCHAR NOT NULL,
    date            DATE,
    product         VARCHAR,
    type            VARCHAR,
    fund            VARCHAR,
    amount          DECIMAL(18, 2),
    description     VARCHAR,
    commercial_name VARCHAR,
    valid_from      DATE NOT NULL
)
"""

# Todas las versiones y operaciones de cada transacción a lo largo de los cortes
CREATE_TRANSACTION_H = """
CREATE TABLE IF NOT EXISTS transaction_h (
    row_hash          VARCHAR NOT NULL,
    previous_row_hash VARCHAR,
    operation         VARCHAR NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
    client_id         VARCHAR NOT NULL,
    date              DATE,
    product           VARCHAR,
    type              VARCHAR,
    fund              VARCHAR,
    amount            DECIMAL(18, 2),
    description       VARCHAR,
    commercial_name   VARCHAR,
    valid_from        DATE NOT NULL,
    valid_to          DATE,
    load_id           INTEGER NOT NULL
)
"""

CREATE_LOAD_ID_SEQ = "CREATE SEQUENCE IF NOT EXISTS load_id_seq START 1"

# Una fila por cada intento de carga de un corte
CREATE_LOAD_AUDIT = """
CREATE TABLE IF NOT EXISTS load_audit (
    load_id        INTEGER PRIMARY KEY DEFAULT nextval('load_id_seq'),
    snapshot_date  DATE,
    file_name      VARCHAR NOT NULL,
    file_checksum  VARCHAR NOT NULL,
    started_at     TIMESTAMP NOT NULL,
    finished_at    TIMESTAMP,
    status         VARCHAR NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    rows_read      BIGINT,
    rows_inserted  BIGINT,
    rows_updated   BIGINT,
    rows_deleted   BIGINT,
    rows_unchanged BIGINT,
    error_message  VARCHAR
)
"""

# Resultado de cada regla de calidad en cada carga
CREATE_DQ_RESULT = """
CREATE TABLE IF NOT EXISTS dq_result (
    load_id     INTEGER NOT NULL,
    rule_name   VARCHAR NOT NULL,
    severity    VARCHAR NOT NULL CHECK (severity IN ('ERROR', 'WARNING')),
    failed_rows BIGINT NOT NULL,
    total_rows  BIGINT NOT NULL,
    failed_pct  DECIMAL(5, 2),
    passed      BOOLEAN NOT NULL
)
"""

ALL_DDL = [
    CREATE_TRANSACTION_C,
    CREATE_TRANSACTION_H,
    CREATE_LOAD_ID_SEQ,
    CREATE_LOAD_AUDIT,
    CREATE_DQ_RESULT,
]
