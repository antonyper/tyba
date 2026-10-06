import hashlib
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

# Mapeo de type
TYPE_MAP = {
    "entrada": "entrada",
    "in": "entrada",
    "salida": "salida",
    "out": "salida",
}

# Formatos de date que existen en data:
DATE_FORMATS = ["%Y-%m-%d", "%d/%m/%Y"]


def read_parquet_file(file_name: str) -> pd.DataFrame:
    path = DATA_DIR / file_name
    return pd.read_parquet(path, engine="pyarrow")


def clean_text(column: str) -> str:
    """Quita espacios en los extremos, colapsa espacios internos y convierte '' en NULL."""
    return f"NULLIF(regexp_replace(trim({column}), '\\s+', ' ', 'g'), '')"


def dq_normalize_type(column: str = "type") -> str:
    """Mapea las variantes de `type` a 'entrada'/'salida'. Valores no mapeados quedan NULL."""
    cases = " ".join(f"WHEN '{raw}' THEN '{canonical}'" for raw, canonical in TYPE_MAP.items())
    return f"CASE lower({clean_text(column)}) {cases} END"


def dq_normalize_fund(column: str = "fund") -> str:
    """Limpia espacios y convierte `fund` a minúsculas."""
    return f"lower({clean_text(column)})"


def dq_normalize_date(column: str = "date") -> str:
    """Convierte `date` a DATE probando cada formato de DATE_FORMATS. Valores no reconocidos quedan NULL."""
    attempts = ", ".join(f"TRY_STRPTIME({clean_text(column)}, '{fmt}')" for fmt in DATE_FORMATS)
    return f"CAST(COALESCE({attempts}) AS DATE)"


def content_hash(columns: list[str]) -> str:
    """Hash md5 del contenido de las columnas. Usa JSON para distinguir NULL de texto y no mezclar campos."""
    fields = ", ".join(f"{column} := {column}" for column in columns)
    return f"md5(CAST(to_json(struct_pack({fields})) AS VARCHAR))"


def file_checksum(path: Path) -> str:
    """sha256 del archivo, leído por bloques para no cargarlo entero en memoria."""
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()
