"""Reglas de DataQuality.

ERROR: impide cargar el corte con integridad, la carga se marca FAILED y no toca las tablas.
WARNING: dato que requiere una decisión de negocio, se carga tal cual y se registra.
"""

# campos mandatorios
REQUIRED_COLUMNS = [
    "id_cliente", "date", "product", "type", "fund", "amount", "description", "commercial_name",
]

# Reglas por fila, evaluadas sobre el corte normalizado. `fails_when` es la condición SQL que incumple la regla.
ROW_RULES = [
    {"name": "client_id_not_null", "severity": "ERROR", "fails_when": "client_id IS NULL"},
    {"name": "date_parsed", "severity": "ERROR", "fails_when": "date IS NULL"},
    {"name": "type_mapped", "severity": "ERROR", "fails_when": "type IS NULL"},
    {"name": "product_not_null", "severity": "WARNING", "fails_when": "product IS NULL"},
    {"name": "fund_not_null", "severity": "WARNING", "fails_when": "fund IS NULL"},
    {"name": "amount_not_null", "severity": "WARNING", "fails_when": "amount IS NULL"},
    {"name": "amount_not_negative", "severity": "WARNING", "fails_when": "amount < 0"},
    {"name": "amount_not_zero", "severity": "WARNING", "fails_when": "amount = 0"},
    {"name": "description_not_null", "severity": "WARNING", "fails_when": "description IS NULL"},
    {"name": "commercial_name_not_null", "severity": "WARNING", "fails_when": "commercial_name IS NULL"},
]
