## ETL a Warehouse (prestamos_dw)

Requisitos: Python 3.9+, MySQL 8+.

Instalar deps:
```bash
pip install -r requirements.txt
```

Configurar `.env` desde `.env.example`, ajustando `DB_*`, `SRC_DB`, `DW_DB` y `DDL_PATH` si quieres aplicar el DDL automáticamente.

Ejecutar:
```bash
python etl/load_dw.py
```

El script cargará dimensiones, `dim_location`, generará `dim_date` y poblará `fact_originations` y `fact_performance_snapshot`.


