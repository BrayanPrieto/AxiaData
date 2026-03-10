## Warehouse de Préstamos - Guía de Uso y Modelo

Objetivos: evolución de montos/tasas, perfiles, morosidad por propósito, DTI vs default y desempeño geográfico.

### Modelo estrella
- Hechos: `fact_originations` (grano: préstamo en decisión), `fact_performance_snapshot` (grano: foto actual por préstamo).
- Dimensiones: `dim_date`, `dim_purpose`, `dim_application_type`, `dim_verification_status`, `dim_policy_code`, `dim_disbursement_method`, `dim_grade`, `dim_sub_grade`, `dim_term`, `dim_home_ownership`, `dim_employment_length`, `dim_location`, `dim_loan_status`, `dim_settlement_status`, `dim_hardship_*`.

### Creación
Ejecuta:
```sql
SOURCE /ruta/absoluta/a/BD/Esquema/BD_Warehouse.sql;
```

### ETL (resumen)
1) Cargar dimensiones por código 1:1 desde `prestamos_norm`.
2) Construir `dim_location` con `application_address` + `dim_state`/`dim_zip3`.
3) Generar `dim_date` para el rango detectado.
4) Poblar `fact_originations` y `fact_performance_snapshot` con INSERT…SELECT.

### Consultas ejemplo
- Evolución monto y tasa por año:
```sql
SELECT d.year, AVG(f.funded_amount) avg_funded, AVG(f.int_rate) avg_rate
FROM prestamos_dw.fact_originations f
JOIN prestamos_dw.dim_date d ON f.decision_date_id = d.date_id
GROUP BY d.year
ORDER BY d.year;
```

- Morosidad por propósito:
```sql
SELECT pu.purpose_code,
       AVG(CASE WHEN ls.loan_status_code IN ('Charged Off','Default') OR ls.loan_status_code LIKE 'Late%'
                THEN 1 ELSE 0 END) AS delinquency_rate
FROM prestamos_dw.fact_originations f
JOIN prestamos_dw.dim_purpose pu ON f.purpose_key = pu.purpose_key
LEFT JOIN prestamos_dw.fact_performance_snapshot p ON p.loan_id = f.loan_id
LEFT JOIN prestamos_dw.dim_loan_status ls ON p.loan_status_key = ls.loan_status_key
GROUP BY pu.purpose_code
ORDER BY delinquency_rate DESC;
```


