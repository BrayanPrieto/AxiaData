## Diccionario de Datos - `prestamos_dw`

Resumen de propósito, PK/FK y campos por tabla.

### Dimensiones
- `dim_date`: PK `date_id` (YYYYMMDD). Jerarquías de fecha.
- `dim_purpose`: PK `purpose_key`, `purpose_code` único.
- `dim_application_type`: PK `application_type_key`, `application_type_code`.
- `dim_verification_status`: PK `verification_status_key`, `verification_status_code`.
- `dim_policy_code`: PK `policy_code_key`, `policy_code`.
- `dim_disbursement_method`: PK `disbursement_method_key`, `disbursement_method_code`.
- `dim_grade` / `dim_sub_grade`: PKs `grade_key`/`sub_grade_key`; `grade_code`/`sub_grade_code`; FK `grade_key`.
- `dim_term`: PK `term_key`, `term_months`.
- `dim_home_ownership`: PK `home_ownership_key`, `home_ownership_code`.
- `dim_employment_length`: PK `employment_length_key`, `years`, `original_text` único.
- `dim_location`: PK `location_key`, `state_code`, `zip3` (únicos combinados).
- `dim_loan_status`: PK `loan_status_key`, `loan_status_code`.
- `dim_settlement_status`: PK `settlement_status_key`, `settlement_status_code`.
- `dim_hardship_*`: PKs y códigos para tipo/estatus/loan_status.

### Hechos
- `fact_originations` (grano: `loan_id` en decisión):
  - Medidas: `requested_amount`, `funded_amount`, `funded_amount_inv`, `installment`, `int_rate`, `annual_inc`, `dti`, historial crediticio.
  - FKs: fechas y todas las dims de perfil/política/ubicación.

- `fact_performance_snapshot` (grano: `loan_id` foto actual):
  - Medidas: pagos, recuperaciones, saldos, flags/montos de settlement y hardship.
  - FKs: `loan_status_key`, fechas relevantes y dims de settlement/hardship.

### Notas
- Si un atributo se usa para filtrar/segmentar y se comparte entre hechos, debe ser dimensión.
- Atributos degenerados: IDs como `loan_id`/`application_id` pueden vivir en la hecho.


