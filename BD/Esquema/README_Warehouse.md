## Warehouse de Préstamos - Guía de Uso y Modelo

Este documento resume el propósito, el modelo estrella (dimensiones y hechos), lineamientos de carga (ETL) y consultas ejemplo para responder las preguntas de negocio.

### Objetivos de análisis
- **Evolución de montos y tasas aprobadas** por año y tendencias del apetito de riesgo.
- **Perfil de clientes** (ingresos, empleo, estado, FICO/historial) con mayores montos y mejor desempeño.
- **Morosidad/cancelación por propósito** del préstamo.
- **Relación DTI/endeudamiento con incumplimiento**.
- **Desempeño geográfico**: montos y recuperación por estado/región.

## Modelo estrella
Esquema dimensional en el schema `prestamos_dw`. Las dimensiones están desnormalizadas (conformed) y se comparten entre hechos.

### Tablas de hechos
- **fact_originations**
  - **Grano**: 1 fila por préstamo al momento de decisión (`loan_id`).
  - **Claves**: fechas de solicitud y decisión (`dim_date`), propósito, tipo de aplicación, estado de verificación, política, método de desembolso, grade/subgrade, plazo, vivienda, antigüedad laboral, ubicación.
  - **Medidas**: `requested_amount`, `funded_amount`, `funded_amount_inv`, `installment`, `int_rate`, `annual_inc`, `annual_inc_joint`, `dti`, `dti_joint`, `inq_last_6mths`, `delinq_2yrs`, `mths_since_last_delinq`, `mths_since_recent_inq`, `pub_rec`, `total_acc`, `open_acc`, `revol_bal`, `revol_util`.
  - **Uso**: tendencias de originación, tasas promedio, segmentación por perfil.

- **fact_performance_snapshot**
  - **Grano**: 1 fila por préstamo con el estado financiero más reciente (foto).
  - **Claves**: `dim_loan_status`, fechas de pago/settlement/hardship en `dim_date`.
  - **Medidas**: `last_pymnt_amnt`, `total_pymnt`, `total_pymnt_inv`, `total_rec_prncp`, `total_rec_int`, `total_rec_late_fee`, `recoveries`, `collection_recovery_fee`, `out_prncp`, `out_prncp_inv`, `pymnt_plan`, y campos de `settlement`/`hardship` (opcionales).
  - **Uso**: morosidad, cancelación/charge-off, recuperación, desempeño por propósito/región.

### Dimensiones
- **dim_date**: calendario con `date_id` = YYYYMMDD, año, trimestre, mes, día.
- **dim_purpose**: propósito de préstamo (ej. debt_consolidation, small_business...).
- **dim_application_type**: individual/joint.
- **dim_verification_status**: verificación de ingresos.
- **dim_policy_code**: variante de política.
- **dim_disbursement_method**: método de desembolso.
- **dim_grade / dim_sub_grade**: rating A–G / A1–G5.
- **dim_term**: plazo en meses.
- **dim_home_ownership**: tenencia de vivienda.
- **dim_employment_length**: años/etiqueta de antigüedad.
- **dim_location**: estado (`state_code`) y/o `zip3`.
- **dim_loan_status**: estado operativo del préstamo.
- **dim_settlement_status**: estado de acuerdo/quitación (opcional).
- **dim_hardship_type / dim_hardship_status / dim_hardship_loan_status**: info de planes de dificultad (opcional).

Todas las tablas se definen en `BD_Warehouse.sql`.

## Creación del esquema
1) Ejecuta el script de DDL:
```sql
SOURCE /ruta/absoluta/a/BD/Esquema/BD_Warehouse.sql;
```
2) Verifica que existan `prestamos_dw.fact_*` y `prestamos_dw.dim_*`.

## Lineamientos de ETL (INSERT…SELECT desde `prestamos_norm`)

### 1) Cargar dimensiones de referencia (1:1 por código)
Ejemplos:
```sql
-- Purpose
INSERT INTO prestamos_dw.dim_purpose (purpose_code)
SELECT DISTINCT purpose_code FROM prestamos_norm.dim_purpose;

-- Application type
INSERT INTO prestamos_dw.dim_application_type (application_type_code)
SELECT DISTINCT application_type_code FROM prestamos_norm.dim_application_type;

-- Verification status
INSERT INTO prestamos_dw.dim_verification_status (verification_status_code)
SELECT DISTINCT verification_status_code FROM prestamos_norm.dim_verification_status;

-- Policy code
INSERT INTO prestamos_dw.dim_policy_code (policy_code)
SELECT DISTINCT policy_code FROM prestamos_norm.dim_policy_code;

-- Disbursement method
INSERT INTO prestamos_dw.dim_disbursement_method (disbursement_method_code)
SELECT DISTINCT disbursement_method_code FROM prestamos_norm.dim_disbursement_method;

-- Grade y Subgrade
INSERT INTO prestamos_dw.dim_grade (grade_code)
SELECT DISTINCT grade_code FROM prestamos_norm.dim_grade;

INSERT INTO prestamos_dw.dim_sub_grade (sub_grade_code, grade_key)
SELECT sg.sub_grade_code, dg.grade_key
FROM prestamos_norm.dim_sub_grade sg
JOIN prestamos_norm.dim_grade g ON sg.grade_id = g.grade_id
JOIN prestamos_dw.dim_grade dg ON dg.grade_code = g.grade_code;

-- Term
INSERT INTO prestamos_dw.dim_term (term_months)
SELECT DISTINCT term_months FROM prestamos_norm.dim_term;

-- Home Ownership
INSERT INTO prestamos_dw.dim_home_ownership (home_ownership_code)
SELECT DISTINCT home_ownership_code FROM prestamos_norm.dim_home_ownership;

-- Employment Length
INSERT INTO prestamos_dw.dim_employment_length (years, original_text)
SELECT DISTINCT years, original_text FROM prestamos_norm.dim_emp_length;

-- Loan Status
INSERT INTO prestamos_dw.dim_loan_status (loan_status_code)
SELECT DISTINCT loan_status_code FROM prestamos_norm.dim_loan_status;

-- Settlement / Hardship (opcionales)
INSERT INTO prestamos_dw.dim_settlement_status (settlement_status_code)
SELECT DISTINCT settlement_status_code FROM prestamos_norm.dim_settlement_status;

INSERT INTO prestamos_dw.dim_hardship_type (hardship_type_code)
SELECT DISTINCT hardship_type_code FROM prestamos_norm.dim_hardship_type;

INSERT INTO prestamos_dw.dim_hardship_status (hardship_status_code)
SELECT DISTINCT hardship_status_code FROM prestamos_norm.dim_hardship_status;

INSERT INTO prestamos_dw.dim_hardship_loan_status (hardship_loan_status_code)
SELECT DISTINCT hardship_loan_status_code FROM prestamos_norm.dim_hardship_loan_status;
```

### 2) Cargar `dim_location` (estado/zip3)
```sql
INSERT INTO prestamos_dw.dim_location (state_code, zip3)
SELECT DISTINCT s.state_code, z.zip3
FROM prestamos_norm.application_address aa
LEFT JOIN prestamos_norm.dim_state s ON aa.state_id = s.state_id
LEFT JOIN prestamos_norm.dim_zip3  z ON aa.zip3_id = z.zip3_id;
```

### 3) Cargar `dim_date` (calendario)
Genera un calendario que cubra el rango de `application_date`, `decision_date` y fechas de pago.
```sql
-- Ejemplo mínimo (MySQL 8+, ajusta el rango):
WITH RECURSIVE seq AS (
  SELECT DATE('2007-01-01') AS d
  UNION ALL
  SELECT DATE_ADD(d, INTERVAL 1 DAY) FROM seq WHERE d < DATE('2030-12-31')
)
INSERT INTO prestamos_dw.dim_date (date_id, full_date, year, quarter, quarter_name, month, month_name, day, day_of_week, day_name, week_of_year)
SELECT CAST(DATE_FORMAT(d, '%Y%m%d') AS UNSIGNED), d,
       YEAR(d), QUARTER(d), CONCAT('Q', QUARTER(d)),
       MONTH(d), DATE_FORMAT(d, '%M'),
       DAY(d), DAYOFWEEK(d), DATE_FORMAT(d, '%W'),
       WEEKOFYEAR(d)
FROM seq;
```

### 4) Poblar `fact_originations`
```sql
INSERT INTO prestamos_dw.fact_originations (
  loan_id, application_id, application_date_id, decision_date_id,
  purpose_key, application_type_key, verification_status_key, policy_code_key, disbursement_method_key,
  grade_key, sub_grade_key, term_key, home_ownership_key, employment_length_key, location_key,
  requested_amount, funded_amount, funded_amount_inv, installment, int_rate,
  annual_inc, annual_inc_joint, dti, dti_joint,
  inq_last_6mths, delinq_2yrs, mths_since_last_delinq, mths_since_recent_inq, pub_rec, total_acc, open_acc, revol_bal, revol_util
)
SELECT
  l.loan_id,
  a.application_id,
  CAST(DATE_FORMAT(a.application_date, '%Y%m%d') AS UNSIGNED) AS application_date_id,
  CAST(DATE_FORMAT(l.decision_date, '%Y%m%d') AS UNSIGNED)     AS decision_date_id,
  dp.purpose_key,
  dat.application_type_key,
  dvs.verification_status_key,
  dpc.policy_code_key,
  ddm.disbursement_method_key,
  dg.grade_key,
  dsg.sub_grade_key,
  dt.term_key,
  dho.home_ownership_key,
  del.employment_length_key,
  dl.location_key,
  lt.requested_amount, lt.funded_amount, lt.funded_amount_inv, lt.installment, lt.int_rate,
  af.annual_inc, af.annual_inc_joint, af.dti, af.dti_joint,
  ch.inq_last_6mths, ch.delinq_2yrs, ch.mths_since_last_delinq, ch.mths_since_recent_inq,
  ch.pub_rec, ch.total_acc, ch.open_acc, ch.revol_bal, ch.revol_util
FROM prestamos_norm.loan l
JOIN prestamos_norm.application a ON l.application_id = a.application_id
LEFT JOIN prestamos_norm.loan_terms lt ON lt.loan_id = l.loan_id
LEFT JOIN prestamos_norm.applicant_financials_snapshot af ON af.application_id = a.application_id
LEFT JOIN prestamos_norm.credit_history_snapshot ch ON ch.application_id = a.application_id
LEFT JOIN prestamos_norm.employment e ON e.application_id = a.application_id
LEFT JOIN prestamos_norm.application_address aa ON aa.application_id = a.application_id

-- Source code lookups
LEFT JOIN prestamos_norm.dim_purpose sp ON sp.purpose_id = a.purpose_id
LEFT JOIN prestamos_norm.dim_application_type sat ON sat.application_type_id = a.application_type_id
LEFT JOIN prestamos_norm.dim_verification_status sv ON sv.verification_status_id = a.verification_status_id
LEFT JOIN prestamos_norm.dim_policy_code spc ON spc.policy_code_id = a.policy_code_id
LEFT JOIN prestamos_norm.dim_disbursement_method sdm ON sdm.disbursement_method_id = a.disbursement_method_id
LEFT JOIN prestamos_norm.dim_grade sg ON sg.grade_id = l.grade_id
LEFT JOIN prestamos_norm.dim_sub_grade ssg ON ssg.sub_grade_id = l.sub_grade_id
LEFT JOIN prestamos_norm.dim_term stm ON stm.term_id = l.term_id
LEFT JOIN prestamos_norm.dim_home_ownership sho ON sho.home_ownership_id = e.home_ownership_id
LEFT JOIN prestamos_norm.dim_emp_length sel ON sel.emp_length_id = e.emp_length_id
LEFT JOIN prestamos_norm.dim_state s ON s.state_id = aa.state_id
LEFT JOIN prestamos_norm.dim_zip3 z ON z.zip3_id = aa.zip3_id

-- DW dimension joins by code
LEFT JOIN prestamos_dw.dim_purpose dp ON dp.purpose_code = sp.purpose_code
LEFT JOIN prestamos_dw.dim_application_type dat ON dat.application_type_code = sat.application_type_code
LEFT JOIN prestamos_dw.dim_verification_status dvs ON dvs.verification_status_code = sv.verification_status_code
LEFT JOIN prestamos_dw.dim_policy_code dpc ON dpc.policy_code = spc.policy_code
LEFT JOIN prestamos_dw.dim_disbursement_method ddm ON ddm.disbursement_method_code = sdm.disbursement_method_code
LEFT JOIN prestamos_dw.dim_grade dg ON dg.grade_code = sg.grade_code
LEFT JOIN prestamos_dw.dim_sub_grade dsg ON dsg.sub_grade_code = ssg.sub_grade_code
LEFT JOIN prestamos_dw.dim_term dt ON dt.term_months = stm.term_months
LEFT JOIN prestamos_dw.dim_home_ownership dho ON dho.home_ownership_code = sho.home_ownership_code
LEFT JOIN prestamos_dw.dim_employment_length del ON del.original_text = sel.original_text
LEFT JOIN prestamos_dw.dim_location dl ON dl.state_code <=> s.state_code AND dl.zip3 <=> z.zip3;
```

### 5) Poblar `fact_performance_snapshot`
```sql
INSERT INTO prestamos_dw.fact_performance_snapshot (
  loan_id,
  loan_status_key, last_payment_date_id, next_payment_date_id,
  last_pymnt_amnt, total_pymnt, total_pymnt_inv, total_rec_prncp, total_rec_int, total_rec_late_fee,
  recoveries, collection_recovery_fee, out_prncp, out_prncp_inv, pymnt_plan,
  settlement_status_key, debt_settlement_flag, settlement_amount, settlement_percentage, settlement_date_id,
  hardship_type_key, hardship_status_key, hardship_loan_status_key, hardship_amount,
  hardship_start_date_id, hardship_end_date_id, payment_plan_start_date_id,
  deferral_term, hardship_length, hardship_dpd
)
SELECT
  l.loan_id,
  dls.loan_status_key,
  CAST(DATE_FORMAT(ps.last_pymnt_d, '%Y%m%d') AS UNSIGNED),
  CAST(DATE_FORMAT(ps.next_pymnt_d, '%Y%m%d') AS UNSIGNED),
  ps.last_pymnt_amnt, ps.total_pymnt, ps.total_pymnt_inv, ps.total_rec_prncp, ps.total_rec_int, ps.total_rec_late_fee,
  ps.recoveries, ps.collection_recovery_fee, ps.out_prncp, ps.out_prncp_inv, ps.pymnt_plan,
  dss.settlement_status_key, sc.debt_settlement_flag, sc.settlement_amount, sc.settlement_percentage,
  CAST(DATE_FORMAT(sc.settlement_date, '%Y%m%d') AS UNSIGNED),
  dht.hardship_type_key, dhs.hardship_status_key, dhl.hardship_loan_status_key, hc.hardship_amount,
  CAST(DATE_FORMAT(hc.hardship_start_date, '%Y%m%d') AS UNSIGNED),
  CAST(DATE_FORMAT(hc.hardship_end_date, '%Y%m%d') AS UNSIGNED),
  CAST(DATE_FORMAT(hc.payment_plan_start_date, '%Y%m%d') AS UNSIGNED),
  hc.deferral_term, hc.hardship_length, hc.hardship_dpd
FROM prestamos_norm.loan l
LEFT JOIN prestamos_norm.payment_status_snapshot ps ON ps.loan_id = l.loan_id
LEFT JOIN prestamos_norm.settlement_case sc ON sc.loan_id = l.loan_id
LEFT JOIN prestamos_norm.hardship_case hc ON hc.loan_id = l.loan_id

LEFT JOIN prestamos_norm.dim_loan_status sls ON sls.loan_status_id = l.loan_status_id
LEFT JOIN prestamos_norm.dim_settlement_status sss ON sss.settlement_status_id = sc.settlement_status_id
LEFT JOIN prestamos_norm.dim_hardship_type sht ON sht.hardship_type_id = hc.hardship_type_id
LEFT JOIN prestamos_norm.dim_hardship_status shs ON shs.hardship_status_id = hc.hardship_status_id
LEFT JOIN prestamos_norm.dim_hardship_loan_status shl ON shl.hardship_loan_status_id = hc.hardship_loan_status_id

LEFT JOIN prestamos_dw.dim_loan_status dls ON dls.loan_status_code = sls.loan_status_code
LEFT JOIN prestamos_dw.dim_settlement_status dss ON dss.settlement_status_code = sss.settlement_status_code
LEFT JOIN prestamos_dw.dim_hardship_type dht ON dht.hardship_type_code = sht.hardship_type_code
LEFT JOIN prestamos_dw.dim_hardship_status dhs ON dhs.hardship_status_code = shs.hardship_status_code
LEFT JOIN prestamos_dw.dim_hardship_loan_status dhl ON dhl.hardship_loan_status_code = shl.hardship_loan_status_code;
```

## Consultas ejemplo (resumen)

### Evolución de monto promedio y tasa
```sql
SELECT d.year,
       AVG(f.funded_amount) AS avg_funded_amount,
       AVG(f.int_rate)      AS avg_interest_rate
FROM prestamos_dw.fact_originations f
JOIN prestamos_dw.dim_date d ON f.decision_date_id = d.date_id
GROUP BY d.year
ORDER BY d.year;
```

### Mejor/peor desempeño por propósito
```sql
SELECT pu.purpose_code,
       AVG(CASE WHEN ls.loan_status_code IN ('Charged Off','Default')
                 OR ls.loan_status_code LIKE 'Late%'
                THEN 1 ELSE 0 END) AS delinquency_rate
FROM prestamos_dw.fact_originations f
JOIN prestamos_dw.dim_purpose pu ON f.purpose_key = pu.purpose_key
LEFT JOIN prestamos_dw.fact_performance_snapshot p ON p.loan_id = f.loan_id
LEFT JOIN prestamos_dw.dim_loan_status ls ON p.loan_status_key = ls.loan_status_key
GROUP BY pu.purpose_code
ORDER BY delinquency_rate DESC;
```

### DTI vs probabilidad de incumplimiento
```sql
SELECT CASE
         WHEN f.dti IS NULL THEN 'Unknown'
         WHEN f.dti < 10 THEN '<10'
         WHEN f.dti < 20 THEN '10-20'
         WHEN f.dti < 30 THEN '20-30'
         WHEN f.dti < 40 THEN '30-40'
         ELSE '>=40'
       END AS dti_bucket,
       AVG(CASE WHEN ls.loan_status_code IN ('Charged Off','Default')
                 OR ls.loan_status_code LIKE 'Late%'
                THEN 1 ELSE 0 END) AS default_rate,
       COUNT(*) AS loans
FROM prestamos_dw.fact_originations f
LEFT JOIN prestamos_dw.fact_performance_snapshot p ON p.loan_id = f.loan_id
LEFT JOIN prestamos_dw.dim_loan_status ls ON p.loan_status_key = ls.loan_status_key
GROUP BY dti_bucket
ORDER BY MIN(f.dti) IS NULL, MIN(f.dti);
```

### Desempeño geográfico
```sql
SELECT loc.state_code,
       SUM(f.funded_amount)                           AS total_funded,
       AVG(f.int_rate)                                AS avg_rate,
       SUM(COALESCE(p.recoveries,0))                  AS total_recoveries,
       SUM(COALESCE(p.total_rec_prncp,0))             AS total_principal_received,
       SUM(COALESCE(p.recoveries,0)) / NULLIF(SUM(COALESCE(p.total_rec_prncp,0)),0) AS recovery_ratio
FROM prestamos_dw.fact_originations f
LEFT JOIN prestamos_dw.dim_location loc ON f.location_key = loc.location_key
LEFT JOIN prestamos_dw.fact_performance_snapshot p ON p.loan_id = f.loan_id
GROUP BY loc.state_code
ORDER BY total_funded DESC;
```

## Notas de modelado
- **Modelo estrella**: consultas rápidas, navegación por dimensiones; evita snowflake salvo `sub_grade`->`grade` que queda referenciado en DW para integridad.
- **Granos**: originación por `loan_id`; desempeño como snapshot actual. Para series temporales de desempeño (mensual), considerar `fact_performance_monthly_snapshot`.
- **dim_date**: conformed calendar; útil para filtros por año/mes/trimestre.
- **SCD**: las dimensiones de códigos (propósito, estatus, etc.) se manejan como tipo 1 (sobrescritura) por simplicidad del caso.

## Siguientes pasos
1) Crear el esquema con `BD_Warehouse.sql`.
2) Cargar dimensiones (códigos), `dim_location` y `dim_date`.
3) Poblar `fact_originations` y `fact_performance_snapshot` con los INSERT…SELECT.
4) Validar conteos y llaves foráneas; correr consultas de ejemplo.
5) (Opcional) Crear vistas por pregunta de negocio para entrega.


