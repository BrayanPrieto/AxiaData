Diccionario de Datos: Data Warehouse prestamos_dw

Este documento es el mapa oficial para el proceso ETL desde la base de datos transaccional (bdPrestamosNormalizada) al data warehouse (prestamos_dw).

1. Visión General del Modelo

El prestamos_dw es un Esquema de Estrella optimizado para análisis en Power BI. Se compone de:

2 Tablas de Hechos (Fact Tables): Almacenan métricas numéricas sobre los préstamos.

15+ Tablas de Dimensiones (Dimension Tables): Almacenan el contexto (quién, qué, dónde, cuándo, por qué) para filtrar y segmentar los hechos.

2. Tablas de Hechos (Fact Tables)

2.1. fact_originations

Descripción: Una "foto" del préstamo en el momento de su creación/decisión. Contiene todas las métricas sobre el perfil del solicitante y los términos iniciales.

Grano (Granularity): Una fila por cada loan_id (préstamo) único.

Campo (Field)

Descripción

Origen de Datos (bdPrestamosNormalizada)

loan_id

(Clave Primaria) ID único del préstamo.

loan.loan_id

application_id

ID de la solicitud original.

application.application_id

application_date_id

(Clave Foránea) Fecha de la solicitud.

application.application_date -> dim_date.date_id

decision_date_id

(Clave Foránea) Fecha de la decisión.

loan.decision_date -> dim_date.date_id

purpose_key

(Clave Foránea) Propósito del préstamo.

application.purpose_id -> dim_purpose.purpose_key

location_key

(Clave Foránea) Ubicación del solicitante.

application_address (ver lógica de dim_location)

grade_key

(Clave Foránea) Calificación (A-G).

loan.grade_id -> dim_grade.grade_key

sub_grade_key

(Clave Foránea) Sub-Calificación (A1-G5).

loan.sub_grade_id -> dim_sub_grade.sub_grade_key

term_key

(Clave Foránea) Plazo del préstamo en meses.

loan.term_id -> dim_term.term_key

...

(Otras Claves Foráneas de dimensión)

...

requested_amount

(Métrica) Monto solicitado por el cliente.

loan_terms.requested_amount

funded_amount

(Métrica) Monto final aprobado y financiado.

loan_terms.funded_amount

installment

(Métrica) Cuota mensual del préstamo.

loan_terms.installment

int_rate

(Métrica) Tasa de interés del préstamo.

loan_terms.int_rate

annual_inc

(Métrica) Ingreso anual reportado.

applicant_financials_snapshot.annual_inc

dti

(Métrica) Debt-to-Income (Ratio Deuda/Ingreso).

applicant_financials_snapshot.dti

revol_bal

(Métrica) Balance revolvente total.

credit_history_snapshot.revol_bal

...

(Otras métricas de credit_history_snapshot)

...

2.2. fact_performance_snapshot

Descripción: Una "foto" del estado actual del préstamo. Contiene todas las métricas sobre pagos, saldos pendientes, moras y recuperaciones.

Grano (Granularity): Una fila por cada loan_id (préstamo) único.

Campo (Field)

Descripción

Origen de Datos (bdPrestamosNormalizada)

loan_id

(Clave Primaria) ID único del préstamo.

loan.loan_id

loan_status_key

(Clave Foránea) Estado actual del préstamo.

loan.loan_status_id -> dim_loan_status.loan_status_key

last_payment_date_id

(Clave Foránea) Fecha del último pago recibido.

payment_status_snapshot.last_pymnt_d -> dim_date.date_id

next_payment_date_id

(Clave Foránea) Fecha del próximo pago esperado.

payment_status_snapshot.next_pymnt_d -> dim_date.date_id

...

(Otras Claves Foráneas de hardship y settlement)

...

total_pymnt

(Métrica) Suma total de pagos recibidos a la fecha.

payment_status_snapshot.total_pymnt

total_rec_prncp

(Métrica) Total de capital (principal) recibido.

payment_status_snapshot.total_rec_prncp

total_rec_int

(Métrica) Total de intereses recibidos.

payment_status_snapshot.total_rec_int

total_rec_late_fee

(Métrica) Total de cargos por mora recibidos.

payment_status_snapshot.total_rec_late_fee

recoveries

(Métrica) Monto recuperado post-mora (ej. agencia de cobro).

payment_status_snapshot.recoveries

out_prncp

(Métrica) Saldo de capital (principal) pendiente.

payment_status_snapshot.out_prncp

settlement_amount

(Métrica) Monto acordado en una liquidación.

settlement_case.settlement_amount

hardship_amount

(Métrica) Monto del plan de dificultad.

hardship_case.hardship_amount

...

(Otras métricas de payment_status_snapshot)

...

3. Tablas de Dimensiones (Dimension Tables)

3.1. dim_date (Dimensión Conformada)

Descripción: La dimensión de calendario. Es la dimensión más importante, usada para filtrar todos los hechos por tiempo (año, mes, trimestre, etc.).

Grano: Una fila por cada día.

Lógica ETL: NO se carga desde la BD transaccional. Se debe generar con un script (Python/SQL) que llene un rango de fechas (ej. 2010 a 2030) antes de cargar los hechos.

Campos Clave:

date_id (Clave Primaria, formato YYYYMMDD, ej. 20251104)

full_date (ej. 2025-11-04)

year (ej. 2025)

month (ej. 11)

month_name (ej. Noviembre)

quarter (ej. 4)

quarter_name (ej. T4)

day_name (ej. Martes)

3.2. dim_location (Dimensión Combinada)

Descripción: Dimensión desnormalizada para la ubicación geográfica del solicitante.

Grano: Una fila por cada combinación única de state_code y zip3.

Lógica ETL:

INSERT INTO prestamos_dw.dim_location (state_code, zip3)
SELECT 
    s.state_code, 
    z.zip3
FROM bdPrestamosNormalizada.application_address aa
JOIN bdPrestamosNormalizada.dim_state s ON aa.state_id = s.state_id
JOIN bdPrestamosNormalizada.dim_zip3 z ON aa.zip3_id = z.zip3_id
GROUP BY s.state_code, z.zip3;


Campos Clave:

location_key (Clave Primaria, Autoincremental)

state_code (ej. 'NY', 'CA')

zip3 (ej. '100', '902')

3.3. dim_grade y dim_sub_grade (Jerarquía Copo de Nieve)

Descripción: Jerarquía que define la calificación (Grado) y sub-calificación (Sub-Grado) del préstamo.

Grano: dim_grade tiene una fila por grado (A, B, C...); dim_sub_grade tiene una fila por sub-grado (A1, A2, B1...).

Lógica ETL: Carga 1 a 1 desde las tablas transaccionales. dim_grade debe cargarse primero.

Campos Clave:

dim_grade: grade_key (PK), grade_code

dim_sub_grade: sub_grade_key (PK), sub_grade_code, grade_key (FK a dim_grade)

3.4. Otras Dimensiones Simples (Carga 1:1)

Descripción: El resto de las dimensiones (dim_purpose, dim_term, dim_loan_status, etc.) que almacenan códigos y descripciones.

Lógica ETL: Se cargan directamente desde sus tablas dim_* correspondientes en bdPrestamosNormalizada.

Ejemplo (dim_purpose):

INSERT INTO prestamos_dw.dim_purpose (purpose_code)
SELECT purpose_code FROM bdPrestamosNormalizada.dim_purpose;


Campos Clave (Ejemplo):

purpose_key (Clave Primaria, Autoincremental)

purpose_code (ej. 'debt_consolidation', 'credit_card', 'wedding')