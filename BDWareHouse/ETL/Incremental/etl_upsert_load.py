#!/usr/bin/env python3
"""
ETL - Carga Incremental (Estrategia "Upsert")

Estrategia: INSERT ... ON DUPLICATE KEY UPDATE
1.  NO borra (TRUNCATE) las tablas de hechos.
2.  Recalcula todos los registros (similar al Full Load).
3.  Usa "INSERT INTO ... ON DUPLICATE KEY UPDATE ..."
    - Si el 'loan_id' es nuevo, lo INSERTA.
    - Si el 'loan_id' ya existe, lo ACTUALIZA (UPDATE).

Ideal para:
-   Sincronizar el DW con el origen sin borrar datos.
-   Es más rápido que TRUNCATE + INSERT si la tabla es masiva
    y solo un pequeño % de filas cambió.
-   Es la estrategia incremental más segura cuando no tienes
    timestamps de 'modified_at' confiables.

Desventaja:
-   Sigue recalculando todas las filas en el SELECT,
    por lo que la parte de "Extract" es igual de lenta que un Full Load.
"""
import os
import sys
import pathlib
import datetime as dt
from typing import Optional, Tuple

import mysql.connector
from mysql.connector.connection import MySQLConnection
from dotenv import load_dotenv

# --- Configuración del Proyecto ---
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent

# --- Funciones de Ayuda (Idénticas a etl_full_load.py) ---

def log(message: str) -> None:
    """Imprime un mensaje con timestamp."""
    now = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{now}] {message}")

def get_env(key: str, default: Optional[str] = None) -> str:
    """Obtiene una variable de entorno o lanza un error si falta."""
    value = os.getenv(key, default)
    if value is None:
        raise RuntimeError(f"Falta la variable de entorno requerida: {key}")
    return value

def connect_mysql(db_name: Optional[str] = None) -> MySQLConnection:
    """Conecta a MySQL usando variables de entorno."""
    return mysql.connector.connect(
        host=get_env('DB_HOST'),
        port=int(get_env('DB_PORT', '3306')),
        user=get_env('DB_USER'),
        password=get_env('DB_PASSWORD'),
        database=db_name,
        autocommit=False, # Importante: manejamos las transacciones
    )

def run_sql(conn: MySQLConnection, sql: str, params: Optional[tuple] = None) -> None:
    """Ejecuta una consulta SQL con manejo de transacciones."""
    cur = conn.cursor()
    try:
        cur.execute(sql, params or ())
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()

def get_db_names() -> Tuple[str, str]:
    """Obtiene los nombres de las BD de origen y destino."""
    src_db = get_env('SRC_DB')
    dw_db = get_env('DW_DB')
    return src_db, dw_db

# --- 1. Carga de Dimensiones (Lógica idéntica, INSERT IGNORE) ---

def load_reference_dimensions(conn: MySQLConnection, src_db: str, dw_db: str) -> None:
    """Carga dimensiones 1:1 usando INSERT IGNORE para evitar duplicados."""
    log('Cargando dimensiones de referencia (INSERT IGNORE)...')
    statements = [
        f"INSERT IGNORE INTO {dw_db}.dim_purpose (purpose_code) SELECT DISTINCT purpose_code FROM {src_db}.dim_purpose",
        f"INSERT IGNORE INTO {dw_db}.dim_application_type (application_type_code) SELECT DISTINCT application_type_code FROM {src_db}.dim_application_type",
        f"INSERT IGNORE INTO {dw_db}.dim_verification_status (verification_status_code) SELECT DISTINCT verification_status_code FROM {src_db}.dim_verification_status",
        f"INSERT IGNORE INTO {dw_db}.dim_policy_code (policy_code) SELECT DISTINCT policy_code FROM {src_db}.dim_policy_code",
        f"INSERT IGNORE INTO {dw_db}.dim_disbursement_method (disbursement_method_code) SELECT DISTINCT disbursement_method_code FROM {src_db}.dim_disbursement_method",
        f"INSERT IGNORE INTO {dw_db}.dim_grade (grade_code) SELECT DISTINCT grade_code FROM {src_db}.dim_grade",
        f"INSERT IGNORE INTO {dw_db}.dim_sub_grade (sub_grade_code, grade_key) SELECT ssg.sub_grade_code, dg.grade_key FROM {src_db}.dim_sub_grade ssg JOIN {src_db}.dim_grade sg ON sg.grade_id = ssg.grade_id JOIN {dw_db}.dim_grade dg ON dg.grade_code = sg.grade_code",
        f"INSERT IGNORE INTO {dw_db}.dim_term (term_months) SELECT DISTINCT term_months FROM {src_db}.dim_term",
        f"INSERT IGNORE INTO {dw_db}.dim_home_ownership (home_ownership_code) SELECT DISTINCT home_ownership_code FROM {src_db}.dim_home_ownership",
        f"INSERT IGNORE INTO {dw_db}.dim_employment_length (years, original_text) SELECT DISTINCT years, original_text FROM {src_db}.dim_emp_length",
        f"INSERT IGNORE INTO {dw_db}.dim_loan_status (loan_status_code) SELECT DISTINCT loan_status_code FROM {src_db}.dim_loan_status",
        f"INSERT IGNORE INTO {dw_db}.dim_settlement_status (settlement_status_code) SELECT DISTINCT settlement_status_code FROM {src_db}.dim_settlement_status",
        f"INSERT IGNORE INTO {dw_db}.dim_hardship_type (hardship_type_code) SELECT DISTINCT hardship_type_code FROM {src_db}.dim_hardship_type",
        f"INSERT IGNORE INTO {dw_db}.dim_hardship_status (hardship_status_code) SELECT DISTINCT hardship_status_code FROM {src_db}.dim_hardship_status",
        f"INSERT IGNORE INTO {dw_db}.dim_hardship_loan_status (hardship_loan_status_code) SELECT DISTINCT hardship_loan_status_code FROM {src_db}.dim_hardship_loan_status",
    ]
    for sql in statements:
        run_sql(conn, sql)
    log('Dimensiones de referencia cargadas.')

def load_dim_location(conn: MySQLConnection, src_db: str, dw_db: str) -> None:
    """Carga dim_location (combinada) usando INSERT IGNORE."""
    log('Cargando dim_location (INSERT IGNORE)...')
    sql = (
        f"INSERT IGNORE INTO {dw_db}.dim_location (state_code, zip3) "
        f"SELECT DISTINCT s.state_code, z.zip3 FROM {src_db}.application_address aa "
        f"LEFT JOIN {src_db}.dim_state s ON aa.state_id = s.state_id "
        f"LEFT JOIN {src_db}.dim_zip3 z ON aa.zip3_id = z.zip3_id"
    )
    run_sql(conn, sql)
    log('dim_location cargada.')

def get_min_max_dates(conn: MySQLConnection, src_db: str) -> Tuple[dt.date, dt.date]:
    """Obtiene el rango de fechas completo de todas las tablas de origen, filtrando fechas '0000-00-00'."""
    log('Buscando rango de fechas en la base de datos de origen...')
    cur = conn.cursor()
    
    query = f"""
    SELECT MIN(min_d), MAX(max_d)
    FROM (
        SELECT MIN(application_date) as min_d, MAX(application_date) as max_d FROM {src_db}.application WHERE application_date > '0001-01-01'
        UNION ALL
        SELECT MIN(decision_date), MAX(decision_date) FROM {src_db}.loan WHERE decision_date > '0001-01-01'
        UNION ALL
        SELECT MIN(last_pymnt_d), MAX(last_pymnt_d) FROM {src_db}.payment_status_snapshot WHERE last_pymnt_d > '0001-01-01'
        UNION ALL
        SELECT MIN(next_pymnt_d), MAX(next_pymnt_d) FROM {src_db}.payment_status_snapshot WHERE next_pymnt_d > '0001-01-01'
        UNION ALL
        SELECT MIN(settlement_date), MAX(settlement_date) FROM {src_db}.settlement_case WHERE settlement_date > '0001-01-01'
        UNION ALL
        SELECT MIN(hardship_start_date), MAX(hardship_start_date) FROM {src_db}.hardship_case WHERE hardship_start_date > '0001-01-01'
        UNION ALL
        SELECT MIN(hardship_end_date), MAX(hardship_end_date) FROM {src_db}.hardship_case WHERE hardship_end_date > '0001-01-01'
        UNION ALL
        SELECT MIN(payment_plan_start_date), MAX(payment_plan_start_date) FROM {src_db}.hardship_case WHERE payment_plan_start_date > '0001-01-01'
    ) t
    """
    cur.execute(query)
    row = cur.fetchone()
    cur.close()
    min_d, max_d = row

    if min_d is None or max_d is None:
        log('No se encontraron fechas válidas. Usando rango por defecto (2007-2030).')
        return dt.date(2007, 1, 1), dt.date(2030, 12, 31)
    
    min_d = min_d - dt.timedelta(days=365)
    max_d = max_d + dt.timedelta(days=365)
    
    return min_d, max_d

def load_dim_date(conn_dw: MySQLConnection, conn_src: MySQLConnection, src_db: str, dw_db: str) -> None:
    """Puebla la dim_date usando una consulta recursiva (CTE) con la sintaxis SQL corregida."""
    d_start, d_end = get_min_max_dates(conn_src, src_db)
    log(f'Poblando dim_date desde {d_start} hasta {d_end}...')
    
    try:
        run_sql(conn_dw, "SET SESSION cte_max_recursion_depth = 100000;")
    except Exception as e:
        log(f"Advertencia: No se pudo establecer cte_max_recursion_depth. {e}")
        log("Continuando... (puede fallar si el rango de fechas es > 1000 días)")

    sql = f"""
    INSERT IGNORE INTO {dw_db}.dim_date 
      (date_id, full_date, year, quarter, quarter_name, month, month_name, day, day_of_week, day_name, week_of_year)
    WITH RECURSIVE seq AS (
      SELECT DATE('{d_start:%Y-%m-%d}') AS d
      UNION ALL
      SELECT DATE_ADD(d, INTERVAL 1 DAY) FROM seq WHERE d < DATE('{d_end:%Y-%m-%d}')
    )
    SELECT 
        CAST(DATE_FORMAT(d, '%Y%m%d') AS UNSIGNED), 
        d,
        YEAR(d), 
        QUARTER(d), 
        CONCAT('Q', QUARTER(d)),
        MONTH(d), 
        DATE_FORMAT(d, '%M'),
        DAY(d), 
        DAYOFWEEK(d), 
        DATE_FORMAT(d, '%W'),
        WEEKOFYEAR(d)
    FROM seq;
    """
    run_sql(conn_dw, sql)
    log('dim_date poblada.')

# --- 2. Carga de Hechos (UPSERT) ---

def load_fact_originations(conn: MySQLConnection, src_db: str, dw_db: str) -> None:
    """Carga fact_originations usando INSERT ... ON DUPLICATE KEY UPDATE."""
    log('Cargando fact_originations (UPSERT)...')
    
    # ¡YA NO HACEMOS TRUNCATE!
    log('-> Insertando/Actualizando registros desde el origen...')
    
    # Esta consulta es idéntica a la de Full Load, pero con una
    # cláusula "ON DUPLICATE KEY UPDATE" al final.
    sql = f"""
    INSERT INTO {dw_db}.fact_originations (
      loan_id, application_id, application_date_id, decision_date_id,
      purpose_key, application_type_key, verification_status_key, policy_code_key, disbursement_method_key,
      grade_key, sub_grade_key, term_key, home_ownership_key, employment_length_key, location_key,
      requested_amount, funded_amount, funded_amount_inv, installment, int_rate,
      annual_inc, annual_inc_joint, dti, dti_joint,
      inq_last_6mths, delinq_2yrs, mths_since_last_delinq, mths_since_recent_inq, pub_rec, total_acc, open_acc, revol_bal, revol_util
    )
    SELECT
      l.loan_id, -- La columna del GROUP BY
      ANY_VALUE(a.application_id) AS val_app_id,
      ANY_VALUE(CAST(DATE_FORMAT(a.application_date, '%Y%m%d') AS UNSIGNED)) AS val_app_date,
      ANY_VALUE(CAST(DATE_FORMAT(l.decision_date, '%Y%m%d') AS UNSIGNED)) AS val_dec_date,
      ANY_VALUE(dp.purpose_key) AS val_purpose,
      ANY_VALUE(dat.application_type_key) AS val_app_type,
      ANY_VALUE(dvs.verification_status_key) AS val_ver_status,
      ANY_VALUE(dpc.policy_code_key) AS val_policy,
      ANY_VALUE(ddm.disbursement_method_key) AS val_disb,
      ANY_VALUE(dg.grade_key) AS val_grade,
      ANY_VALUE(dsg.sub_grade_key) AS val_sub_grade,
      ANY_VALUE(dt.term_key) AS val_term,
      ANY_VALUE(dho.home_ownership_key) AS val_home,
      ANY_VALUE(del.employment_length_key) AS val_emp_len,
      ANY_VALUE(dl.location_key) AS val_loc,
      ANY_VALUE(lt.requested_amount) AS val_req_amt, 
      ANY_VALUE(lt.funded_amount) AS val_fund_amt, 
      ANY_VALUE(lt.funded_amount_inv) AS val_fund_inv, 
      ANY_VALUE(lt.installment) AS val_install, 
      ANY_VALUE(lt.int_rate) AS val_rate,
      ANY_VALUE(af.annual_inc) AS val_inc, 
      ANY_VALUE(af.annual_inc_joint) AS val_inc_joint, 
      ANY_VALUE(af.dti) AS val_dti, 
      ANY_VALUE(af.dti_joint) AS val_dti_joint,
      ANY_VALUE(ch.inq_last_6mths) AS val_inq_6m, 
      ANY_VALUE(ch.delinq_2yrs) AS val_delinq_2y, 
      ANY_VALUE(ch.mths_since_last_delinq) AS val_last_delinq, 
      ANY_VALUE(ch.mths_since_recent_inq) AS val_rec_inq,
      ANY_VALUE(ch.pub_rec) AS val_pub_rec, 
      ANY_VALUE(ch.total_acc) AS val_total_acc, 
      ANY_VALUE(ch.open_acc) AS val_open_acc, 
      ANY_VALUE(ch.revol_bal) AS val_revol_bal, 
      ANY_VALUE(ch.revol_util) AS val_revol_util
    
    FROM (
        SELECT * FROM {src_db}.loan 
        WHERE decision_date > '0001-01-01' OR decision_date IS NULL
    ) l
    JOIN (
        SELECT * FROM {src_db}.application 
        WHERE application_date > '0001-01-01'
    ) a ON l.application_id = a.application_id
    LEFT JOIN {src_db}.loan_terms lt ON lt.loan_id = l.loan_id
    LEFT JOIN {src_db}.applicant_financials_snapshot af ON af.application_id = a.application_id
    LEFT JOIN {src_db}.credit_history_snapshot ch ON ch.application_id = a.application_id
    LEFT JOIN {src_db}.employment e ON e.application_id = a.application_id
    LEFT JOIN {src_db}.application_address aa ON aa.application_id = a.application_id
    LEFT JOIN {src_db}.dim_purpose sp ON sp.purpose_id = a.purpose_id
    LEFT JOIN {src_db}.dim_application_type sat ON sat.application_type_id = a.application_type_id
    LEFT JOIN {src_db}.dim_verification_status sv ON sv.verification_status_id = a.verification_status_id
    LEFT JOIN {src_db}.dim_policy_code spc ON spc.policy_code_id = a.policy_code_id
    LEFT JOIN {src_db}.dim_disbursement_method sdm ON sdm.disbursement_method_id = a.disbursement_method_id
    LEFT JOIN {src_db}.dim_grade sg ON sg.grade_id = l.grade_id
    LEFT JOIN {src_db}.dim_sub_grade ssg ON ssg.sub_grade_id = l.sub_grade_id
    LEFT JOIN {src_db}.dim_term stm ON stm.term_id = l.term_id
    LEFT JOIN {src_db}.dim_home_ownership sho ON sho.home_ownership_id = e.home_ownership_id
    LEFT JOIN {src_db}.dim_emp_length sel ON sel.emp_length_id = e.emp_length_id
    LEFT JOIN {src_db}.dim_state s ON s.state_id = aa.state_id
    LEFT JOIN {src_db}.dim_zip3 z ON z.zip3_id = aa.zip3_id
    LEFT JOIN {dw_db}.dim_purpose dp ON dp.purpose_code = sp.purpose_code
    LEFT JOIN {dw_db}.dim_application_type dat ON dat.application_type_code = sat.application_type_code
    LEFT JOIN {dw_db}.dim_verification_status dvs ON dvs.verification_status_code = sv.verification_status_code
    LEFT JOIN {dw_db}.dim_policy_code dpc ON dpc.policy_code = spc.policy_code
    LEFT JOIN {dw_db}.dim_disbursement_method ddm ON ddm.disbursement_method_code = sdm.disbursement_method_code
    LEFT JOIN {dw_db}.dim_grade dg ON dg.grade_code = sg.grade_code
    LEFT JOIN {dw_db}.dim_sub_grade dsg ON dsg.sub_grade_code = ssg.sub_grade_code
    LEFT JOIN {dw_db}.dim_term dt ON dt.term_months = stm.term_months
    LEFT JOIN {dw_db}.dim_home_ownership dho ON dho.home_ownership_code = sho.home_ownership_code
    LEFT JOIN {dw_db}.dim_employment_length del ON del.original_text = sel.original_text
    LEFT JOIN {dw_db}.dim_location dl ON dl.state_code <=> s.state_code AND dl.zip3 <=> z.zip3
    
    GROUP BY l.loan_id
    
    -- ESTA ES LA MAGIA DEL UPSERT:
    ON DUPLICATE KEY UPDATE
      application_id = VALUES(application_id),
      application_date_id = VALUES(application_date_id),
      decision_date_id = VALUES(decision_date_id),
      purpose_key = VALUES(purpose_key),
      application_type_key = VALUES(application_type_key),
      verification_status_key = VALUES(verification_status_key),
      policy_code_key = VALUES(policy_code_key),
      disbursement_method_key = VALUES(disbursement_method_key),
      grade_key = VALUES(grade_key),
      sub_grade_key = VALUES(sub_grade_key),
      term_key = VALUES(term_key),
      home_ownership_key = VALUES(home_ownership_key),
      employment_length_key = VALUES(employment_length_key),
      location_key = VALUES(location_key),
      requested_amount = VALUES(requested_amount),
      funded_amount = VALUES(funded_amount),
      funded_amount_inv = VALUES(funded_amount_inv),
      installment = VALUES(installment),
      int_rate = VALUES(int_rate),
      annual_inc = VALUES(annual_inc),
      annual_inc_joint = VALUES(annual_inc_joint),
      dti = VALUES(dti),
      dti_joint = VALUES(dti_joint),
      inq_last_6mths = VALUES(inq_last_6mths),
      delinq_2yrs = VALUES(delinq_2yrs),
      mths_since_last_delinq = VALUES(mths_since_last_delinq),
      mths_since_recent_inq = VALUES(mths_since_recent_inq),
      pub_rec = VALUES(pub_rec),
      total_acc = VALUES(total_acc),
      open_acc = VALUES(open_acc),
      revol_bal = VALUES(revol_bal),
      revol_util = VALUES(revol_util);
    """
    run_sql(conn, sql)
    log('fact_originations cargada/actualizada.')

def load_fact_performance_snapshot(conn: MySQLConnection, src_db: str, dw_db: str) -> None:
    """Carga fact_performance_snapshot usando INSERT ... ON DUPLICATE KEY UPDATE."""
    log('Cargando fact_performance_snapshot (UPSERT)...')
    
    # ¡YA NO HACEMOS TRUNCATE!
    log('-> Insertando/Actualizando registros desde el origen...')

    sql = f"""
    INSERT INTO {dw_db}.fact_performance_snapshot (
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
      l.loan_id, -- La columna del GROUP BY
      ANY_VALUE(dls.loan_status_key),
      ANY_VALUE(CAST(DATE_FORMAT(NULLIF(ps.last_pymnt_d, '0000-00-00'), '%Y%m%d') AS UNSIGNED)),
      ANY_VALUE(CAST(DATE_FORMAT(NULLIF(ps.next_pymnt_d, '0000-00-00'), '%Y%m%d') AS UNSIGNED)),
      ANY_VALUE(ps.last_pymnt_amnt), 
      ANY_VALUE(ps.total_pymnt), 
      ANY_VALUE(ps.total_pymnt_inv), 
      ANY_VALUE(ps.total_rec_prncp), 
      ANY_VALUE(ps.total_rec_int), 
      ANY_VALUE(ps.total_rec_late_fee),
      ANY_VALUE(ps.recoveries), 
      ANY_VALUE(ps.collection_recovery_fee), 
      ANY_VALUE(ps.out_prncp), 
      ANY_VALUE(ps.out_prncp_inv), 
      ANY_VALUE(ps.pymnt_plan),
      ANY_VALUE(dss.settlement_status_key), 
      ANY_VALUE(sc.debt_settlement_flag), 
      ANY_VALUE(sc.settlement_amount), 
      ANY_VALUE(sc.settlement_percentage),
      ANY_VALUE(CAST(DATE_FORMAT(NULLIF(sc.settlement_date, '0000-00-00'), '%Y%m%d') AS UNSIGNED)),
      ANY_VALUE(dht.hardship_type_key), 
      ANY_VALUE(dhs.hardship_status_key), 
      ANY_VALUE(dhl.hardship_loan_status_key), 
      ANY_VALUE(hc.hardship_amount),
      ANY_VALUE(CAST(DATE_FORMAT(NULLIF(hc.hardship_start_date, '0000-00-00'), '%Y%m%d') AS UNSIGNED)),
      ANY_VALUE(CAST(DATE_FORMAT(NULLIF(hc.hardship_end_date, '0000-00-00'), '%Y%m%d') AS UNSIGNED)),
      ANY_VALUE(CAST(DATE_FORMAT(NULLIF(hc.payment_plan_start_date, '0000-00-00'), '%Y%m%d') AS UNSIGNED)),
      ANY_VALUE(hc.deferral_term), 
      ANY_VALUE(hc.hardship_length), 
      ANY_VALUE(hc.hardship_dpd)
      
    FROM (
        SELECT * FROM {src_db}.loan 
        WHERE decision_date > '0001-01-01' OR decision_date IS NULL
    ) l
    
    LEFT JOIN (
        SELECT * FROM {src_db}.payment_status_snapshot
        WHERE (last_pymnt_d > '0001-01-01' OR last_pymnt_d IS NULL)
          AND (next_pymnt_d > '0001-01-01' OR next_pymnt_d IS NULL)
    ) ps ON ps.loan_id = l.loan_id
    
    LEFT JOIN (
        SELECT * FROM {src_db}.settlement_case
        WHERE (settlement_date > '0001-01-01' OR settlement_date IS NULL)
    ) sc ON sc.loan_id = l.loan_id
    
    LEFT JOIN (
        SELECT * FROM {src_db}.hardship_case
        WHERE (hardship_start_date > '0001-01-01' OR hardship_start_date IS NULL)
          AND (hardship_end_date > '0001-01-01' OR hardship_end_date IS NULL)
          AND (payment_plan_start_date > '0001-01-01' OR payment_plan_start_date IS NULL)
    ) hc ON hc.loan_id = l.loan_id
    
    LEFT JOIN {src_db}.dim_loan_status sls ON sls.loan_status_id = l.loan_status_id
    LEFT JOIN {src_db}.dim_settlement_status sss ON sss.settlement_status_id = sc.settlement_status_id
    LEFT JOIN {src_db}.dim_hardship_type sht ON sht.hardship_type_id = hc.hardship_type_id
    LEFT JOIN {src_db}.dim_hardship_status shs ON shs.hardship_status_id = hc.hardship_status_id
    LEFT JOIN {src_db}.dim_hardship_loan_status shl ON shl.hardship_loan_status_id = hc.hardship_loan_status_id
    LEFT JOIN {dw_db}.dim_loan_status dls ON dls.loan_status_code = sls.loan_status_code
    LEFT JOIN {dw_db}.dim_settlement_status dss ON dss.settlement_status_code = sss.settlement_status_code
    LEFT JOIN {dw_db}.dim_hardship_type dht ON dht.hardship_type_code = sht.hardship_type_code
    LEFT JOIN {dw_db}.dim_hardship_status dhs ON dhs.hardship_status_code = shs.hardship_status_code
    LEFT JOIN {dw_db}.dim_hardship_loan_status dhl ON dhl.hardship_loan_status_code = shl.hardship_loan_status_code
    
    GROUP BY l.loan_id
    
    -- ESTA ES LA MAGIA DEL UPSERT:
    ON DUPLICATE KEY UPDATE
      loan_status_key = VALUES(loan_status_key),
      last_payment_date_id = VALUES(last_payment_date_id),
      next_payment_date_id = VALUES(next_payment_date_id),
      last_pymnt_amnt = VALUES(last_pymnt_amnt),
      total_pymnt = VALUES(total_pymnt),
      total_pymnt_inv = VALUES(total_pymnt_inv),
      total_rec_prncp = VALUES(total_rec_prncp),
      total_rec_int = VALUES(total_rec_int),
      total_rec_late_fee = VALUES(total_rec_late_fee),
      recoveries = VALUES(recoveries),
      collection_recovery_fee = VALUES(collection_recovery_fee),
      out_prncp = VALUES(out_prncp),
      out_prncp_inv = VALUES(out_prncp_inv),
      pymnt_plan = VALUES(pymnt_plan),
      settlement_status_key = VALUES(settlement_status_key),
      debt_settlement_flag = VALUES(debt_settlement_flag),
      settlement_amount = VALUES(settlement_amount),
      settlement_percentage = VALUES(settlement_percentage),
      settlement_date_id = VALUES(settlement_date_id),
      hardship_type_key = VALUES(hardship_type_key),
      hardship_status_key = VALUES(hardship_status_key),
      hardship_loan_status_key = VALUES(hardship_loan_status_key),
      hardship_amount = VALUES(hardship_amount),
      hardship_start_date_id = VALUES(hardship_start_date_id),
      hardship_end_date_id = VALUES(hardship_end_date_id),
      payment_plan_start_date_id = VALUES(payment_plan_start_date_id),
      deferral_term = VALUES(deferral_term),
      hardship_length = VALUES(hardship_length),
      hardship_dpd = VALUES(hardship_dpd);
    """
    run_sql(conn, sql)
    log('fact_performance_snapshot cargada/actualizada.')

# --- Bucle Principal ---

def main():
    log('--- Iniciando ETL: INCREMENTAL (UPSERT) ---')
    try:
        load_dotenv(PROJECT_ROOT / '.env')
        src_db, dw_db = get_db_names()
        
        # Usamos dos conexiones: una para leer del origen, otra para escribir al destino
        with connect_mysql(db_name=src_db) as conn_src, \
             connect_mysql(db_name=dw_db) as conn_dw:
            
            # 1. Poblar Dimensiones (Esta parte es incremental por diseño)
            log("Paso 1/3: Poblando Dimensiones...")
            load_dim_date(conn_dw, conn_src, src_db, dw_db)
            load_reference_dimensions(conn_dw, src_db, dw_db)
            load_dim_location(conn_dw, src_db, dw_db)
            log("Dimensiones pobladas.")
            
            # 2. Cargar Hechos (Método UPSERT: INSERT ... ON DUPLICATE KEY UPDATE)
            log("Paso 2/3: Poblando Hechos de Originación...")
            load_fact_originations(conn_dw, src_db, dw_db)
            log("Hechos de Originación poblados.")

            log("Paso 3/3: Poblando Hechos de Rendimiento...")
            load_fact_performance_snapshot(conn_dw, src_db, dw_db)
            log("Hechos de Rendimiento poblados.")
            
        log('--- ETL (UPSERT) completado exitosamente ---')

    except mysql.connector.Error as err:
        log(f"--- ETL (UPSERT) FALLÓ (Error de MySQL) ---")
        log(f"Error: {err}")
        sys.exit(1)
    except Exception as e:
        log(f'--- ETL (UPSERT) FALLÓ (Error General) ---')
        log(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()