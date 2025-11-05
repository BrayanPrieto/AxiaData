#!/usr/bin/env python3
"""
ETL - Carga Completa (Full Load)

Estrategia: TRUNCATE + INSERT
1.  Borra (TRUNCATE) todas las filas de las tablas de hechos.
2.  Recalcula y vuelve a insertar (INSERT) todos los registros
    desde la base de datos de origen.
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

# --- Funciones de Ayuda ---

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

# --- 1. Carga de Dimensiones ---

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
    
    # Añadir un búfer para asegurar que cubrimos todo
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

# --- 2. Carga de Hechos (FULL LOAD) ---

def load_fact_originations(conn: MySQLConnection, src_db: str, dw_db: str) -> None:
    """Carga fact_originations usando TRUNCATE + INSERT."""
    log('Cargando fact_originations (FULL)...')
    
    log('-> Vaciando fact_originations...')
    run_sql(conn, "SET FOREIGN_KEY_CHECKS = 0;")
    run_sql(conn, f"TRUNCATE TABLE {dw_db}.fact_originations;")
    run_sql(conn, "SET FOREIGN_KEY_CHECKS = 1;")

    log('-> Insertando todos los registros desde el origen...')
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
      l.loan_id,
      a.application_id,
      CAST(DATE_FORMAT(a.application_date, '%Y%m%d') AS UNSIGNED),
      CAST(DATE_FORMAT(l.decision_date, '%Y%m%d') AS UNSIGNED),
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
    
    FROM (
        -- Subconsulta 1: Filtra 'loan'
        SELECT * FROM {src_db}.loan 
        WHERE decision_date > '0001-01-01' OR decision_date IS NULL
    ) l
    
    JOIN (
        -- Subconsulta 2: Filtra 'application'
        SELECT * FROM {src_db}.application 
        WHERE application_date > '0001-01-01'
    ) a ON l.application_id = a.application_id
    
    -- El resto de los JOINS se mantienen igual
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
    
    -- CORRECCIÓN: Añadido GROUP BY para prevenir duplicados (fan-out)
    GROUP BY l.loan_id;
    """
    run_sql(conn, sql)
    log('fact_originations cargada.')

def load_fact_performance_snapshot(conn: MySQLConnection, src_db: str, dw_db: str) -> None:
    """Carga fact_performance_snapshot usando TRUNCATE + INSERT."""
    log('Cargando fact_performance_snapshot (FULL)...')
    
    log('-> Vaciando fact_performance_snapshot...')
    run_sql(conn, "SET FOREIGN_KEY_CHECKS = 0;")
    run_sql(conn, f"TRUNCATE TABLE {dw_db}.fact_performance_snapshot;")
    run_sql(conn, "SET FOREIGN_KEY_CHECKS = 1;")

    log('-> Insertando todos los registros desde el origen...')
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
      l.loan_id,
      dls.loan_status_key,
      CAST(DATE_FORMAT(NULLIF(ps.last_pymnt_d, '0000-00-00'), '%Y%m%d') AS UNSIGNED),
      CAST(DATE_FORMAT(NULLIF(ps.next_pymnt_d, '0000-00-00'), '%Y%m%d') AS UNSIGNED),
      ps.last_pymnt_amnt, ps.total_pymnt, ps.total_pymnt_inv, ps.total_rec_prncp, ps.total_rec_int, ps.total_rec_late_fee,
      ps.recoveries, ps.collection_recovery_fee, ps.out_prncp, ps.out_prncp_inv, ps.pymnt_plan,
      dss.settlement_status_key, sc.debt_settlement_flag, sc.settlement_amount, sc.settlement_percentage,
      CAST(DATE_FORMAT(NULLIF(sc.settlement_date, '0000-00-00'), '%Y%m%d') AS UNSIGNED),
      dht.hardship_type_key, dhs.hardship_status_key, dhl.hardship_loan_status_key, hc.hardship_amount,
      CAST(DATE_FORMAT(NULLIF(hc.hardship_start_date, '0000-00-00'), '%Y%m%d') AS UNSIGNED),
      CAST(DATE_FORMAT(NULLIF(hc.hardship_end_date, '0000-00-00'), '%Y%m%d') AS UNSIGNED),
      CAST(DATE_FORMAT(NULLIF(hc.payment_plan_start_date, '0000-00-00'), '%Y%m%d') AS UNSIGNED),
      hc.deferral_term, hc.hardship_length, hc.hardship_dpd
      
    FROM (
        -- Subconsulta: Filtra 'loan'
        SELECT * FROM {src_db}.loan 
        WHERE decision_date > '0001-01-01' OR decision_date IS NULL
    ) l
    
    LEFT JOIN {src_db}.payment_status_snapshot ps ON ps.loan_id = l.loan_id
    LEFT JOIN {src_db}.settlement_case sc ON sc.loan_id = l.loan_id
    LEFT JOIN {src_db}.hardship_case hc ON hc.loan_id = l.loan_id
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
    
    -- CORRECCIÓN: Añadido GROUP BY para prevenir duplicados (fan-out)
    GROUP BY l.loan_id;
    """
    run_sql(conn, sql)
    log('fact_performance_snapshot cargada.')

# --- Bucle Principal ---

def main():
    log('--- Iniciando ETL: FULL LOAD (Carga Completa) ---')
    try:
        load_dotenv(PROJECT_ROOT / '.env')
        src_db, dw_db = get_db_names()
        
        # Usamos dos conexiones: una para leer del origen, otra para escribir al destino
        with connect_mysql(db_name=src_db) as conn_src, \
             connect_mysql(db_name=dw_db) as conn_dw:
            
            # 1. Poblar Dimensiones
            log("Paso 1/3: Poblando Dimensiones...")
            load_dim_date(conn_dw, conn_src, src_db, dw_db)
            load_reference_dimensions(conn_dw, src_db, dw_db)
            load_dim_location(conn_dw, src_db, dw_db)
            log("Dimensiones pobladas.")
            
            # 2. Cargar Hechos (Método FULL: TRUNCATE + INSERT)
            log("Paso 2/3: Poblando Hechos de Originación...")
            load_fact_originations(conn_dw, src_db, dw_db)
            log("Hechos de Originación poblados.")

            log("Paso 3/3: Poblando Hechos de Rendimiento...")
            load_fact_performance_snapshot(conn_dw, src_db, dw_db)
            log("Hechos de Rendimiento poblados.")
            
        log('--- ETL (FULL LOAD) completado exitosamente ---')

    except mysql.connector.Error as err:
        log(f"--- ETL (FULL LOAD) FALLÓ (Error de MySQL) ---")
        log(f"Error: {err}")
        sys.exit(1)
    except Exception as e:
        log(f'--- ETL (FULL LOAD) FALLÓ (Error General) ---')
        log(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()