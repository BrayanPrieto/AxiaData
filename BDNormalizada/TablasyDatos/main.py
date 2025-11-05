import subprocess
import os
import sys

# -------------------------------------------------------------------
# --- CONFIGURACIÓN ---
# ¡Asegúrate de cambiar estos valores por los de tu conexión local!
# -------------------------------------------------------------------
DB_USER = "root"
DB_PASS = "root"  
DB_HOST = "localhost"
DB_NAME = "bdPrestamosNormalizada"
# -------------------------------------------------------------------

# Directorio base donde se encuentran las carpetas Parte1, Parte2, etc.
# Se asume que es el mismo directorio donde se ejecuta el script.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Lista de archivos en el ORDEN DE CARGA CORRECTO (según las dependencias)
sql_files_in_order = [
    # --- 1: Catálogos Independientes ---
    'Parte2/prestamos_norm_borrower.sql',
    'Parte1/prestamos_norm_dim_application_type.sql',
    'Parte3/prestamos_norm_dim_disbursement_method.sql',
    'Parte3/prestamos_norm_dim_emp_length.sql',
    'Parte4/prestamos_norm_dim_emp_title.sql',
    'Parte3/prestamos_norm_dim_grade.sql',
    'Parte4/prestamos_norm_dim_hardship_loan_status.sql',
    'Parte2/prestamos_norm_dim_hardship_status.sql',
    'Parte2/prestamos_norm_dim_hardship_type.sql',
    'Parte2/prestamos_norm_dim_home_ownership.sql',
    'Parte1/prestamos_norm_dim_initial_list_status.sql',
    'Parte2/prestamos_norm_dim_loan_status.sql',
    'Parte4/prestamos_norm_dim_policy_code.sql',
    'Parte2/prestamos_norm_dim_purpose.sql',
    'Parte1/prestamos_norm_dim_settlement_status.sql',
    'Parte1/prestamos_norm_dim_state.sql',
    'Parte1/prestamos_norm_dim_term.sql',
    'Parte2/prestamos_norm_dim_verification_status.sql',
    'Parte4/prestamos_norm_dim_zip3.sql',

    # --- 2: Catálogos Dependientes ---
    'Parte1/prestamos_norm_dim_sub_grade.sql',
    'Parte1/prestamos_norm_listing.sql',

    # --- 3: Transacción Principal ---
    'Parte2/prestamos_norm_application.sql',

    # --- 4: Detalles (OJO: 'loan' va primero) ---
    'Parte4/prestamos_norm_loan.sql',
    'Parte1/prestamos_norm_applicant_financials_snapshot.sql',
    'Parte1/prestamos_norm_application_address.sql',
    'Parte3/prestamos_norm_credit_history_snapshot.sql',
    'Parte2/prestamos_norm_employment.sql',

    # --- 5: Detalles de Loan ---
    'Parte2/prestamos_norm_hardship_case.sql',
    'Parte3/prestamos_norm_loan_terms.sql',
    'Parte3/prestamos_norm_payment_status_snapshot.sql',
    'Parte4/prestamos_norm_settlement_case.sql',
    
    # --- 6: Rutinas ---
    'Parte1/prestamos_norm_routines.sql'
]

def run_sql_script(file_path):
    """Ejecuta un archivo .sql usando el cliente mysql."""
    
    # Construye el comando
    command = [
        "mysql",
        f"-u{DB_USER}",
        f"-p{DB_PASS}",
        f"-h{DB_HOST}",
        DB_NAME
    ]
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # Ejecuta el comando y pasa el contenido del archivo como stdin
            result = subprocess.run(
                command,
                stdin=f,
                capture_output=True,  # Captura stdout y stderr
                text=True,
                check=True            # Lanza una excepción si el comando falla
            )
        return True, ""
    
    except subprocess.CalledProcessError as e:
        # Error devuelto por el comando mysql (ej. error de sintaxis SQL)
        return False, e.stderr
    except FileNotFoundError:
        # Error si el comando 'mysql' no se encuentra
        error_msg = ("Error: El comando 'mysql' no se encontró.\n"
                     "Asegúrate de que MySQL Client (o MySQL Workbench) esté instalado "
                     "y que la carpeta 'bin' esté en el PATH de tu sistema.")
        return False, error_msg
    except Exception as e:
        # Cualquier otro error (ej. permisos de archivo)
        return False, str(e)

# --- Bucle Principal de Ejecución ---
print(f"--- 🚀 Iniciando la carga de datos en '{DB_NAME}' ---")
total_files = len(sql_files_in_order)

for i, relative_path in enumerate(sql_files_in_order):
    file_path = os.path.join(BASE_DIR, relative_path.replace('/', os.sep))
    
    print(f"[{i+1}/{total_files}] Ejecutando: {relative_path} ... ", end="")
    
    if not os.path.exists(file_path):
        print("¡ERROR!")
        print(f"  Archivo no encontrado: {file_path}")
        print("--- ⛔ ABORTANDO SCRIPT ---")
        sys.exit(1) # Termina el script
        
    success, error_message = run_sql_script(file_path)
    
    if success:
        print("¡Éxito!")
    else:
        print("¡ERROR!")
        print("------------------- MENSAJE DE ERROR -------------------")
        print(error_message)
        print("--------------------------------------------------------")
        print("--- ⛔ ABORTANDO SCRIPT DEBIDO A UN ERROR ---")
        sys.exit(1) # Termina el script

print(f"--- ✅ Proceso de carga finalizado. {total_files} archivos ejecutados. ---")