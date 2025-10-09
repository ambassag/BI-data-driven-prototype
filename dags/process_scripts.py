from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys
import shutil
import unicodedata
import re

# -------------------------------
# Répertoires
# -------------------------------
SCRIPTS_DIR = Path("/opt/airflow/scripts")
INPUT_DIR = Path("/opt/airflow/data/inbox")
ARCHIVE_DIR = Path("/opt/airflow/data/archive")
ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

# -------------------------------
# Arguments par défaut du DAG
# -------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

# -------------------------------
# Fonctions Python
# -------------------------------
def run_script(script_path: str):
    """Exécute un script Python et lève une exception si erreur"""
    result = subprocess.run([sys.executable, script_path], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(
            f"Script {script_path} failed\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    print(result.stdout)


def run_scd2_loader():
    """Charge les fichiers de sortie dans le Data Warehouse"""
    from scd2_loader import load_all_out_files_to_dw
    load_all_out_files_to_dw()


def normalize_filename(name: str) -> str:
    """Normalise un nom de fichier (accents, caractères spéciaux)"""
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    name = re.sub(r"[^\w\d-]+", "_", name)
    return name.lower()


def archive_input_files():

    today_str = datetime.now().strftime("%Y%m%d")

    for file_path in INPUT_DIR.glob("*.xlsx"):
        if file_path.is_file():
            normalized_name = normalize_filename(file_path.stem)
            new_name = f"{normalized_name}_{today_str}{file_path.suffix}"
            dest_path = ARCHIVE_DIR / new_name
            shutil.move(str(file_path), dest_path)
            print(f"📦 Archived {file_path.name} -> {dest_path.name}")


# -------------------------------
# DAG Airflow
# -------------------------------
with DAG(
    dag_id="etl_pipeline_scd2",
    default_args=default_args,
    start_date=datetime.now(timezone.utc),
    schedule_interval=None,
    catchup=False,
) as dag:

    previous_task = None

    # 1) Exécution de tous les scripts
    for script_path in sorted(SCRIPTS_DIR.glob("*.py")):
        t = PythonOperator(
            task_id=f"run_{script_path.stem}",
            python_callable=run_script,
            op_args=[str(script_path)],
        )
        if previous_task:
            previous_task >> t
        previous_task = t

    # 2) Chargement vers le Data Warehouse
    load_task = PythonOperator(
        task_id="load_to_dw_scd2",
        python_callable=run_scd2_loader,
    )
    if previous_task:
        previous_task >> load_task

    # 3) Archivage de tous les fichiers d'entrée traités
    archive_task = PythonOperator(
        task_id="archive_input_files",
        python_callable=archive_input_files,
    )
    load_task >> archive_task
