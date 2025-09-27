from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import sys
from pathlib import Path

# Dossier contenant les scripts
SCRIPTS_DIR = Path('/opt/airflow/scripts')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

def run_script(script_path: str):
    """Exécute un script Python via subprocess et logge la sortie."""
    result = subprocess.run(
        [sys.executable, script_path],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(
            f"Échec du script {script_path}\n"
            f"STDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"
        )
    else:
        print(f"Succès {script_path}\n{result.stdout}")

# Définir le DAG
with DAG(
    'dynamic_process_scripts_dag',
    default_args=default_args,
    description='Exécuter tous les scripts dans le dossier scripts dans l’ordre',
    schedule_interval=None,  # ou '@daily', '@hourly', etc.
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    previous_task = None

    # Lister tous les scripts Python et trier (ordre alphabétique)
    scripts = sorted(SCRIPTS_DIR.glob('*.py'))

    for script_path in scripts:
        task = PythonOperator(
            task_id=f"run_{script_path.stem}",
            python_callable=run_script,
            op_args=[str(script_path)]
        )

        # Créer la séquence : chaque script dépend du précédent
        if previous_task:
            previous_task >> task

        previous_task = task
