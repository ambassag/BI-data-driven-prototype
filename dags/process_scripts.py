from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys


SCRIPTS_DIR = Path("/opt/airflow/scripts")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

def run_script(script_path: str):
    result = subprocess.run([sys.executable, script_path], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Script {script_path} failed\nSTDOUT:{result.stdout}\nSTDERR:{result.stderr}")
    print(result.stdout)

# wrapper for plugin loader
def run_scd2_loader():
    # note: plugin function will read env var DW_ENGINE or fallback to airflow DB

    from scd2_loader import load_all_out_files_to_dw
    load_all_out_files_to_dw()

with DAG(
    dag_id="etl_pipeline_scd2",
    default_args=default_args,
    start_date=datetime.now(timezone.utc),
    schedule_interval=None,
    catchup=False,
) as dag:

    previous = None

    for script_path in sorted(SCRIPTS_DIR.glob("*.py")):
        t = PythonOperator(
            task_id=f"run_{script_path.stem}",
            python_callable=run_script,
            op_args=[str(script_path)]
        )
        if previous:
            previous >> t
        previous = t

    load_task = PythonOperator(
        task_id="load_to_dw_scd2",
        python_callable=run_scd2_loader
    )

    if previous:
        previous >> load_task
    else:
        load_task
