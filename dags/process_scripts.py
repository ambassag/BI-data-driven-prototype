from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys
import shutil
import unicodedata
import re


SCRIPTS_DIR = Path("/opt/airflow/scripts")
INPUT_DIR = Path("/opt/airflow/data/inbox")
OUT_DIR = Path("/opt/airflow/data/out")
ARCHIVE_DIR = Path("/opt/airflow/data/archive")
ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

def run_script(script_path: str, output_prefix=None):

    existing_files = []
    if output_prefix:
        if isinstance(output_prefix, list):
            for pfx in output_prefix:
                existing_files.extend(list(OUT_DIR.glob(f"{pfx}*.xlsx")))
        else:
            existing_files = list(OUT_DIR.glob(f"{output_prefix}*.xlsx"))

    existing_files = sorted(existing_files, key=lambda p: p.stat().st_mtime, reverse=True)
    if existing_files:
        print(f"Skip {script_path} car OUT déjà présent : {existing_files[0].name}")
        return

    print(f"Exécution du script : {script_path}")
    result = subprocess.run([sys.executable, script_path], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(
            f"Script {script_path} failed\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    print(result.stdout or f"Script {script_path} exécuté avec succès")


def run_scd2_loader():
    """Charge les fichiers de sortie dans le Data Warehouse"""
    from scd2_loader import load_all_out_files_to_dw
    load_all_out_files_to_dw()


def normalize_filename(name: str) -> str:
    """Normalise un nom de fichier"""
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    name = re.sub(r"[^\w\d-]+", "_", name)
    return name.lower()


def archive_input_files():
    """Archive tous les fichiers .xlsx sauf ceux contenant 'invariant'"""
    today_str = datetime.now().strftime("%Y%m%d")
    files = list(INPUT_DIR.glob("*.xlsx"))

    for file_path in files:
        if file_path.is_file() and "invariant" not in file_path.name.lower():
            normalized_name = normalize_filename(file_path.stem)
            new_name = f"{normalized_name}_{today_str}{file_path.suffix}"
            dest_path = ARCHIVE_DIR / new_name
            shutil.move(str(file_path), dest_path)
            print(f" Archived {file_path.name} -> {dest_path.name}")
        else:
            print(f"Skip {file_path.name} (contient 'invariant')")


# -------------------------------
# DAG Airflow
# -------------------------------
with DAG(
    dag_id="etl_pipeline_scd2",
    default_args=default_args,
    start_date=datetime(2025, 10, 18, tzinfo=timezone.utc),
    schedule_interval=None,
    catchup=False,
) as dag:

    # Mapping des scripts -> préfixes de sortie attendus
    SCRIPT_PREFIX_MAPPING = {
        "processor_extract_station.py": "Extract_Station",
        "processor_extract_invariant.py": ["Invariants_details", "Invariants_study"],
        "processor_extract_country.py": "Country_code",
        "processor_extract_ep11.py": "Invariants_study_enriched",
        "processor_harmonizer.py": None,  # harmonizer est un script final, pas de file prefix attendu
        # ajoute ici tes scripts et leur préfixe de sortie
    }

    # créer toutes les tasks (one task per script)
    tasks = {}
    for script_path in sorted(SCRIPTS_DIR.glob("*.py")):
        prefix = SCRIPT_PREFIX_MAPPING.get(script_path.name, None)
        task = PythonOperator(
            task_id=f"run_{script_path.stem}",
            python_callable=run_script,
            op_args=[str(script_path), prefix],
        )
        tasks[script_path.name] = task


    ep11_task = tasks.get("processor_extract_ep11.py")
    station_task = tasks.get("processor_extract_station.py")
    invariant_task = tasks.get("processor_extract_invariant.py")
    country_task = tasks.get("processor_extract_country.py")

    prereqs_for_ep11 = [t for t in (station_task, invariant_task, country_task) if t is not None]
    if ep11_task and prereqs_for_ep11:
        for pre in prereqs_for_ep11:
            pre >> ep11_task


    harmonizer_task = tasks.get("processor_harmonizer.py")
    if harmonizer_task:
        for name, t in tasks.items():
            if name == "processor_harmonizer.py":
                continue

            t >> harmonizer_task

    load_task = PythonOperator(
        task_id="load_to_dw_scd2",
        python_callable=run_scd2_loader,
    )

    if harmonizer_task:
        harmonizer_task >> load_task
    else:

        for t in tasks.values():
            t >> load_task

    # Archivage des fichiers d'entrée après le chargement
    archive_task = PythonOperator(
        task_id="archive_input_files",
        python_callable=archive_input_files,
    )
    load_task >> archive_task
