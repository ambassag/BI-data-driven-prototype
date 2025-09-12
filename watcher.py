# watcher.py
import time
import shutil
import logging
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime

from processor import process_excel  # lit et traite toutes les feuilles Excel

# --- Dossiers surveillés ---
BASE = Path("data")
INBOX = BASE / "inbox"
ARCHIVE = BASE / "archive"
ERROR = BASE / "error"

for folder in [INBOX, ARCHIVE, ERROR]:
    folder.mkdir(parents=True, exist_ok=True)

# --- Logger configuré ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("watcher.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def wait_for_file_complete(path: Path, timeout: int = 30, poll_interval: float = 1.0) -> bool:
    """
    Vérifie que la taille du fichier reste stable pendant un temps donné.
    Permet de s’assurer que la copie/écriture est terminée.
    """
    start = time.time()
    last_size = -1
    while True:
        try:
            current_size = path.stat().st_size
        except OSError:
            return False
        if current_size == last_size:
            return True
        last_size = current_size
        if time.time() - start > timeout:
            return False
        time.sleep(poll_interval)


def _unique_dest_path(dest_folder: Path, filename: str) -> Path:
    """Ajoute un timestamp si un fichier du même nom existe déjà."""
    dest = dest_folder / filename
    if not dest.exists():
        return dest
    name, ext = filename.rsplit(".", 1)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return dest_folder / f"{name}_{timestamp}.{ext}"


class ExcelHandler(FileSystemEventHandler):
    """Handler Watchdog qui réagit à l’arrivée d’un fichier Excel."""

    def on_created(self, event):
        if event.is_directory:
            return

        src_path = Path(event.src_path)

        # Vérifie le format
        if src_path.suffix.lower() not in [".xlsx", ".xls"]:
            logger.info(f"⏩ Ignoré (format non supporté) : {src_path}")
            return

        logger.info(f"📂 Nouveau fichier détecté : {src_path}")

        # Vérifie la complétude du fichier
        if not wait_for_file_complete(src_path, timeout=60, poll_interval=1):
            logger.warning(f"⚠️ Fichier incomplet ou verrouillé : {src_path}")
            return

        try:
            # 🔑 Traitement centralisé via processor.py
            results = process_excel(str(src_path))

            if not results:
                logger.warning(f"⚠️ Aucun DataFrame exploitable pour {src_path}")

            for sheet, df in results.items():
                logger.info(f"✅ Feuille '{sheet}' traitée ({len(df)} lignes, {len(df.columns)} colonnes)")

            # Déplacement en archive si traitement ok
            dest = _unique_dest_path(ARCHIVE, src_path.name)
            shutil.move(str(src_path), str(dest))
            logger.info(f"📦 Archivé -> {dest}")

        except Exception as e:
            logger.error(f"❌ Erreur lors du traitement de {src_path} : {e}", exc_info=True)
            dest = _unique_dest_path(ERROR, src_path.name)
            try:
                shutil.move(str(src_path), str(dest))
                logger.info(f"🚩 Déplacé en erreur -> {dest}")
            except Exception as mv_err:
                logger.error(f"❌ Impossible de déplacer en erreur : {mv_err}")


if __name__ == "__main__":
    event_handler = ExcelHandler()
    observer = Observer()
    observer.schedule(event_handler, path=str(INBOX), recursive=False)
    observer.start()
    logger.info(f"👀 Surveillance active sur {INBOX}... (Ctrl+C pour arrêter)")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("🛑 Arrêt demandé, fermeture du watcher...")
        observer.stop()
    observer.join()
    logger.info("✅ Watcher arrêté proprement.")
