# scripts/watcher.py
import time
import shutil
import logging
import pandas as pd
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime
from scripts.processor import process_excel  # unchanged signature

BASE = Path.cwd()
INBOX = BASE / "data" / "inbox"
ARCHIVE = BASE / "data" / "archive"
ERROR = BASE / "data" / "error"

for folder in [INBOX, ARCHIVE, ERROR]:
    folder.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("watcher.log", encoding="utf-8"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def wait_for_file_complete(path: Path, timeout: int = 60, poll_interval: float = 1.0) -> bool:
    """
    Attend que le fichier cesse de changer de taille pendant deux itérations consécutives,
    ou que le timeout soit atteint. Retourne True si stable.
    """
    start = time.time()
    last_size = -1
    stable_count = 0
    while True:
        try:
            current_size = path.stat().st_size
        except OSError:
            return False
        if current_size == last_size:
            stable_count += 1
            if stable_count >= 2:
                return True
        else:
            stable_count = 0
        last_size = current_size
        if time.time() - start > timeout:
            return False
        time.sleep(poll_interval)

def _unique_dest_path(dest_folder: Path, filename: str) -> Path:
    dest = dest_folder / filename
    if not dest.exists():
        return dest
    name = Path(filename).stem
    ext = Path(filename).suffix
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return dest_folder / f"{name}_{timestamp}{ext}"

class ExcelHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        src = Path(event.src_path)
        if src.suffix.lower() not in [".xlsx", ".xls"]:
            logger.info(f"Ignored non excel: {src}")
            return
        logger.info(f"New file detected: {src}")
        if not wait_for_file_complete(src):
            logger.warning(f"File incomplete or locked: {src}")
            return
        try:
            results = process_excel(str(src), flatten=True)
            if isinstance(results, pd.DataFrame) and not results.empty:
                logger.info(f"Processed {src.name}: {len(results)} rows")
            else:
                logger.warning(f"No usable data in {src.name}")
            dest = _unique_dest_path(ARCHIVE, src.name)
            shutil.move(str(src), str(dest))
            logger.info(f"Archived -> {dest}")
        except Exception as e:
            logger.exception(f"Error processing {src}: {e}")
            dest = _unique_dest_path(ERROR, src.name)
            try:
                shutil.move(str(src), str(dest))
                logger.info(f"Moved to error -> {dest}")
            except Exception as mv_err:
                logger.exception(f"Failed to move to error: {mv_err}")

if __name__ == "__main__":
    event_handler = ExcelHandler()
    observer = Observer()
    observer.schedule(event_handler, path=str(INBOX), recursive=False)
    observer.start()
    logger.info(f"Watching {INBOX} ... (Ctrl+C to stop)")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping watcher...")
        observer.stop()
    observer.join()
    logger.info("Watcher stopped.")
