# watcher.py
import time
import os
import shutil
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from processor import process_excel  # on réutilise

INBOX = "data/inbox"
ARCHIVE = "data/archive"
ERROR = "data/error"

os.makedirs(INBOX, exist_ok=True)
os.makedirs(ARCHIVE, exist_ok=True)
os.makedirs(ERROR, exist_ok=True)

def wait_for_file_complete(path, timeout=30, poll_interval=1):
    """
    Attendre que la taille du fichier soit stable (copie terminée).
    Retourne True si stable avant timeout, False sinon.
    """
    start = time.time()
    last_size = -1
    while True:
        try:
            current_size = os.path.getsize(path)
        except OSError:
            # fichier peut disparaître entre temps
            return False
        if current_size == last_size:
            return True
        last_size = current_size
        if time.time() - start > timeout:
            return False
        time.sleep(poll_interval)

def _unique_dest_path(dest_folder, filename):
    dest = os.path.join(dest_folder, filename)
    if not os.path.exists(dest):
        return dest
    name, ext = os.path.splitext(filename)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(dest_folder, f"{name}_{timestamp}{ext}")

class ExcelHandler(FileSystemEventHandler):
    def on_created(self, event):
        # Ne pas traiter les dossiers
        if event.is_directory:
            return

        src_path = event.src_path
        lower = src_path.lower()
        if not (lower.endswith(".xlsx") or lower.endswith(".xls") or lower.endswith(".csv")):
            print(f"Ignoré (format non supporté) : {src_path}")
            return

        print(f"📂 Nouveau fichier détecté : {src_path}")

        # Attendre que la copie soit terminée
        ok = wait_for_file_complete(src_path, timeout=60, poll_interval=1)
        if not ok:
            print(f"⚠️ Le fichier semble incomplet ou verrouillé : {src_path}")
            # on peut soit réessayer plus tard, soit le déplacer en erreur
            # Ici on choisit de ne pas le supprimer pour permettre nouvelle tentative
            return

        try:
            # Appel centralisé au processor
            df = process_excel(src_path)
            # Si tout s'est bien passé, on archive le fichier
            dest = _unique_dest_path(ARCHIVE, os.path.basename(src_path))
            shutil.move(src_path, dest)
            print(f"✅ Traitement OK. Fichier archivé -> {dest}")
        except Exception as e:
            print(f"❌ Erreur lors de l’import : {e}")
            # Déplacer en dossier error pour analyse manuelle
            dest = _unique_dest_path(ERROR, os.path.basename(src_path))
            try:
                shutil.move(src_path, dest)
                print(f"🚩 Fichier déplacé en erreur -> {dest}")
            except Exception as mv_err:
                print(f"❌ Impossible de déplacer le fichier en erreur: {mv_err}")

if __name__ == "__main__":
    path = INBOX
    event_handler = ExcelHandler()
    observer = Observer()
    observer.schedule(event_handler, path=path, recursive=False)
    observer.start()
    print(f"👀 Surveillance du dossier {path}... (Ctrl+C pour arrêter)")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Arrêt demandé, arrêt de l'observer...")
        observer.stop()
    observer.join()
    print("Observer arrêté.")
