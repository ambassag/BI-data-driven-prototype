# processor.py
import os
from typing import List, Dict
import pandas as pd
from excel_schema_reader import Schema, ExcelSchemaReader

# --- Schéma léger pour les colonnes statiques (optionnel) ---
# Tu peux enrichir / vider ce schéma selon les cas
schema = Schema.from_dict({
    "filiale_code": ["Filiale Code"],
    "station_code": ["Station Code"],
    "segmentation": ["Segmentation"],
    "management_mode": ["Management Mode"],
    "station_name": ["Station Name"],
    "enregistres": ["Enregistrés"],
    "audites": ["Audités"],
    "statut": ["Statut"]
})

# ---------------- Utils ----------------

def _list_dynamic_scores(cols: List[str], prefix: str) -> List[str]:
    """Retourne la liste triée des colonnes qui commencent par un préfixe donné."""
    return sorted([c for c in cols if str(c).lower().startswith(prefix.lower())])

def safe_read_excel(path: str, sheet_name: str, header_row: int = 4) -> pd.DataFrame:
    """
    Lit une feuille Excel de manière sécurisée :
    - Vérifie qu'il y a assez de lignes pour contenir l'en-tête.
    - Retourne un DataFrame vide sinon.
    """
    try:
        # Lecture brute pour compter les lignes
        temp_df = pd.read_excel(path, sheet_name=sheet_name, header=None, engine="openpyxl")
        if temp_df.shape[0] <= header_row:
            print(f"⚠️ Feuille '{sheet_name}' ignorée : moins de {header_row + 1} lignes.")
            return pd.DataFrame()

        # Lecture réelle avec header
        return pd.read_excel(path, sheet_name=sheet_name, header=header_row, engine="openpyxl")

    except Exception as e:
        print(f"⚠️ Impossible de lire la feuille '{sheet_name}' ({e})")
        return pd.DataFrame()

# ---------------- Processing ----------------

def process_excel(path: str, header_row: int = 4) -> Dict[str, pd.DataFrame]:
    """
    Traite un fichier Excel avec toutes ses feuilles :
    - lit chaque feuille
    - normalise et mappe les colonnes
    - détecte les colonnes dynamiques Excel/Eris
    Retourne {nom_feuille: DataFrame}
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ Fichier introuvable: {path}")

    results: Dict[str, pd.DataFrame] = {}
    print(f"\n=== PROCESSOR : traitement du fichier {os.path.basename(path)} ===")

    # Récupération des noms de feuilles
    try:
        xls = pd.ExcelFile(path, engine="openpyxl")
        sheets = xls.sheet_names
    except Exception as e:
        print(f"❌ Impossible d'ouvrir le fichier Excel ({e})")
        return results

    for sheet in sheets:
        df = safe_read_excel(path, sheet, header_row)
        if df.empty:
            continue

        print(f"\n--- Feuille : {sheet} ---")
        print(f"Lignes : {len(df)} | Colonnes : {len(df.columns)}")

        # Mapping avec ExcelSchemaReader (sécurisé)
        try:
            reader = ExcelSchemaReader(
                path,
                schema=schema,
                sheet_name=sheet,
                header_row=header_row,
                detect_dynamic_pairs=True,
                all_sheets=False
            ).read()

            mapped = reader.mapped_columns(sheet)
        except Exception as e:
            print(f"⚠️ Mapping non disponible pour {sheet} ({e})")
            mapped = {}

        print("\nMapping original -> canonical (extrait) :")
        for k, v in list(mapped.items())[:30]:
            print(f"  {k!r} -> {v}")

        print("\nAperçu (5 premières lignes) :")
        print(df.head(5).to_string(index=False))

        # Détection des colonnes dynamiques
        cols = list(df.columns)
        excel_cols = _list_dynamic_scores(cols, "excel_score_")
        eris_cols = _list_dynamic_scores(cols, "eris_score_")

        print("\nColonnes dynamiques détectées :")
        print(f"  excel_score_* : {len(excel_cols)} (ex: {excel_cols[:5]})")
        print(f"  eris_score_*  : {len(eris_cols)} (ex: {eris_cols[:5]})")

        if excel_cols and eris_cols:
            print("✅ Colonnes Excel/Eris détectées.")
        else:
            print("⚠️ Aucune paire Excel/Eris trouvée.")

        # Sauvegarde du DataFrame nettoyé
        results[sheet] = df

    print("\n=== FIN DU PROCESSING ===\n")
    return results
