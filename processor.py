# processor.py
import os
from typing import List
from excel_schema_reader import Schema, Field, ExcelSchemaReader

# --- Schema léger pour les colonnes statiques (on laisse la détection dynamique pour Excel/Eris) ---
# Les aliases correspondent aux en-têtes réels dans ton fichier Excel.
schema = Schema.from_dict({
    "filiale_code": ["Filiale Code"],
    "station_code": ["Station Code"],
    "segmentation": ["Segmentation "],
    "management_mode": ["Management Mode "],
    "station_name": ["Station name"],
    "enregistres": ["Enregistrés "],
    "audites": ["Audités "],
    "statut": ["Statut"]
})

def _list_dynamic_scores(cols: List[str], prefix: str) -> List[str]:
    """Retourne la liste triée des colonnes qui commencent par un préfixe (ex: excel_score_)."""
    return sorted([c for c in cols if str(c).lower().startswith(prefix.lower())])

def process_excel(path: str):
    """
    Traite un fichier Excel spécifique 'Score Excel Vs Eris'.
    - lit la feuille "Score Excel Vs Eris" avec header_row=4
    - normalise/renomme les colonnes (excel_score_0.., eris_score_0.., etc.)
    - affiche en console mapping, aperçu, compte de lignes et colonnes dynamiques.
    Retourne le DataFrame Pandas normalisé.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Fichier introuvable: {path}")

    # Construction du reader : on force la feuille et la ligne d'entête connues
    reader = ExcelSchemaReader(
        path,
        schema=schema,
        sheet_name="Score Excel Vs Eris",
        header_row=4,
        detect_dynamic_pairs=True
    ).read()

    df = reader.df

    # Affichages utiles pour debug / vérification
    print("=== PROCESSOR : aperçu du fichier importé ===")
    print(f"Fichier lu : {path}")
    print(f"Nombre de lignes : {len(df)}")
    print(f"Nombre de colonnes : {len(df.columns)}")
    print("\nMapping original -> canonical (extrait 0..30) :")
    mapped = reader.mapped_columns()
    # affiche jusqu'à 40 mappings pour ne pas inonder la console
    for k, v in list(mapped.items())[:40]:
        print(f"  {k!r}  ->  {v}")

    print("\n--- Aperçu (5 premières lignes) ---")
    print(df.head(5).to_string(index=False))

    # Lister les colonnes dynamiques 'excel_score_X' et 'eris_score_X'
    excel_cols = _list_dynamic_scores(df.columns, "excel_score_")
    eris_cols = _list_dynamic_scores(df.columns, "eris_score_")

    print("\nColonnes dynamiques détectées :")
    print(f"  excel score columns   : {len(excel_cols)} (ex: {excel_cols[:5]})")
    print(f"  eris  score columns   : {len(eris_cols)} (ex: {eris_cols[:5]})")

    # Vérification simple : présence d'au moins excel_score_0
    if len(excel_cols) == 0 or len(eris_cols) == 0:
        print("⚠️  Aucune colonne 'excel_score_' ou 'eris_score_' détectée — vérifie header_row / feuille.")
    else:
        print("✅ Colonnes Excel/Eris dynamiques détectées correctement.")

    print("\n=== FIN DU PROCESSING ===\n")
    return df
