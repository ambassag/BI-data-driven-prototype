# scripts/run_reader.py
from pathlib import Path
from excel_schema_reader import ExcelSchemaReader

if __name__ == "__main__":
    # Dossier inbox relatif au projet
    base_dir = Path(__file__).resolve().parent.parent
    inbox_dir = base_dir / "data" / "inbox"

    # Lister tous les fichiers Excel dans le dossier
    excel_files = list(inbox_dir.glob("*.xlsx")) + list(inbox_dir.glob("*.xls"))

    if not excel_files:
        print(f"⚠️ Aucun fichier Excel trouvé dans {inbox_dir}")
    else:
        for path in excel_files:
            print(f"\n=== Traitement du fichier : {path.name} ===")
            reader = ExcelSchemaReader(
                str(path),
                schema=None,      # pas de schéma obligatoire
                header_row=4,     # ligne d'en-tête
                all_sheets=True   # lire toutes les feuilles
            ).read()

            print("Feuilles détectées :", list(reader.sheets.keys()))

            for sheet, df in reader.sheets.items():
                if df.empty:
                    print(f"⚠️ Feuille '{sheet}' vide ou trop petite, ignorée.")
                    continue

                print(f"\n--- Feuille : {sheet} ---")
                print("Colonnes mappées (original -> canonical) :")
                print(reader.mapped_columns(sheet))
                print(df.head(5))  # aperçu

                # Accès sécurisé aux colonnes dynamiques
                def try_get_value(sheet, row, field):
                    try:
                        return reader.get_value(sheet, row, field)
                    except Exception as e:
                        return f"⚠️ '{field}' non trouvé: {e}"

                print("Station (ligne 0):", try_get_value(sheet, 0, "station_code"))
                print("Excel score pair 0 (ligne 0):", try_get_value(sheet, 0, "excel_score_0"))
                print("Eris score pair 0 (ligne 0):", try_get_value(sheet, 0, "eris_score_0"))
