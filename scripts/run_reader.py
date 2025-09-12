from pathlib import Path
from excel_schema_reader import ExcelSchemaReader

if __name__ == "__main__":
    # Base du projet (2 niveaux au-dessus car ce script est dans /scripts)
    base_dir = Path(__file__).resolve().parent.parent

    # Fichier Excel attendu
    path = base_dir / "data" / "inbox" / "exemple_conformite.xlsx"

    # Lecture avec le schema dynamique
    reader = ExcelSchemaReader(
        str(path),
        schema=None,
        sheet_name="Score Excel Vs Eris",  # le nom exact de ta feuille
        header_row=4                       # ligne d'en-tête
    ).read()

    # Affichage des colonnes mappées
    print("Colonnes mappées (original -> canonical) :")
    print(reader.mapped_columns())

    # Aperçu du DataFrame
    print(reader.df.head(5))

    # Exemples d'accès direct aux données
    print("Station (ligne 0):", reader.get_value(0, "station_code"))
    print("Nom Station (ligne 0):", reader.get_value(0, "station_name"))
    print("Excel score pair 0 (ligne 0):", reader.get_value(0, "excel_score_0"))
    print("Eris score pair 0 (ligne 0):", reader.get_value(0, "eris_score_0"))
