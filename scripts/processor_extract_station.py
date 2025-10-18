import pandas as pd
from pathlib import Path
import re
import pycountry
import unicodedata
from datetime import datetime

_safe = lambda x: "" if pd.isna(x) else str(x).strip()

# ---------------------------
# Fonctions utilitaires
# ---------------------------
def normalize_name(name: str) -> str:
    if not name:
        return ""
    nfkd_form = unicodedata.normalize('NFKD', name)
    without_accents = "".join([c for c in nfkd_form if not unicodedata.combining(c)])
    cleaned = without_accents.replace("'", "").replace("’", "")
    return re.sub(r'\s+', ' ', cleaned).lower().strip()

# Tous les noms de pays de pycountry normalisés
countries_normalized = [normalize_name(c.name) for c in pycountry.countries]

# Exceptions ou noms spécifiques présents dans tes feuilles
exceptions_raw = [
    "Tanzania", "Guinée Equitoriale", "Congo Brazza", "RDC", "Cameroun",
    "Zambie", "Namibie", "Afrique du Sud", "Guinée", "Côte Ivoire", "Centafrique",
    "Egypte", "Erythrée", "Ethiopie", "Maroc", "Maurice", "Tunisie", "Burkina", "Tchad"
]
exceptions_normalized = [normalize_name(name) for name in exceptions_raw]

# ---------------------------
# Dictionnaire des codes pays
# ---------------------------
COUNTRY_CODES = {
    "burkina": "BF",
    "south africa": "ZA", "afrique du sud": "ZA",
    "mauritius": "MU", "maurice": "MU",
    "cameroon": "CM", "cameroun": "CM",
    "cote divoire": "CI", "cote d'ivoire": "CI", "ivory coast": "CI", "cote ivoire": "CI",
    "senegal": "SN", "sénégal": "SN",
    "reunion": "RE", "réunion": "RE",
    "eswatini": "SZ",
    "togo": "TG",
    "ghana": "GH",
    "uganda": "UG",
    "congo": "CG", "congo brazzaville": "CG","congo brazza": "CG",
    "ethiopia": "ET", "ethiopie": "ET",
    "tanzania": "TZ", "tanzanie": "TZ",
    "gabon": "GA",
    "guinea": "GN", "guinée": "GN",
    "equatorial guinea": "GQ", "guinée équatoriale": "GQ","guinée equitoriale": "GQ",
    "kenya": "KE",
    "mayotte": "YT",
    "malawi": "MW",
    "morocco": "MA", "maroc": "MA",
    "mozambique": "MZ",
    "tunisia": "TN", "tunisie": "TN",
    "namibia": "NA", "namibie":"NA",
    "nigeria": "NG", "nigéria": "NG",
    "zambia": "ZM", "zambie": "ZM",
    "zimbabwe": "ZW",
    "madagascar": "MG",
    "rdc": "CD", "democratic republic of congo": "CD", "congo rdc":"CD", "Congo RDC":"CD",
    "burkina faso": "BF",
    "erythree": "ER", "érythrée": "ER",
    "chad": "TD", "tchad": "TD",
    "mali": "ML",
    "angola": "AO",
    "egypt": "EG", "egypte": "EG",
    "botswana": "BW",
    "centafrique": "CE", "central african republic": "CE",
}

# Mapping nom_normalisé -> code
COUNTRY_NAME_TO_CODE = {normalize_name(k): v for k, v in COUNTRY_CODES.items()}

manual_aliases = {
    "congo brazza": "CG", "rdc": "CD", "cote d'ivoire": "CI", "cote divoire": "CI", "cote ivoire": "CI", "rca": "CE"
}
for alias, code in manual_aliases.items():
    COUNTRY_NAME_TO_CODE[normalize_name(alias)] = code

# ---------------------------
# Détection entête
# ---------------------------
def detect_header(df: pd.DataFrame, default: int = 4) -> int:
    keywords = [
        "cost center", "segmentation", "city", "management", "station", "name",
        "cost", "centre de coût", "methode de gestion", "ville"
    ]
    for i in range(min(12, len(df))):
        txt = " ".join(_safe(x).lower() for x in df.iloc[i])
        if any(k in txt for k in keywords):
            return i
    return default

# ---------------------------
# Assurer l'unicité des colonnes
# ---------------------------
def make_unique_cols(df: pd.DataFrame) -> pd.DataFrame:
    cols = pd.Series(df.columns.astype(str))
    for dup in cols[cols.duplicated()].unique():
        dups_idx = cols[cols == dup].index.tolist()
        for i, idx in enumerate(dups_idx[1:], start=1):
            cols[idx] = f"{dup}_{i}"
    df.columns = cols
    return df

# ---------------------------
# Traitement d'une feuille
# ---------------------------
def process_sheet(df: pd.DataFrame, sheet_name: str, station_cols: int = 5) -> pd.DataFrame:
    if df.empty or len(df) < 1:
        return None
    hr = detect_header(df, default=4)
    data = df.iloc[hr+1:].reset_index(drop=True)
    if data.empty:
        return None
    pays = data.iloc[:, :station_cols].copy()
    new_cols = ["Cost Center", "Station name", "City", "Segmentation", "Management method"]
    pays.columns = new_cols[:pays.shape[1]]
    sheet_norm = normalize_name(sheet_name)
    country_code = COUNTRY_NAME_TO_CODE.get(sheet_norm, None)
    pays["Country code"] = country_code if country_code else sheet_name
    pays = make_unique_cols(pays)
    cols_to_check = pays.columns.difference(["Country code"])
    pays = pays.dropna(how="all", subset=cols_to_check)
    return pays if not pays.empty else None

# ---------------------------
# Traitement toutes feuilles
# ---------------------------
def process_all_sheets_one_df(input_file: str, station_cols: int = 5, exclude_patterns: list = None) -> pd.DataFrame:
    inp = Path(input_file)
    if not inp.exists():
        raise FileNotFoundError(f"{input_file} not found")
    exclude_patterns = exclude_patterns or []
    exclude_regexes = [re.compile(p, re.I) for p in exclude_patterns]
    xls = pd.ExcelFile(inp)
    all_data = []
    for sheet_name in xls.sheet_names:
        if any(rx.search(sheet_name) for rx in exclude_regexes):
            print(f"{sheet_name:40s} -> IGNORED (regex)")
            continue
        sheet_norm = normalize_name(sheet_name)
        if sheet_norm not in countries_normalized and sheet_norm not in exceptions_normalized:
            print(f"{sheet_name:40s} -> IGNORED (not a country)")
            continue
        df = pd.read_excel(xls, sheet_name=sheet_name, header=None, dtype=object)
        processed_df = process_sheet(df, sheet_name, station_cols=station_cols)
        if processed_df is None:
            print(f"{sheet_name:40s} -> EMPTY or no data")
            continue
        all_data.append(processed_df)
        print(f"{sheet_name:40s} -> OK : {processed_df.shape[0]} rows")
    result_df = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
    return result_df

# ---------------------------
# Script principal
# ---------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", "-i", default="data/inbox", help="Répertoire des fichiers d'entrée")
    parser.add_argument("--output-dir", "-o", default="data/out", help="Répertoire des fichiers de sortie")
    parser.add_argument("--exclude", "-e", default="", help="comma-separated regex patterns to ignore")
    parser.add_argument("--station-cols", "-c", type=int, default=5)
    args = parser.parse_args()

    exclude_patterns = [s.strip() for s in args.exclude.split(",") if s.strip()] if args.exclude else []
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Détection automatique des fichiers d'entrée par mot-clé
    keywords = ["invariant", "invariants"]
    input_files = [f for f in input_dir.glob("*.xlsx") if any(k in f.name.lower() for k in keywords)]

    if not input_files:
        print("Aucun fichier d'entrée correspondant aux mots-clés trouvé.")
        exit(0)

    for inp_file in input_files:
        df_all = process_all_sheets_one_df(
            input_file=str(inp_file),
            station_cols=args.station_cols,
            exclude_patterns=exclude_patterns
        )
        if df_all.empty:
            print(f"{inp_file.name} -> Aucun data à traiter")
            continue

        # Nom de sortie fixe Extract_Station avec date
        date_str = datetime.now().strftime("%Y%m%d")
        out_path = output_dir / f"Extract_Station_{date_str}.xlsx"

        # Gérer les doublons si fichier existe déjà
        i = 1
        while out_path.exists():
            out_path = output_dir / f"Extract_Station_{date_str}_{i}.xlsx"
            i += 1

        df_all = df_all.fillna("")
        df_all.to_excel(out_path, sheet_name="Données", index=False)
        print(f"\nFichier de sortie: {out_path}")