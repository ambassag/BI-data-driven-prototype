import pandas as pd
from pathlib import Path
import unicodedata
import re
from datetime import datetime

# ---------------------------
# Fonctions utilitaires
# ---------------------------
def detect_header(df: pd.DataFrame, default: int = 0) -> int:
    keywords = ["hse invariant", "question", "inspection", "description", "commentaire", "action"]
    for i in range(min(12, len(df))):
        txt = " ".join(str(x).lower() for x in df.iloc[i].fillna(""))
        if any(k in txt for k in keywords):
            return i
    return default


def make_unique_cols(df: pd.DataFrame) -> pd.DataFrame:
    cols = pd.Series(df.columns.astype(str))
    for dup in cols[cols.duplicated()].unique():
        dups_idx = cols[cols == dup].index.tolist()
        for i, idx in enumerate(dups_idx[1:], start=1):
            cols[idx] = f"{dup}_{i}"
    df.columns = cols
    return df


def normalize_text(s: str) -> str:
    if pd.isna(s):
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r'[\s\-_.,/\\]+', ' ', s)
    return s.strip()


def get_country_code(affiliate: str) -> str:
    if pd.isna(affiliate) or not affiliate.strip():
        return ""
    affiliate_norm = normalize_text(affiliate)
    country_map = {
        "burkina": "BF",
        "south africa": "ZA",
        "afrique du sud": "ZA",
        "mauritius": "MU", "maurice": "MU",
        "cameroon": "CM", "cameroun": "CM",
        "cote divoire": "CI", "cote d'ivoire": "CI", "ivory coast": "CI", "cote ivoire": "CI",
        "senegal": "SN", "sénégal": "SN",
        "reunion": "RE", "réunion": "RE",
        "eswatini": "SZ", "togo": "TG", "ghana": "GH", "uganda": "UG",
        "congo": "CG", "congo brazzaville": "CG", "congo brazza": "CG",
        "ethiopia": "ET", "ethiopie": "ET", "tanzania": "TZ", "tanzanie": "TZ",
        "gabon": "GA", "guinea": "GN", "guinée": "GN", "equatorial guinea": "GQ",
        "guinée équatoriale": "GQ", "guinée equitoriale": "GQ", "kenya": "KE",
        "mayotte": "YT", "malawi": "MW", "morocco": "MA", "maroc": "MA",
        "mozambique": "MZ", "tunisia": "TN", "tunisie": "TN", "namibia": "NA",
        "namibie": "NA", "nigeria": "NG", "nigéria": "NG", "zambia": "ZM",
        "zambie": "ZM", "zimbabwe": "ZW", "madagascar": "MG", "rdc": "CD",
        "democratic republic of congo": "CD","Congo RDC": "CD", "congo rdc":"CD", "burkina faso": "BF",
        "erythree": "ER", "érythrée": "ER", "chad": "TD", "tchad": "TD",
        "mali": "ML", "angola": "AO", "egypt": "EG", "egypte": "EG",
        "botswana": "BW", "centafrique": "CE", "central african republic": "CE",
    }
    for name, code in country_map.items():
        if name in affiliate_norm:
            return code
    return ""


# ---------------------------
# Traitement feuille unique
# ---------------------------
def process_single_sheet(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    hr = detect_header(df, default=0)
    if hr >= len(df):
        return pd.DataFrame()

    hdr = df.iloc[hr].fillna("").astype(str)
    data = df.iloc[hr + 1:].reset_index(drop=True)
    data.columns = hdr
    data = make_unique_cols(data)

    mapping = {}
    for col in data.columns:
        low = col.lower().strip()
        if low == "affiliate":
            mapping[col] = "Affiliate"
        elif low in ["station code", "code station", "cost center"]:
            mapping[col] = "Station Code"
        elif low in ["station name", "name"]:
            mapping[col] = "Station name"
        elif low in ["management method", "mode de gestion"]:
            mapping[col] = "Mode de gestion"

    # On garde toutes les colonnes, mais on renomme celles qu’on reconnaît
    data.rename(columns=mapping, inplace=True)
    if "Affiliate" in data.columns:
        data["Country Code"] = data["Affiliate"].apply(get_country_code)
    return data


# ---------------------------
# Extraction multi-feuilles
# ---------------------------
def extract_hse_invariants(input_dir: str, output_dir: str, sheets_to_extract=None) -> dict:
    if sheets_to_extract is None:
        sheets_to_extract = ["HSE Invariants", "Inspections", "Questions"]

    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    updated_subdir = output_path / "updated"
    updated_subdir.mkdir(parents=True, exist_ok=True)

    # Rechercher le fichier contenant "extraction"
    extraction_files = list(input_path.glob("*Extraction*.xlsx"))
    if not extraction_files:
        raise FileNotFoundError("Aucun fichier contenant 'extraction' trouvé dans le répertoire input.")
    input_file = extraction_files[0]

    # Tentative d'extraction de la date du nom du fichier
    filename = input_file.stem
    date_patterns = [
        r"(\d{2})[_\-\.](\d{2})[_\-\.](\d{2,4})",  # ex: 12_10_25 ou 12-10-2025
        r"(\d{4})[_\-\.](\d{2})[_\-\.](\d{2})",    # ex: 2025_10_12
    ]
    extracted_date = None
    for pattern in date_patterns:
        m = re.search(pattern, filename)
        if m:
            parts = [int(x) for x in m.groups()]
            if len(str(parts[0])) == 4:  # format YYYY-MM-DD
                extracted_date = datetime(parts[0], parts[1], parts[2])
            else:  # format DD-MM-YY
                year = parts[2] + 2000 if parts[2] < 100 else parts[2]
                extracted_date = datetime(year, parts[1], parts[0])
            break

    if extracted_date:
        date_str = extracted_date.strftime("%Y%m%d")
        print(f" Date extraite du nom du fichier : {extracted_date.strftime('%d/%m/%Y')}")
    else:
        date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        print(f"Aucune date trouvée dans le nom, utilisation de la date courante : {date_str}")

    # Lecture et traitement
    xls = pd.ExcelFile(input_file)
    available_sheets = set(xls.sheet_names)

    results = {"input": str(input_file), "generated": []}

    for sheet_name in sheets_to_extract:
        if sheet_name not in available_sheets:
            print(f"Onglet '{sheet_name}' absent — ignoré.")
            continue

        df = pd.read_excel(xls, sheet_name=sheet_name, header=None, dtype=object)
        processed = process_single_sheet(df)

        if processed.empty:
            print(f"'{sheet_name}' traité mais vide — fichier non généré.")
            continue

        safe_name = re.sub(r'[^0-9A-Za-z_\-]', '_', sheet_name)
        # Si Questions ou Inspections -> dans updated/, sinon à la racine de output_dir
        if sheet_name.lower() in ("questions", "inspections"):
            target_dir = updated_subdir
        else:
            target_dir = output_path

        out_file = target_dir / f"{safe_name}_{date_str}.xlsx"
        target_dir.mkdir(parents=True, exist_ok=True)
        processed.to_excel(out_file, sheet_name=sheet_name, index=False)

        results["generated"].append({
            "sheet": sheet_name,
            "file": str(out_file),
            "rows": int(processed.shape[0]),
            "cols": int(processed.shape[1])
        })
        print(f"'{sheet_name}' sauvegardé -> {out_file}")

    return results



# ---------------------------
# Script principal
# ---------------------------
if __name__ == "__main__":
    res = extract_hse_invariants(
        input_dir="data/inbox",
        output_dir="data/out"
    )

    print("\n=== Extraction terminée ===")
    print("Fichier source :", res["input"])
    for g in res["generated"]:
        print(f" - {g['sheet']} -> {g['file']} ({g['rows']} lignes, {g['cols']} colonnes)")
