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
        "burkina": "BF", "south africa": "ZA", "afrique du sud": "ZA",
        "mauritius": "MU", "maurice": "MU", "cameroon": "CM", "cameroun": "CM",
        "cote divoire": "CI", "cote d'ivoire": "CI", "ivory coast": "CI", "cote ivoire": "CI",
        "senegal": "SN", "sénégal": "SN", "reunion": "RE", "réunion": "RE",
        "eswatini": "SZ", "togo": "TG", "ghana": "GH", "uganda": "UG",
        "congo": "CG", "congo brazzaville": "CG", "congo brazza": "CG",
        "ethiopia": "ET", "ethiopie": "ET", "tanzania": "TZ", "tanzanie": "TZ",
        "gabon": "GA", "guinea": "GN", "guinée": "GN", "equatorial guinea": "GQ",
        "guinée équatoriale": "GQ", "guinée equitoriale": "GQ", "kenya": "KE",
        "mayotte": "YT", "malawi": "MW", "morocco": "MA", "maroc": "MA",
        "mozambique": "MZ", "tunisia": "TN", "tunisie": "TN", "namibia": "NA",
        "namibie": "NA", "nigeria": "NG", "nigéria": "NG", "zambia": "ZM",
        "zambie": "ZM", "zimbabwe": "ZW", "madagascar": "MG", "rdc": "CD",
        "democratic republic of congo": "CD", "burkina faso": "BF",
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

    data = data[[c for c in data.columns if c in mapping]]
    data.rename(columns=mapping, inplace=True)
    data["Country Code"] = data["Affiliate"].apply(get_country_code) if "Affiliate" in data.columns else ""
    return data

# ---------------------------
# Extraction HSE Invariants
# ---------------------------
def extract_hse_invariants(input_dir: str, output_dir: str) -> dict:
    input_path = Path(input_dir)
    output_path = Path(output_dir)

    # Rechercher le fichier contenant "extraction"
    extraction_files = list(input_path.glob("*Extraction*.xlsx"))
    if not extraction_files:
        raise FileNotFoundError("Aucun fichier contenant 'extraction' trouvé dans le répertoire input.")

    input_file = extraction_files[0]
    date_str = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Nom de sortie avec suffixe date
    output_file = output_path / f"HSE_Invariants_{date_str}.xlsx"

    # Lecture et traitement
    xls = pd.ExcelFile(input_file)
    if "HSE Invariants" not in xls.sheet_names:
        raise ValueError("Feuille 'HSE Invariants' absente.")
    df = pd.read_excel(xls, sheet_name="HSE Invariants", header=None, dtype=object)
    processed = process_single_sheet(df)

    if processed.empty:
        raise ValueError("La feuille 'HSE Invariants' est vide après traitement.")

    # Sauvegarde
    output_path.mkdir(parents=True, exist_ok=True)
    processed.to_excel(output_file, sheet_name="HSE Invariants", index=False)


    return {
        "inbox": str(input_file),
        "output": str(output_file),
        "rows": processed.shape[0],
        "cols": processed.shape[1]
    }

# ---------------------------
# Script principal
# ---------------------------
if __name__ == "__main__":
    result = extract_hse_invariants(
        input_dir="data/inbox",
        output_dir="data/out",
    )

    print("=== Extraction terminée ===")
    print(f"Entrée : {result['inbox']}")
    print(f"Sortie : {result['output']}")
    print(f"Lignes : {result['rows']} | Colonnes : {result['cols']}")
