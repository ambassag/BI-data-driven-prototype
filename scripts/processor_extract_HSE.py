import pandas as pd
from pathlib import Path
import unicodedata
import re

# ---------------------------
# Fonctions utilitaires
# ---------------------------
def detect_header(df: pd.DataFrame, default: int = 0) -> int:
    """Détecter la ligne d'entête probable"""
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
    """Normaliser un texte pour les comparaisons (retirer accents, espaces)"""
    if pd.isna(s):
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r'[\s\-_.,/\\]+', ' ', s)
    return s.strip()

def get_country_code(affiliate: str) -> str:
    """Retourne le code pays à partir du nom dans Affiliate"""
    if pd.isna(affiliate) or not affiliate.strip():
        return ""

    affiliate_norm = normalize_text(affiliate)

    # Dictionnaire unifié avec noms français et anglais
    country_map = {
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
        "namibia": "NA", "namibie": "NA",
        "nigeria": "NG", "nigéria": "NG",
        "zambia": "ZM", "zambie": "ZM",
        "zimbabwe": "ZW",
        "madagascar": "MG",
        "rdc": "CD", "democratic republic of congo": "CD",
        "burkina faso": "BF",
        "erythree": "ER", "érythrée": "ER",
        "chad": "TD", "tchad": "TD",
        "mali": "ML",
        "angola": "AO",
        "egypt": "EG", "egypte": "EG",
        "botswana": "BW",
        "centafrique": "CE", "central african republic": "CE",
    }

    for name, code in country_map.items():
        if name in affiliate_norm:
            return code
    return ""  # Aucun match trouvé

# ---------------------------
# Traitement feuille unique
# ---------------------------
def process_single_sheet(df: pd.DataFrame) -> pd.DataFrame:
    """Nettoyer et structurer la feuille HSE Invariants"""
    if df.empty:
        return pd.DataFrame()

    hr = detect_header(df, default=0)
    if hr >= len(df):
        return pd.DataFrame()

    hdr = df.iloc[hr].fillna("").astype(str)

    data = df.iloc[hr + 1:].reset_index(drop=True) if hr > 0 else df.iloc[1:].reset_index(drop=True)
    data.columns = hdr
    data = make_unique_cols(data)

    # Sélection des colonnes importantes
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

    selected_cols = [c for c in data.columns if c in mapping]
    data = data[selected_cols]
    data.rename(columns=mapping, inplace=True)

    # Ajouter la colonne Country Code
    data["Country Code"] = data["Affiliate"].apply(get_country_code) if "Affiliate" in data.columns else ""

    return data

# ---------------------------
# Extraction HSE Invariants
# ---------------------------
def extract_hse_invariants(input_file: str, output_file: str) -> dict:
    inp = Path(input_file)
    if not inp.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    xls = pd.ExcelFile(inp)
    sheet_name = "HSE Invariants"

    if sheet_name not in xls.sheet_names:
        raise ValueError(f"La feuille '{sheet_name}' est absente du fichier source.")

    df = pd.read_excel(xls, sheet_name=sheet_name, header=None, dtype=object)
    processed = process_single_sheet(df)

    if processed.empty:
        raise ValueError(f"La feuille '{sheet_name}' est vide après nettoyage.")

    outp = Path(output_file)
    outp.parent.mkdir(parents=True, exist_ok=True)
    processed.to_excel(outp, sheet_name=sheet_name[:31], index=False)

    return {"input": str(inp), "output": str(outp), "rows": processed.shape[0], "cols": processed.shape[1]}

# ---------------------------
# Script principal
# ---------------------------
if __name__ == "__main__":
    input_file = "data/inbox/Extraction_test.xlsx"
    output_file = "data/out/HSE_Invariants.xlsx"

    result = extract_hse_invariants(input_file, output_file)

    print("=== HSE Invariants extrait ===")
    print(f"Lignes : {result['rows']}  |  Colonnes : {result['cols']}")
    print(f"Fichier de sortie : {result['output']}")
