from pathlib import Path
import unicodedata
import re
import pycountry
import pandas as pd
from datetime import datetime

# ------------------------ Utils ------------------------
def normalize_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.replace("’", "'").replace("`", "'")
    s = s.lower().strip()
    s = re.sub(r'[\s\-_\.]+', ' ', s)
    s = s.strip(" -_.")
    return s

def build_pycountry_map():
    """Retourne dict: normalized_name -> pycountry.Country"""
    mp = {}
    for c in pycountry.countries:
        candidates = set()
        if getattr(c, "name", None):
            candidates.add(c.name)
        if getattr(c, "official_name", None):
            candidates.add(c.official_name)
        if getattr(c, "common_name", None):
            candidates.add(c.common_name)
        for nm in candidates:
            if not nm:
                continue
            key = normalize_text(nm)
            mp[key] = c
    return mp

# ------------------------ Exceptions utilisateur ------------------------
USER_CODE_TO_NAME = {
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
USER_NAME_TO_CODE = {normalize_text(k): v.upper() for k, v in USER_CODE_TO_NAME.items()}

# ------------------------ Traitement ------------------------
def map_sheets_to_countries_auto(input_folder="data/inbox", output_folder="data/out/updated"):
    folder = Path(input_folder)
    files = sorted(folder.glob("Invariants*.xlsx"), key=lambda f: f.stat().st_mtime, reverse=True)
    if not files:
        raise FileNotFoundError("Aucun fichier Invariants*.xlsx trouvé dans le dossier d'entrée")
    last_file = files[0]

    xls = pd.ExcelFile(last_file)
    py_map = build_pycountry_map()
    rows = []

    for s in xls.sheet_names:
        norm = normalize_text(s)
        matched_code = None

        if norm in py_map:
            matched_code = py_map[norm].alpha_2.upper()
        elif norm in USER_NAME_TO_CODE:
            matched_code = USER_NAME_TO_CODE[norm]

        if matched_code:
            rows.append({"country": s, "country_code": matched_code})

    if not rows:
        raise ValueError("Aucun onglet n'a été mappé à un pays.")

    df_out = pd.DataFrame(rows, columns=["country", "country_code"])

    date_str = datetime.now().strftime("%Y%m%d")
    out_path = Path(output_folder) / f"Country_code_{date_str}.xlsx"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_excel(out_path, index=False)
    print(f"[DONE] Fichier écrit : {out_path}")
    return out_path

# ------------------------ MAIN ------------------------
if __name__ == "__main__":
    map_sheets_to_countries_auto()
