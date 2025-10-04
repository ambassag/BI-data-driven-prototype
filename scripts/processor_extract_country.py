"""
Lit les onglets d'un fichier Excel et produit un fichier XLSX avec :
 sheet_name, country_code (alpha-2)
On garde uniquement les onglets correspondant à un pays (pycountry) ou
présents dans le dictionnaire d'exceptions fourni.
"""

from pathlib import Path
import unicodedata
import re
import pycountry
import pandas as pd
import argparse

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

USER_CODE_TO_NAME = {
    "GH": "Ghana",
    "UG": "Uganda",
    "CG": "Congo",                # République du Congo (Brazzaville) - user also added Congo Brazza
    "CI": "Côte d'Ivoire",
    "CM": "Cameroun",
    "ZA": "Afrique du Sud",
    "TG": "Togo",
    "SN": "Sénégal",
    "SZ": "Eswatini",
    "RE": "Réunion",
    "MU": "Maurice",
    "ET": "Ethiopie",
    "GA": "Gabon",
    "GN": "Guinée",
    "GQ": "Guinée équatoriale",
    "KE": "Kenya",
    "YT": "Mayotte",
    "MW": "Malawi",
    "MA": "Maroc",
    "MZ": "Mozambique",
    "TN": "Tunisie",
    "NA": "Namibie",
    "NG": "Nigeria",
    "TZ": "Tanzanie",
    # user wrote "ZM- Zmbabwe" and "ZM-Zambie" in list; we map ZM -> Zambie, ZW -> Zimbabwe
    "ZM": "Zambie",
    "ZW": "Zimbabwe",
    "MG": "Madagascar",
    "CD": "RDC",
    "BF": "Burkina Faso",
    "ER": "Erythrée",
    "TD": "Tchad",
    "ML": "Mali",
    "AO": "Angola"
    # si d'autres codes manquent tu peux les ajouter ici
}

# Construire map name_normalized -> code_alpha2 depuis USER_CODE_TO_NAME
USER_NAME_TO_CODE = { normalize_text(v): k for k, v in USER_CODE_TO_NAME.items() }

def map_sheets_to_countries(input_xlsx: str, output_xlsx: str):
    inp = Path(input_xlsx)
    if not inp.exists():
        raise FileNotFoundError(f"Input file not found: {input_xlsx}")

    py_map = build_pycountry_map()

    xls = pd.ExcelFile(inp)
    sheets = xls.sheet_names

    rows = []
    for s in sheets:
        norm = normalize_text(s)
        matched_code = None
        method = None

        # 1) exact match pycountry
        if norm in py_map:
            c = py_map[norm]
            matched_code = c.alpha_2
            method = "pycountry_exact_name"

        # 2) exact match user exceptions (name -> code)
        if matched_code is None and norm in USER_NAME_TO_CODE:
            matched_code = USER_NAME_TO_CODE[norm]
            method = "user_exception_name"

        # 3) if sheet itself is an alpha2 code (two letters), prefer pycountry if valid, else user exceptions
        if matched_code is None and re.fullmatch(r'[A-Za-z]{2}', s.strip()):
            code_try = s.strip().upper()
            c = pycountry.countries.get(alpha_2=code_try)
            if c:
                matched_code = code_try
                method = "sheet_is_alpha2_pycountry"
            elif code_try in USER_CODE_TO_NAME:
                matched_code = code_try
                method = "sheet_is_alpha2_user_exception"

        # 4) as fallback try to match user exception keys by fuzzy? user requested strict; we will NOT fuzzy by default

        if matched_code:
            rows.append({"country": s, "country_code": matched_code, "match_method": method})
        else:
            # ignore sheet if nothing matched
            pass

    df = pd.DataFrame(rows, columns=["country", "country_code", "match_method"])

    outp = Path(output_xlsx)
    outp.parent.mkdir(parents=True, exist_ok=True)
    # write only country + country_code as requested
    df_out = df[["country", "country_code"]]
    df_out.to_excel(outp, index=False)
    print(f"[DONE] Wrote {len(df_out)} matched sheets to {outp}")
    return df_out, df  # return both (with methods) if caller wants more info

# ------------- CLI -------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Map Excel sheet names to country alpha-2 using pycountry + exceptions.")
    parser.add_argument("--input", "-i", default="data/inbox/Invariants - calculs_test.xlsx")
    parser.add_argument("--output", "-o", default="data/out/Country_code.xlsx")
    args = parser.parse_args()

    df_result, df_debug = map_sheets_to_countries(args.input, args.output)
    if not df_result.empty:
        print(df_result.to_string(index=False))
    else:
        print("No sheet matched pycountry or exceptions.")
