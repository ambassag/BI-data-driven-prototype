import pandas as pd
from pathlib import Path
from typing import List, Optional
import re
import pycountry
import unicodedata

_safe = lambda x: "" if pd.isna(x) else str(x).strip()

# ---------------------------
# Fonctions pour normaliser les noms
# ---------------------------
def normalize_name(name: str) -> str:
    """Supprime accents, apostrophes et met en minuscule"""
    if not name:
        return ""
    # enlever accents
    nfkd_form = unicodedata.normalize('NFKD', name)
    without_accents = "".join([c for c in nfkd_form if not unicodedata.combining(c)])
    # enlever apostrophes et "d " ou "l "
    cleaned = without_accents.replace("'", "").replace("’", "")
    return cleaned.lower().strip()

# Liste normalisée des noms de pays (pycountry)
countries_normalized = [normalize_name(c.name) for c in pycountry.countries]

# Liste des exceptions
exceptions_raw = [
    "Tanzania", "Guinée Equitoriale", "Congo Brazza", "RDC", "Cameroun",
    "Zambie", "Namibie", "Afrique du Sud", "Guinée", "Côte Ivoire", "Centafrique",
    "Egypte", "Erythrée", "Ethiopie", "Maroc", "Maurice", "Tunisie", "Burkina", "Tchad"
]
exceptions_normalized = [normalize_name(name) for name in exceptions_raw]

# ---------------------------
# Fonctions principales
# ---------------------------
def detect_header(df: pd.DataFrame, default: int = 4) -> int:
    keywords = ["cost center", "segmentation", "city", "management", "station", "name", "cost"]
    for i in range(min(12, len(df))):
        txt = " ".join(_safe(x).lower() for x in df.iloc[i])
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

def process_single_sheet(df: pd.DataFrame, sheet_name: str, station_cols: int = 5) -> Optional[pd.DataFrame]:
    if df.empty or len(df) < 1:
        return None
    hr = detect_header(df, default=4)
    if hr >= len(df):
        return None  # pas assez de lignes pour le header

    hdr_top = df.iloc[hr-1].fillna("").map(_safe) if hr > 0 else None
    hdr_bot = df.iloc[hr].fillna("").map(_safe)

    headers = []
    for i in range(len(df.columns)):
        top_val = hdr_top.iat[i] if hdr_top is not None and i < len(hdr_top) else ""
        bot_val = hdr_bot.iat[i] if i < len(hdr_bot) else f"col_{i}"
        if top_val:
            headers.append(f"{top_val} | {bot_val}".strip())
        else:
            headers.append(bot_val or f"col_{i}")

    data = df.iloc[hr+1:].reset_index(drop=True)
    if data.empty:
        return None

    pays = data.iloc[:, :station_cols].copy()
    pays.columns = headers[:station_cols]
    pays["Pays"] = sheet_name
    pays = make_unique_cols(pays)
    return pays

def process_all_sheets_stream(
    input_file: str,
    output_file: str,
    station_cols: int = 5,
    exclude_patterns: Optional[List[str]] = None
) -> dict:

    inp = Path(input_file)
    if not inp.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    exclude_patterns = exclude_patterns or []
    exclude_regexes = [re.compile(p, re.I) for p in exclude_patterns]

    xls = pd.ExcelFile(inp)
    summary_rows = []

    outp = Path(output_file)
    outp.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(outp, engine="openpyxl") as writer:
        sheets_written = 0
        for sheet_name in xls.sheet_names:

            # 1️⃣ Exclure feuilles selon regex
            if any(rx.search(sheet_name) for rx in exclude_regexes):
                summary_rows.append({"sheet": sheet_name, "status": "ignored_regex", "rows": 0, "cols": 0})
                continue

            # 2️⃣ Exclure feuilles dont le nom n'est pas un pays ni une exception
            sheet_name_normalized = normalize_name(sheet_name)
            if sheet_name_normalized not in countries_normalized and sheet_name_normalized not in exceptions_normalized:
                summary_rows.append({"sheet": sheet_name, "status": "ignored_not_country", "rows": 0, "cols": 0})
                continue

            df = pd.read_excel(xls, sheet_name=sheet_name, header=None, dtype=object)
            processed_df = process_single_sheet(df, sheet_name, station_cols=station_cols)

            if processed_df is None:
                summary_rows.append({
                    "sheet": sheet_name,
                    "status": "empty_or_no_data",
                    "rows": 0,
                    "cols": 0
                })
                continue

            # Écriture de la feuille traitée
            sheet_title = str(processed_df["Pays"].iat[0])[:31]
            base_title = sheet_title
            idx = 1
            while sheet_title in writer.book.sheetnames:
                idx += 1
                sheet_title = f"{base_title}_{idx}"
            processed_df.to_excel(writer, sheet_name=sheet_title, index=False)
            sheets_written += 1

            summary_rows.append({
                "sheet": sheet_name,
                "status": "ok",
                "rows": processed_df.shape[0],
                "cols": processed_df.shape[1]
            })

        # Si aucune feuille n’a été écrite, créer une feuille vide
        if sheets_written == 0:
            pd.DataFrame([{"info": "No sheets with data"}]).to_excel(writer, sheet_name="Info", index=False)

        # Écriture du résumé à la fin
        pd.DataFrame(summary_rows).to_excel(writer, sheet_name="Résumé", index=False)

    return {"input": str(inp), "output": str(outp), "summary": summary_rows}

# ---------------------------
# Script principal
# ---------------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", "-i", default="data/inbox/Invariants - calculs_test.xlsx")
    parser.add_argument("--output", "-o", default="data/out/Extract_Station.xlsx")
    parser.add_argument("--exclude", "-e", default="", help="comma-separated regex patterns to ignore")
    parser.add_argument("--station-cols", "-c", type=int, default=5)
    args = parser.parse_args()

    exclude_patterns = [s.strip() for s in args.exclude.split(",") if s.strip()] if args.exclude else []

    result = process_all_sheets_stream(
        input_file=args.input,
        output_file=args.output,
        station_cols=args.station_cols,
        exclude_patterns=exclude_patterns
    )

    print("=== Résumé console ===")
    for r in result["summary"]:
        if r["status"] == "ok":
            print(f"{r['sheet']:40s} -> OK    : {r['rows']:4d} rows x {r['cols']:2d} cols")
        else:
            print(f"{r['sheet']:40s} -> {r['status'].upper()}")
    print(f"\nFichier de sortie: {result['output']}")
