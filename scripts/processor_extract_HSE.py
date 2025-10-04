import pandas as pd
from pathlib import Path

# ---------------------------
# Fonctions utilitaires
# ---------------------------
def detect_header(df: pd.DataFrame, default: int = 0) -> int:
    """Détecter la ligne d'entête probable"""
    keywords = ["hse invariant", "question", "inspection", "description", "commentaire", "action"]
    for i in range(min(12, len(df))):  # on ne regarde que les 12 premières lignes
        txt = " ".join(str(x).lower() for x in df.iloc[i].fillna(""))
        if any(k in txt for k in keywords):
            return i
    return default

def make_unique_cols(df: pd.DataFrame) -> pd.DataFrame:
    """S'assurer que les colonnes sont uniques"""
    cols = pd.Series(df.columns.astype(str))
    for dup in cols[cols.duplicated()].unique():
        dups_idx = cols[cols == dup].index.tolist()
        for i, idx in enumerate(dups_idx[1:], start=1):
            cols[idx] = f"{dup}_{i}"
    df.columns = cols
    return df

def process_single_sheet(df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    """Nettoyer et structurer une feuille avec détection de l'entête"""
    if df.empty:
        return pd.DataFrame()

    hr = detect_header(df, default=0)
    if hr >= len(df):
        return pd.DataFrame()

    # ✅ On prend la ligne détectée comme entête
    hdr = df.iloc[hr].fillna("").astype(str)

    # ✅ Si l’entête est à la 1ère ligne → on garde les colonnes telles quelles
    if hr == 0:
        data = df.iloc[1:].reset_index(drop=True)
    else:
        data = df.iloc[hr+1:].reset_index(drop=True)

    data.columns = hdr
    data = make_unique_cols(data)
    return data

def process_hse_file(input_file: str, output_file: str) -> dict:
    inp = Path(input_file)
    if not inp.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    xls = pd.ExcelFile(inp)
    summary_rows = []
    outp = Path(output_file)
    outp.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(outp, engine="openpyxl") as writer:
        for sheet_name in ["HSE Invariants", "Questions", "Inspections"]:
            if sheet_name not in xls.sheet_names:
                summary_rows.append({"sheet": sheet_name, "status": "missing"})
                continue

            df = pd.read_excel(xls, sheet_name=sheet_name, header=None, dtype=object)
            processed = process_single_sheet(df, sheet_name)

            if processed.empty:
                summary_rows.append({"sheet": sheet_name, "status": "empty"})
                continue

            processed.to_excel(writer, sheet_name=sheet_name[:31], index=False)
            summary_rows.append({
                "sheet": sheet_name,
                "status": "ok",
                "rows": processed.shape[0],
                "cols": processed.shape[1]
            })

    return {"input": str(inp), "output": str(outp), "summary": summary_rows}


# ---------------------------
# Script principal
# ---------------------------
if __name__ == "__main__":
    input_file = "data/inbox/Extraction_test.xlsx"
    output_file = "data/out/HSE_extract.xlsx"

    result = process_hse_file(input_file, output_file)

    print("=== Résumé console ===")
    for r in result["summary"]:
        print(f"{r['sheet']:15s} -> {r['status']}")
    print(f"\nFichier de sortie: {result['output']}")
