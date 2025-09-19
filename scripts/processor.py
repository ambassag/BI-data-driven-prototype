# processor_single_sheet.py
import pandas as pd

from pathlib import Path

_safe = lambda x: "" if pd.isna(x) else str(x).strip()

def detect_header(df, default=4):
    for i in range(min(12, len(df))):
        txt = " ".join(_safe(x).lower() for x in df.iloc[i])
        if any(k in txt for k in ["cost center", "segmentation", "city", "management"]):
            return i
    return default

def make_unique_cols(df: pd.DataFrame) -> pd.DataFrame:
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique():
        dups_idx = cols[cols == dup].index.tolist()
        for i, idx in enumerate(dups_idx[1:], start=1):
            cols[idx] = f"{dup}_{i}"
    df.columns = cols
    return df

def process_single_sheet(input_file: str, sheet_name: str, station_cols: int = 5, output_file: str = None):
    xl = pd.ExcelFile(input_file)
    df = pd.read_excel(xl, sheet_name=sheet_name, header=None, dtype=object)
    if df.empty:
        print(f"[WARN] Sheet '{sheet_name}' is empty.")
        return None

    hr = detect_header(df, default=4)
    hdr_top = df.iloc[hr-1].fillna("").map(_safe) if hr-1 >= 0 else None
    hdr_bot = df.iloc[hr].fillna("").map(_safe)

    headers = [
        (hdr_top.iat[i] + " | " + hdr_bot.iat[i]).strip()
        if hdr_top is not None and hdr_top.iat[i] else
        (hdr_bot.iat[i] or f"col_{i}")
        for i in range(len(df.columns))
    ]

    data = df.iloc[hr+1:].reset_index(drop=True)
    if data.empty:
        print(f"[WARN] No data rows found after header in sheet '{sheet_name}'.")
        return None

    pays = data.iloc[:, :station_cols].copy()
    pays.columns = headers[:station_cols]
    pays["Pays"] = sheet_name
    pays = make_unique_cols(pays)

    out_path = output_file or f"{sheet_name}_extract.xlsx"
    outp = Path(out_path)
    outp.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(outp, engine="openpyxl") as writer:
        pays.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"[OK] Extracted {len(pays)} rows x {len(pays.columns)} cols from sheet '{sheet_name}'.")
    print(pays.head(20).to_string(index=False))
    return str(outp)

if __name__ == "__main__":
    # Exemple d'utilisation (remplace input_path par ton fichier réel)
    input_path = "data/inbox/Invariants - calculs_test.xlsx"
    sheet = "Ethiopie"
    output_path = "data/out/Ethiopie_extract.xlsx"
    process_single_sheet(input_path, sheet_name=sheet, station_cols=5, output_file=output_path)
