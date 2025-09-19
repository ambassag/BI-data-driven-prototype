# scripts/run_reader.py
from pathlib import Path
from scripts.processor import process_excel
import pandas as pd

if __name__ == "__main__":
    base_dir = Path(__file__).resolve().parent.parent
    inbox = base_dir / "data" / "inbox"
    outbox = base_dir / "data" / "outbox"
    outbox.mkdir(parents=True, exist_ok=True)

    files = sorted([p for p in inbox.glob("*.xlsx")] + [p for p in inbox.glob("*.xls")])
    if not files:
        print(f"⚠️ Aucun fichier Excel trouvé dans {inbox}")
    total_files = 0
    total_rows = 0
    for path in files:
        print(f"\n=== Processing {path.name} ===")
        df_flat = process_excel(str(path), header_row=0, flatten=True)
        if isinstance(df_flat, pd.DataFrame) and not df_flat.empty:
            out_csv = outbox / (path.stem + "_flatten.csv")
            df_flat.to_csv(out_csv, index=False, encoding="utf-8-sig")
            print(f"Saved flattened CSV -> {out_csv}")
            total_files += 1
            total_rows += len(df_flat)
        else:
            print(f"No data produced for {path.name}")
    print(f"\n=== SUMMARY === Files processed: {total_files} | Total rows: {total_rows}")
