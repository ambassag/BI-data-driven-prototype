# scripts/flatten_reader.py
from pathlib import Path
import re
import pandas as pd
from typing import List, Optional

def read_all_sheets_flatten(
    path: str,
    header_row: int = 0,
    exclude_sheet_patterns: Optional[List[str]] = None,
    keep_source_cols_first: bool = True
) -> pd.DataFrame:
    """
    Lit toutes les feuilles et renvoie un DataFrame plat.
    - dtype=str + keep_default_na=False pour garder exactement les valeurs (100 reste '100').
    - exclude_sheet_patterns : liste de regex à ignorer (ex: ['AFR'])
    """
    exclude_sheet_patterns = exclude_sheet_patterns or [r'AFR']
    path = Path(path)
    xls = pd.read_excel(path, sheet_name=None, header=header_row, dtype=str, keep_default_na=False, engine="openpyxl")
    rows = []
    for sheet_name, df in xls.items():
        if any(re.search(pat, sheet_name, flags=re.IGNORECASE) for pat in exclude_sheet_patterns):
            continue
        df = df.dropna(axis=1, how='all')
        if df.empty:
            continue
        df = df.astype(object)  # assure object for mixed types
        df['sheet_name'] = sheet_name
        df['source_file'] = path.name
        rows.append(df)
    if not rows:
        return pd.DataFrame()
    combined = pd.concat(rows, ignore_index=True, sort=False).fillna('')
    if keep_source_cols_first:
        cols = list(combined.columns)
        for c in ['source_file', 'sheet_name']:
            if c in cols:
                cols.remove(c)
        combined = combined[['source_file', 'sheet_name'] + cols]
    return combined
