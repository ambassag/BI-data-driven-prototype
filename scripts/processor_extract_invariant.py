#!/usr/bin/env python3
# process_invariants_costcenter_studydomain_filtered.py
import math
import re
import pandas as pd
from pathlib import Path

_safe = lambda x: "" if pd.isna(x) else str(x).strip()

def detect_header(df: pd.DataFrame, default: int = 4) -> int:
    keywords = [
        "cost center","centre de coût","cost centre","name","nom",
        "city","ville","segmentation","management","management method",
        "method","gestion","station","cost"
    ]
    max_check = min(12, len(df))
    for i in range(max_check):
        txt = " ".join(_safe(x).lower() for x in df.iloc[i])
        if any(k in txt for k in keywords):
            return i
    return default

def build_combined_headers(df: pd.DataFrame, hr: int) -> list:
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
    return headers

def simple_field_name(combined_header: str) -> str:
    if "|" in combined_header:
        return combined_header.split("|", 1)[1].strip()
    return combined_header.strip()

def extract_invariant_id(raw_hdr: str, fallback_idx: int) -> str:
    if not raw_hdr or not raw_hdr.strip():
        return f"Inv{fallback_idx+1:02d}"
    left = raw_hdr.split("|", 1)[0].strip() if "|" in raw_hdr else raw_hdr.strip()
    m = re.search(r'\b([A-Za-z]{1,4}\d{1,3})\b', left)
    if m:
        return m.group(1)
    toks = [t for t in re.split(r'[\s\-_/.]+', left) if t]
    if toks and re.search(r'\d', toks[-1]):
        return toks[-1]
    return f"Inv{fallback_idx+1:02d}"

def looks_meaningful_name(name: str) -> bool:
    """Retourne True si name ressemble à un vrai nom (pas 'col_123' ou vide)."""
    if not name or name.lower().startswith("col_") or name.lower().startswith("unnamed"):
        return False
    # si contient des lettres et au moins 3 caractères, on considère ça utile
    return bool(re.search(r'[A-Za-z]', name)) and len(name.strip()) >= 3

def process_invariants_costcenter_studydomain_filtered(
    input_file: str,
    sheet_name: str,
    station_cols: int = 5,
    group_size: int = 3,
    output_file: str = None,
    overwrite_output: bool = True
) -> pd.DataFrame:
    xls = pd.ExcelFile(input_file)
    if sheet_name not in xls.sheet_names:
        raise ValueError(f"Sheet '{sheet_name}' non trouvée dans {input_file}")

    df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None, dtype=object)
    if df_raw.empty:
        raise ValueError("Feuille vide")

    hr = detect_header(df_raw, default=4)
    headers = build_combined_headers(df_raw, hr)
    data = df_raw.iloc[hr+1:].reset_index(drop=True)
    if data.empty:
        raise ValueError("Aucune ligne de données après l'entête détectée")

    # station block -> cost center detection
    station_block = data.iloc[:, :station_cols].copy().reset_index(drop=True)
    station_headers = [headers[i] if i < len(headers) else f"station_col_{i+1}" for i in range(station_cols)]
    cost_idx = None
    for i, hdr in enumerate(station_headers):
        if any(k in hdr.lower() for k in ("cost", "centre", "center")):
            cost_idx = i
            break
    if cost_idx is None:
        cost_idx = 0
    cost_series = station_block.iloc[:, cost_idx].copy().reset_index(drop=True)

    # remainder -> couper colonnes vides à droite
    remainder = data.iloc[:, station_cols:].copy().reset_index(drop=True)
    if remainder.shape[1] == 0:
        raise ValueError("Aucune colonne après les colonnes station (pas d'invariants trouvés)")
    valid_cols = [c for c in range(remainder.shape[1]) if remainder.iloc[:, c].notna().any()]
    if not valid_cols:
        raise ValueError("Aucune donnée d'invariant trouvée")
    last_col = max(valid_cols) + 1
    remainder = remainder.iloc[:, :last_col]
    n_cols_remainder = remainder.shape[1]

    # déterminer lignes utiles
    mask_station_rows = station_block.notna().any(axis=1)
    last_station_row = mask_station_rows[mask_station_rows].index.max() if mask_station_rows.any() else -1
    mask_remainder_rows = remainder.notna().any(axis=1)
    last_remainder_row = mask_remainder_rows[mask_remainder_rows].index.max() if mask_remainder_rows.any() else -1
    last_row_idx = max(last_station_row, last_remainder_row)
    if last_row_idx < 0:
        raise ValueError("Aucune ligne utile trouvée")
    remainder = remainder.iloc[: last_row_idx + 1].reset_index(drop=True)
    cost_series = cost_series.iloc[: last_row_idx + 1].reset_index(drop=True)

    # pré-calcul et FILTRAGE des blocs : on ne garde QUE les blocs "significatifs"
    n_invariants_raw = math.ceil(n_cols_remainder / group_size)
    kept_blocks = []  # list of dicts {inv_idx, cols_slice, inv_id, study}
    for inv_idx in range(n_invariants_raw):
        start = inv_idx * group_size
        end = min(start + group_size, n_cols_remainder)
        cols_slice = list(range(start, end))

        hdr_idx = station_cols + start
        raw_hdr = headers[hdr_idx] if hdr_idx < len(headers) else ""
        # étude + id
        if "|" in raw_hdr:
            left, right = raw_hdr.split("|", 1)
            study = right.strip()
            inv_id = extract_invariant_id(left.strip(), inv_idx)
        else:
            # fallback : take header text
            study = simple_field_name(raw_hdr)
            inv_id = extract_invariant_id(raw_hdr, inv_idx)

        # noms descriptifs (pour les 3 colonnes du bloc) : réutiliser headers si possibles
        descriptive_names = []
        for c in cols_slice:
            hdr_index = station_cols + c
            hdr_text = headers[hdr_index] if hdr_index < len(headers) else ""
            descriptive_names.append(simple_field_name(hdr_text))

        # condition de conservation : au moins une des conditions suivantes
        cond_has_pipe = "|" in raw_hdr
        cond_has_real_id = bool(re.search(r'[A-Za-z]{1,4}\d{1,3}', inv_id))
        cond_descriptive_name = any(looks_meaningful_name(n or "") for n in descriptive_names)
        cond_block_has_data = any(remainder.iloc[:, c].notna().any() for c in cols_slice)

        if (cond_has_pipe or cond_has_real_id or cond_descriptive_name) and cond_block_has_data:
            kept_blocks.append({
                "inv_idx": inv_idx,
                "cols_slice": cols_slice,
                "inv_id": inv_id,
                "study": study or f"Study_{inv_idx+1:02d}",
                "descriptive_names": descriptive_names
            })
        else:
            # debug print facultatif
            # print(f"[SKIP] bloc {inv_idx+1} colonnes {cols_slice} -> raw_hdr='{raw_hdr}'")
            pass

    if not kept_blocks:
        raise ValueError("Aucun bloc d'invariant significatif détecté après filtrage")

    print(f"[INFO] feuille='{sheet_name}'  blocs_detectes_brut={n_invariants_raw}  blocs_gardes={len(kept_blocks)}  lignes_utiles={last_row_idx+1}")

    # construire DataFrame long à partir des blocs gardés
    rows = []
    for row_idx in range(last_row_idx + 1):
        cost_val = cost_series.iat[row_idx] if row_idx < len(cost_series) else ""
        for b in kept_blocks:
            cols_slice = b["cols_slice"]
            inv_label = b["inv_id"]
            study_label = b["study"]

            row = {"Pays": sheet_name, "Cost Center": cost_val, "Invariant": inv_label, "Study Domain": study_label}
            # Compliance / Year = valeur sous 1ère colonne du bloc (si presente)
            if len(cols_slice) >= 1:
                try:
                    row["Compliance / Year"] = remainder.iat[row_idx, cols_slice[0]]
                except IndexError:
                    row["Compliance / Year"] = None
            # autres champs
            for idx_offset, c in enumerate(cols_slice[1:], start=2):
                hdr_index = station_cols + c
                hdr_text = headers[hdr_index] if hdr_index < len(headers) else ""
                field_name = simple_field_name(hdr_text) or f"Field_{b['inv_idx']+1}_{idx_offset}"
                try:
                    row[field_name] = remainder.iat[row_idx, c]
                except IndexError:
                    row[field_name] = None
            rows.append(row)

    df_out = pd.DataFrame(rows)

    # réordonner colonnes
    cols_order = ["Pays", "Cost Center", "Invariant", "Study Domain", "Compliance / Year"]
    other_cols = [c for c in df_out.columns if c not in cols_order]
    final_cols = cols_order + other_cols
    df_out = df_out[[c for c in final_cols if c in df_out.columns]]

    # écriture optionnelle
    if output_file:
        outp = Path(output_file)
        outp.parent.mkdir(parents=True, exist_ok=True)
        mode = "w" if overwrite_output else "a"
        with pd.ExcelWriter(outp, engine="openpyxl", mode=mode) as writer:
            sheetname = (sheet_name + "_invariants_cc")[:31]
            df_out.to_excel(writer, sheet_name=sheetname, index=False)
        print(f"[WRITE] écrit dans : {outp} / onglet: {sheetname} (mode={mode})")

    return df_out

# usage example
if __name__ == "__main__":
    INPUT_FILE = "data/inbox/Invariants - calculs_test.xlsx"
    SHEET_NAME = "Ethiopie"
    OUTPUT_FILE = "data/out/Ethiopie_invariants_cc_study_filtered.xlsx"

    df_result = process_invariants_costcenter_studydomain_filtered(
        INPUT_FILE, SHEET_NAME, station_cols=5, group_size=3, output_file=OUTPUT_FILE, overwrite_output=True
    )
    print(df_result.head(30))
