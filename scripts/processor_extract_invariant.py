import math
import re
import unicodedata
import pandas as pd
from pathlib import Path
from datetime import datetime

# ------------------------
# Liste des pays fournie par l'utilisateur
# ------------------------
PCOUNTRY = {
    "Tanzania", "Guinée Equitoriale", "Congo Brazza", "RDC", "Cameroun",
    "Zambie", "Namibie", "Afrique du Sud", "Guinée", "Côte Ivoire",
    "Egypte", "Erythrée", "Ethiopie", "Maroc", "Maurice", "Tunisie", "Burkina", "Tchad", "Centafrique"
}

# ------------------------
# Utils généraux
# ------------------------
_safe = lambda x: "" if pd.isna(x) else str(x).strip()


def _normalize_name(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")  # remove accents
    s = re.sub(r'[\s\-_.,/\\]+', ' ', s)
    return s.strip()


def _build_allowed_countries_set(user_list: set) -> set:
    allowed = set()
    for n in user_list:
        allowed.add(_normalize_name(n))
    manual_aliases = ["rdc", "cd", "congo brazza", "congo", "congo rep", "congo republic",
                      "cote d'ivoire", "cote divoire", "cote ivoire", "centafrique"]
    for a in manual_aliases:
        allowed.add(_normalize_name(a))
    try:
        import pycountry
        for c in pycountry.countries:
            allowed.add(_normalize_name(getattr(c, "name", "")))
            if hasattr(c, "common_name"):
                allowed.add(_normalize_name(getattr(c, "common_name", "")))
            if hasattr(c, "official_name"):
                allowed.add(_normalize_name(getattr(c, "official_name", "")))
            if hasattr(c, "alpha_2"):
                allowed.add(_normalize_name(getattr(c, "alpha_2", "")))
            if hasattr(c, "alpha_3"):
                allowed.add(_normalize_name(getattr(c, "alpha_3", "")))
    except Exception:
        print("[WARN] pycountry non disponible — utilisation uniquement de la liste fournie + alias manuels.")
    allowed.discard("")
    return allowed


_ALLOWED_COUNTRIES_NORM = _build_allowed_countries_set(PCOUNTRY)

# ------------------------
# Dictionnaire des codes pays
# ------------------------
COUNTRY_CODES = {
    "burkina": "BF",
    "south africa": "ZA", "afrique du sud": "ZA",
    "mauritius": "MU", "maurice": "MU",
    "cameroon": "CM", "cameroun": "CM",
    "cote divoire": "CI", "cote d'ivoire": "CI", "ivory coast": "CI", "cote ivoire": "CI",
    "congo": "CG", "congo brazzaville": "CG", "congo brazza": "CG",
    "rdc": "CD", "democratic republic of congo": "CD",
    "zambia": "ZM", "zambie": "ZM",
    "centafrique": "CE", "central african republic": "CE",
    "tunisia": "TN", "tunisie": "TN",
    "morocco": "MA", "maroc": "MA",
    "ethiopia": "ET", "ethiopie": "ET",
    "érythrée": "ER","erythree": "ER",
    "tanzania": "TZ", "tanzanie": "TZ",
    "mozambique": "MZ",
    "sénégal": "SN", "senegal": "SN",
    "guinea": "GN", "guinée": "GN",
    "equatorial guinea": "GQ", "guinée équatoriale": "GQ","guinée equitoriale": "GQ",
    "egypt": "EG", "egypte": "EG",
    "namibia": "NA", "namibie": "NA",
    "malawi": "MW",
    "ghana": "GH",
    "uganda": "UG",
    "zimbabwe": "ZW",
    "madagascar": "MG",
    "botswana": "BW",
    "chad": "TD", "tchad": "TD",
    "mali": "ML",
    "angola": "AO",
    "reunion": "RE", "réunion": "RE",
    "mayotte": "YT",
    "nigeria": "NG", "nigéria": "NG",
}

COUNTRY_NAME_TO_CODE = {_normalize_name(name): code for name, code in COUNTRY_CODES.items()}


# ------------------------
# Fonctions d'extraction
# ------------------------
def detect_header(df: pd.DataFrame, default: int = 4) -> int:
    keywords = ["cost center", "centre de coût", "cost centre", "name", "nom",
                "city", "ville", "segmentation", "management", "management method",
                "method", "gestion", "station", "cost", "methode de gestion"]
    max_check = min(12, len(df))
    for i in range(max_check):
        txt = " ".join(_safe(x).lower() for x in df.iloc[i])
        if any(k in txt for k in keywords):
            return i
    return default


def build_combined_headers(df: pd.DataFrame, hr: int) -> list:
    hdr_top = df.iloc[hr - 1].fillna("").map(_safe) if hr > 0 else None
    hdr_bot = df.iloc[hr].fillna("").map(_safe)
    headers = []
    for i in range(len(df.columns)):
        top_val = hdr_top.iat[i] if hdr_top is not None and i < len(hdr_top) else ""
        bot_val = hdr_bot.iat[i] if i < len(hdr_bot) else f"col_{i}"
        headers.append(f"{top_val} | {bot_val}".strip() if top_val else bot_val)
    return headers


def simple_field_name(combined_header: str) -> str:
    return combined_header.split("|", 1)[1].strip() if "|" in combined_header else combined_header.strip()


def extract_invariant_id(raw_hdr: str, fallback_idx: int) -> str:
    if not raw_hdr or not raw_hdr.strip():
        return f"Inv{fallback_idx + 1:02d}"
    left = raw_hdr.split("|", 1)[0].strip() if "|" in raw_hdr else raw_hdr.strip()
    m = re.search(r'\b([A-Za-z]{1,4}\d{1,3})\b', left)
    if m:
        return m.group(1)
    toks = [t for t in re.split(r'[\s\-_/.]+', left) if t]
    if toks and re.search(r'\d', toks[-1]):
        return toks[-1]
    return f"Inv{fallback_idx + 1:02d}"


def looks_meaningful_name(name: str) -> bool:
    if not name or name.lower().startswith("col_") or name.lower().startswith("unnamed"):
        return False
    return bool(re.search(r'[A-Za-z]', name)) and len(name.strip()) >= 3


def find_col_by_keywords(columns, keywords):
    cols_l = [str(c).lower() for c in columns]
    for kw in keywords:
        for i, c in enumerate(cols_l):
            if kw in c:
                return columns[i]
    return None


def is_country_sheet_normalized(sheet_name: str, allowed_norm_set: set) -> bool:
    if not sheet_name:
        return False
    return _normalize_name(sheet_name) in allowed_norm_set


# ------------------------
# Extraction single-sheet avec code pays
# ------------------------
def _extract_from_sheet(xls: pd.ExcelFile, sheet_name: str, station_cols: int = 5, group_size: int = 3):
    df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None, dtype=object)
    if df_raw.empty:
        raise ValueError("Feuille vide")
    df_raw = df_raw.applymap(lambda x: x.strip() if isinstance(x, str) else x).replace(r'^\s*$', pd.NA, regex=True)
    hr = detect_header(df_raw, default=4)
    headers = build_combined_headers(df_raw, hr)
    data = df_raw.iloc[hr + 1:].reset_index(drop=True)
    data = data.applymap(lambda x: x.strip() if isinstance(x, str) else x).replace(r'^\s*$', pd.NA, regex=True)
    if data.empty:
        raise ValueError("Aucune ligne de données après l'entête détectée")

    station_block = data.iloc[:, :station_cols].copy().reset_index(drop=True)
    station_block = station_block.applymap(lambda x: x.strip() if isinstance(x, str) else x).replace(r'^\s*$', pd.NA,
                                                                                                     regex=True)
    station_headers = [headers[i] if i < len(headers) else f"station_col_{i + 1}" for i in range(station_cols)]
    cost_idx = next(
        (i for i, h in enumerate(station_headers) if any(k in str(h).lower() for k in ("cost", "centre", "center"))), 0)
    cost_series = station_block.iloc[:, cost_idx].copy().reset_index(drop=True)

    remainder = data.iloc[:, station_cols:].copy().reset_index(drop=True)
    remainder = remainder.applymap(lambda x: x.strip() if isinstance(x, str) else x).replace(r'^\s*$', pd.NA,
                                                                                             regex=True)

    limit_idx = None
    for j in range(station_cols, len(headers)):
        txt = headers[j].lower()
        if any(kw in txt for kw in ["suivi", "compensat", "atg", "type of farm", "mesures",
                                    "100% des inv", "nombre de site ayant", "sécurité",
                                    "nombre de réservoir", "date du dernier contrôle", "type de fosse",
                                    "ep01", "ep02", "ep03", "ep04"]):
            limit_idx = j
            break
    if limit_idx is not None:
        remainder = remainder.iloc[:, :limit_idx - station_cols].copy()
        headers = headers[:limit_idx]

    n_cols_remainder = remainder.shape[1]
    n_invariants_raw = math.ceil(n_cols_remainder / group_size)
    kept_blocks = []

    for inv_idx in range(n_invariants_raw):
        start, end = inv_idx * group_size, min((inv_idx + 1) * group_size, n_cols_remainder)
        cols_slice = list(range(start, end))
        hdr_idx = station_cols + start
        raw_hdr = headers[hdr_idx] if hdr_idx < len(headers) else ""
        study, inv_id = (raw_hdr.split("|", 1)[1].strip(),
                         extract_invariant_id(raw_hdr.split("|", 1)[0].strip(), inv_idx)) if "|" in raw_hdr else (
            simple_field_name(raw_hdr), extract_invariant_id(raw_hdr, inv_idx))
        descriptive_names = [simple_field_name(headers[station_cols + c]) if station_cols + c < len(headers) else "" for
                             c in cols_slice]

        cond_has_data = any(remainder.iloc[:, c].notna().any() for c in cols_slice)
        cond_name_ok = "|" in raw_hdr or bool(re.search(r'[A-Za-z]{1,4}\d{1,3}', inv_id)) or any(
            looks_meaningful_name(n or "") for n in descriptive_names)
        cond_not_technical = not any(kw in raw_hdr.lower() or kw in " ".join(descriptive_names).lower() for kw in
                                     ["ep01", "ep02", "ep03", "ep04", "nombre de réservoir", "date du dernier contrôle",
                                      "type de fosse"])
        if cond_has_data and cond_name_ok and cond_not_technical:
            kept_blocks.append({"inv_idx": inv_idx, "cols_slice": cols_slice, "inv_id": inv_id,
                                "study": study or f"Study_{inv_idx + 1:02d}", "descriptive_names": descriptive_names})

    if not kept_blocks:
        raise ValueError("Aucun bloc d'invariant significatif détecté")

    rows = []
    norm_name = _normalize_name(sheet_name)
    country_code = COUNTRY_NAME_TO_CODE.get(norm_name, sheet_name)

    last_row_idx = max(station_block.notna().any(axis=1).idxmax(), remainder.notna().any(axis=1).idxmax())
    for row_idx in range(last_row_idx + 1):
        cost_val = cost_series.iat[row_idx] if row_idx < len(cost_series) else pd.NA
        for b in kept_blocks:
            row = {
                "Country code": country_code,
                "Cost Center": cost_val,
                "Invariant": b["inv_id"],
                "Study Domain": b["study"],
            }
            if b["cols_slice"]:
                val0 = remainder.iat[row_idx, b["cols_slice"][0]] if 0 < remainder.shape[1] else pd.NA
                row["Compliance / Year"] = val0
                for idx_offset, c in enumerate(b["cols_slice"][1:], start=2):
                    hdr_index = station_cols + c
                    hdr_text = headers[hdr_index] if hdr_index < len(headers) else ""
                    field_name = simple_field_name(hdr_text) or f"Field_{b['inv_idx'] + 1}_{idx_offset}"
                    row[field_name] = remainder.iat[row_idx, c] if c < remainder.shape[1] else pd.NA
            rows.append(row)

    df_out = pd.DataFrame(rows)
    df_out = df_out.applymap(lambda x: x.strip() if isinstance(x, str) else x).replace(r'^\s*$', pd.NA, regex=True)
    summary = {"sheet": sheet_name, "n_invariants_raw": n_invariants_raw, "kept_blocks": len(kept_blocks),
               "n_rows": last_row_idx + 1, "n_cols_invariants": n_cols_remainder}
    return df_out, summary


# ------------------------
# Traitement multi-sheet et écriture fichiers
# ------------------------
def process_workbook_to_two_files(input_file: str, station_cols: int = 5, group_size: int = 3):
    date_suffix = datetime.now().strftime("%Y%m%d")
    output_base = Path("data/out")
    output_base.mkdir(parents=True, exist_ok=True)

    xls = pd.ExcelFile(input_file)
    candidate_sheets = list(xls.sheet_names)
    df_parts, summaries = [], []

    for sh in candidate_sheets:
        if not is_country_sheet_normalized(sh, _ALLOWED_COUNTRIES_NORM):
            continue
        try:
            df_sh, summ = _extract_from_sheet(xls, sh, station_cols=station_cols, group_size=group_size)
            df_parts.append(df_sh)
            summaries.append(summ)
        except Exception:
            continue

    if not df_parts:
        raise ValueError("Aucun onglet valide traité")

    df_all = pd.concat(df_parts, ignore_index=True, sort=False).applymap(
        lambda x: x.strip() if isinstance(x, str) else x).replace(r'^\s*$', pd.NA, regex=True)

    # Fichier study
    df_file1 = df_all[["Study Domain", "Invariant", "Country code","Cost Center"]].copy().set_index("Country code")
    # Fichier détails
    cols_all = list(df_all.columns)
    year_col = find_col_by_keywords(cols_all, ["year of compliance", "year", "annee", "année", "year_of_compliance"])
    cost_col = find_col_by_keywords(cols_all, ["cost estimate", "cost", "estimate", "coût", "cout", "estimation", "cost_estimate"])
    status_col_src = "Compliance / Year" if "Compliance / Year" in df_all.columns else find_col_by_keywords(cols_all, ["compliance", "status", "statut", "etat", "state"])

    df_file2 = pd.DataFrame()
    df_file2["Country code"] = df_all["Country code"]
    df_file2["Cost Center"] = df_all.get("Cost Center", pd.NA)
    df_file2["Status"] = df_all.get(status_col_src) if status_col_src else pd.NA
    df_file2["Year of Compliance"] = df_all.get(year_col) if year_col and year_col not in (status_col_src, "Cost Center") else pd.NA
    df_file2["Cost Estimate (K local currency)"] = df_all.get(cost_col) if cost_col and cost_col not in (status_col_src, year_col, "Cost Center") else pd.NA

    # noms de fichiers avec date
    file1 = output_base / f"Invariants_study_{date_suffix}.xlsx"
    file2 = output_base / f"Invariants_details_{date_suffix}.xlsx"

    df_file1.to_excel(file1, index=True)
    df_file2.to_excel(file2, index=False)

    return str(file1), str(file2)


def find_invariant_file(inbox_dir="data/inbox"):
    inbox = Path(inbox_dir)
    candidates = [f for f in inbox.glob("*.xlsx") if "invariant" in f.name.lower()]
    if not candidates:
        raise FileNotFoundError("Aucun fichier contenant 'Invariant' ou 'Invariants' trouvé dans data/inbox")
    # si plusieurs fichiers, prend le plus récent
    candidates.sort(key=lambda f: f.stat().st_mtime, reverse=True)
    return str(candidates[0])


if __name__ == "__main__":
    input_file = find_invariant_file()
    p1, p2 = process_workbook_to_two_files(input_file)
    print("Fichiers générés :")
    print(" -", p1)
    print(" -", p2)