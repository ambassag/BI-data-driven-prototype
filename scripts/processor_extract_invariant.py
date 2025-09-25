import math
import re
import unicodedata
import pandas as pd
from pathlib import Path

# ------------------------
# Liste des pays fournie par l'utilisateur
# ------------------------
PCOUNTRY = {
    "Tanzania", "Guinée Equitoriale", "Congo Brazza", "RDC", "Cameroun",
    "Zambie", "Namibie", "Afrique du Sud", "Guinée", "Côte Ivoire", "Centafrique",
    "Egypte", "Erythrée", "Ethiopie", "Maroc", "Maurice", "Tunisie", "Burkina", "Tchad"
}

# ------------------------
# Utils généraux
# ------------------------
_safe = lambda x: "" if pd.isna(x) else str(x).strip()

def _normalize_name(s: str) -> str:
    """Normalise une chaîne (lower, strip, remove accents, collapse separators)."""
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")  # remove accents
    s = re.sub(r'[\s\-_.,/\\]+', ' ', s)  # collapse separators to single space
    s = s.strip()
    return s

def _build_allowed_countries_set(user_list: set) -> set:
    """
    Construit un set normalisé de noms de pays autorisés :
      - ajoute les noms de user_list,
      - essaye d'ajouter les noms/aliases fournis par pycountry si disponible,
      - ajoute des alias manuels utiles.
    """
    allowed = set()
    # user-provided
    for n in user_list:
        allowed.add(_normalize_name(n))

    # manual aliases commonly found in sheets
    manual_aliases = [
        "rdc", "cd", "congo brazza", "congo", "congo rep", "congo republic",
        "cote d'ivoire", "cote d'ivoire", "cote divoire", "cote ivoire"
    ]
    for a in manual_aliases:
        allowed.add(_normalize_name(a))

    # augment avec pycountry si disponible
    try:
        import pycountry
        for c in pycountry.countries:
            # name
            allowed.add(_normalize_name(getattr(c, "name", "")))
            # common_name & official_name si présents
            if hasattr(c, "common_name"):
                allowed.add(_normalize_name(getattr(c, "common_name", "")))
            if hasattr(c, "official_name"):
                allowed.add(_normalize_name(getattr(c, "official_name", "")))
            # codes alpha
            if hasattr(c, "alpha_2"):
                allowed.add(_normalize_name(getattr(c, "alpha_2", "")))
            if hasattr(c, "alpha_3"):
                allowed.add(_normalize_name(getattr(c, "alpha_3", "")))
    except Exception:
        # pas de pycountry -> on garde juste la liste utilisateur + manual_aliases
        print("[WARN] pycountry non disponible — utilisation uniquement de la liste fournie + alias manuels.")

    # retirer éventuels vides
    allowed.discard("")
    return allowed

# pré-calcul du set normalisé global (par défaut)
_ALLOWED_COUNTRIES_NORM = _build_allowed_countries_set(PCOUNTRY)

def is_country_sheet_normalized(sheet_name: str, allowed_norm_set: set) -> bool:
    """Test de correspondance en utilisant un set déjà normalisé."""
    if not sheet_name:
        return False
    return _normalize_name(sheet_name) in allowed_norm_set

# ------------------------
# Fonctions d'extraction (inchangées fonctionnellement, nettoyées)
# ------------------------
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
    return combined_header.split("|", 1)[1].strip() if "|" in combined_header else combined_header.strip()

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
    if not name or name.lower().startswith("col_") or name.lower().startswith("unnamed"):
        return False
    return bool(re.search(r'[A-Za-z]', name)) and len(name.strip()) >= 3

def find_col_by_keywords(columns, keywords):
    """Retourne la première colonne parmi 'columns' contenant l'un des keywords (insensible à la casse)."""
    cols_l = [str(c).lower() for c in columns]
    for kw in keywords:
        for i, c in enumerate(cols_l):
            if kw in c:
                return columns[i]
    return None

# ------------------------
# single-sheet extractor -> retourne df_out (long) pour l'onglet
# ------------------------
def _extract_from_sheet(xls: pd.ExcelFile, sheet_name: str, station_cols: int =5, group_size: int =3):
    """Retourne (df_out_long, summary_dict) ou lève ValueError si rien d'utile."""
    df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None, dtype=object)
    if df_raw.empty:
        raise ValueError("Feuille vide")

    # strip strings and convert pure-space to NA
    df_raw = df_raw.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df_raw = df_raw.replace(r'^\s*$', pd.NA, regex=True)

    hr = detect_header(df_raw, default=4)
    headers = build_combined_headers(df_raw, hr)
    data = df_raw.iloc[hr+1:].reset_index(drop=True)

    data = data.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    data = data.replace(r'^\s*$', pd.NA, regex=True)
    if data.empty:
        raise ValueError("Aucune ligne de données après l'entête détectée")

    # station block
    station_block = data.iloc[:, :station_cols].copy().reset_index(drop=True)
    station_block = station_block.applymap(lambda x: x.strip() if isinstance(x, str) else x).replace(r'^\s*$', pd.NA, regex=True)
    station_headers = [headers[i] if i < len(headers) else f"station_col_{i+1}" for i in range(station_cols)]
    cost_idx = next((i for i, h in enumerate(station_headers) if any(k in str(h).lower() for k in ("cost","centre","center"))), 0)
    cost_series = station_block.iloc[:, cost_idx].copy().reset_index(drop=True)

    # remainder (invariants)
    remainder = data.iloc[:, station_cols:].copy().reset_index(drop=True)
    remainder = remainder.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    remainder = remainder.replace(r'^\s*$', pd.NA, regex=True)

    # cut automatique
    limit_idx, trigger_header = None, None
    for j in range(station_cols, len(headers)):
        txt = headers[j].lower()
        if any(kw in txt for kw in [
            "suivi","compensat","atg","type of farm","mesures",
            "100% des inv","nombre de site ayant","sécurité",
            "nombre de réservoir","date du dernier contrôle","type de fosse",
            "ep01","ep02","ep03","ep04"
        ]):
            limit_idx, trigger_header = j, headers[j]
            break

    if limit_idx is not None:
        remainder = data.iloc[:, station_cols:limit_idx].copy().reset_index(drop=True)
        remainder = remainder.applymap(lambda x: x.strip() if isinstance(x, str) else x).replace(r'^\s*$', pd.NA, regex=True)
        headers = headers[:limit_idx]
    else:
        valid_cols = [c for c in range(remainder.shape[1]) if remainder.iloc[:, c].notna().any()]
        if not valid_cols:
            raise ValueError("Aucune donnée d'invariant trouvée")
        last_col = max(valid_cols) + 1
        remainder = remainder.iloc[:, :last_col]
        headers = headers[: station_cols + last_col]

    remainder = remainder.applymap(lambda x: x.strip() if isinstance(x, str) else x).replace(r'^\s*$', pd.NA, regex=True)
    n_cols_remainder = remainder.shape[1]

    # lignes utiles
    mask_station_rows = station_block.notna().any(axis=1)
    mask_remainder_rows = remainder.notna().any(axis=1)
    last_row_idx = max(
        mask_station_rows[mask_station_rows].index.max() if mask_station_rows.any() else -1,
        mask_remainder_rows[mask_remainder_rows].index.max() if mask_remainder_rows.any() else -1,
    )
    if last_row_idx < 0:
        raise ValueError("Aucune ligne utile trouvée")

    remainder = remainder.iloc[: last_row_idx + 1].reset_index(drop=True)
    cost_series = cost_series.iloc[: last_row_idx + 1].reset_index(drop=True)

    # blocs invariants
    n_invariants_raw = math.ceil(n_cols_remainder / group_size)
    kept_blocks = []
    for inv_idx in range(n_invariants_raw):
        start, end = inv_idx * group_size, min((inv_idx+1)*group_size, n_cols_remainder)
        cols_slice = list(range(start, end))
        hdr_idx = station_cols + start
        raw_hdr = headers[hdr_idx] if hdr_idx < len(headers) else ""
        if "|" in raw_hdr:
            left, right = raw_hdr.split("|", 1)
            study, inv_id = right.strip(), extract_invariant_id(left.strip(), inv_idx)
        else:
            study, inv_id = simple_field_name(raw_hdr), extract_invariant_id(raw_hdr, inv_idx)
        descriptive_names = [simple_field_name(headers[station_cols+c]) if station_cols+c < len(headers) else "" for c in cols_slice]

        cond_has_data = any(remainder.iloc[:, c].notna().any() for c in cols_slice)
        cond_name_ok = (
            "|" in raw_hdr
            or bool(re.search(r'[A-Za-z]{1,4}\d{1,3}', inv_id))
            or any(looks_meaningful_name(n or "") for n in descriptive_names)
        )
        cond_not_technical = not any(
            kw in raw_hdr.lower() or kw in " ".join(descriptive_names).lower()
            for kw in ["ep01","ep02","ep03","ep04",
                       "nombre de réservoir","date du dernier contrôle","type de fosse"]
        )
        cond_keep = cond_name_ok and cond_has_data and cond_not_technical

        if cond_keep:
            kept_blocks.append({
                "inv_idx": inv_idx,
                "cols_slice": cols_slice,
                "inv_id": inv_id,
                "study": study or f"Study_{inv_idx+1:02d}",
                "descriptive_names": descriptive_names
            })

    if not kept_blocks:
        raise ValueError("Aucun bloc d'invariant significatif détecté")

    # construire DF long
    rows = []
    for row_idx in range(last_row_idx + 1):
        cost_val = cost_series.iat[row_idx] if row_idx < len(cost_series) else pd.NA
        for b in kept_blocks:
            row = {
                "Pays": sheet_name,
                "Cost Center": cost_val,
                "Invariant": b["inv_id"],
                "Study Domain": b["study"],
            }
            if b["cols_slice"]:
                try:
                    val0 = remainder.iat[row_idx, b["cols_slice"][0]]
                except IndexError:
                    val0 = pd.NA
                row["Compliance / Year"] = val0
                for idx_offset, c in enumerate(b["cols_slice"][1:], start=2):
                    hdr_index = station_cols + c
                    hdr_text = headers[hdr_index] if hdr_index < len(headers) else ""
                    field_name = simple_field_name(hdr_text) or f"Field_{b['inv_idx']+1}_{idx_offset}"
                    try:
                        row[field_name] = remainder.iat[row_idx, c]
                    except IndexError:
                        row[field_name] = pd.NA
            rows.append(row)

    df_out = pd.DataFrame(rows)
    df_out = df_out.applymap(lambda x: x.strip() if isinstance(x, str) else x).replace(r'^\s*$', pd.NA, regex=True)

    summary = {
        "sheet": sheet_name,
        "n_invariants_raw": n_invariants_raw,
        "kept_blocks": len(kept_blocks),
        "n_rows": last_row_idx+1,
        "n_cols_invariants": n_cols_remainder
    }
    return df_out, summary

# ------------------------
# Multi-sheet aggregator + écriture 2 fichiers (global)
# ------------------------
def process_workbook_all_sheets_to_two_files(
    input_file: str,
    output_prefix: str = None,
    station_cols: int = 5,
    group_size: int = 3,
    sheets: list = None,            # None => toutes les sheets
    allowed_countries: set = None,  # si None => on utilise _ALLOWED_COUNTRIES_NORM
    overwrite_output: bool = True
) -> tuple:
    """
    Parcourt toutes les sheets (ou la liste fournie), mais TRAITE uniquement les sheets dont le nom
    correspond à un pays dans 'allowed_countries' (ou dans la liste combinée PCOUNTRY+pycountry).
    Concatène les résultats en un seul DataFrame, et écrit 2 fichiers Excel (study_invariant + details)
    contenant les données de tous les onglets valides.
    Retourne (path_study_file, path_details_file)
    """
    # préparer xls et liste candidate de sheets
    xls = pd.ExcelFile(input_file)
    candidate_sheets = sheets if sheets is not None else list(xls.sheet_names)

    # construire set normalisé autorisé
    if allowed_countries is None:
        allowed_norm = _ALLOWED_COUNTRIES_NORM
    else:
        allowed_norm = _build_allowed_countries_set(allowed_countries)

    df_parts = []
    summaries = []
    for sh in candidate_sheets:
        if not is_country_sheet_normalized(sh, allowed_norm):
            print(f"[SKIP] sheet='{sh}' (nom d'onglet non présent dans la liste de pays autorisés)")
            continue
        try:
            df_sh, summ = _extract_from_sheet(xls, sh, station_cols=station_cols, group_size=group_size)
            df_parts.append(df_sh)
            summaries.append(summ)
            print(f"[OK] sheet='{sh}' traité : blocs gardés={summ['kept_blocks']}, lignes={summ['n_rows']}")
        except Exception as e:
            print(f"[SKIP] sheet='{sh}' -> {e}")

    if not df_parts:
        raise ValueError("Aucun onglet valide et traité. Rien à concaténer.")

    df_all = pd.concat(df_parts, ignore_index=True, sort=False)
    df_all = df_all.applymap(lambda x: x.strip() if isinstance(x, str) else x).replace(r'^\s*$', pd.NA, regex=True)

    # --- Fichier 1 : Study Domain, Invariant, index = Pays (global)
    df_file1 = df_all[["Study Domain", "Invariant", "Pays"]].copy()
    df_file1.set_index("Pays", inplace=True)

    # --- Fichier 2 : Pays, Status, Cost Center, Year of Compliance, Cost Estimate (K local currency)
    cols_all = list(df_all.columns)
    year_col = find_col_by_keywords(cols_all, ["year of compliance", "year", "annee", "année", "year_of_compliance"])
    cost_col = find_col_by_keywords(cols_all, ["cost estimate", "cost", "estimate", "coût", "cout", "estimation", "cost_estimate"])
    status_col_src = "Compliance / Year" if "Compliance / Year" in df_all.columns else find_col_by_keywords(cols_all, ["compliance","status","statut","etat","state"])

    df_file2 = pd.DataFrame()
    df_file2["Pays"] = df_all["Pays"]
    df_file2["Cost Center"] = df_all.get("Cost Center", pd.NA)
    df_file2["Status"] = df_all.get(status_col_src) if status_col_src else pd.NA
    df_file2["Year of Compliance"] = df_all.get(year_col) if year_col and year_col not in (status_col_src,"Cost Center") else pd.NA
    df_file2["Cost Estimate (K local currency)"] = df_all.get(cost_col) if cost_col and cost_col not in (status_col_src, year_col, "Cost Center") else pd.NA

    # écriture fichiers Excel (global)
    base = Path(output_prefix) if output_prefix else Path("data/out/invariants_all")
    out_dir = base.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    file1 = base.with_name(base.name + "_all_study_invariant.xlsx")
    file2 = base.with_name(base.name + "_all_details.xlsx")

    def _ensure_path(pth: Path):
        p = Path(pth)
        if overwrite_output or not p.exists():
            return p
        i = 1
        while True:
            p2 = p.with_name(f"{p.stem}_{i}{p.suffix}")
            if not p2.exists():
                return p2
            i += 1

    p1 = _ensure_path(file1)
    p2 = _ensure_path(file2)

    try:
        df_file1.to_excel(p1, index=True)
        df_file2.to_excel(p2, index=False)
    except Exception as e:
        print(f"[ERROR] écriture fichiers : {e}")
        raise

    # résumé console
    total_blocks = sum(s["kept_blocks"] for s in summaries)
    total_rows = sum(s["n_rows"] for s in summaries)
    print(f"[WRITE] Fichiers écrits :\n - {p1}\n - {p2}")
    print(f"[SUMMARY] sheets_processed={len(summaries)}, total_blocks_kept={total_blocks}, total_rows={total_rows}")

    return str(p1), str(p2)

# ------------------------
# Exemple d'usage
# ------------------------
if __name__ == "__main__":
    INPUT_FILE = "data/inbox/Invariants - calculs_test.xlsx"
    OUTPUT_PREFIX = "data/out/invariants_all"   # base name
    # optional: pass sheets=["Ethiopie","Ghana"] or None for all
    p1, p2 = process_workbook_all_sheets_to_two_files(
        INPUT_FILE,
        output_prefix=OUTPUT_PREFIX,
        station_cols=5,
        group_size=3,
        sheets=None,
        allowed_countries=None,   # None -> utilisera PCOUNTRY + pycountry si dispo
        overwrite_output=True
    )
    print("Generated:", p1, p2)
