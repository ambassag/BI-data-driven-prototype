import pandas as pd
from pathlib import Path
import unicodedata
import re
from rapidfuzz import fuzz, process
import numpy as np
import shutil
from datetime import datetime
from typing import Optional

# ---------- Config ----------
CDIST_ROW_LIMIT = 5000
CDIST_CHOICES_LIMIT = 1000
# ----------------------------

# === Normalisation des chaînes ===
def normalize(s: Optional[str]) -> str:
    if pd.isna(s) or s is None:
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return s.strip()

# === Normalisation des codes (cost center / station code) ===
def normalize_code(s: Optional[str], keep_digits: bool = True, pad_width: int = 0) -> str:
    """
    Garde les chiffres (si keep_digits=True), supprime espaces/lettres, puis left-pad si pad_width>0.
    Ex: " 001-23 " -> "00123" -> pad à pad_width si demandé.
    """
    if pd.isna(s) or s is None:
        return ""
    raw = str(s).strip()
    if keep_digits:
        cleaned = "".join(ch for ch in raw if ch.isdigit())
    else:
        # si on veut garder alnum, on garde tout alphanumérique
        cleaned = "".join(ch for ch in raw if ch.isalnum())
    if not cleaned:
        return ""
    if pad_width and len(cleaned) < pad_width:
        cleaned = cleaned.zfill(pad_width)
    return cleaned

# === Vérifie si le fichier doit être traité ===
def should_process(df: pd.DataFrame) -> bool:
    cols = [c.lower() for c in df.columns]
    return ("station name" in cols and "cost center" in cols) or \
           (("station name" in cols or "cost center" in cols) and "country code" in cols)

# === Mise à jour d'un fichier avec correspondances batch ===
def update_file_batch(ref_data: pd.DataFrame, file_path: Path, updated_dir: Path, backup_dir: Path,
                      log_root: Path, score_threshold=50, code_pad_width: int = 0):

    # lecture
    df = pd.read_excel(file_path, dtype=str)
    df.columns = [c.strip().lower() for c in df.columns]
    ref = ref_data.copy()
    ref.columns = [c.strip().lower() for c in ref.columns]

    if not should_process(df):
        print(f"⚠ Ignoré (colonnes manquantes) : {file_path.name}")
        return

    station_col = "station name" if "station name" in df.columns else None
    cost_col = "cost center" if "cost center" in df.columns else None
    country_col = "country code" if "country code" in df.columns else None

    # Normaliser noms
    if station_col:
        df["norm_name"] = df[station_col].map(normalize)
    if "norm_name" not in ref.columns and "station name" in ref.columns:
        ref["norm_name"] = ref["station name"].map(normalize)

    # Normaliser codes (cost/station codes) pour comparaison stricte
    if cost_col:
        df["norm_cost"] = df[cost_col].map(lambda x: normalize_code(x, keep_digits=True, pad_width=code_pad_width))
    else:
        df["norm_cost"] = ""
    if "station code" in ref.columns:
        ref["norm_station_code"] = ref["station code"].map(lambda x: normalize_code(x, keep_digits=True, pad_width=code_pad_width))
    else:
        ref["norm_station_code"] = ""

    # Normaliser country code simple (strip/upper) pour comparer
    if country_col:
        df["norm_country"] = df[country_col].fillna("").astype(str).map(lambda s: s.strip().upper())
    else:
        df["norm_country"] = ""
    if "country code" in ref.columns:
        ref["norm_country"] = ref["country code"].fillna("").astype(str).map(lambda s: s.strip().upper())
    else:
        ref["norm_country"] = ""

    # Préparer choices fuzzy (norm_name unique)
    choices = []
    choice_to_ref_idx = {}
    if station_col and "norm_name" in ref.columns:
        for i, nm in enumerate(ref["norm_name"].astype(str).tolist()):
            if nm and nm not in choice_to_ref_idx:
                choice_to_ref_idx[nm] = i
                choices.append(nm)

    # map normalized name -> first matching ref row
    norm_to_ref_row = {}
    if "norm_name" in ref.columns:
        for _, r in ref.iterrows():
            key = str(r.get("norm_name", "")).strip()
            if key and key not in norm_to_ref_row:
                norm_to_ref_row[key] = r

    updated_count = 0
    ignored_count = 0
    total = len(df)
    log = []

    # Choix entre cdist (batch) ou extractOne (par ligne) selon taille
    use_cdist = True
    if total > CDIST_ROW_LIMIT or len(choices) > CDIST_CHOICES_LIMIT:
        use_cdist = False

    if station_col and choices and use_cdist:
        # Batch cdist (rapide mais mémoire dépendante)
        query_list = df["norm_name"].astype(str).tolist()
        scores = process.cdist(query_list, choices, scorer=fuzz.token_sort_ratio, score_cutoff=score_threshold)
        scores = np.asarray(scores, dtype=float)
        best_scores = scores.max(axis=1)
        best_match_pos = scores.argmax(axis=1)
    else:
        # Prepare placeholders; we'll call extractOne per row when needed
        best_scores = np.full(len(df), -1.0)
        best_match_pos = np.full(len(df), -1, dtype=int)

    # Mapping df col -> ref col
    ref_map = {}
    if station_col:
        ref_map[station_col] = "station name"
    if cost_col:
        ref_map[cost_col] = "station code"
    if country_col:
        ref_map[country_col] = "country code"

    # Parcours des lignes
    for idx, row in df.iterrows():
        matched = False
        match_row = None
        match_method = None

        # priorité : cost center present et non vide -> match strict sur codes + country
        cost_val_norm = (row.get("norm_cost", "") or "").strip()
        if cost_val_norm:
            # Filter reference by norm_station_code equality
            matches = ref
            if "norm_station_code" in ref.columns:
                matches = matches[matches["norm_station_code"].astype(str).str.strip() == cost_val_norm]
            else:
                matches = matches.iloc[0:0]
            # si country fourni, filtrer par country normalisé aussi
            country_val_norm = (row.get("norm_country", "") or "").strip()
            if country_val_norm and "norm_country" in matches.columns:
                matches = matches[matches["norm_country"].astype(str).str.strip() == country_val_norm]
            if not matches.empty:
                match_row = matches.iloc[0]
                matched = True
                match_method = "cost_center"
        # sinon (cost absent) -> fuzzy sur station name si station_col présent
        elif station_col:
            if use_cdist and choices:
                if best_scores[idx] >= score_threshold:
                    pos = int(best_match_pos[idx])
                    matched_norm = choices[pos]
                    match_row = norm_to_ref_row.get(matched_norm)
                    if match_row is not None:
                        matched = True
                        match_method = "fuzzy_station_cdist"
            elif station_col and choices:

                res = process.extractOne(str(row.get("norm_name", "")), choices, scorer=fuzz.token_sort_ratio, score_cutoff=score_threshold)
                if res:
                    matched_norm, score, pos = res
                    match_row = norm_to_ref_row.get(matched_norm)
                    if match_row is not None:
                        matched = True
                        match_method = "fuzzy_station_extractOne"

        # Appliquer mapping si trouvé
        if matched and match_row is not None:
            needs_update = False
            original = row.to_dict()
            new_values = {}
            for tgt_col, ref_col in ref_map.items():
                current_val = "" if pd.isna(row.get(tgt_col, "")) else str(row.get(tgt_col, ""))
                ref_val = "" if pd.isna(match_row.get(ref_col, "")) else str(match_row.get(ref_col, ""))
                if tgt_col == station_col:
                    if not current_val or normalize(current_val) != normalize(ref_val):
                        df.at[idx, tgt_col] = ref_val
                        needs_update = True
                else:
                    if not current_val or current_val != ref_val:
                        df.at[idx, tgt_col] = ref_val
                        needs_update = True
                new_values[tgt_col] = df.at[idx, tgt_col]
            if needs_update:
                updated_count += 1
                log.append({
                    "action": "update",
                    "index": int(idx),
                    "method": match_method,
                    "original": original,
                    "new": new_values
                })
            else:
                log.append({
                    "action": "match_no_change",
                    "index": int(idx),
                    "method": match_method
                })
        else:
            # aucun match -> IGNORER (pas d'insertion)
            ignored_count += 1
            log.append({
                "action": "no_match",
                "index": int(idx),
                "norm_cost": cost_val_norm,
                "station_name": row.get(station_col, "") if station_col else "",
            })

    # Préparer dossiers
    updated_dir.mkdir(parents=True, exist_ok=True)
    backup_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    log_dir = log_root / timestamp
    log_dir.mkdir(parents=True, exist_ok=True)

    # Backup original
    bak_path = backup_dir / f"{file_path.stem}_{timestamp}{file_path.suffix}"
    shutil.copy2(file_path, bak_path)

    # Sauvegarde fichier mis à jour (même si aucune modif)
    out_path = updated_dir / file_path.name
    df.to_excel(out_path, index=False)

    # Logs
    log_df = pd.DataFrame(log)
    log_df.to_csv(log_dir / f"{file_path.stem}_log.csv", index=False)
    summary = {
        "file": file_path.name,
        "timestamp": timestamp,
        "total_rows": total,
        "updated": updated_count,
        "ignored": ignored_count,
        "method": "cdist" if use_cdist else "extractOne_per_row"
    }
    pd.DataFrame([summary]).to_csv(log_dir / f"{file_path.stem}_summary.csv", index=False)

    print(f"{updated_count}/{total} lignes mises à jour, {ignored_count} lignes ignorées dans {file_path.name} (logs → {log_dir})")


# === Détection automatique du dernier HSE_Invariants ===
def find_latest_hse_invariants(folder: Path):
    candidates = [f for f in folder.glob("*.xlsx") if "hse_invariants" in f.name.lower()]
    if not candidates:
        raise FileNotFoundError(f"Aucun fichier HSE_Invariants trouvé dans {folder}")
    return max(candidates, key=lambda f: f.stat().st_mtime)

# === Traitement de tout le dossier out ===
def update_all_in_out(out_folder="data/out", score_threshold=50, code_pad_width: int = 0):
    folder = Path(out_folder)
    ref_path = find_latest_hse_invariants(folder)

    ref = pd.read_excel(ref_path, dtype=str)
    ref.columns = [c.strip().lower() for c in ref.columns]

    updated_dir = folder / "updated"
    backup_dir = folder / "backup"
    log_root = folder / "Harmonize log"

    for file_path in folder.glob("*.xlsx"):
        if file_path == ref_path:
            continue
        name_lower = file_path.name.lower()
        if "question" in name_lower or "inspection" in name_lower:
            print(f"⏭ Ignoré (Questions/Inspections) : {file_path.name}")
            continue
        update_file_batch(ref, file_path, updated_dir, backup_dir, log_root,
                          score_threshold=score_threshold, code_pad_width=code_pad_width)

# === Exécution ===
if __name__ == "__main__":
    # Exemple d'appel : threshold 50, zero-pad station codes à 4 chiffres si besoin
    update_all_in_out(score_threshold=50, code_pad_width=0)
