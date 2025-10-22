import pandas as pd
from pathlib import Path
import unicodedata
import re
from rapidfuzz import fuzz, process

# === Normalisation des chaînes ===
def normalize(s: str) -> str:
    if pd.isna(s):
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return s.strip()

# === Vérifie si le fichier doit être traité ===
def should_process(df: pd.DataFrame) -> bool:
    cols = [c.lower() for c in df.columns]
    return ("station name" in cols and "cost center" in cols) or \
           (("station name" in cols or "cost center" in cols) and "country code" in cols)

# === Mise à jour d'un fichier avec correspondances batch ===
def update_file_batch(ref_data: pd.DataFrame, file_path: Path, updated_dir: Path, backup_dir: Path, score_threshold=85):
    df = pd.read_excel(file_path, dtype=str)
    df.columns = [c.strip().lower() for c in df.columns]
    ref_data.columns = [c.strip().lower() for c in ref_data.columns]

    if not should_process(df):
        print(f"⚠ Ignoré (colonnes manquantes) : {file_path.name}")
        return

    station_col = "station name" if "station name" in df.columns else None
    cost_col = "cost center" if "cost center" in df.columns else None
    country_col = "country code" if "country code" in df.columns else None

    # Colonnes normalisées
    if station_col:
        df["norm_name"] = df[station_col].map(normalize)
    if "norm_name" not in ref_data.columns and "station name" in ref_data.columns:
        ref_data["norm_name"] = ref_data["station name"].map(normalize)

    updated_count = 0
    inserted_count = 0
    log = []

    # Préparer les choices pour rapidfuzz
    choices = ref_data["norm_name"].astype(str).tolist() if station_col else []

    if station_col:
        # Calculer toutes les correspondances en batch
        scores = process.cdist(
            df["norm_name"].astype(str).tolist(),
            choices,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=score_threshold
        )
        best_matches = scores.argmax(axis=1)  # index des meilleurs scores
        best_scores = scores.max(axis=1)
    else:
        best_matches = [None]*len(df)
        best_scores = [0]*len(df)

    for idx, row in df.iterrows():
        match_row = None
        matched = False

        if station_col:
            if best_scores[idx] >= score_threshold:
                match_str = choices[best_matches[idx]]
                match_row = ref_data[ref_data["norm_name"] == match_str].iloc[0]
                matched = True
        else:
            # Matching par cost + country
            matches = ref_data.copy()
            if cost_col:
                matches = matches[matches["station code"] == str(row.get(cost_col, ""))]
            if country_col:
                matches = matches[matches["country code"] == str(row.get(country_col, ""))]
            if not matches.empty:
                match_row = matches.iloc[0]
                matched = True

        # Mapping cible → référence
        ref_map = {}
        if station_col:
            ref_map[station_col] = "station name"
        if cost_col:
            ref_map[cost_col] = "station code"
        if country_col:
            ref_map[country_col] = "country code"

        if matched and match_row is not None:
            needs_update = False
            for tgt_col, ref_col in ref_map.items():
                current_val = str(row.get(tgt_col, ""))
                ref_val = str(match_row.get(ref_col, ""))
                if tgt_col == station_col:
                    if not current_val or normalize(current_val) != normalize(ref_val):
                        df.at[idx, tgt_col] = ref_val
                        needs_update = True
                else:
                    if not current_val or current_val != ref_val:
                        df.at[idx, tgt_col] = ref_val
                        needs_update = True
            if needs_update:
                updated_count += 1
                log.append({"action": "update", "index": idx, "original": row.to_dict(), "new": df.loc[idx].to_dict()})
        else:
            # Pas de correspondance → insertion
            new_row = {}
            for tgt_col, ref_col in ref_map.items():
                new_row[tgt_col] = row.get(tgt_col, "")
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            inserted_count += 1
            log.append({"action": "insert", "index": idx, "row": new_row})

    # Dossiers
    updated_dir.mkdir(parents=True, exist_ok=True)
    backup_dir.mkdir(parents=True, exist_ok=True)

    bak_path = backup_dir / file_path.name
    out_path = updated_dir / file_path.name
    df.to_excel(out_path, index=False)
    df.to_excel(bak_path, index=False)

    # Log CSV
    log_df = pd.DataFrame(log)
    log_df.to_csv(updated_dir / f"{file_path.stem}_log.csv", index=False)

    print(f"{updated_count}/{len(df)} lignes mises à jour, {inserted_count} lignes insérées dans {file_path.name}")

# === Détection automatique du dernier HSE_Invariants ===
def find_latest_hse_invariants(folder: Path):
    candidates = [f for f in folder.glob("*.xlsx") if "hse_invariants" in f.name.lower()]
    if not candidates:
        raise FileNotFoundError(f"Aucun fichier HSE_Invariants trouvé dans {folder}")
    return max(candidates, key=lambda f: f.stat().st_mtime)

# === Traitement de tout le dossier out ===
def update_all_in_out(out_folder="data/out", score_threshold=85):
    folder = Path(out_folder)
    ref_path = find_latest_hse_invariants(folder)

    ref = pd.read_excel(ref_path, dtype=str)
    ref.columns = [c.strip().lower() for c in ref.columns]

    updated_dir = folder / "updated"
    backup_dir = folder / "backup"

    for file_path in folder.glob("*.xlsx"):
        # Ignorer le fichier de référence
        if file_path == ref_path:
            continue

        # Ignorer les fichiers contenant "Question" ou "Inspection" dans le nom
        name_lower = file_path.name.lower()
        if "question" in name_lower or "inspection" in name_lower:
            print(f"⏭ Ignoré (Questions/Inspections) : {file_path.name}")
            continue

        # Traitement normal
        update_file_batch(ref, file_path, updated_dir, backup_dir, score_threshold=score_threshold)

# === Exécution ===
if __name__ == "__main__":
    update_all_in_out()