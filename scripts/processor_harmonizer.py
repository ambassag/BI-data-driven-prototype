import pandas as pd
from pathlib import Path
import unicodedata
import re
from fuzzywuzzy import fuzz, process


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
    columns = [c.lower() for c in df.columns]
    has_station = "station name" in columns
    has_cost = "cost center" in columns
    has_country = "country code" in columns

    if has_station and has_cost:
        return True
    if (has_station or has_cost) and has_country:
        return True
    return False


# === Mise à jour d'un fichier avec toutes les correspondances ===
def update_file(ref_data: pd.DataFrame, file_path: Path, updated_dir: Path, backup_dir: Path, score_threshold=85):
    df = pd.read_excel(file_path, dtype=str)
    # Normaliser les noms de colonnes
    df.columns = [c.strip().lower() for c in df.columns]
    ref_data.columns = [c.strip().lower() for c in ref_data.columns]

    if not should_process(df):
        print(f"⚠ Ignoré (colonnes manquantes) : {file_path.name}")
        return

    station_col = "station name" if "station name" in df.columns else None
    cost_col = "cost center" if "cost center" in df.columns else None
    country_col = "country code" if "country code" in df.columns else None

    # Normalisation pour fuzzy match
    df["norm_name"] = df[station_col].map(normalize) if station_col else ""
    if "norm_name" not in ref_data.columns:
        ref_data["norm_name"] = ref_data["station name"].map(normalize)

    updated_count = 0
    total_rows = len(df)

    for idx, row in df.iterrows():
        if station_col:
            match = process.extractOne(row.get(station_col, ""), ref_data["norm_name"].tolist(), scorer=fuzz.token_sort_ratio)
            if not match or match[1] < score_threshold:
                continue
            match_row = ref_data[ref_data["norm_name"] == match[0]].iloc[0]
        else:
            matches = ref_data.copy()
            if cost_col:
                target_cost = row.get(cost_col, "")
                matches = matches[matches["station code"] == target_cost]
            if country_col:
                target_country = row.get(country_col, "")
                matches = matches[matches["country code"] == target_country]
            if matches.empty:
                continue
            match_row = matches.iloc[0]

        # Mapping cible → référence
        ref_map = {}
        if station_col:
            ref_map[station_col] = "station name"
        if cost_col:
            ref_map[cost_col] = "station code"
        if country_col:
            ref_map[country_col] = "country code"

        # Mise à jour
        needs_update = False
        for tgt_col, ref_col in ref_map.items():
            current_val = row.get(tgt_col, "")
            ref_val = match_row.get(ref_col, "")
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

    updated_dir.mkdir(parents=True, exist_ok=True)
    backup_dir.mkdir(parents=True, exist_ok=True)

    bak_path = backup_dir / file_path.name
    out_path = updated_dir / file_path.name
    df.to_excel(out_path, index=False)
    df.to_excel(bak_path, index=False)

    print(f"✅ {updated_count}/{total_rows} lignes mises à jour dans {file_path.name}")


# === Traitement de tout le dossier out ===
def update_all_in_out(out_folder="data/out", reference_name="HSE_Invariants.xlsx", score_threshold=85):
    folder = Path(out_folder)
    ref_path = folder / reference_name

    if not ref_path.exists():
        raise FileNotFoundError(f"Fichier de référence introuvable : {ref_path}")

    ref = pd.read_excel(ref_path, dtype=str)
    ref.columns = [c.strip().lower() for c in ref.columns]

    updated_dir = folder / "updated"
    backup_dir = folder / "backup"

    for file_path in folder.glob("*.xlsx"):
        if file_path.name == reference_name:
            continue
        update_file(ref, file_path, updated_dir, backup_dir, score_threshold=score_threshold)


# === Exécution ===
if __name__ == "__main__":
    update_all_in_out()
