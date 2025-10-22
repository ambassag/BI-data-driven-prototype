import re
import pandas as pd
from pathlib import Path
from datetime import datetime

_safe = lambda x: "" if pd.isna(x) else str(x).strip()

# ------------------------
# Extraction D.02/EP11 depuis Extraction
# ------------------------
def extract_ep11_d02_from_extraction(extraction_file: str) -> pd.DataFrame:
    """
    Extrait D.02 OU EP11 depuis l'onglet 'question' du fichier Extraction
    Transforme: D.02 Yes→0/No→1, EP11 Yes→100/No→0
    Status final: 0 ou 100
    """
    try:
        xls = pd.ExcelFile(extraction_file)

        if "Questions" not in xls.sheet_names:
            print("⚠ Onglet 'questions' introuvable")
            return pd.DataFrame(columns=["Cost Center", "Status_EP11", "D.02_value", "EP11_value"])

        df = pd.read_excel(xls, sheet_name="Questions", dtype=object)
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        df.columns = [str(c).strip() for c in df.columns]
        cols_lower = {col.lower(): col for col in df.columns}

        # Trouver Cost Center
        cost_col = None
        for pattern in ["cost center", "station code", "code station", "centre de coût", "centre de cout"]:
            if pattern in cols_lower:
                cost_col = cols_lower[pattern]
                break

        if not cost_col:
            print("⚠ Colonne Cost Center introuvable")
            return pd.DataFrame(columns=["Cost Center", "Status_EP11", "D.02_value", "EP11_value"])

        # Trouver D.02 et EP11
        d02_col = None
        for pattern in ["d.02", "d02", "d 02", "d_02"]:
            if pattern in cols_lower:
                d02_col = cols_lower[pattern]
                break

        ep11_col = None
        for pattern in ["ep11", "ep.11", "ep 11", "ep_11"]:
            if pattern in cols_lower:
                ep11_col = cols_lower[pattern]
                break

        if not d02_col and not ep11_col:
            print("⚠ Ni D.02 ni EP11 trouvés")
            return pd.DataFrame(columns=["Cost Center", "Status_EP11", "D.02_value", "EP11_value"])

        def transform_d02(val):
            if pd.isna(val) or str(val).strip() == "":
                return pd.NA
            val_str = str(val).strip().lower()
            if val_str in ["yes", "y", "oui"]:
                return 0
            elif val_str in ["no", "n", "non"]:
                return 1
            return pd.NA

        def transform_ep11(val):
            if pd.isna(val) or str(val).strip() == "":
                return pd.NA
            val_str = str(val).strip().lower()
            if val_str in ["yes", "y", "oui"]:
                return 100
            elif val_str in ["no", "n", "non"]:
                return 0
            return pd.NA

        result = pd.DataFrame()
        result["Cost Center"] = df[cost_col].apply(_safe)
        result["Status_EP11"] = pd.NA
        result["D.02_value"] = pd.NA
        result["EP11_value"] = pd.NA

        for idx, row in df.iterrows():
            cost_center = _safe(row[cost_col])
            if not cost_center:
                continue

            d02_val = row[d02_col] if d02_col else pd.NA
            ep11_val = row[ep11_col] if ep11_col else pd.NA

            d02_filled = d02_col and not pd.isna(d02_val) and str(d02_val).strip() != ""
            ep11_filled = ep11_col and not pd.isna(ep11_val) and str(ep11_val).strip() != ""

            if d02_filled:
                d02_transformed = transform_d02(d02_val)
                result.loc[idx, "D.02_value"] = d02_transformed
                if pd.notna(d02_transformed):
                    result.loc[idx, "Status_EP11"] = 0 if d02_transformed == 0 else 100
            elif ep11_filled:
                ep11_transformed = transform_ep11(ep11_val)
                result.loc[idx, "EP11_value"] = ep11_transformed
                if pd.notna(ep11_transformed):
                    result.loc[idx, "Status_EP11"] = ep11_transformed

        result = result[(result["Cost Center"] != "") & (result["Status_EP11"].notna())]

        nb_d02 = result["D.02_value"].notna().sum()
        nb_ep11 = result["EP11_value"].notna().sum()

        print(f"✅ Extraction terminée:")
        print(f"   - {nb_d02} stations avec D.02")
        print(f"   - {nb_ep11} stations avec EP11")

        return result

    except Exception as e:
        print(f"❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame(columns=["Cost Center", "Status_EP11", "D.02_value", "EP11_value"])


# ------------------------
# Helpers pour nommage de fichier (détection date + unique path)
# ------------------------
def extract_date_from_filename(filename: str) -> str | None:
    """
    Cherche une date dans le nom de fichier et retourne 'YYYYMMDD' ou None.
    Gère plusieurs patterns courants : YYYYMMDD, YYYY-MM-DD, YYYY_MM_DD, YYYY.MM.DD, DDMMYYYY.
    """
    if not filename:
        return None
    name = Path(filename).name

    patterns = [
        r'(?P<d>\d{4}[01]\d[0-3]\d)',            # 20251021
        r'(?P<d>\d{4}[-_]\d{2}[-_]\d{2})',       # 2025-10-21 or 2025_10_21
        r'(?P<d>\d{4}[.]\d{2}[.]\d{2})',         # 2025.10.21
        r'(?P<d>\d{2}\d{2}\d{4})'                # 21102025 (DDMMYYYY)
    ]
    for pat in patterns:
        m = re.search(pat, name)
        if not m:
            continue
        s = m.group('d')
        try:
            # YYYYMMDD
            if re.fullmatch(r'\d{8}', s):
                # si les 4 premiers chiffres raisonnables (>=1900) -> YYYYMMDD
                if int(s[:4]) >= 1900:
                    dt = datetime.strptime(s, "%Y%m%d")
                else:
                    # sinon traiter comme DDMMYYYY
                    dt = datetime.strptime(s, "%d%m%Y")
            elif re.fullmatch(r'\d{4}[-_.]\d{2}[-_.]\d{2}', s):
                dt = datetime.strptime(re.sub(r'[_\.]', '-', s), "%Y-%m-%d")
            else:
                continue
            return dt.strftime("%Y%m%d")
        except Exception:
            continue
    return None


def unique_filepath(path: Path) -> Path:
    """
    Si path existe, ajoute _1, _2, ... avant l'extension jusqu'à avoir un nom libre.
    """
    if not path.exists():
        return path
    base = path.with_suffix('').name
    parent = path.parent
    ext = path.suffix
    i = 1
    while True:
        candidate = parent / f"{base}_{i}{ext}"
        if not candidate.exists():
            return candidate
        i += 1


# ------------------------
# Enrichissement des lignes EP11
# ------------------------
def enrich_ep11_from_extraction(invariants_file: str, extraction_file: str, output_dir: str = "data/out"):
    """
    Enrichit les lignes EP11 du fichier Invariant study (ou similaire) avec D.02/EP11 depuis Extraction

    Args:
        invariants_file: Chemin vers Invariant study_*.xlsx
        extraction_file: Chemin vers Extraction_*.xlsx
        output_dir: Dossier de sortie

    Returns:
        Chemin du fichier enrichi
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    print(f"\n🔄 Enrichissement des lignes EP11...")
    print(f"📂 Fichier Invariants (source): {Path(invariants_file).name}")
    print(f"📂 Fichier Extraction: {Path(extraction_file).name}")

    # Lire le fichier Invariant study
    df_invariants = pd.read_excel(invariants_file)

    # Extraire les données D.02/EP11 depuis Extraction
    df_ep11_data = extract_ep11_d02_from_extraction(extraction_file)

    if df_ep11_data.empty:
        print("⚠ Aucune donnée D.02/EP11 extraite, fichier non modifié")
        return invariants_file

    # Initialiser les colonnes D.02 et EP11 à NA pour TOUS
    df_invariants["D.02"] = pd.NA
    df_invariants["EP11"] = pd.NA

    # Filtrer uniquement les lignes EP11
    # On protège contre valeurs NA dans la colonne 'Invariant'
    if "Invariant" not in df_invariants.columns:
        print("⚠ Colonne 'Invariant' introuvable dans le fichier Invariant study")
        return invariants_file

    mask_ep11 = df_invariants["Invariant"].astype(str).str.upper() == "EP11"

    if mask_ep11.sum() == 0:
        print("⚠ Aucune ligne EP11 trouvée dans Invariant study")
        return invariants_file

    # Merger avec les données de l'onglet question
    df_ep11_rows = df_invariants[mask_ep11].copy()
    df_ep11_merged = df_ep11_rows.merge(df_ep11_data, on="Cost Center", how="left")

    # Mettre à jour Status, D.02 et EP11 pour les lignes EP11
    df_invariants.loc[mask_ep11, "Status"] = df_ep11_merged["Status_EP11"].values
    df_invariants.loc[mask_ep11, "D.02"] = df_ep11_merged["D.02_value"].values
    df_invariants.loc[mask_ep11, "EP11"] = df_ep11_merged["EP11_value"].values

    # Statistiques
    nb_ep11_total = int(mask_ep11.sum())
    nb_enriched = int(df_invariants.loc[mask_ep11, "Status"].notna().sum())
    nb_with_d02 = int(df_invariants.loc[mask_ep11, "D.02"].notna().sum())
    nb_with_ep11_val = int(df_invariants.loc[mask_ep11, "EP11"].notna().sum())

    print(f"\n✅ Enrichissement terminé:")
    print(f"   - {nb_ep11_total} lignes EP11 trouvées")
    print(f"   - {nb_enriched} lignes EP11 enrichies avec Status (0 ou 100)")
    print(f"   - {nb_with_d02} via D.02")
    print(f"   - {nb_with_ep11_val} via EP11")

    # ------------------------
    # Sauvegarder le fichier enrichi
    # - on tente d'extraire une date depuis le nom du fichier invariants (priorité)
    # - sinon depuis le nom du fichier extraction
    # - sinon date d'exécution
    # - on évite l'écrasement en ajoutant _1, _2 si nécessaire
    # ------------------------
    date_suffix = extract_date_from_filename(invariants_file) or extract_date_from_filename(extraction_file)
    if not date_suffix:
        date_suffix = datetime.now().strftime("%Y%m%d")

    # Utiliser un nom stable avec underscore (évite problèmes d'espaces)
    base_output_name = f"Invariants_study_enriched_{date_suffix}.xlsx"
    output_file = output_path / base_output_name
    output_file = unique_filepath(output_file)

    df_invariants.to_excel(output_file, index=False)

    print(f"\n📊 Fichier enrichi sauvegardé: {output_file}")

    return str(output_file)


# ------------------------
# Détection automatique des fichiers
# ------------------------
def find_latest_file(directory: str, pattern: str):
    """Trouve le fichier le plus récent correspondant au pattern (case-insensitive dans le nom)"""
    folder = Path(directory)
    if not folder.exists():
        return None
    candidates = [f for f in folder.glob("*.xlsx") if pattern.lower() in f.name.lower()]
    if not candidates:
        return None
    candidates.sort(key=lambda f: f.stat().st_mtime, reverse=True)
    return str(candidates[0])


# ------------------------
# Script principal
# ------------------------
if __name__ == "__main__":
    invariants_file = find_latest_file("data/out", "invariants_study")
    extraction_file = find_latest_file("data/inbox", "extraction")

    if not invariants_file:
        print("❌ Aucun fichier Invariant study trouvé dans data/out")
        exit(1)

    if not extraction_file:
        print("❌ Aucun fichier Extraction trouvé dans data/inbox")
        exit(1)

    # Enrichir
    output_file = enrich_ep11_from_extraction(
        invariants_file=invariants_file,
        extraction_file=extraction_file,
        output_dir="data/out/updated"
    )

    print(f"\n✅ Processus terminé!")
    print(f"📄 Fichier final: {output_file}")
