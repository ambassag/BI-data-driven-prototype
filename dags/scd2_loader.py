import os
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Tuple
import unicodedata  # ajouté pour normalisation city

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

# ------------------------
# Configuration Logging
# ------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ------------------------
# Config Générale
# ------------------------
DW_ENGINE = os.environ.get("DW_ENGINE", "mysql+mysqldb://airflow:airflow@mysql/airflow")
OUT_DIR = Path(os.environ.get("OUT_DIR", "/opt/airflow/data/out"))

TABLE_MAP = {
    "country_code": ("dim_pays", ["country_code"]),
    "extract_station": ("dim_stations", ["cost_center"]),
    "hse_invariants": ("fact_hse_invariants", ["station_name"]),
    "invariants_study": ("dim_invariants", ["country_code", "invariant"]),
    "invariants_details": ("dim_invariants_details", ["country_code", "cost_center"]),
}

INSERT_CHUNK = 500

# ------------------------
# Fonctions Utilitaires
# ------------------------
def _norm(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = s.replace("\r", " ").replace("\n", " ")
    s = " ".join(s.split()).replace(" ", "_")
    return s


def _normalize_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_norm(c) for c in df.columns.astype(str)]
    df = df.where(pd.notna(df), None)
    return df


def _sql_safe_colname(col: str) -> str:
    # Nettoie les noms pour être SQL/bind-friendly : remplace parenthèses, espaces, slash, % etc.
    if col is None:
        return col
    c = str(col)
    c = c.replace("(", "_").replace(")", "")
    c = c.replace(" ", "_").replace("/", "_")
    c = c.replace("%", "pct")
    # Collapse double underscores
    while "__" in c:
        c = c.replace("__", "_")
    return c


def _build_normalized_table_map(raw_map: dict) -> dict:
    new = {}
    for key, (table, keys) in raw_map.items():
        key_norm = _norm(key)
        keys_norm = [_norm(k) for k in keys]
        new[key_norm] = (table, keys_norm)
    return new


TABLE_MAP_N = _build_normalized_table_map(TABLE_MAP)

# ------------------------
# Vérification / Création Table
# ------------------------
def _ensure_table_exists(engine, table_name: str, df: pd.DataFrame, business_keys: List[str]):
    with engine.connect() as conn:
        res = conn.execute(text("SHOW TABLES LIKE :t"), {"t": table_name}).fetchall()
        if res:
            return

        cols = []
        for c, dtype in df.dtypes.items():
            if pd.api.types.is_integer_dtype(dtype):
                cols.append(f"`{c}` BIGINT")
            elif pd.api.types.is_float_dtype(dtype):
                cols.append(f"`{c}` DOUBLE")
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                cols.append(f"`{c}` DATETIME")
            else:
                cols.append(f"`{c}` VARCHAR(255)")

        uniq_cols = ""
        if business_keys == ["uniq_business"]:
            if "uniq_business" not in df.columns:
                cols.append("`uniq_business` VARCHAR(255)")
            uniq_cols = "`uniq_business`"
        else:
            uniq_cols = ", ".join([f"`{k}`" for k in business_keys]) if business_keys else ""

        cols_sql = ",\n  ".join(cols)

        create_sql = f"""
        CREATE TABLE `{table_name}` (
          id BIGINT AUTO_INCREMENT PRIMARY KEY,
          {cols_sql},
          is_current TINYINT(1) DEFAULT 1,
          start_date DATETIME,
          end_date DATETIME,
          UNIQUE KEY uniq_business ({uniq_cols})
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        conn.execute(text(create_sql))
        logger.info(f"✅ Table `{table_name}` créée avec clés uniques: {uniq_cols}")

# ------------------------
# Comparaison de lignes
# ------------------------
def _rows_equal_series(a: pd.Series, b: pd.Series) -> bool:
    for k in a.index:
        va, vb = a.get(k), b.get(k)
        # normaliser et strip avant comparaison
        va_s = unicodedata.normalize('NFC', str(va).strip()) if va not in [None, ""] else None
        vb_s = unicodedata.normalize('NFC', str(vb).strip()) if vb not in [None, ""] else None
        if va_s != vb_s:
            return False
    return True

# ------------------------
# Chargement SCD2
# ------------------------
def _process_table_batch(engine, table_name: str, df: pd.DataFrame, business_keys: List[str], now: datetime):
    if df.empty:
        logger.info(f"⏩ Aucun enregistrement à traiter pour {table_name}")
        return 0, 0

    # Nettoyage: inclure city
    for c in ["cost_center", "country_code", "station_name", "city"]:
        if c in df.columns:
            df[c] = df[c].apply(lambda x: None if x is None else str(x).strip())

    if table_name == "dim_stations":
        def make_uniq(r):
            cc = (r.get("country_code") or "").strip()
            ccst = (r.get("cost_center") or "").strip()
            sname = (r.get("station_name") or "").strip()
            if ccst and len(ccst) > 2 and ccst.lower() != cc.lower():
                return f"{cc}_{ccst}"
            return f"{cc}_{sname}"

        df["uniq_business"] = df.apply(make_uniq, axis=1)
        business_keys = ["uniq_business"]
        logger.info(f"🔑 Pour {table_name} on utilise la clé calculée 'uniq_business' (cost_center fallback station_name)")

    before = len(df)
    df = df.dropna(subset=business_keys, how="any")
    df = df.drop_duplicates(subset=business_keys, keep="first")
    dropped = before - len(df)
    if dropped:
        logger.warning(f"{dropped} lignes ignorées dans {table_name} (clés manquantes ou doublons dans le fichier).")

    _ensure_table_exists(engine, table_name, df, business_keys)

    def _row_key_tuple(row):
        return tuple(row[k] for k in business_keys)

    keys = [_row_key_tuple(row) for _, row in df.iterrows()]
    unique_keys = list(dict.fromkeys(keys))

    existing_map: Dict[Tuple, Dict] = {}
    if unique_keys:
        with engine.connect() as conn:
            select_sql = f"SELECT * FROM `{table_name}` WHERE is_current=1"
            existing_df = pd.read_sql(select_sql, conn)
            if not existing_df.empty:
                existing_df["_key"] = existing_df.apply(lambda r: tuple(r[k] for k in business_keys), axis=1)
                existing_map = existing_df.set_index("_key").to_dict(orient="index")

    inserts, updates_ids = [], []
    compare_cols = list(df.columns)

    for _, row in df.iterrows():
        key = tuple(row[k] for k in business_keys)
        existing = existing_map.get(key)
        if not existing:
            data = {c: row[c] for c in compare_cols}
            data.update({"is_current": 1, "start_date": now})
            inserts.append(data)
        else:
            existing_series = pd.Series({c: existing.get(c) for c in compare_cols})
            if not _rows_equal_series(pd.Series(row[compare_cols]), existing_series):
                updates_ids.append(existing.get("id"))
                data = {c: row[c] for c in compare_cols}
                data.update({"is_current": 1, "start_date": now})
                inserts.append(data)
            else:
                logger.info(f"⚠️ Doublon ignoré pour {table_name} clé={key}")

    if updates_ids:
        with engine.begin() as conn:
            for _id in updates_ids:
                conn.execute(
                    text(f"UPDATE `{table_name}` SET is_current=0, end_date=:end WHERE id=:id"),
                    {"end": now, "id": _id},
                )

    inserted_count = updated_count = 0
    if inserts:
        insert_cols = compare_cols + ["is_current", "start_date"]
        insert_sql = text(
            f"INSERT INTO `{table_name}` ({', '.join(f'`{c}`' for c in insert_cols)}) "
            f"VALUES ({', '.join(f':{c}' for c in insert_cols)})"
        )
        with engine.begin() as conn:
            for i in range(0, len(inserts), INSERT_CHUNK):
                chunk = inserts[i: i + INSERT_CHUNK]
                try:
                    conn.execute(insert_sql, chunk)
                    inserted_count += len(chunk)
                except IntegrityError as e:
                    logger.warning(f"⚠️ IntegrityError lors d'un chunk insert dans {table_name}: {e}. Tentative ligne-à-ligne.")
                    for rowdata in chunk:
                        try:
                            conn.execute(insert_sql, rowdata)
                            inserted_count += 1
                        except IntegrityError:
                            logger.warning(f"Ignoré (duplicate) ligne avec clé business dans {table_name}: {rowdata.get('uniq_business') if 'uniq_business' in rowdata else 'n/a'}")

    updated_count = len(updates_ids)
    logger.info(f"[SCD2] {table_name} → inserts={inserted_count}, updates_closed={updated_count}")
    return inserted_count, updated_count

# ------------------------
# Loader Principal
# ------------------------
def load_all_out_files_to_dw():
    engine = create_engine(DW_ENGINE, pool_pre_ping=True)
    now = datetime.now(timezone.utc)

    if not OUT_DIR.exists():
        logger.warning(f"❌ OUT_DIR n'existe pas: {OUT_DIR}")
        return 0, 0

    files = sorted(OUT_DIR.glob("*.xlsx"))
    total_ins = total_upd = 0

    for f in files:
        logger.info(f"📂 Traitement du fichier: {f.name}")
        stem = _norm(f.stem)

        try:
            xls = pd.ExcelFile(f)
        except Exception as e:
            logger.exception(f"Erreur ouverture {f}: {e}")
            continue

        mappings_to_check = []
        if stem in TABLE_MAP_N:
            mappings_to_check.append((stem, 0))
        for sheet_name in xls.sheet_names:
            sheet_norm = _norm(sheet_name)
            if sheet_norm in TABLE_MAP_N:
                mappings_to_check.append((sheet_norm, sheet_name))

        if not mappings_to_check:
            logger.warning(f"⚠️ Fichier {f.name} non mappé dans TABLE_MAP — ignoré")
            continue

        for map_key, sheet_ref in mappings_to_check:
            table_name, business_keys = TABLE_MAP_N[map_key]
            try:
                df = pd.read_excel(xls, sheet_name=sheet_ref, dtype=object)
            except Exception as e:
                logger.exception(f"Erreur lecture {f.name}::{sheet_ref}: {e}")
                continue

            # Normalisation colonnes + rendu SQL-friendly (corrige les parenthèses / espaces / % / slash)
            df = _normalize_df_columns(df)
            df.columns = [_sql_safe_colname(c) for c in df.columns]

            for col in business_keys:
                if col not in df.columns:
                    df[col] = None
                    logger.warning(f"{f.name}::{sheet_ref} — colonne manquante ajoutée: {col}")

            before = len(df)
            df = df.dropna(subset=business_keys, how="all")
            dropped = before - len(df)
            if dropped:
                logger.warning(f"{dropped} lignes ignorées (clés manquantes) dans {f.name}::{sheet_ref}")

            ins, upd = _process_table_batch(engine, table_name, df, business_keys, now)
            total_ins += ins
            total_upd += upd

    logger.info(f"✅ FIN CHARGEMENT — inserts={total_ins}, updates_closed={total_upd}")
    return total_ins, total_upd

# ------------------------
# CLI
# ------------------------
if __name__ == "__main__":
    try:
        inserted, updated = load_all_out_files_to_dw()
        logger.info(f"✅ Chargement terminé — {inserted} inserts, {updated} updates")
    except Exception as e:
        logger.exception(f"🚨 Erreur fatale lors du chargement: {e}")
