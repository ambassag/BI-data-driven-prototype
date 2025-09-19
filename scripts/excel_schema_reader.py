# scripts/excel_schema_reader.py
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Union, Any
import pandas as pd
import difflib
import re
import os

@dataclass
class Field:
    name: str
    aliases: List[str] = field(default_factory=list)
    dtype: Optional[type] = None

class Schema:
    def __init__(self, fields: List[Field]):
        self.fields: Dict[str, Field] = {f.name: f for f in fields}
        self._alias_index: Dict[str, str] = {}
        for f in fields:
            keys = [f.name] + f.aliases
            for k in keys:
                self._alias_index[str(k).strip().lower()] = f.name

    @classmethod
    def from_dict(cls, d: Dict[str, List[str]]):
        fields = [Field(name=k, aliases=v) for k, v in d.items()]
        return cls(fields)

    def canonical_for(self, col_name: str) -> Optional[str]:
        if not col_name:
            return None
        key = str(col_name).strip().lower()
        if key in self._alias_index:
            return self._alias_index[key]
        matches = difflib.get_close_matches(key, list(self._alias_index.keys()), n=1, cutoff=0.75)
        if matches:
            return self._alias_index[matches[0]]
        return None

class ExcelSchemaReader:
    """
    Lit un fichier Excel (option multi-feuilles), normalise/renomme les colonnes
    et fournit un mapping original->canonical par feuille.
    """
    def __init__(self,
                 path: Union[str, os.PathLike],
                 schema: Schema = None,
                 sheet_name: Optional[Union[str, int]] = None,
                 header_row: int = 0,
                 detect_dynamic_pairs: bool = True,
                 all_sheets: bool = False):
        self.path = str(path)
        self.schema = schema
        self.sheet_name = sheet_name
        self.header_row = header_row
        self.detect_dynamic_pairs = detect_dynamic_pairs
        self.all_sheets = all_sheets

        self.sheets: Dict[str, pd.DataFrame] = {}
        self.col_maps: Dict[str, Dict[str, str]] = {}

    def read(self):
        try:
            xls = pd.ExcelFile(self.path, engine="openpyxl")
            sheet_names = xls.sheet_names
        except Exception as e:
            print(f"⚠️ Impossible de lire le fichier {self.path}: {e}")
            return self

        sheets_to_read = sheet_names if self.all_sheets else [self.sheet_name or sheet_names[0]]

        for sheet in sheets_to_read:
            try:
                # quick probe for size
                temp_df = pd.read_excel(self.path, sheet_name=sheet, header=None, engine="openpyxl")
                if temp_df.shape[0] <= self.header_row:
                    # feuille trop courte => on ignore
                    continue

                df = pd.read_excel(self.path,
                                   sheet_name=sheet,
                                   header=self.header_row,
                                   engine="openpyxl",
                                   dtype=str,
                                   keep_default_na=False)
                # drop totally empty cols
                df = df.dropna(axis=1, how='all')
                df, col_map = self._normalize_columns(df)
                self.sheets[sheet] = df
                self.col_maps[sheet] = col_map
            except Exception as e:
                print(f"⚠️ Impossible de lire la feuille '{sheet}': {e}")
                continue

        return self

    def _normalize_columns(self, df: pd.DataFrame):
        cols = list(df.columns)
        new_cols = {}
        col_map: Dict[str, str] = {}

        excel_idx = eris_idx = 0
        for col in cols:
            name = str(col).strip()
            # détecte Excel / Eris même si écrits "Excel .1" / "Eris 2" etc.
            if self.detect_dynamic_pairs and re.match(r"^Excel(\s*[.\-_]?\s*\d*)?$", name, flags=re.IGNORECASE):
                new_name = f"excel_score_{excel_idx}"
                excel_idx += 1
            elif self.detect_dynamic_pairs and re.match(r"^Eris(\s*[.\-_]?\s*\d*)?$", name, flags=re.IGNORECASE):
                new_name = f"eris_score_{eris_idx}"
                eris_idx += 1
            else:
                if self.schema:
                    canon = self.schema.canonical_for(name)
                    new_name = canon if canon else re.sub(r"\s+", "_", name).strip().lower()
                else:
                    new_name = re.sub(r"\s+", "_", name).strip().lower()

            base = new_name
            i = 1
            # ensure uniqueness
            while new_name in new_cols.values():
                new_name = f"{base}_{i}"
                i += 1

            new_cols[col] = new_name
            col_map[str(col)] = new_name

        df = df.rename(columns=new_cols)
        return df, col_map

    def mapped_columns(self, sheet: Optional[str] = None) -> dict[str, str] | dict[str, dict[str, str]]:
        if sheet:
            return self.col_maps.get(sheet, {})
        return self.col_maps

    def get_value(self, sheet: str, row_index: int, field_name: str) -> Any:
        if sheet not in self.sheets:
            raise KeyError(f"Feuille '{sheet}' introuvable.")
        df = self.sheets[sheet]
        if field_name in df.columns:
            col = field_name
        else:
            candidates = [c for c in df.columns if str(c).lower() == str(field_name).lower() or str(c).lower().startswith(str(field_name).lower())]
            if not candidates:
                raise KeyError(f"Champ '{field_name}' introuvable dans '{sheet}'. Colonnes: {list(df.columns)[:30]} ...")
            col = candidates[0]
        # safe get
        try:
            return df.iloc[row_index][col]
        except Exception:
            return None
