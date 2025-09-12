# excel_schema_reader.py
from dataclasses import dataclass, field
from typing import List, Dict, Any, Callable, Optional
import pandas as pd
import difflib
import re

@dataclass
class Field:
    name: str
    aliases: List[str] = field(default_factory=list)
    dtype: Optional[type] = None

class Schema:
    def __init__(self, fields: List[Field]):
        self.fields: Dict[str, Field] = {f.name: f for f in fields}
        self._alias_index = {}
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
        candidates = list(self._alias_index.keys())
        matches = difflib.get_close_matches(key, candidates, n=1, cutoff=0.75)
        if matches:
            return self._alias_index[matches[0]]
        return None

class ExcelSchemaReader:
    def __init__(self, path: str, schema: Schema = None,
                 sheet_name: Optional[str] = 0, header_row: int = 0,
                 detect_dynamic_pairs: bool = True):
        """
        path: chemin vers le fichier Excel
        schema: objet Schema facultatif (pour col canonicales)
        sheet_name: nom de la feuille ou index
        header_row: index 0-based de la ligne d'en-tête réelle dans la feuille
        detect_dynamic_pairs: si True, mappe "Excel", "Excel .1", ... -> excel_score_0, excel_score_1...
        """
        self.path = path
        self.schema = schema
        self.sheet_name = sheet_name
        self.header_row = header_row
        self.df: Optional[pd.DataFrame] = None
        self.col_map: Dict[str, str] = {}
        self.detect_dynamic_pairs = detect_dynamic_pairs

    def read(self):
        # lit la feuille avec le header user-specified
        self.df = pd.read_excel(self.path, sheet_name=self.sheet_name, header=self.header_row, engine="openpyxl")
        # nettoyage de colonnes vides
        self.df = self.df.dropna(axis=1, how='all')
        self._normalize_columns()
        return self

    def _normalize_columns(self):
        assert self.df is not None
        cols = list(self.df.columns)
        new_cols = {}
        # 1) si activation détection dynamique -> détecter les colonnes Excel / Eris répétées
        if self.detect_dynamic_pairs:
            excel_idx = 0
            eris_idx = 0
            for col in cols:
                name = str(col).strip()
                # détecte "Excel" (ou "Excel .1", "Excel .2"...)
                if re.match(r'^Excel(\s*\.\s*\d+)?$', name, flags=re.IGNORECASE):
                    new_name = f"excel_score_{excel_idx}"
                    excel_idx += 1
                elif re.match(r'^Eris(\s*\.\s*\d+)?$', name, flags=re.IGNORECASE):
                    new_name = f"eris_score_{eris_idx}"
                    eris_idx += 1
                else:
                    # si schema fourni on tente correspondance canonique
                    if self.schema:
                        canon = self.schema.canonical_for(name)
                        if canon:
                            new_name = canon
                        else:
                            # fallback: normalise le nom (minuscules, underscores)
                            new_name = re.sub(r'\s+', '_', name).strip().lower()
                    else:
                        new_name = re.sub(r'\s+', '_', name).strip().lower()
                # évite collisions : suffixe numérique si déjà utilisé
                base = new_name
                i = 1
                while new_name in new_cols.values():
                    new_name = f"{base}_{i}"
                    i += 1
                new_cols[col] = new_name
                self.col_map[col] = new_name
        else:
            # fallback simple (sans dynamique)
            for col in cols:
                name = str(col).strip()
                if self.schema:
                    canon = self.schema.canonical_for(name)
                    new_name = canon if canon else re.sub(r'\s+', '_', name).strip().lower()
                else:
                    new_name = re.sub(r'\s+', '_', name).strip().lower()
                # gestion collision
                base = new_name
                i = 1
                while new_name in new_cols.values():
                    new_name = f"{base}_{i}"
                    i += 1
                new_cols[col] = new_name
                self.col_map[col] = new_name

        # appliquer renommage
        self.df = self.df.rename(columns=new_cols)

    def get_value(self, row_index: int, field_name: str) -> Any:
        if self.df is None:
            raise RuntimeError("Data not loaded. Call .read() first.")
        # si field_name est une clé canonique existante, on l'utilise direct
        if field_name in self.df.columns:
            col = field_name
        else:
            # essaye de trouver une colonne commençant par field_name (utile pour excel_score_ etc.)
            candidates = [c for c in self.df.columns if str(c).lower() == str(field_name).lower() or str(c).lower().startswith(str(field_name).lower())]
            if not candidates:
                raise KeyError(f"Champ '{field_name}' introuvable. Colonnes disponibles: {list(self.df.columns)[:50]} ...")
            col = candidates[0]
        return self.df.iloc[row_index][col]

    def query_rows(self, field_name: str, predicate: Callable[[Any], bool]):
        if self.df is None:
            raise RuntimeError("Data not loaded. Call .read() first.")
        if field_name not in self.df.columns:
            # try startswith match
            cols = [c for c in self.df.columns if c.startswith(field_name)]
            if not cols:
                raise KeyError(f"Champ '{field_name}' introuvable.")
            field_name = cols[0]
        ser = self.df[field_name]
        mask = ser.apply(lambda v: predicate(v))
        return self.df[mask]

    def mapped_columns(self) -> Dict[str, str]:
        # retourne le mapping original -> canonical
        return self.col_map
