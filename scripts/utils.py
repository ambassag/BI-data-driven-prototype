# scripts/utils.py
import re
from typing import Dict, Optional
import unicodedata
from difflib import get_close_matches

def normalize_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    s = unicodedata.normalize('NFKD', s)
    s = re.sub(r'\s+', ' ', s)
    return s

def generate_filiale_code(country_name: str, country_map: Optional[Dict[str, str]] = None) -> str:
    """
    Génère filiale_code (2 lettres) : si mapping fourni, utilises-le,
    sinon prends les deux premières lettres (normalisées) en majuscules.
    """
    if not country_name:
        return "XX"
    if country_map:
        key = normalize_text(country_name).lower()
        for k, v in country_map.items():
            if normalize_text(k).lower() == key:
                return v.upper()
    # fallback : deux premières lettres alphas
    clean = re.sub(r'[^A-Za-z]', '', country_name)
    clean = clean.upper()
    return (clean[:2] if len(clean) >= 2 else (clean + "X")) if clean else "XX"

def fuzzy_match_name(target: str, choices: list, cutoff: float = 0.8):
    """
    Utilise difflib.get_close_matches comme fallback pour faire un fuzzy match simple.
    Retourne meilleur match ou None si trop faible.
    """
    if not target or not choices:
        return None
    norm = normalize_text(target).lower()
    cands = [normalize_text(c).lower() for c in choices]
    matches = get_close_matches(norm, cands, n=1, cutoff=cutoff)
    if not matches:
        return None
    # retourne original choice (pas normalisé)
    idx = cands.index(matches[0])
    return choices[idx]
