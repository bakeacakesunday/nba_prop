"""
utils.py — Player name normalization and fuzzy matching helpers.
"""
from __future__ import annotations

import re
import unicodedata
from typing import Optional

try:
    from rapidfuzz import fuzz, process as rf_process
    _HAS_RAPIDFUZZ = True
except ImportError:
    _HAS_RAPIDFUZZ = False

# Suffixes to strip during normalization
_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}


def normalize_name(name: str) -> str:
    """
    Normalize a player name for matching:
    - Unicode → ASCII
    - Lowercase, strip whitespace
    - Remove periods, apostrophes, hyphens
    - Remove name suffixes (Jr, Sr, II, III…)
    """
    if not name:
        return ""
    # NFKD decompose then drop non-ASCII bytes
    name = unicodedata.normalize("NFKD", name)
    name = name.encode("ascii", "ignore").decode("ascii")
    name = name.lower().strip()
    # Remove punctuation that appears in names
    name = re.sub(r"[.\'\-]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    # Drop suffix tokens
    parts = [p for p in name.split() if p not in _SUFFIXES]
    return " ".join(parts)


def fuzzy_match(
    query: str,
    candidates: list[str],
    threshold: int = 80,
) -> tuple[Optional[str], float, list[tuple[str, float]]]:
    """
    Find the best match for `query` in `candidates`.

    Returns
    -------
    best_match : str or None — best candidate above threshold, else None
    score      : float      — match score 0–100
    top_others : list       — top alternatives [(name, score), ...]
    """
    if not candidates:
        return None, 0.0, []

    norm_map: dict[str, str] = {}
    for c in candidates:
        norm_map[normalize_name(c)] = c

    norm_query = normalize_name(query)
    norm_keys  = list(norm_map.keys())

    if _HAS_RAPIDFUZZ:
        results = rf_process.extract(
            norm_query, norm_keys, scorer=fuzz.token_sort_ratio, limit=5
        )
        top = [(norm_map[r[0]], float(r[1])) for r in results if r[0] in norm_map]
    else:
        # Simple fallback: score by common characters
        def simple_score(a: str, b: str) -> float:
            if a == b:
                return 100.0
            if a in b or b in a:
                return 90.0
            common = sum(1 for c in a if c in b)
            return common / max(len(a), len(b)) * 100
        scored = sorted([(norm_map[k], simple_score(norm_query, k)) for k in norm_keys],
                        key=lambda x: -x[1])
        top = scored[:5]

    if not top:
        return None, 0.0, []

    best_name, best_score = top[0]
    others = top[1:]

    if best_score >= threshold:
        return best_name, best_score, others

    return None, best_score, top
