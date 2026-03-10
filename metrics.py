"""
metrics.py — Stat computations: avg, median, min, max, stddev, hit rates.
Also handles implied probability and edge calculation for the value finder.
"""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

# All supported stat types
STAT_TYPES = ["PTS", "REB", "AST", "FG3M", "STL", "BLK", "TOV", "PRA", "PR", "PA", "RA",
              "PTS_1Q", "REB_1Q", "AST_1Q", "PTS_F3", "REB_F3", "AST_F3"]

# Combo stats and their components
COMBO_DEFS: dict[str, list[str]] = {
    "PRA": ["PTS", "REB", "AST"],
    "PR":  ["PTS", "REB"],
    "PA":  ["PTS", "AST"],
    "RA":  ["REB", "AST"],
}


def add_combo_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived combo-stat columns (PRA, PR, PA, RA)."""
    df = df.copy()
    for combo, components in COMBO_DEFS.items():
        if combo not in df.columns and all(c in df.columns for c in components):
            df[combo] = df[components].sum(axis=1)
    return df


def compute_metrics(
    df: pd.DataFrame,
    n_games: int,
    line_map: Optional[dict[str, list[float]]] = None,
) -> dict:
    """
    Compute stats for the last n_games rows of df (sorted newest-first).

    Parameters
    ----------
    df       : game log DataFrame
    n_games  : window size (5, 10, or 20)
    line_map : {stat_type: [line_value, ...]} prop lines to compare against

    Returns
    -------
    Flat dict of all computed metrics.
    """
    df = add_combo_stats(df)
    # Use standard head(n_games) for aggregate stats (avg, median, std).
    # Hit rates are computed separately below using per-stat dropna().head(n)
    # to match the L5 strip behavior exactly.
    recent = df.head(n_games).copy()

    if recent.empty:
        return {}

    # Minutes average
    if "MIN" in recent.columns:
        minutes_avg = round(float(pd.to_numeric(recent["MIN"], errors="coerce").mean()), 2)
    else:
        minutes_avg = None

    result: dict = {
        "games_count": len(recent),
        "minutes_avg": minutes_avg,
    }

    for stat in STAT_TYPES:
        if stat not in df.columns:
            continue

        # For aggregate stats: use recent window (may have fewer values if nulls present)
        vals = pd.to_numeric(recent[stat], errors="coerce").dropna()
        if vals.empty:
            continue

        result[f"{stat}_avg"]    = round(float(vals.mean()), 2)
        result[f"{stat}_median"] = round(float(vals.median()), 2)
        result[f"{stat}_min"]    = round(float(vals.min()), 2)
        result[f"{stat}_max"]    = round(float(vals.max()), 2)
        result[f"{stat}_std"]    = round(float(vals.std(ddof=0)) if len(vals) > 1 else 0.0, 2)

        # Hit rates: use dropna().head(n) — skip nulls first, then take n games.
        # This matches the L5 strip exactly. A DNP row doesn't steal a slot.
        if line_map and stat in line_map:
            hr_vals = pd.to_numeric(df[stat], errors="coerce").dropna().head(n_games)
            if hr_vals.empty:
                continue
            lines_for_stat = line_map[stat]
            multi = len(lines_for_stat) > 1
            for i, line_val in enumerate(lines_for_stat, start=1):
                suffix = str(i) if multi else ""
                result[f"{stat}_line{suffix}"]     = line_val
                result[f"{stat}_hit_rate{suffix}"] = round(float((hr_vals > line_val).mean()), 3)

    return result


def build_stat_columns() -> list[str]:
    """Ordered list of all base metric column headers (no line/hit_rate)."""
    base = ["player_name", "team", "games_count", "minutes_avg"]
    for stat in STAT_TYPES:
        for suffix in ["avg", "median", "min", "max", "std"]:
            base.append(f"{stat}_{suffix}")
    return base


# ── Value Finder: implied probability & edge ──────────────────────────────────

def american_odds_to_implied_prob(odds: float) -> Optional[float]:
    """
    Convert American odds to implied probability (0–1), including vig.

    Examples:
      -110 → 0.524 (52.4%)
      +120 → 0.455 (45.5%)
      -115 → 0.535 (53.5%)
    """
    try:
        odds = float(odds)
    except (TypeError, ValueError):
        return None

    if odds == 0:
        return None
    if odds < 0:
        return round(-odds / (-odds + 100), 4)
    else:
        return round(100 / (odds + 100), 4)


def compute_edge(hit_rate: float, implied_prob: float) -> float:
    """
    Edge = your hit rate minus the book's implied probability.
    Positive = you have an edge on the over.
    Negative = book has the edge (or fade opportunity).
    """
    return round(hit_rate - implied_prob, 4)


def edge_signal(edge: float, threshold: float = 0.05) -> str:
    """
    Return a simple signal based on edge size.
    threshold of 0.05 = 5% edge required to flag as a bet.
    """
    if edge >= threshold:
        return "✅ Bet"
    elif edge <= -threshold:
        return "❌ Fade"
    else:
        return "⚪ No Edge"
