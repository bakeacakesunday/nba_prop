"""
distribution.py — Outcome distribution analysis and hook number detection.

The core insight: books set lines at hook numbers — just past a player's
most common outcome. Averages lie. Medians and distributions tell the truth.

Key concepts:
  - Modal outcome: the single most common result (where he "lives")
  - Hook zone: line within 0.5 of the modal outcome = danger
  - Distribution skew: does he boom/bust or grind consistent numbers?
  - Push frequency: how often does he hit exactly the line?
  - True over rate: at THIS specific line, how often does he actually go over?
  - Median vs mean gap: large gap = avg is misleading, use median
"""
from __future__ import annotations

import logging
from collections import Counter
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ── Core distribution metrics ─────────────────────────────────────────────────

def compute_distribution(
    values: pd.Series,
    line: float,
) -> dict:
    """
    Full distribution analysis for a player stat vs a specific line.

    Returns everything needed to evaluate line quality and hook risk.
    """
    vals = pd.to_numeric(values, errors="coerce").dropna()

    if len(vals) < 3:
        return {"error": "insufficient data"}

    arr = vals.values
    n   = len(arr)

    # ── Basic stats ───────────────────────────────────────────────────────────
    mean   = float(np.mean(arr))
    median = float(np.median(arr))
    std    = float(np.std(arr, ddof=0))
    q25    = float(np.percentile(arr, 25))
    q75    = float(np.percentile(arr, 75))

    # ── Modal outcome (rounded to nearest integer) ────────────────────────────
    rounded = [round(v) for v in arr]
    counts  = Counter(rounded)
    modal_outcome = counts.most_common(1)[0][0]
    modal_freq    = counts.most_common(1)[0][1]
    modal_pct     = modal_freq / n

    # Top 3 most common outcomes
    top_outcomes = [
        {"value": val, "count": cnt, "pct": round(cnt/n*100, 1)}
        for val, cnt in counts.most_common(3)
    ]

    # ── Line-specific metrics ─────────────────────────────────────────────────
    over_count  = sum(1 for v in arr if v > line)
    under_count = sum(1 for v in arr if v < line)
    push_count  = sum(1 for v in arr if v == line)

    # Near-miss: went over by less than 1 (would have lost on a half-point higher line)
    near_miss_over  = sum(1 for v in arr if 0 < v - line < 1)
    # Near-win: missed under by less than 1 (would have won on a half-point lower line)
    near_miss_under = sum(1 for v in arr if 0 < line - v < 1)

    true_over_rate  = round(over_count  / n, 3)
    true_under_rate = round(under_count / n, 3)
    push_rate       = round(push_count  / n, 3)

    # ── Hook number detection ─────────────────────────────────────────────────
    # A hook number is dangerous when the line sits just above a common outcome
    # i.e. modal outcome is just below the line (0 to 1 points below)
    distance_from_modal = line - modal_outcome  # positive = line is above modal

    hook_severity = _classify_hook(
        line             = line,
        modal_outcome    = modal_outcome,
        median           = median,
        mean             = mean,
        near_miss_over   = near_miss_over,
        near_miss_under  = near_miss_under,
        n                = n,
    )

    # ── Median vs mean divergence ─────────────────────────────────────────────
    mean_median_gap   = round(mean - median, 2)
    mean_misleading   = abs(mean_median_gap) >= 1.5

    # Which is more favorable for the over?
    if mean > median + 1.5:
        avg_skew = "📈 Mean inflated by outliers — median more reliable"
    elif median > mean + 1.5:
        avg_skew = "📈 Median above mean — consistent upside"
    else:
        avg_skew = "➡️ Mean and median aligned"

    # ── Line quality score ────────────────────────────────────────────────────
    # How well-set is this line? Higher = harder to beat
    # Book sets the line well when it's near the median and modal outcome
    line_vs_median = abs(line - median)
    if line_vs_median <= 0.5:
        line_quality = "🔴 Sharp line — right on median"
    elif line_vs_median <= 1.5:
        line_quality = "🟡 Fair line — close to median"
    else:
        line_quality = "🟢 Soft line — away from median"

    # ── Distribution shape ────────────────────────────────────────────────────
    # Skewness: positive = right tail (boom games pull avg up)
    if n >= 5:
        skewness = float(pd.Series(arr).skew())
        if skewness > 0.5:
            dist_shape = "📈 Right-skewed (big games pull avg up — median more reliable for unders)"
        elif skewness < -0.5:
            dist_shape = "📉 Left-skewed (bad games drag avg down — median more reliable for overs)"
        else:
            dist_shape = "➡️ Roughly symmetric"
    else:
        skewness   = 0.0
        dist_shape = "—"

    return {
        # Basic
        "n":              n,
        "mean":           round(mean, 1),
        "median":         round(median, 1),
        "std":            round(std, 1),
        "q25":            round(q25, 1),
        "q75":            round(q75, 1),
        "skewness":       round(skewness, 2),

        # Modal
        "modal_outcome":  modal_outcome,
        "modal_pct":      round(modal_pct * 100, 1),
        "top_outcomes":   top_outcomes,

        # Line specific
        "line":              line,
        "true_over_rate":    true_over_rate,
        "true_under_rate":   true_under_rate,
        "push_rate":         push_rate,
        "near_miss_over":    near_miss_over,
        "near_miss_under":   near_miss_under,
        "near_miss_over_pct":  round(near_miss_over / n * 100, 1),
        "near_miss_under_pct": round(near_miss_under / n * 100, 1),

        # Hook
        "distance_from_modal": round(distance_from_modal, 1),
        "hook_severity":       hook_severity,

        # Mean/median
        "mean_median_gap":  mean_median_gap,
        "mean_misleading":  mean_misleading,
        "avg_skew":         avg_skew,

        # Line quality
        "line_quality":     line_quality,
        "line_vs_median":   round(line_vs_median, 1),

        # Distribution shape
        "dist_shape":       dist_shape,
    }


def _classify_hook(
    line: float,
    modal_outcome: float,
    median: float,
    mean: float,
    near_miss_over: int,
    near_miss_under: int,
    n: int,
) -> dict:
    """
    Classify hook severity and generate a warning label.

    Hook for OVER: line is just above modal/median — you keep hitting the number
    Hook for UNDER: line is just below modal/median — you keep missing under
    """
    distance = line - modal_outcome  # positive = line above modal

    near_miss_pct = near_miss_over / n if n > 0 else 0

    # Severe hook: line is 0-0.5 above modal AND near-miss rate is high
    if 0 <= distance <= 0.5 and near_miss_pct >= 0.20:
        return {
            "level":   "🚨 SEVERE HOOK",
            "warning": f"Line sits {distance} above his most common outcome ({modal_outcome}). "
                       f"{near_miss_over}/{n} games ({near_miss_pct:.0%}) he hit just below this line.",
            "score":   -3,
            "avoid":   True,
        }

    # Strong hook: line 0-1 above modal
    elif 0 <= distance <= 1.0 and near_miss_pct >= 0.15:
        return {
            "level":   "⚠️ HOOK WARNING",
            "warning": f"Line is above his modal outcome ({modal_outcome}) by {distance}. "
                       f"Near-miss rate: {near_miss_pct:.0%}.",
            "score":   -2,
            "avoid":   False,
        }

    # Mild hook: line near modal but not extreme
    elif 0 <= distance <= 1.0:
        return {
            "level":   "🟡 MILD HOOK",
            "warning": f"Line ({line}) close to modal outcome ({modal_outcome}). Proceed with caution.",
            "score":   -1,
            "avoid":   False,
        }

    # Line well above modal — favorable for under
    elif distance > 1.0:
        return {
            "level":   "🟢 UNDER FRIENDLY",
            "warning": f"Line is {distance} above modal outcome ({modal_outcome}). Under-friendly setup.",
            "score":   -1,
            "avoid":   False,
        }

    # Line below modal — favorable for over
    elif distance < 0:
        abs_dist = abs(distance)
        if abs_dist >= 1.0:
            return {
                "level":   "🔥 PRIME OVER",
                "warning": f"Line is {abs_dist} BELOW modal outcome ({modal_outcome}). Strong over edge.",
                "score":   2,
                "avoid":   False,
            }
        else:
            return {
                "level":   "🟢 SLIGHT OVER EDGE",
                "warning": f"Line slightly below modal ({modal_outcome}). Mild over edge.",
                "score":   1,
                "avoid":   False,
            }

    return {
        "level":   "⚪ Neutral",
        "warning": "Line near modal outcome.",
        "score":   0,
        "avoid":   False,
    }


# ── Per-window distribution builder ──────────────────────────────────────────

def build_distribution_profile(
    player_log: pd.DataFrame,
    stat: str,
    line: float,
    windows: list[int] = [10, 20],
) -> dict:
    """
    Build distribution profiles for multiple lookback windows.
    Returns the most relevant window's analysis plus a combined summary.
    """
    if player_log.empty or stat not in player_log.columns:
        return {}

    results = {}
    for w in windows:
        vals = pd.to_numeric(
            player_log[stat], errors="coerce"
        ).dropna().head(w)

        if len(vals) >= 3:
            results[f"L{w}"] = compute_distribution(vals, line)

    if not results:
        return {}

    # Primary window: use L10 if available, else L20
    primary = results.get("L10") or results.get("L20") or {}

    # Build a clean summary dict for the sheet
    hook = primary.get("hook_severity", {})
    return {
        "stat":              stat,
        "line":              line,
        "median_l10":        results.get("L10", {}).get("median"),
        "median_l20":        results.get("L20", {}).get("median"),
        "mean_l10":          results.get("L10", {}).get("mean"),
        "modal_outcome":     primary.get("modal_outcome"),
        "modal_pct":         primary.get("modal_pct"),
        "true_over_rate_l10": results.get("L10", {}).get("true_over_rate"),
        "true_over_rate_l20": results.get("L20", {}).get("true_over_rate"),
        "near_miss_pct":     primary.get("near_miss_over_pct"),
        "push_rate":         primary.get("push_rate"),
        "hook_level":        hook.get("level", "—"),
        "hook_warning":      hook.get("warning", "—"),
        "hook_score":        hook.get("score", 0),
        "avoid":             hook.get("avoid", False),
        "line_quality":      primary.get("line_quality", "—"),
        "mean_median_gap":   primary.get("mean_median_gap"),
        "mean_misleading":   primary.get("mean_misleading", False),
        "avg_skew":          primary.get("avg_skew", "—"),
        "dist_shape":        primary.get("dist_shape", "—"),
        "top_outcomes":      primary.get("top_outcomes", []),
        "line_vs_median":    primary.get("line_vs_median"),
        "q25":               primary.get("q25"),
        "q75":               primary.get("q75"),
        # Standard deviation — needed for line credibility check
        "std_l10":           results.get("L10", {}).get("std"),
        "std_l20":           results.get("L20", {}).get("std"),
    }


def compute_ghost_profile(
    player_log: pd.DataFrame,
    stat: str,
    n_games: int = 20,
    min_minutes_threshold: float = 15.0,
) -> dict:
    """
    Detect players who have a historical pattern of playing real minutes
    but producing near-zero output — the 'ghost game' signature.

    This is distinct from a bad shooting night. A ghost game is when a player
    is present on the floor but invisible across the board. Examples:
    - Duncan Robinson: 25 MIN, 0 PTS, 3 REB, 2 AST
    - Payton Pritchard: 16 MIN, 0 PTS, 1 REB, 1 AST

    Three metrics computed over the last n_games with MIN >= min_minutes_threshold:

    ghost_rate:  games where stat == 0 while playing real minutes.
                 The hardest signal — they were there and produced nothing.

    floor_rate:  games where stat was <= 25% of the player's own median
                 for that stat. Captures near-zeros and severe underperformances
                 even when the absolute value isn't zero.

    zero_rate:   games where stat == 0 regardless of minutes played.
                 Broader view — includes DNP-adjacent games.

    Parameters
    ----------
    player_log          : game log sorted newest-first, with MIN and stat columns
    stat                : the stat column to analyze (PTS, AST, REB, etc.)
    n_games             : lookback window (default 20)
    min_minutes_threshold: minimum minutes to count a game as 'real playing time'
    """
    empty = {
        "ghost_rate":      None,
        "floor_rate":      None,
        "zero_rate":       None,
        "ghost_games":     0,
        "real_games":      0,
        "ghost_flag":      "—",
        "ghost_label":     "—",
    }

    if player_log.empty:
        return empty
    if stat not in player_log.columns or "MIN" not in player_log.columns:
        return empty

    window = player_log.head(n_games).copy()
    mins   = pd.to_numeric(window["MIN"],  errors="coerce").fillna(0)
    vals   = pd.to_numeric(window[stat],   errors="coerce").fillna(0)

    total_games = len(window)
    if total_games < 5:
        return empty

    # ── Zero rate (all games regardless of minutes) ───────────────────────────
    zero_games = int((vals == 0).sum())
    zero_rate  = round(zero_games / total_games, 3)

    # ── Real playing time games only ──────────────────────────────────────────
    real_mask  = mins >= min_minutes_threshold
    real_games = int(real_mask.sum())

    if real_games < 3:
        # Not enough real-minute games to compute meaningful rates
        return {**empty, "zero_rate": zero_rate, "zero_games": zero_games}

    real_vals = vals[real_mask]

    # ── Ghost rate: played real minutes, stat == 0 ────────────────────────────
    ghost_games = int((real_vals == 0).sum())
    ghost_rate  = round(ghost_games / real_games, 3)

    # ── Floor rate: played real minutes, stat <= 25% of own median ───────────
    # Use the non-zero median to avoid being skewed by the zeros themselves
    nonzero_vals = vals[vals > 0]
    if len(nonzero_vals) >= 3:
        player_median = float(nonzero_vals.median())
        floor_threshold = player_median * 0.25
        floor_games = int((real_vals <= floor_threshold).sum())
        floor_rate  = round(floor_games / real_games, 3)
    else:
        floor_rate  = None
        floor_games = 0

    # ── Label ─────────────────────────────────────────────────────────────────
    # Ghost rate is the primary signal — it requires real minutes + zero output.
    # Thresholds:
    #   0–9%  : within normal variance, no flag
    #  10–19% : elevated, worth noting (1 in 8–10 real-minutes games = ghost)
    #  20–29% : significant risk — 1 in 4-5 games they disappear
    #  30%+   : systemic issue — nearly 1 in 3 real games is a ghost
    if ghost_rate >= 0.30:
        flag  = "👻 HIGH GHOST RISK"
        label = (f"Disappeared in {ghost_games}/{real_games} real-minute games "
                 f"({ghost_rate:.0%}) — systemic underperformance risk")
    elif ghost_rate >= 0.20:
        flag  = "⚠️ GHOST RISK"
        label = (f"Produced 0 in {ghost_games}/{real_games} real-minute games "
                 f"({ghost_rate:.0%}) — elevated disappearance rate")
    elif ghost_rate >= 0.10:
        flag  = "🟡 GHOST WATCH"
        label = (f"0 output in {ghost_games}/{real_games} real-minute games "
                 f"({ghost_rate:.0%}) — slightly elevated")
    else:
        flag  = "—"
        label = "—"

    return {
        "ghost_rate":   ghost_rate,
        "floor_rate":   floor_rate,
        "zero_rate":    zero_rate,
        "ghost_games":  ghost_games,
        "real_games":   real_games,
        "ghost_flag":   flag,
        "ghost_label":  label,
    }


def format_top_outcomes(top_outcomes: list[dict]) -> str:
    """Format top outcomes as a readable string for the sheet."""
    if not top_outcomes:
        return "—"
    return " | ".join([
        f"{o['value']} ({o['pct']}%)" for o in top_outcomes
    ])


# ── Milestone distribution (DD, TD) ──────────────────────────────────────────

def compute_milestone_profile(log: "pd.DataFrame", stat: str, windows: list = None) -> dict:
    """
    For binary milestone props (DD, TD), calculate historical achievement rates
    directly from game logs — no line needed.

    DD = pts >= 10 AND (reb >= 10 OR ast >= 10)
    TD = pts >= 10 AND reb >= 10 AND ast >= 10

    Returns a profile compatible with card rendering, using true_over_rate_l10
    as the achievement rate (probability the player hits the milestone tonight).
    """
    if windows is None:
        windows = [5, 10, 20]

    if log.empty:
        return {}

    def achieved(row, s):
        try:
            pts = float(row.get("PTS", 0) or 0)
            reb = float(row.get("REB", 0) or 0)
            ast = float(row.get("AST", 0) or 0)
            if s == "DD":
                cats = sum([pts >= 10, reb >= 10, ast >= 10])
                return cats >= 2
            if s == "TD":
                return pts >= 10 and reb >= 10 and ast >= 10
        except Exception:
            pass
        return False

    records = [achieved(r, stat) for _, r in log.iterrows()]
    rates = {}
    for w in windows:
        window_records = records[:w]
        if len(window_records) >= 2:
            rates[w] = round(sum(window_records) / len(window_records), 3)

    l10_rate = rates.get(10)
    l20_rate = rates.get(20)
    l5_rate  = rates.get(5)

    # Use achievement rate as "true over rate" for card compatibility
    return {
        "stat":               stat,
        "line":               0.5,
        "median_l10":         None,   # not meaningful for binary
        "modal_outcome":      None,
        "true_over_rate_l10": l10_rate,
        "true_over_rate_l20": l20_rate,
        "near_miss_pct":      None,
        "hook_level":         "⚪ Milestone",
        "hook_warning":       "",
        "hook_score":         0,
        "avoid":              False,
        "line_quality":       "—",
        "milestone_l5_rate":  l5_rate,
        "milestone_l10_rate": l10_rate,
        "milestone_l20_rate": l20_rate,
        "is_milestone":       True,
    }
