"""
dfs_projections.py — Fantasy point projection engine for DFS optimizer.

Converts your existing NBA props model data (medians, averages, context,
injury intelligence, blowout risk) into a single projected fantasy point
number per player per platform.

Scoring formulas are exact DraftKings and FanDuel NBA rules.
"""
from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ── Scoring formulas ──────────────────────────────────────────────────────────

DK_SCORING = {
    "PTS":  1.0,
    "REB":  1.25,
    "AST":  1.5,
    "STL":  2.0,
    "BLK":  2.0,
    "TOV": -0.5,
    "FG3M": 0.5,   # bonus on top of PTS
    # Double-double bonus: +1.5 (any combo of 2+ cats >= 10)
    # Triple-double bonus: +3.0 (total, not additional)
}

FD_SCORING = {
    "PTS":  1.0,
    "REB":  1.2,
    "AST":  1.5,
    "STL":  3.0,
    "BLK":  3.0,
    "TOV": -1.0,
    # No 3PM bonus, no DD/TD bonus on FanDuel
}

# DraftKings NBA lineup structure
DK_POSITIONS = {
    "PG": 1, "SG": 1, "SF": 1, "PF": 1, "C": 1,
    "G": 1,   # PG or SG
    "F": 1,   # SF or PF
    "UTIL": 1  # any position
}
DK_SALARY_CAP = 50_000
DK_ROSTER_SIZE = 8

# FanDuel NBA lineup structure
FD_POSITIONS = {
    "PG": 2, "SG": 2, "SF": 2, "PF": 2, "C": 1
}
FD_SALARY_CAP = 60_000
FD_ROSTER_SIZE = 9

# Pick6 / Best Ball — no salary cap, pick top player per tier
PICK6_TIERS = ["PG", "SG", "SF", "PF", "C", "FLEX"]


def compute_dk_points(stats: dict) -> float:
    """Compute expected DraftKings fantasy points from projected stat line."""
    pts  = stats.get("PTS", 0) or 0
    reb  = stats.get("REB", 0) or 0
    ast  = stats.get("AST", 0) or 0
    stl  = stats.get("STL", 0) or 0
    blk  = stats.get("BLK", 0) or 0
    tov  = stats.get("TOV", 0) or 0
    fg3m = stats.get("FG3M", 0) or 0

    fpts = (pts * 1.0 + reb * 1.25 + ast * 1.5 +
            stl * 2.0 + blk * 2.0 + tov * -0.5 + fg3m * 0.5)

    # Double-double bonus: 2+ stats >= 10
    dd_cats = sum([pts >= 10, reb >= 10, ast >= 10, stl >= 10, blk >= 10])
    if dd_cats >= 3:
        fpts += 3.0   # triple-double
    elif dd_cats >= 2:
        fpts += 1.5   # double-double

    return round(fpts, 2)


def compute_fd_points(stats: dict) -> float:
    """Compute expected FanDuel fantasy points from projected stat line."""
    pts = stats.get("PTS", 0) or 0
    reb = stats.get("REB", 0) or 0
    ast = stats.get("AST", 0) or 0
    stl = stats.get("STL", 0) or 0
    blk = stats.get("BLK", 0) or 0
    tov = stats.get("TOV", 0) or 0

    return round(pts * 1.0 + reb * 1.2 + ast * 1.5 +
                 stl * 3.0 + blk * 3.0 + tov * -1.0, 2)


def project_stat(log: pd.DataFrame, stat: str,
                 ctx: dict, blowout: Optional[dict] = None) -> Optional[float]:
    """
    Project a single stat for tonight using weighted windows + context.

    Weighting: L5 (45%) + L10 (35%) + L20 (20%), median-anchored.
    Context adjustments: matchup, B2B, minutes trend, blowout risk.
    """
    if log.empty or stat not in log.columns:
        return None

    vals = pd.to_numeric(log[stat], errors="coerce").dropna()
    if len(vals) < 3:
        return None

    def window_median(n):
        w = vals.head(n)
        return float(w.median()) if len(w) >= 2 else None

    m5  = window_median(5)
    m10 = window_median(10)
    m20 = window_median(20)

    # Weighted median projection
    weights, medians = [], []
    for m, w in [(m5, 0.45), (m10, 0.35), (m20, 0.20)]:
        if m is not None:
            medians.append(m)
            weights.append(w)

    if not medians:
        return None

    total_w = sum(weights)
    proj = sum(m * w for m, w in zip(medians, weights)) / total_w

    # ── Context adjustments ───────────────────────────────────────────────────

    # Matchup quality
    matchup = ctx.get(f"opp_{stat.lower()}_matchup", ctx.get("opp_pts_matchup", ""))
    if "🟢" in str(matchup):   proj *= 1.06
    elif "🔴" in str(matchup): proj *= 0.94

    # Back-to-back fatigue
    if ctx.get("is_back_to_back") == "🔴 YES":
        mins = ctx.get("minutes_l5_avg") or 0
        if mins >= 35:   proj *= 0.92
        elif mins >= 28: proj *= 0.95
        else:            proj *= 0.98

    # Trending
    trend = ctx.get(f"{stat}_trend", "")
    if "📈" in str(trend):   proj *= 1.03
    elif "📉" in str(trend): proj *= 0.97

    # Blowout risk — suppresses counting stats
    if blowout:
        bl = blowout.get("level", "LOW")
        mins = ctx.get("minutes_l5_avg") or 0
        if bl == "EXTREME" and mins < 30: proj *= 0.80
        elif bl == "HIGH" and mins < 30:  proj *= 0.87
        elif bl == "MODERATE":            proj *= 0.95

    return round(max(proj, 0), 2)


def build_player_projection(
    player_name: str,
    team: str,
    log: pd.DataFrame,
    ctx: dict,
    blowout: Optional[dict],
    opportunity: Optional[dict],
    salary_dk: Optional[int] = None,
    salary_fd: Optional[int] = None,
    position_dk: Optional[str] = None,
    position_fd: Optional[str] = None,
) -> dict:
    """
    Build complete DFS projection for one player.
    Returns dict with proj_dk, proj_fd, proj_stats, value_dk, value_fd.
    """
    stats_to_project = ["PTS", "REB", "AST", "STL", "BLK", "TOV", "FG3M"]

    proj_stats = {}
    for stat in stats_to_project:
        p = project_stat(log, stat, ctx, blowout)
        if p is not None:
            proj_stats[stat] = p

    if not proj_stats:
        return {}

    # Injury opportunity bump — if a key player is out, scale up stats
    if opportunity:
        opp_level = opportunity.get("opp_level", "")
        multiplier = 1.0
        if "MAJOR" in opp_level:   multiplier = 1.18
        elif "SOLID" in opp_level: multiplier = 1.09
        elif "MINOR" in opp_level: multiplier = 1.04
        if multiplier > 1.0:
            for stat in ["PTS", "REB", "AST"]:
                if stat in proj_stats:
                    proj_stats[stat] = round(proj_stats[stat] * multiplier, 2)

    proj_dk = compute_dk_points(proj_stats)
    proj_fd = compute_fd_points(proj_stats)

    # Value = projected points per $1000 of salary (standard DFS metric)
    value_dk = round(proj_dk / (salary_dk / 1000), 2) if salary_dk else None
    value_fd = round(proj_fd / (salary_fd / 1000), 2) if salary_fd else None

    # Ceiling and floor from distribution
    pts_vals = pd.to_numeric(log.get("PTS", pd.Series()), errors="coerce").dropna() if not log.empty else pd.Series()

    return {
        "player_name":  player_name,
        "team":         team,
        "position_dk":  position_dk,
        "position_fd":  position_fd,
        "salary_dk":    salary_dk,
        "salary_fd":    salary_fd,
        "proj_stats":   proj_stats,
        "proj_dk":      proj_dk,
        "proj_fd":      proj_fd,
        "value_dk":     value_dk,
        "value_fd":     value_fd,
        "mins_avg":     ctx.get("minutes_l5_avg"),
        "is_b2b":       ctx.get("is_back_to_back", "") == "🔴 YES",
        "blowout_level": blowout.get("level", "UNKNOWN") if blowout else "UNKNOWN",
        "has_opp":      bool(opportunity),
        "opp_level":    opportunity.get("opp_level", "") if opportunity else "",
    }
