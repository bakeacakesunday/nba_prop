"""
thresholds.py — Single source of truth for L20 hit-rate thresholds.

Three contexts use L20 thresholds and they used to be defined separately
with different scales and inconsistent values. Everything imports from here.

Context map
-----------
L20_VETO_FLOOR   Used by app.py _compute_dist_profile to hard-DQ props
                 that are genuine outliers on the low end (mean - 1.5 SD
                 of the empirical L20 distribution). Very permissive —
                 only catches clear statistical outliers.

L20_EDGE_MIN     Used by scoring.py _check_hard_vetoes. Slightly tighter
                 than the parlay floor — props below this threshold skip
                 the edge path entirely because the hit-rate history
                 doesn't support the direction.

L20_PARLAY_MIN   Used by app.py _parlay_is_clean. Highest bar — a 50%
                 L20 is a coin flip over the real sample, not a parlay leg.

All values are in 0-1 (fraction) scale. Use normalize_rate() to accept
either scale safely.

Regression spec (must stay true):
  RA  59% → parlay BLOCKED   (59 < 60)
  RA  60% → parlay PASSES    (60 >= 60)
  PTS 54% → parlay BLOCKED   (54 < 55)
  PTS 55% → parlay PASSES    (55 >= 55)
  BLK 49% → parlay BLOCKED   (49 < 50)
  BLK 50% → parlay PASSES    (50 >= 50)
"""
from __future__ import annotations
from typing import Optional

# ── Empirical veto floor (mean - 1.5 SD per stat) ────────────────────────────
L20_VETO_FLOOR: dict[str, float] = {
    "AST":  0.12,   # mean=37.6% sd=16.9%
    "BLK":  0.10,   # mean=26.7% sd=17.0% — noisy, low floor
    "FG3M": 0.10,   # mean=34.8% sd=16.6%
    "PA":   0.12,
    "PR":   0.15,
    "PRA":  0.13,
    "PTS":  0.14,
    "RA":   0.13,
    "REB":  0.12,
    "STL":  0.13,
}

# ── Scoring veto — props below this skip the edge path ───────────────────────
L20_EDGE_MIN: dict[str, float] = {
    "AST":  0.45, "REB": 0.45, "RA": 0.45,
    "PTS":  0.40, "PR":  0.40, "PRA": 0.40, "PA": 0.40,
    "FG3M": 0.40, "BLK": 0.35, "STL": 0.38,
}

# ── Parlay eligibility floor ──────────────────────────────────────────────────
L20_PARLAY_MIN: dict[str, float] = {
    "AST":  0.60,
    "BLK":  0.50,   # blocks are noisier, slightly more lenient
    "FG3M": 0.55,
    "PA":   0.55,
    "PR":   0.55,
    "PRA":  0.55,
    "PTS":  0.55,
    "RA":   0.60,
    "REB":  0.60,
    "STL":  0.55,
}

# Legacy aliases — do not remove, referenced by existing code
L20_THRESHOLDS = L20_PARLAY_MIN          # generic alias used in spec examples
_L20_WEAK_THRESHOLDS_PARLAY = L20_PARLAY_MIN  # app.py backward compat


# ── Helpers ───────────────────────────────────────────────────────────────────

def normalize_rate(value: Optional[float]) -> Optional[float]:
    """
    Accept either fraction scale (0.55) or percent scale (55.0), return 0-1.
    Returns None if value is None.
    """
    if value is None:
        return None
    return value / 100.0 if value > 1.0 else float(value)


def l20_threshold_for_stat(stat: str, context: str = "parlay",
                            default: float = 0.55) -> float:
    """
    Return the L20 threshold for a given stat and context.

    context:
        "veto"   — empirical floor DQ (most permissive)
        "edge"   — scoring veto (mid)
        "parlay" — parlay eligibility (strictest, default)

    Returns threshold as a 0-1 fraction.
    """
    stat_up = (stat or "").upper()
    if context == "veto":
        return L20_VETO_FLOOR.get(stat_up, 0.12)
    elif context == "edge":
        return L20_EDGE_MIN.get(stat_up, 0.40)
    else:   # parlay
        return L20_PARLAY_MIN.get(stat_up, default)


def is_l20_below_threshold(l20_rate: Optional[float], stat: str,
                            context: str = "parlay") -> bool:
    """
    Return True if l20_rate is below the threshold for this stat/context.
    Accepts either 0-1 or 0-100 scale for l20_rate.
    Returns False (passes) when l20_rate is None (insufficient sample).
    """
    if l20_rate is None:
        return False
    norm = normalize_rate(l20_rate)
    thresh = l20_threshold_for_stat(stat, context)
    return norm < thresh
