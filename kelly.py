"""
kelly.py — Kelly Criterion bet sizing and correlated parlay adjustment.

Kelly Criterion: the mathematically optimal fraction of your bankroll to bet
given your edge and the odds. Used by every serious long-term bettor.

  Full Kelly: f = (bp - q) / b
    where b = decimal odds - 1 (profit per unit)
          p = your true probability (hit rate)
          q = 1 - p

  We default to QUARTER Kelly (25% of full Kelly) because:
    1. Our p estimates have uncertainty — overestimating edge is common
    2. Even professional sports bettors use half or quarter Kelly to manage
       variance and avoid ruin from model error

Correlated Parlay Adjustment:
  Standard parlay math assumes legs are independent. NBA props are NOT:
    - Jokic PTS + Jokic REB: highly correlated (same player, same game)
    - Jokic PTS + Porter REB: positively correlated (same team's offense)
    - LeBron PTS + opposing player PTS: slight negative correlation (pace)
    - Two players on different games: nearly independent

  Ignoring correlation inflates the apparent edge of same-team same-player
  parlays. This module adjusts for known correlation patterns.
"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Kelly fraction to use (0.25 = quarter Kelly, recommended)
KELLY_FRACTION = 0.25

# Maximum recommended bet size as % of bankroll (regardless of Kelly output)
MAX_BET_PCT = 5.0

# Minimum edge required before recommending any bet
MIN_EDGE_TO_BET = 0.04   # 4% edge


# ── Kelly Criterion ────────────────────────────────────────────────────────────

def full_kelly(
    hit_rate: float,
    american_odds: float,
) -> Optional[float]:
    """
    Compute full Kelly fraction for a single bet.

    Parameters
    ----------
    hit_rate      : your estimated probability of winning (0-1)
    american_odds : American odds (e.g. -110, +120)

    Returns
    -------
    Kelly fraction (0-1) as a fraction of bankroll.
    Returns None if odds are invalid or edge is negative.
    """
    try:
        odds = float(american_odds)
    except (TypeError, ValueError):
        return None

    # Convert to decimal
    if odds >= 100:
        decimal = odds / 100.0 + 1.0
    else:
        decimal = 100.0 / abs(odds) + 1.0

    b = decimal - 1.0    # profit per unit bet
    p = float(hit_rate)
    q = 1.0 - p

    if b <= 0 or p <= 0:
        return None

    kelly = (b * p - q) / b

    # Negative Kelly = no edge
    if kelly <= 0:
        return None

    return round(kelly, 4)


def recommended_bet(
    hit_rate: float,
    american_odds: float,
    bankroll: float = 1000.0,
    kelly_fraction: float = KELLY_FRACTION,
    max_pct: float = MAX_BET_PCT,
) -> dict:
    """
    Full bet sizing recommendation for a single prop.

    Parameters
    ----------
    hit_rate      : your estimated probability of winning (0-1)
    american_odds : American odds
    bankroll      : your total bankroll in dollars
    kelly_fraction: fraction of Kelly to use (default: 0.25 = quarter Kelly)
    max_pct       : hard cap as % of bankroll

    Returns
    -------
    dict with: kelly_full, kelly_fractional, bet_units, bet_dollars,
               edge_pct, implied_prob, ev_pct, signal
    """
    from line_shopping import american_to_implied_prob, calculate_ev

    try:
        odds = float(american_odds)
    except (TypeError, ValueError):
        return {"signal": "No odds data", "bet_units": 0, "bet_dollars": 0}

    implied_prob = american_to_implied_prob(odds)
    edge         = hit_rate - implied_prob
    ev           = calculate_ev(hit_rate, odds)

    if edge < MIN_EDGE_TO_BET:
        return {
            "signal":        f"⚪ No bet (edge {edge:.1%} < minimum {MIN_EDGE_TO_BET:.0%})",
            "edge_pct":      round(edge * 100, 1),
            "ev_pct":        round(ev * 100, 1),
            "implied_prob":  round(implied_prob * 100, 1),
            "bet_units":     0,
            "bet_dollars":   0,
            "kelly_full":    None,
            "kelly_fractional": None,
        }

    kelly_f = full_kelly(hit_rate, odds)
    if kelly_f is None:
        return {
            "signal":      "⚪ No edge",
            "edge_pct":    round(edge * 100, 1),
            "bet_units":   0,
            "bet_dollars": 0,
        }

    # Apply fraction and hard cap
    fractional = kelly_f * kelly_fraction
    capped     = min(fractional, max_pct / 100.0)
    bet_dollars = round(bankroll * capped, 2)

    # Unit sizing: 1 unit = 1% of bankroll by convention
    bet_units = round(capped * 100, 2)

    # Signal label
    if ev >= 0.15:
        signal = f"🔥 Strong bet ({bet_units:.1f}u)"
    elif ev >= 0.08:
        signal = f"✅ Good bet ({bet_units:.1f}u)"
    elif ev >= MIN_EDGE_TO_BET:
        signal = f"〰 Lean ({bet_units:.1f}u)"
    else:
        signal = "⚪ No bet"

    return {
        "signal":           signal,
        "edge_pct":         round(edge * 100, 1),
        "ev_pct":           round(ev * 100, 1),
        "implied_prob":     round(implied_prob * 100, 1),
        "kelly_full":       round(kelly_f * 100, 2),       # as % of bankroll
        "kelly_fractional": round(fractional * 100, 2),    # after fraction
        "kelly_capped":     round(capped * 100, 2),        # after hard cap
        "bet_units":        bet_units,
        "bet_dollars":      bet_dollars,
        "bankroll_used":    round(capped * 100, 1),        # % of bankroll
    }


# ── Correlated Parlay Adjustment ───────────────────────────────────────────────

# Correlation estimates between stats for same player, same team, different teams.
# Scale: 0.0 = independent, 1.0 = perfect positive, -1.0 = perfect negative.
# These are informed estimates; true correlation varies by player/matchup.

_SAME_PLAYER_CORRELATION = {
    # Same player, same game — high positive correlation
    ("PTS", "REB"):  0.30,
    ("PTS", "AST"):  0.25,
    ("PTS", "PRA"):  0.85,
    ("PTS", "PR"):   0.75,
    ("PTS", "PA"):   0.70,
    ("REB", "AST"):  0.15,
    ("REB", "PRA"):  0.80,
    ("AST", "PRA"):  0.75,
    ("PTS", "FG3M"): 0.65,   # scoring props very correlated with 3PM
    ("FG3M", "PRA"): 0.60,
    ("STL", "AST"):  0.20,
    ("BLK", "REB"):  0.25,
    # TOV is mildly positively correlated with usage (PTS, AST) — more ball handling = more turnovers
    ("TOV", "AST"):  0.25,
    ("TOV", "PTS"):  0.20,
}

_SAME_TEAM_CORRELATION = {
    # Two different players on the same team — usage correlation
    ("PTS", "PTS"):  -0.15,  # slight negative — if one player scores big, slightly less for others
    ("REB", "REB"):  -0.10,
    ("AST", "AST"):  -0.05,  # assists shared but weakly
    ("PTS", "REB"):   0.05,
    ("PTS", "AST"):   0.05,
}

_DIFFERENT_TEAM_CORRELATION = {
    # Same game, different teams — game pace correlation
    ("PTS", "PTS"):  0.10,   # high pace games boost both teams
    ("REB", "REB"):  0.05,
    ("PTS", "REB"):  0.03,
}


def get_correlation(
    stat_a: str,
    stat_b: str,
    player_a: str,
    player_b: str,
    team_a: str,
    team_b: str,
    game_id_a: Optional[str] = None,
    game_id_b: Optional[str] = None,
) -> float:
    """
    Estimate correlation between two prop legs.

    Returns a float from -1.0 to 1.0.
    0.0 means independent (different games or no known correlation).
    """
    # Different games = independent
    if game_id_a and game_id_b and game_id_a != game_id_b:
        return 0.0
    if not game_id_a or not game_id_b:
        # If we don't know game IDs, use team as proxy
        if team_a != team_b:
            return 0.0

    stat_a_up = stat_a.upper()
    stat_b_up = stat_b.upper()

    same_player = player_a == player_b
    same_team   = team_a == team_b

    def _lookup(d: dict, s1: str, s2: str) -> Optional[float]:
        return d.get((s1, s2)) or d.get((s2, s1))

    if same_player:
        corr = _lookup(_SAME_PLAYER_CORRELATION, stat_a_up, stat_b_up)
        return corr if corr is not None else 0.15  # default same-player = mildly correlated

    if same_team:
        corr = _lookup(_SAME_TEAM_CORRELATION, stat_a_up, stat_b_up)
        return corr if corr is not None else -0.05

    # Different teams, same game
    corr = _lookup(_DIFFERENT_TEAM_CORRELATION, stat_a_up, stat_b_up)
    return corr if corr is not None else 0.05


def adjusted_parlay_probability(legs: list[dict]) -> dict:
    """
    Compute the true probability of a parlay hitting, accounting for
    correlations between legs.

    Standard parlay math: P = p1 × p2 × ... × pN
    Correlation-adjusted: P = standard P × correlation_factor

    Parameters
    ----------
    legs : list of dicts, each with:
        player_name, stat, team, game_id (optional),
        hit_rate (0-1), american_odds (optional), line

    Returns
    -------
    dict with:
        standard_prob:   naive independent probability
        adjusted_prob:   correlation-adjusted probability
        correlation_adj: the multiplier applied (>1 = correlation helps, <1 = hurts)
        legs_analysis:   per-pair correlation breakdown
        ev_adjusted:     expected value at standard -110 odds for the parlay
        recommendation:  signal string
        warning:         any correlation warnings
    """
    if not legs:
        return {}

    n = len(legs)
    # Standard independent probability
    standard_prob = 1.0
    for leg in legs:
        hr = leg.get("hit_rate")
        if hr is None:
            return {"error": "Missing hit_rate in one or more legs"}
        standard_prob *= float(hr)

    # Correlation adjustment using pairwise correlations
    # Method: bivariate normal approximation
    # For small correlations: P_adj ≈ P_indep × (1 + sum of pairwise adjustments)
    total_corr_adj = 0.0
    pair_analysis  = []
    warnings       = []

    for i in range(n):
        for j in range(i + 1, n):
            leg_a = legs[i]
            leg_b = legs[j]

            corr = get_correlation(
                stat_a    = leg_a.get("stat", ""),
                stat_b    = leg_b.get("stat", ""),
                player_a  = leg_a.get("player_name", ""),
                player_b  = leg_b.get("player_name", ""),
                team_a    = leg_a.get("team", ""),
                team_b    = leg_b.get("team", ""),
                game_id_a = str(leg_a.get("game_id", "")),
                game_id_b = str(leg_b.get("game_id", "")),
            )

            # Correlation adjustment contribution
            # Derived from bivariate normal approximation for correlated Bernoulli
            hr_a = float(leg_a["hit_rate"])
            hr_b = float(leg_b["hit_rate"])
            adj  = corr * (hr_a * (1 - hr_a) * hr_b * (1 - hr_b)) ** 0.5

            total_corr_adj += adj

            pair_label = (
                f"{leg_a.get('player_name','')} {leg_a.get('stat','')} × "
                f"{leg_b.get('player_name','')} {leg_b.get('stat','')}"
            )
            pair_analysis.append({
                "pair":        pair_label,
                "correlation": round(corr, 3),
                "adjustment":  round(adj, 4),
            })

            if corr >= 0.5:
                warnings.append(
                    f"⚠️ HIGH CORRELATION ({corr:.0%}): {pair_label} — "
                    "these legs are not independent. True edge may be overstated."
                )
            elif corr >= 0.3:
                warnings.append(
                    f"🟡 MODERATE CORRELATION ({corr:.0%}): {pair_label}"
                )

    adjusted_prob = standard_prob + total_corr_adj
    adjusted_prob = max(0.001, min(0.999, adjusted_prob))  # clamp

    corr_factor = adjusted_prob / standard_prob if standard_prob > 0 else 1.0

    # Implied parlay odds at standard -110 per leg (DK default)
    # Parlay payout: multiply each leg's decimal odds
    decimal_product = 1.0
    for leg in legs:
        odds = leg.get("american_odds")
        if odds:
            try:
                o = float(odds)
                decimal_product *= (o / 100 + 1) if o >= 100 else (100 / abs(o) + 1)
            except Exception:
                decimal_product *= 1.91  # assume -110

    ev_adjusted = (adjusted_prob * (decimal_product - 1)) - (1 - adjusted_prob)
    ev_standard = (standard_prob  * (decimal_product - 1)) - (1 - standard_prob)

    # Recommendation
    if adjusted_prob >= 0.65:
        rec = f"✅ Strong parlay ({adjusted_prob:.0%} adj. prob)"
    elif adjusted_prob >= 0.55:
        rec = f"〰 Lean parlay ({adjusted_prob:.0%} adj. prob)"
    elif adjusted_prob >= 0.45:
        rec = f"⚪ Marginal ({adjusted_prob:.0%} adj. prob)"
    else:
        rec = f"❌ Skip — low probability ({adjusted_prob:.0%})"

    return {
        "n_legs":          n,
        "standard_prob":   round(standard_prob * 100, 1),
        "adjusted_prob":   round(adjusted_prob * 100, 1),
        "corr_factor":     round(corr_factor, 4),
        "total_corr_adj":  round(total_corr_adj, 4),
        "ev_standard":     round(ev_standard * 100, 1),
        "ev_adjusted":     round(ev_adjusted * 100, 1),
        "decimal_payout":  round(decimal_product, 2),
        "implied_payout_x": round(decimal_product, 1),
        "legs_analysis":   pair_analysis,
        "recommendation":  rec,
        "warnings":        warnings,
    }


def size_parlay(
    legs: list[dict],
    bankroll: float = 1000.0,
    kelly_fraction: float = KELLY_FRACTION,
) -> dict:
    """
    Full Kelly sizing for a parlay, accounting for leg correlations.

    Returns recommended bet size in dollars and units.
    """
    analysis = adjusted_parlay_probability(legs)
    if "error" in analysis:
        return analysis

    adj_prob = analysis["adjusted_prob"] / 100.0
    decimal_payout = analysis.get("decimal_payout", 4.0)

    # Kelly on the parlay as a single bet
    b = decimal_payout - 1.0
    p = adj_prob
    q = 1.0 - p

    if b <= 0 or p <= 0:
        return {**analysis, "bet_dollars": 0, "bet_units": 0, "signal": "No edge"}

    kelly_f = (b * p - q) / b
    if kelly_f <= 0:
        return {**analysis, "bet_dollars": 0, "bet_units": 0,
                "signal": f"⚪ No edge (Kelly={kelly_f:.1%})"}

    fractional = kelly_f * kelly_fraction
    capped     = min(fractional, 0.03)  # never more than 3% of bankroll on a parlay
    bet_dollars = round(bankroll * capped, 2)
    bet_units   = round(capped * 100, 2)

    return {
        **analysis,
        "kelly_full":       round(kelly_f * 100, 2),
        "kelly_fractional": round(fractional * 100, 2),
        "bet_units":        bet_units,
        "bet_dollars":      bet_dollars,
        "bankroll_used":    round(capped * 100, 1),
        "signal":           f"{'✅' if kelly_f >= 0.05 else '〰'} {bet_units:.1f}u (${bet_dollars:.0f}) on {analysis['n_legs']}-leg parlay",
    }
