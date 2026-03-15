"""
scoring.py — NBA player prop edge scoring engine.

ARCHITECTURE
============
A prop is likely to hit when three things are true simultaneously:
  1. The line is set below the player's true output level  (VALUE)
  2. The player is consistent and trustworthy              (RELIABILITY)
  3. Tonight's context amplifies rather than suppresses    (SITUATION)

Everything else — usage, contract year, pace, net rating — is evidence
that helps us estimate those three things more accurately. Signals that
don't reliably predict outcomes get no weight.

SCORING MODEL
=============
We do NOT use a fixed-weight layer system. Fixed weights assume all
signals are equally available for all props, which is false. Instead:

  edge_score = value_score * reliability_multiplier * situation_multiplier

  value_score         : 0–100, anchored on Z-score of (median - line) / std
                        The most predictive single number. If the line is
                        2 standard deviations below the player's median,
                        that's real edge. If it's at the median, there's none.

  reliability_multiplier : 0.0–1.3
                        Scales value score up or down based on how much we
                        trust the hit rate signal:
                          - High: CONSISTENT dist, good L10/L20, no outlier
                          - Low:  VOLATILE dist, outlier inflation, ghost risk

  situation_multiplier   : 0.7–1.2
                        Scales the result up or down based on tonight's
                        game environment. This is intentionally small —
                        situational context rarely flips a bad prop to a
                        good one. It tilts close calls.

HARD VETOES (return 0 immediately)
===================================
  - SEVERE HOOK: line is structurally wrong, not a prop at all
  - B2B: fatigue makes all counting stat overs unreliable
  - Regression risk: L5 running 45%+ hotter than L20 baseline
  - Outlier inflated: single monster game masking true baseline

OUTPUT
======
  edge_score    0–100   Final score. >65 = strong, >55 = solid, <45 = skip
  direction     OVER | UNDER | NONE
  final_call    Human-readable verdict
  value_score   Raw value before multipliers (for debugging)
  reliability   The reliability multiplier applied
  situation     The situation multiplier applied
  verdict_tags  List of the key signals that drove the score
"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

# League averages used for opponent defense normalization
_LG_AVG = {
    "PTS": 111.0, "REB": 44.0, "AST": 25.0, "FG3M": 13.5,
    "STL": 8.0,   "BLK": 5.0,  "TOV": 14.0,
    "PRA": 40.0,  "PR": 35.0,  "PA": 22.0,  "RA": 18.0,
}

_LG_PACE   = 98.5   # possessions per 48 min
_LG_DEF_RT = 112.0  # defensive rating (pts per 100 poss)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — VALUE SCORE  (0–100)
# ─────────────────────────────────────────────────────────────────────────────

def _compute_value(
    line: float,
    median_l10: Optional[float],
    median_l20: Optional[float],
    modal_outcome: Optional[float],
    std_l10: Optional[float],
    std_l20: Optional[float],
    true_over_rate_l10: Optional[float],
    true_over_rate_l20: Optional[float],
    season_avg_vs_line: Optional[float],
    l5_hr: Optional[float],
    l10_hr: Optional[float],
    l20_hr: Optional[float],
) -> tuple[float, int, str, list]:
    """
    Returns (value_score 0-100, direction_sign +1/-1, label, tags).

    Core insight: the Z-score of (median - line) / std is the single best
    predictor of whether a prop will hit. Everything else is secondary.

    Z-score interpretation:
      0.0 = line is right at the median — no edge
      0.5 = line is half a std dev below median — mild lean
      1.0 = one std dev below median — real edge
      1.5 = 1.5 std devs below — strong edge
      2.0+ = very mispriced

    We blend L10 (60%) and L20 (40%) for both median and std to balance
    recency against sample stability.
    """
    tags = []

    if median_l10 is None and median_l20 is None:
        # No distribution data — fall back to pure hit rate if available
        whr = _weighted_hit_rate(l5_hr, l10_hr, l20_hr)
        if whr is not None and whr >= 0.65:
            # Hit rate only path — less reliable but better than nothing
            # Scale 65%→100% hit rate to 20→60 value score
            vs = round(min(60.0, max(20.0, (whr - 0.65) / 0.35 * 40 + 20)), 1)
            direction = 1
            tags.append(f"hit-rate only: {whr*100:.0f}%")
            return vs, direction, f"no dist — WHR={whr*100:.0f}%", tags
        return 0.0, 0, "no data", tags

    # Blend median and std across windows
    if median_l10 is not None and median_l20 is not None:
        median = median_l10 * 0.60 + median_l20 * 0.40
    else:
        median = median_l10 if median_l10 is not None else median_l20

    if std_l10 is not None and std_l20 is not None:
        std = std_l10 * 0.60 + std_l20 * 0.40
    elif std_l10 is not None:
        std = std_l10
    else:
        std = std_l20 or 1.0
    std = max(std, 0.5)

    gap = median - line
    direction_sign = 1 if gap > 0 else (-1 if gap < 0 else 0)
    if direction_sign == 0:
        return 5.0, 0, "line at median", tags

    z = abs(gap) / std
    tags.append(f"Z={z:.2f}")

    # Z-score → base value score
    # Calibration: Z=0.5→25, Z=1.0→50, Z=1.5→70, Z=2.0→85, Z=2.5→95
    # Using a smooth curve rather than step function
    z_capped = min(z, 3.0)
    base_vs = min(95.0, (z_capped / 2.5) ** 0.75 * 90.0)

    # Modal outcome confirmation — if the MODE also agrees with direction, add up to 8 pts
    # Modal is the most common outcome bucket; when it agrees with median it's strong confirmation
    modal_boost = 0.0
    if modal_outcome is not None:
        modal_gap = modal_outcome - line
        if (direction_sign == 1 and modal_gap > 0) or (direction_sign == -1 and modal_gap < 0):
            modal_z = abs(modal_gap) / std
            modal_boost = min(8.0, modal_z * 4.0)
            tags.append(f"modal confirms {'+' if direction_sign==1 else '-'}{abs(modal_gap):.1f}")

    # True over rate confirmation — how often has the player actually exceeded this line?
    # This is direct empirical evidence. A 75% over rate on L10 is strong confirmation.
    tor_boost = 0.0
    if true_over_rate_l10 is not None or true_over_rate_l20 is not None:
        if true_over_rate_l10 is not None and true_over_rate_l20 is not None:
            tor = true_over_rate_l10 * 0.60 + true_over_rate_l20 * 0.40
        else:
            tor = true_over_rate_l10 if true_over_rate_l10 is not None else true_over_rate_l20

        if direction_sign == 1:
            # Over bet: 50% TOR = neutral, 75% TOR = strong, 90%+ = very strong
            tor_edge = max(0.0, (tor - 0.50) / 0.40)  # 0 at 50%, 1.0 at 90%
        else:
            # Under bet: 50% TOR = neutral, 25% = strong (player goes under a lot)
            tor_edge = max(0.0, (0.50 - tor) / 0.40)

        tor_boost = min(8.0, tor_edge * 8.0)
        if tor_boost > 2:
            tags.append(f"TOR={tor*100:.0f}%")

    # Season average confirmation — if season avg aligns with direction it validates the edge.
    # This matters most when we suspect the book set the line using only recent data.
    season_boost = 0.0
    if season_avg_vs_line is not None:
        season_z = season_avg_vs_line / std
        if direction_sign == 1 and season_z > 0:
            season_boost = min(5.0, season_z * 2.5)
            if season_boost > 1:
                tags.append(f"season avg +{season_avg_vs_line:.1f} vs line")
        elif direction_sign == -1 and season_z < 0:
            season_boost = min(5.0, abs(season_z) * 2.5)
        elif direction_sign == 1 and season_z < -0.5:
            season_boost = -3.0  # season avg contradicts the over
            tags.append("season avg below line")
        elif direction_sign == -1 and season_z > 0.5:
            season_boost = -3.0

    value_score = base_vs + modal_boost + tor_boost + season_boost
    value_score = round(max(0.0, min(100.0, value_score)), 1)

    if z >= 2.0:
        label = f"Z={z:.1f} — strongly mispriced"
    elif z >= 1.2:
        label = f"Z={z:.1f} — mispriced"
    elif z >= 0.6:
        label = f"Z={z:.1f} — mild edge"
    else:
        label = f"Z={z:.1f} — line near median"

    return value_score, direction_sign, label, tags


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — RELIABILITY MULTIPLIER  (0.0–1.3)
# ─────────────────────────────────────────────────────────────────────────────

def _compute_reliability(
    direction_sign: int,
    l5_hr: Optional[float],
    l10_hr: Optional[float],
    l20_hr: Optional[float],
    cv: Optional[float],
    dist_profile: Optional[str],
    ghost_rate: Optional[float],
    n_games: int,
    near_miss_pct: Optional[float],
    real_usage_pct: Optional[float],
    net_rating_l10: Optional[float],
    usage_tier: Optional[str],
    min_rank: Optional[int],
    minutes_trend: Optional[str],
    opportunity_label: Optional[str],
    hist_tier: Optional[str],
    hist_stat_tier: Optional[str],
    regression_soft: bool,
    # Percentile profile signals
    spike_ratio: Optional[float] = None,
    tail_risk_low: Optional[float] = None,
    prob_over_plus1: Optional[float] = None,
    consistency_score: Optional[float] = None,
) -> tuple[float, list]:
    """
    Returns (reliability_multiplier, tags).

    Reliability answers: "How much do we trust the value signal?"

    Start at 1.0 (neutral). Scale up when the player is consistent and the
    hit rate backs up the Z-score. Scale down when the player is volatile,
    has ghost risk, or the hit rates don't agree with the median signal.

    Key insight: a high Z-score on a VOLATILE player is not reliable.
    The player hits 25 one game and 4 the next — the median is 14 but
    it doesn't tell you much about tonight.

    Caps at 1.3 to prevent single signals from dominating.
    """
    tags = []
    mult = 1.0

    whr = _weighted_hit_rate(l5_hr, l10_hr, l20_hr)

    # ── Hit rate alignment ────────────────────────────────────────────────────
    # When the Z-score says OVER and the hit rates confirm it, reliability goes up.
    # When they disagree (high Z-score but low hit rate), something is off.
    if whr is not None:
        if direction_sign == 1:
            if whr >= 0.80:
                mult += 0.14    # strong hit rate confirmation
                tags.append(f"WHR={whr*100:.0f}% ✓")
            elif whr >= 0.70:
                mult += 0.07
            elif whr >= 0.60:
                mult += 0.02
            elif whr < 0.45:
                mult -= 0.20    # Z-score says over but player rarely hits it
                tags.append(f"WHR={whr*100:.0f}% conflicts")
            elif whr < 0.55:
                mult -= 0.10
        elif direction_sign == -1:
            if whr <= 0.30:
                mult += 0.14
                tags.append(f"under WHR={whr*100:.0f}% ✓")
            elif whr <= 0.40:
                mult += 0.07
            elif whr > 0.55:
                mult -= 0.20

    # ── Distribution profile ──────────────────────────────────────────────────
    if dist_profile == "CONSISTENT":
        mult += 0.08
        tags.append("CONSISTENT dist")
    elif dist_profile == "MODERATE":
        mult += 0.03
    elif dist_profile == "VOLATILE":
        mult -= 0.15
        tags.append("VOLATILE dist")
    elif dist_profile == "VOLATILE-FLOOR":
        mult -= 0.25
        tags.append("VOLATILE-FLOOR dist")

    # CV — more precise than profile label
    if cv is not None:
        if cv <= 0.20:
            mult += 0.06
        elif cv <= 0.30:
            mult += 0.02
        elif cv >= 0.45:
            mult -= 0.08
        elif cv >= 0.55:
            mult -= 0.18

    # ── Ghost rate ────────────────────────────────────────────────────────────
    if ghost_rate is not None:
        if ghost_rate >= 0.30:
            mult -= 0.35
            tags.append(f"ghost risk {ghost_rate*100:.0f}%")
        elif ghost_rate >= 0.20:
            mult -= 0.20
            tags.append(f"ghost risk {ghost_rate*100:.0f}%")
        elif ghost_rate >= 0.10:
            mult -= 0.08

    # ── Sample size ───────────────────────────────────────────────────────────
    if n_games >= 15:
        mult += 0.03
    elif n_games <= 5:
        mult -= 0.10

    # ── Near-miss pattern ─────────────────────────────────────────────────────
    # Near-miss: player keeps finishing just BELOW the line (within 1-2 pts)
    # Strong signal for under bets. For over bets it's concerning.
    if near_miss_pct is not None and direction_sign == 1 and near_miss_pct >= 0.30:
        mult -= 0.12
        tags.append(f"near-miss {near_miss_pct*100:.0f}%")
    elif near_miss_pct is not None and direction_sign == -1 and near_miss_pct >= 0.30:
        mult += 0.10

    # ── Regression (soft) ─────────────────────────────────────────────────────
    # L5 running 30-44% hotter than L20 — hot streak not yet captured by line
    if regression_soft:
        mult -= 0.12
        tags.append("soft regression risk")

    # ── Percentile profile signals ────────────────────────────────────────────
    # spike_ratio: (p90-p50)/(p50-p10). Measures distribution asymmetry.
    #   > 1.5: player spikes upward more than they crash — GOOD for over props
    #          because upside outliers pull the over rate higher than median suggests
    #   < 0.7: player crashes more than they spike — BAD for over props
    #          player often goes well under the line even when median is above it
    if spike_ratio is not None:
        if direction_sign == 1:   # over bet
            if spike_ratio >= 2.0:
                mult += 0.10   # strong upside skew — outlier games often push over
                tags.append(f"upside skew (spike={spike_ratio:.1f})")
            elif spike_ratio >= 1.5:
                mult += 0.05
            elif spike_ratio <= 0.7:
                mult -= 0.10   # downside skew — player craters more than spikes
                tags.append(f"downside skew (spike={spike_ratio:.1f})")
            elif spike_ratio <= 1.0:
                mult -= 0.04
        elif direction_sign == -1:  # under bet
            if spike_ratio <= 0.7:
                mult += 0.08   # downside skew confirms under
                tags.append(f"downside skew confirms under (spike={spike_ratio:.1f})")
            elif spike_ratio >= 2.0:
                mult -= 0.08   # upside spiker — under is risky

    # tail_risk_low: P(outcome <= p10) — probability of a catastrophic under
    # High tail risk on an over bet means even if median is above the line,
    # there's a meaningful chance of a nothing game that kills the prop
    if tail_risk_low is not None and direction_sign == 1:
        if tail_risk_low >= 0.25:
            mult -= 0.12   # 1 in 4 chance of floor game — serious over risk
            tags.append(f"floor tail risk {tail_risk_low*100:.0f}%")
        elif tail_risk_low >= 0.15:
            mult -= 0.06

    # prob_over_plus1: P(outcome > line + 1) — "comfortable over" probability
    # This is more conservative than prob_over (exact threshold).
    # High prob_over_plus1 means player consistently clears the line with room to spare.
    # Low prob_over_plus1 despite high prob_over means player scrapes over by 0.5 frequently.
    if prob_over_plus1 is not None and direction_sign == 1:
        if prob_over_plus1 >= 0.65:
            mult += 0.06   # player clears with margin — not just scraping
            tags.append(f"clears with margin ({prob_over_plus1*100:.0f}%)")
        elif prob_over_plus1 <= 0.35:
            mult -= 0.05   # barely clears when it does — fragile edge

    # consistency_score: 0-100, normalized IQR tightness
    # This is redundant with CV when CV is available, but fills the gap when
    # dist_profile is UNKNOWN or CV hasn't been computed yet
    if consistency_score is not None and cv is None:
        if consistency_score >= 70:
            mult += 0.06
        elif consistency_score <= 30:
            mult -= 0.06

    # ── Real usage percentage ─────────────────────────────────────────────────
    # High usage = player is reliably in the offense. Low usage = volatile role.
    # This directly affects how predictable their output is.
    if real_usage_pct is not None:
        if real_usage_pct >= 0.30:
            mult += 0.08   # primary option — reliable
        elif real_usage_pct >= 0.25:
            mult += 0.03
        elif real_usage_pct <= 0.15:
            mult -= 0.12   # very low usage — could disappear any night
        elif real_usage_pct <= 0.18:
            mult -= 0.06

    # ── Net rating (rolling L10) ──────────────────────────────────────────────
    # Players with strongly negative net rating get benched in blowouts.
    # This directly suppresses their output on nights when the game is decided early.
    if net_rating_l10 is not None:
        if net_rating_l10 <= -8.0:
            mult -= 0.15   # coaches actively bench this player when losing
            tags.append(f"net_rating={net_rating_l10:.0f}")
        elif net_rating_l10 <= -4.0:
            mult -= 0.07
        elif net_rating_l10 >= 8.0:
            mult += 0.07   # coach trusts this player — stays on floor

    # ── Role tier ─────────────────────────────────────────────────────────────
    # CO-STAR is the sweet spot: enough usage to be predictable, lines often lazy.
    # BENCH players have too much variance (DNP risk, volatile minutes).
    if usage_tier == "CO-STAR":
        mult += 0.05
    elif usage_tier == "BENCH":
        mult -= 0.10
        tags.append("bench player")

    # ── Minutes rank on team ──────────────────────────────────────────────────
    if min_rank is not None:
        if min_rank <= 2:
            mult += 0.05   # guaranteed heavy usage
        elif min_rank >= 8:
            mult -= 0.08   # deep rotation — volatile minutes

    # ── Minutes trend ─────────────────────────────────────────────────────────
    if minutes_trend:
        if "More MPG" in minutes_trend and direction_sign == 1:
            mult += 0.08
            tags.append("role expanding")
        elif "Less MPG" in minutes_trend and direction_sign == 1:
            mult -= 0.08
            tags.append("role shrinking")

    # ── Injury opportunity ────────────────────────────────────────────────────
    if opportunity_label and opportunity_label not in ("—", "", None):
        if direction_sign == 1:
            mult += 0.12
            tags.append(f"opportunity: {opportunity_label}")

    # ── Historical edge tier ──────────────────────────────────────────────────
    # The most powerful reliability signal: if graded outcomes show this player
    # has historically beaten their props, that's validated empirical evidence.
    effective_hist = hist_stat_tier or hist_tier
    if direction_sign == 1 and effective_hist:
        if effective_hist == "PROVEN":
            mult += 0.20   # 75%+ hit rate on 15+ graded props
            tags.append("PROVEN hist tier")
        elif effective_hist == "TRENDING":
            mult += 0.12
            tags.append("TRENDING hist tier")
        elif effective_hist == "WATCH":
            mult += 0.06

    # Clamp to reasonable range
    mult = round(max(0.30, min(1.30, mult)), 3)
    return mult, tags


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — SITUATION MULTIPLIER  (0.70–1.20)
# ─────────────────────────────────────────────────────────────────────────────

def _compute_situation(
    stat: str,
    direction_sign: int,
    # Game environment
    is_back_to_back: Optional[str],
    days_rest: Optional[int],
    blowout_level: Optional[str],
    game_pace: Optional[float],
    record_diff: Optional[float],
    playoff_push: bool,
    tanking: bool,
    location_win_pct: Optional[float],
    # Matchup quality
    matchup_label: Optional[str],
    pos_matchup_label: Optional[str],
    pos_mismatch: bool,
    opp_pts_allowed: Optional[float],
    opp_reb_allowed: Optional[float],
    opp_ast_allowed: Optional[float],
    opp_fg3m_allowed: Optional[float],
    opp_def_rating: Optional[float],
    # Player-specific tonight
    h2h_hit_rate: Optional[float],
    h2h_total: Optional[int],
    home_away_split: Optional[float],
    overall_avg: Optional[float],
    tonight_location: Optional[str],
    trend_direction: Optional[str],
    def_trend_delta: Optional[float],
    revenge_game: Optional[str],
    minutes_stability: Optional[str],
    # Sharp action
    sharp_move: bool,
    steam_move: bool,
    line_move_direction: Optional[str],
    # Extra
    contract_year: bool,
    pos_line_hit_rate: Optional[float],
) -> tuple[float, list]:
    """
    Returns (situation_multiplier, tags).

    Situation answers: "Is tonight a good night for this prop?"

    Deliberately small range (0.70–1.20). Context rarely turns a bad prop
    into a good one. It tilts close calls and validates edge props.

    The most powerful positive signals: soft defense (real data), fast pace,
    sharp money confirmation.

    The most powerful negative signals: B2B (hard veto handled upstream),
    tough defense (real data), tanking team.
    """
    tags = []
    # Start neutral at 1.0
    raw = 0.0  # additive adjustments, then convert to multiplier

    stat_up = (stat or "").upper()

    # ── Opponent defense quality (real BDL data) ──────────────────────────────
    # This is the most reliable situation signal because it's based on actual
    # season-long data, not a label. 8%+ above league avg = real soft defense.
    opp_allowed = None
    if stat_up == "PTS":    opp_allowed = opp_pts_allowed
    elif stat_up == "REB":  opp_allowed = opp_reb_allowed
    elif stat_up == "AST":  opp_allowed = opp_ast_allowed
    elif stat_up == "FG3M": opp_allowed = opp_fg3m_allowed

    if opp_allowed is not None and stat_up in _LG_AVG:
        lg_avg = _LG_AVG[stat_up]
        allowed_pct = (opp_allowed - lg_avg) / lg_avg
        if direction_sign == 1:
            if allowed_pct >= 0.10:
                raw += 0.15
                tags.append(f"soft D (+{allowed_pct*100:.0f}% allowed)")
            elif allowed_pct >= 0.05:
                raw += 0.07
            elif allowed_pct <= -0.10:
                raw -= 0.12
                tags.append(f"tough D ({allowed_pct*100:.0f}% allowed)")
            elif allowed_pct <= -0.05:
                raw -= 0.06
        elif direction_sign == -1:
            if allowed_pct <= -0.10:
                raw += 0.12
            elif allowed_pct >= 0.10:
                raw -= 0.12

    # Opponent defensive rating for combined stats
    if opp_def_rating is not None and stat_up in ("PRA", "PR", "PA", "RA"):
        def_delta = opp_def_rating - _LG_DEF_RT
        if direction_sign == 1:
            if def_delta >= 4.0:
                raw += 0.08
            elif def_delta >= 2.0:
                raw += 0.04
            elif def_delta <= -4.0:
                raw -= 0.08
            elif def_delta <= -2.0:
                raw -= 0.04

    # ── Game pace ─────────────────────────────────────────────────────────────
    # More possessions = more opportunities for counting stats.
    # Only meaningful for counting stats, not % or milestone props.
    if game_pace is not None and stat_up in ("PTS", "REB", "AST", "RA", "PR", "PA", "PRA", "FG3M"):
        pace_delta = game_pace - _LG_PACE
        if direction_sign == 1:
            if pace_delta >= 5.0:
                raw += 0.10
                tags.append(f"fast pace ({game_pace:.0f})")
            elif pace_delta >= 2.5:
                raw += 0.05
            elif pace_delta <= -5.0:
                raw -= 0.10
                tags.append(f"slow pace ({game_pace:.0f})")
            elif pace_delta <= -2.5:
                raw -= 0.05

    # ── Positional matchup ────────────────────────────────────────────────────
    effective_matchup = pos_matchup_label or matchup_label or ""
    if direction_sign == 1:
        if "Soft" in effective_matchup:
            raw += 0.08
        elif "Tough" in effective_matchup:
            raw -= 0.08
    elif direction_sign == -1:
        if "Tough" in effective_matchup:
            raw += 0.08
        elif "Soft" in effective_matchup:
            raw -= 0.08

    # Hard positional mismatch (opponent specifically bad vs this position+stat)
    if pos_mismatch and direction_sign == 1:
        raw += 0.06

    # Positional line hit rate (how often same-position players hit vs this opponent)
    if pos_line_hit_rate is not None:
        plhr = pos_line_hit_rate / 100.0
        if plhr >= 0.80 and direction_sign == 1:
            raw += 0.06
        elif plhr <= 0.30 and direction_sign == 1:
            raw -= 0.06

    # ── Rest / fatigue ────────────────────────────────────────────────────────
    if days_rest is not None and days_rest >= 3 and direction_sign == 1:
        raw += 0.04

    # ── Blowout risk ─────────────────────────────────────────────────────────
    if blowout_level == "EXTREME" and direction_sign == 1:
        raw -= 0.12
        tags.append("EXTREME blowout risk")
    elif blowout_level == "HIGH" and direction_sign == 1:
        raw -= 0.06

    # ── Record differential ───────────────────────────────────────────────────
    # Big favorite → starters may be rested in Q4 → prop under risk
    # Big underdog → plays harder / more desperate minutes → prop over boost
    if record_diff is not None:
        if direction_sign == 1:
            if record_diff >= 0.25:
                raw -= 0.06   # heavy fav — rest risk
            elif record_diff <= -0.20:
                raw += 0.06   # underdog — plays harder
        elif direction_sign == -1:
            if record_diff >= 0.20:
                raw += 0.06
            elif record_diff <= -0.20:
                raw -= 0.06

    # ── Playoff push / tanking ────────────────────────────────────────────────
    if playoff_push and direction_sign == 1:
        raw += 0.07
        tags.append("playoff push")
    elif tanking and direction_sign == 1:
        raw -= 0.10
        tags.append("tanking team")

    # ── Location win % ────────────────────────────────────────────────────────
    if location_win_pct is not None:
        loc_delta = location_win_pct - 0.50
        if direction_sign == 1:
            if loc_delta >= 0.15:
                raw += 0.04
            elif loc_delta <= -0.15:
                raw -= 0.04

    # ── Head-to-head vs tonight's opponent ───────────────────────────────────
    if h2h_hit_rate is not None and h2h_total is not None and h2h_total >= 3:
        weight = min(1.0, h2h_total / 8.0)
        if direction_sign == 1:
            h2h_adj = (h2h_hit_rate - 50.0) / 50.0 * weight * 0.06
        else:
            h2h_adj = (50.0 - h2h_hit_rate) / 50.0 * weight * 0.06
        raw += max(-0.06, min(0.06, h2h_adj))
        if abs(h2h_adj) > 0.03:
            tags.append(f"H2H={h2h_hit_rate:.0f}%")

    # ── Player home/away split ────────────────────────────────────────────────
    if home_away_split is not None and overall_avg is not None and overall_avg > 0:
        split_pct = (home_away_split - overall_avg) / overall_avg
        if direction_sign == 1 and split_pct >= 0.12:
            raw += 0.04
        elif direction_sign == 1 and split_pct <= -0.12:
            raw -= 0.04

    # ── Defensive trend ───────────────────────────────────────────────────────
    if def_trend_delta is not None:
        trend_adj = min(0.06, abs(def_trend_delta) / 5.0 * 0.06)
        if def_trend_delta > 0 and direction_sign == 1:
            raw += trend_adj
        elif def_trend_delta < 0 and direction_sign == 1:
            raw -= trend_adj

    # ── Player form ───────────────────────────────────────────────────────────
    if trend_direction:
        if "Hot" in trend_direction and direction_sign == 1:
            raw += 0.04
        elif "Cold" in trend_direction and direction_sign == 1:
            raw -= 0.04

    # ── Minutes stability ─────────────────────────────────────────────────────
    if minutes_stability and "Volatile" in minutes_stability:
        raw -= 0.06

    # ── Revenge game ─────────────────────────────────────────────────────────
    if revenge_game == "🔥 Revenge Game" and direction_sign == 1:
        raw += 0.03

    # ── Sharp / steam money ──────────────────────────────────────────────────
    # Sharp money is the only external validation that the market agrees with us.
    # When sharp money moves the same direction as our edge, that's significant.
    if sharp_move:
        same_dir = (direction_sign == 1 and line_move_direction == "UP") or \
                   (direction_sign == -1 and line_move_direction == "DOWN")
        if same_dir:
            raw += 0.12
            tags.append("sharp confirms")
        else:
            raw -= 0.10
            tags.append("sharp AGAINST")
    elif steam_move:
        same_dir = (direction_sign == 1 and line_move_direction == "UP") or \
                   (direction_sign == -1 and line_move_direction == "DOWN")
        if same_dir:
            raw += 0.06
        else:
            raw -= 0.06

    # ── Contract year ─────────────────────────────────────────────────────────
    # Walk-year players play harder. Small but consistent signal.
    if contract_year and direction_sign == 1:
        raw += 0.03

    # Convert additive adjustments to a multiplier centered on 1.0
    situation_mult = round(max(0.70, min(1.20, 1.0 + raw)), 3)
    return situation_mult, tags


# ─────────────────────────────────────────────────────────────────────────────
# HARD VETOES
# ─────────────────────────────────────────────────────────────────────────────

def _check_hard_vetoes(
    hook_level: str,
    is_back_to_back: Optional[str],
    regression_risk: bool,
    outlier_inflated: bool,
    cv: Optional[float],
    dist_profile: Optional[str],
    l20_hr: Optional[float],
    stat: str,
    direction_sign: int,
) -> Optional[str]:
    """
    Returns a veto reason string if this prop should score 0, else None.

    Hard vetoes are non-negotiable. No amount of good signals overrides them.
    They exist because these specific scenarios have repeatedly produced losses
    that the scoring model cannot compensate for.
    """
    # SEVERE hook: line is structurally wrong (set at 0.5 for a player who
    # routinely scores 0 in that stat, or similar structural issues)
    if "SEVERE" in str(hook_level):
        return f"SEVERE HOOK: {hook_level}"

    # Back-to-back: fatigue materially affects output for counting stats
    # The data is clear — over props on B2B hit at below-baseline rates
    if is_back_to_back == "🔴 YES" and direction_sign == 1:
        return "B2B — over props blocked"

    # Regression risk: L5 running 45%+ above L20 baseline
    # The player is on a hot streak and the line has been adjusted up.
    # The mean reversion effect is very real.
    if regression_risk:
        return "regression risk — L5 far above L20"

    # Outlier inflation: a single monster game is masking a weak baseline
    if outlier_inflated:
        return "outlier inflated — true baseline lower"

    # Extreme volatility: CV > 0.55 means the prop is essentially a coin flip
    # regardless of what the median says
    if cv is not None and cv > 0.55:
        return f"extreme volatility: CV={cv:.2f}"

    # Volatile-floor: player routinely goes near-zero
    if dist_profile == "VOLATILE-FLOOR":
        return "VOLATILE-FLOOR dist profile"

    # Weak L20 by stat: the hit rate over 20 games doesn't support an over bet
    _l20_thresholds = {
        "AST": 0.55, "REB": 0.55, "RA": 0.55,
        "PTS": 0.50, "PR": 0.50, "PRA": 0.50, "PA": 0.50,
        "FG3M": 0.50, "BLK": 0.45, "STL": 0.45,
    }
    if l20_hr is not None and direction_sign == 1:
        # Normalize: app.py stores as 0-100, internal pipeline as 0-1
        l20_norm = l20_hr / 100.0 if l20_hr > 1.0 else l20_hr
        thresh = _l20_thresholds.get(stat.upper(), 0.50)
        if l20_norm < thresh:
            return f"weak L20: {l20_norm*100:.0f}% < {thresh*100:.0f}% threshold"

    return None


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _weighted_hit_rate(
    l5: Optional[float],
    l10: Optional[float],
    l20: Optional[float],
) -> Optional[float]:
    """Blend L5/L10/L20 hit rates with recency weighting."""
    pairs = [(l5, 0.45), (l10, 0.35), (l20, 0.20)]
    avail = [(hr, w) for hr, w in pairs if hr is not None]
    if not avail:
        return None
    total_w = sum(w for _, w in avail)
    return sum(hr * w for hr, w in avail) / total_w


# ─────────────────────────────────────────────────────────────────────────────
# MASTER SCORER
# ─────────────────────────────────────────────────────────────────────────────

def compute_edge_score(
    # Core
    line: float,
    stat: str,
    hook_score: int,
    hook_level: str,

    # Distribution
    median_l10: Optional[float] = None,
    median_l20: Optional[float] = None,
    modal_outcome: Optional[float] = None,
    std_l10: Optional[float] = None,
    std_l20: Optional[float] = None,
    true_over_rate_l10: Optional[float] = None,
    true_over_rate_l20: Optional[float] = None,
    line_vs_median: Optional[float] = None,

    # Hit rates (0.0–1.0)
    l5_hit_rate: Optional[float] = None,
    l10_hit_rate: Optional[float] = None,
    l20_hit_rate: Optional[float] = None,

    # Reliability signals
    cv: Optional[float] = None,
    dist_profile: Optional[str] = None,
    ghost_rate: Optional[float] = None,
    n_games: int = 10,
    near_miss_pct: Optional[float] = None,
    regression_risk: bool = False,
    regression_soft: bool = False,
    outlier_inflated: bool = False,
    season_avg_vs_line: Optional[float] = None,
    real_usage_pct: Optional[float] = None,
    net_rating_l10: Optional[float] = None,
    usage_tier: Optional[str] = None,
    min_rank: Optional[int] = None,
    minutes_trend: Optional[str] = None,
    opportunity_label: Optional[str] = None,
    hist_tier: Optional[str] = None,
    hist_stat_tier: Optional[str] = None,

    # Situation signals
    is_back_to_back: Optional[str] = None,
    days_rest: Optional[int] = None,
    matchup_label: Optional[str] = None,
    pos_matchup_label: Optional[str] = None,
    pos_mismatch: bool = False,
    trend_direction: Optional[str] = None,
    h2h_hit_rate: Optional[float] = None,
    h2h_total: Optional[int] = None,
    home_away_split: Optional[float] = None,
    overall_avg: Optional[float] = None,
    revenge_game: Optional[str] = None,
    def_trend_delta: Optional[float] = None,
    tonight_location: Optional[str] = None,
    minutes_stability: Optional[str] = None,
    blowout_level: Optional[str] = None,
    game_pace: Optional[float] = None,
    record_diff: Optional[float] = None,
    playoff_push: bool = False,
    tanking: bool = False,
    location_win_pct: Optional[float] = None,
    is_home_game: bool = False,
    opp_pts_allowed: Optional[float] = None,
    opp_reb_allowed: Optional[float] = None,
    opp_ast_allowed: Optional[float] = None,
    opp_fg3m_allowed: Optional[float] = None,
    opp_def_rating: Optional[float] = None,
    sharp_move: bool = False,
    steam_move: bool = False,
    line_move_direction: Optional[str] = None,
    contract_year: bool = False,
    pos_line_hit_rate: Optional[float] = None,
    stat_leader_rank: Optional[int] = None,
    # Percentile profile signals
    spike_ratio: Optional[float] = None,
    tail_risk_low: Optional[float] = None,
    prob_over_plus1: Optional[float] = None,
    consistency_score: Optional[float] = None,
) -> dict:
    """
    Compute edge score for a prop.

    Returns dict with edge_score (0-100), direction, final_call, and
    component scores for debugging.
    """

    # ── Step 1: Check hard vetoes ─────────────────────────────────────────────
    # Compute direction first for veto check
    if median_l10 is not None or median_l20 is not None:
        m = median_l10 if median_l10 is not None else median_l20
        preliminary_direction = 1 if m > line else (-1 if m < line else 0)
    else:
        preliminary_direction = 1  # assume over for veto check purposes

    veto = _check_hard_vetoes(
        hook_level=hook_level,
        is_back_to_back=is_back_to_back,
        regression_risk=regression_risk,
        outlier_inflated=outlier_inflated,
        cv=cv,
        dist_profile=dist_profile,
        l20_hr=l20_hit_rate,
        stat=stat,
        direction_sign=preliminary_direction,
    )

    if veto:
        return {
            "edge_score":        0,
            "direction":         "NONE",
            "final_call":        f"⛔ Vetoed — {veto}",
            "value_score":       0,
            "reliability":       0,
            "situation":         1.0,
            "misprice_score":    0,
            "misprice_label":    veto,
            "confidence_score":  0,
            "context_score":     0,
            "role_score":        0,
            "veto":              True,
            "veto_reason":       veto,
            "verdict_tags":      [veto],
            "is_parlay_ready":   False,
            "is_hammer":         False,
            "is_lock":           False,
        }

    # ── Step 2: Compute value score ───────────────────────────────────────────
    value_score, direction_sign, value_label, value_tags = _compute_value(
        line=line,
        median_l10=median_l10,
        median_l20=median_l20,
        modal_outcome=modal_outcome,
        std_l10=std_l10,
        std_l20=std_l20,
        true_over_rate_l10=true_over_rate_l10,
        true_over_rate_l20=true_over_rate_l20,
        season_avg_vs_line=season_avg_vs_line,
        l5_hr=l5_hit_rate,
        l10_hr=l10_hit_rate,
        l20_hr=l20_hit_rate,
    )

    if value_score < 5.0 or direction_sign == 0:
        return {
            "edge_score":        round(value_score, 1),
            "direction":         "NONE",
            "final_call":        "⚪ Skip — no edge",
            "value_score":       round(value_score, 1),
            "reliability":       1.0,
            "situation":         1.0,
            "misprice_score":    round(value_score, 1),
            "misprice_label":    value_label,
            "confidence_score":  0,
            "context_score":     0,
            "role_score":        0,
            "veto":              False,
            "veto_reason":       None,
            "verdict_tags":      ["no edge"],
            "is_parlay_ready":   False,
            "is_hammer":         False,
            "is_lock":           False,
        }

    # ── Step 3: Compute reliability multiplier ────────────────────────────────
    reliability, rel_tags = _compute_reliability(
        direction_sign=direction_sign,
        l5_hr=l5_hit_rate,
        l10_hr=l10_hit_rate,
        l20_hr=l20_hit_rate,
        cv=cv,
        dist_profile=dist_profile,
        ghost_rate=ghost_rate,
        n_games=n_games,
        near_miss_pct=near_miss_pct,
        real_usage_pct=real_usage_pct,
        net_rating_l10=net_rating_l10,
        usage_tier=usage_tier,
        min_rank=min_rank,
        minutes_trend=minutes_trend,
        opportunity_label=opportunity_label,
        hist_tier=hist_tier,
        hist_stat_tier=hist_stat_tier,
        regression_soft=regression_soft,
        spike_ratio=spike_ratio,
        tail_risk_low=tail_risk_low,
        prob_over_plus1=prob_over_plus1,
        consistency_score=consistency_score,
    )

    # ── Step 4: Compute situation multiplier ──────────────────────────────────
    situation, sit_tags = _compute_situation(
        stat=stat,
        direction_sign=direction_sign,
        is_back_to_back=is_back_to_back,
        days_rest=days_rest,
        blowout_level=blowout_level,
        game_pace=game_pace,
        record_diff=record_diff,
        playoff_push=playoff_push,
        tanking=tanking,
        location_win_pct=location_win_pct,
        matchup_label=matchup_label,
        pos_matchup_label=pos_matchup_label,
        pos_mismatch=pos_mismatch,
        opp_pts_allowed=opp_pts_allowed,
        opp_reb_allowed=opp_reb_allowed,
        opp_ast_allowed=opp_ast_allowed,
        opp_fg3m_allowed=opp_fg3m_allowed,
        opp_def_rating=opp_def_rating,
        h2h_hit_rate=h2h_hit_rate,
        h2h_total=h2h_total,
        home_away_split=home_away_split,
        overall_avg=overall_avg,
        tonight_location=tonight_location,
        trend_direction=trend_direction,
        def_trend_delta=def_trend_delta,
        revenge_game=revenge_game,
        minutes_stability=minutes_stability,
        sharp_move=sharp_move,
        steam_move=steam_move,
        line_move_direction=line_move_direction,
        contract_year=contract_year,
        pos_line_hit_rate=pos_line_hit_rate,
    )

    # ── Step 5: Final score ───────────────────────────────────────────────────
    edge_score = value_score * reliability * situation
    edge_score = round(max(0.0, min(100.0, edge_score)), 1)

    direction = "OVER" if direction_sign == 1 else "UNDER"
    dir_label = direction

    if edge_score >= 75:
        final_call = f"🔥🔥 LOCK {dir_label}"
    elif edge_score >= 62:
        final_call = f"🔥 STRONG {dir_label}"
    elif edge_score >= 50:
        final_call = f"✅ BET {dir_label}"
    elif edge_score >= 38:
        final_call = f"〰 LEAN {dir_label}"
    else:
        final_call = "⚪ Skip"

    verdict_tags = value_tags + rel_tags + sit_tags

    # ── Parlay / hammer eligibility ───────────────────────────────────────────
    # Parlay: requires high score AND tight distribution AND no ghost risk
    is_parlay_ready = (
        edge_score >= 58
        and reliability >= 0.90
        and (cv is None or cv < 0.35)
        and (ghost_rate is None or ghost_rate < 0.10)
    )
    # Hammer: very high confidence
    is_hammer = edge_score >= 72 and reliability >= 1.05
    # Lock: value is very high AND reliability confirms it
    is_lock = value_score >= 65 and reliability >= 1.10

    # ── Legacy layer fields (for compatibility with existing app.py) ───────────
    # app.py reads misprice_score, confidence_score, context_score, role_score
    # Map our new model's components to these fields for backward compatibility
    misprice_score   = round(min(40.0, value_score * 0.40), 1)
    confidence_score = round(min(25.0, value_score * reliability * 0.25), 1)
    context_score    = round(min(20.0, value_score * reliability * situation * 0.20), 1)
    role_score       = round(min(15.0, (reliability - 0.7) / 0.6 * 15.0), 1)

    return {
        "edge_score":        edge_score,
        "direction":         direction,
        "final_call":        final_call,
        # New model components
        "value_score":       round(value_score, 1),
        "reliability":       reliability,
        "situation":         situation,
        # Legacy layer fields for app.py compatibility
        "misprice_score":    misprice_score,
        "misprice_label":    value_label,
        "confidence_score":  confidence_score,
        "context_score":     context_score,
        "role_score":        role_score,
        # Meta
        "veto":              False,
        "veto_reason":       None,
        "verdict_tags":      verdict_tags,
        "weighted_hit_rate": round(_weighted_hit_rate(l5_hit_rate, l10_hit_rate, l20_hit_rate) * 100, 1)
                             if _weighted_hit_rate(l5_hit_rate, l10_hit_rate, l20_hit_rate) is not None else None,
        "is_parlay_ready":   is_parlay_ready,
        "is_hammer":         is_hammer,
        "is_lock":           is_lock,
    }


# ─────────────────────────────────────────────────────────────────────────────
# CONVENIENCE: score_from_vrow
# ─────────────────────────────────────────────────────────────────────────────

def score_from_vrow(vrow: dict, context: dict) -> dict:
    """
    Accepts a value_row dict and context dict from app.py/main.py.
    Unchanged interface — drop-in replacement for the old scoring.py.
    """
    def _pct(v):
        if v is None or v == "—":
            return None
        try:
            s = str(v).replace("%", "").strip()
            f = float(s)
            return f / 100.0 if f > 1.0 else f
        except (ValueError, TypeError):
            return None

    stat = vrow.get("stat_type", "")
    line = float(vrow.get("line", 0))
    dist = vrow.get("distribution", {})

    median_l10        = dist.get("median_l10")
    median_l20        = dist.get("median_l20")
    modal_outcome     = dist.get("modal_outcome")
    std_l10           = dist.get("std_l10")
    std_l20           = dist.get("std_l20")
    true_over_rate_l10 = dist.get("true_over_rate_l10")
    true_over_rate_l20 = dist.get("true_over_rate_l20")
    line_vs_median    = dist.get("line_vs_median")
    hook_score        = dist.get("hook_score", 0)
    hook_level        = dist.get("hook_level", "")
    near_miss_pct     = dist.get("near_miss_pct")
    ghost             = dist.get("ghost_rate") or context.get("ghost_rate")
    n_games           = dist.get("n") or 10

    mean_l10 = dist.get("mean_l10")
    regression_risk = False
    if mean_l10 is not None and median_l10 is not None:
        regression_risk = (mean_l10 - median_l10) >= 1.5

    l5_hr  = _pct(vrow.get("last5_hit_rate"))
    l10_hr = _pct(vrow.get("last10_hit_rate"))
    l20_hr = _pct(vrow.get("last20_hit_rate"))

    cv          = context.get(f"{stat}_cv")
    is_b2b      = context.get("is_back_to_back")
    matchup     = context.get(f"opp_{stat.lower()}_matchup")
    pos_matchup = context.get(f"opp_{stat.lower()}_pos_matchup")
    trend_dir   = context.get(f"{stat}_trend")
    h2h_hr      = context.get("h2h_hit_rate")
    h2h_total   = context.get("h2h_total")
    days_rest   = context.get("days_rest")
    revenge     = context.get("revenge_game")
    min_stab    = context.get("minutes_stability")
    tonight_loc = context.get("tonight_location")
    def_delta   = context.get(f"opp_{stat.lower()}_def_delta")

    home_away_split = None
    overall_avg_val = None
    if tonight_loc in ("Home", "Away"):
        home_away_split = context.get(f"{stat}_avg_{tonight_loc.lower()}")
        overall_avg_val = mean_l10 or context.get(f"{stat}_l20_avg")

    lm = vrow.get("line_movement") or {}

    return compute_edge_score(
        line=line,
        stat=stat,
        hook_score=hook_score,
        hook_level=hook_level,
        median_l10=median_l10,
        median_l20=median_l20,
        modal_outcome=modal_outcome,
        std_l10=std_l10,
        std_l20=std_l20,
        true_over_rate_l10=true_over_rate_l10,
        true_over_rate_l20=true_over_rate_l20,
        line_vs_median=line_vs_median,
        l5_hit_rate=l5_hr,
        l10_hit_rate=l10_hr,
        l20_hit_rate=l20_hr,
        cv=cv,
        dist_profile=vrow.get("dist_profile"),
        ghost_rate=ghost,
        n_games=n_games,
        near_miss_pct=near_miss_pct,
        regression_risk=regression_risk,
        regression_soft=bool(vrow.get("regression_soft", False)),
        outlier_inflated=bool(vrow.get("outlier_inflated", False)),
        season_avg_vs_line=context.get("season_avg_vs_line"),
        real_usage_pct=context.get("real_usage_pct"),
        net_rating_l10=context.get("net_rating_l10"),
        usage_tier=context.get("usage_tier"),
        min_rank=context.get("min_rank"),
        minutes_trend=context.get("minutes_trend"),
        opportunity_label=vrow.get("opportunity", ""),
        hist_tier=vrow.get("hist_tier"),
        hist_stat_tier=vrow.get("hist_stat_tier"),
        is_back_to_back=is_b2b,
        days_rest=days_rest,
        matchup_label=matchup,
        pos_matchup_label=pos_matchup,
        pos_mismatch=context.get(f"opp_{stat.lower()}_pos_weak", False),
        trend_direction=trend_dir,
        h2h_hit_rate=h2h_hr,
        h2h_total=h2h_total,
        home_away_split=home_away_split,
        overall_avg=overall_avg_val,
        revenge_game=revenge,
        def_trend_delta=def_delta,
        tonight_location=tonight_loc,
        minutes_stability=min_stab,
        blowout_level=context.get("blowout_level"),
        game_pace=context.get("game_pace"),
        record_diff=context.get("record_diff"),
        playoff_push=bool(context.get("playoff_push", False)),
        tanking=bool(context.get("tanking", False)),
        location_win_pct=context.get("location_win_pct"),
        is_home_game=bool(context.get("is_home_game", False)),
        opp_pts_allowed=context.get("opp_pts_allowed"),
        opp_reb_allowed=context.get("opp_reb_allowed"),
        opp_ast_allowed=context.get("opp_ast_allowed"),
        opp_fg3m_allowed=context.get("opp_fg3m_allowed"),
        opp_def_rating=context.get("opp_def_rating"),
        sharp_move=bool(lm.get("sharp_move", False)),
        steam_move=bool(lm.get("steam_move", False)),
        line_move_direction=lm.get("direction"),
        contract_year=bool(context.get("contract_year", False)),
        pos_line_hit_rate=context.get("pos_line_hit_rate"),
        stat_leader_rank=context.get("stat_leader_rank"),
        spike_ratio=context.get("spike_ratio"),
        tail_risk_low=context.get("tail_risk_low"),
        prob_over_plus1=context.get("prob_over_plus1"),
        consistency_score=context.get("consistency_score"),
    )
