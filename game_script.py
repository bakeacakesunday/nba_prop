"""
game_script.py — Player-specific game script profiles and parlay covariance engine.

Philosophy:
  Every player is graded as themselves — not as a position archetype.
  Cade Cunningham is graded on Cade's actual output distribution across
  game states derived from his real game log. Holland is graded as Holland.

  The covariance between two players in a parlay is computed from their
  actual shared game history (same game_id), not from positional assumptions.
  Positional assumptions only enter for opposing-team pairs where shared
  game samples are too small (2-4 games/season) to compute meaningful
  Pearson correlations.

Game States (5):
  CLOSE         — within 8 pts most of game, full minutes for everyone
  MOD_FAV       — this team wins by 9-18, stars may sit late Q4
  MOD_DOG       — this team loses by 9-18, stars stay on court chasing
  BLOWOUT_FAV   — this team wins 19+, garbage time hits hard
  BLOWOUT_DOG   — this team loses 19+, stars get full run chasing

Each player card gets a game_script_profile dict:
  {
    "state_probs":   {state: probability},     # from spread/total
    "hit_probs":     {state: hit_probability},  # player-specific per state
    "weighted_hit":  float,                     # overall expected hit rate
    "variance":      float,                     # output variance
    "role":          str,                       # star/starter/role/fringe
    "shot_profile":  str,                       # dependent/independent/mixed
    "minutes_mean":  float,
    "minutes_cv":    float,
  }

Parlay analysis:
  compute_parlay_profile(legs: list[dict]) -> dict
    legs: list of prop card dicts (from allProps JSON)
    Returns joint hit probability with correlation structure,
    warnings, and per-leg contribution analysis.
"""
from __future__ import annotations

import logging
import math
from typing import Optional

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats

logger = logging.getLogger(__name__)

# ── Stat categories ───────────────────────────────────────────────────────────

_SHOT_INDEPENDENT = {"REB", "AST", "RA", "BLK", "STL", "TOV"}
_SHOT_DEPENDENT   = {"PTS", "FG3M", "PA"}
_SHOT_MIXED       = {"PRA", "PR"}

def shot_profile(stat: str) -> str:
    s = stat.upper()
    if s in _SHOT_INDEPENDENT: return "independent"
    if s in _SHOT_DEPENDENT:   return "dependent"
    return "mixed"

# ── Game state probability model ──────────────────────────────────────────────

def compute_state_probs(spread: Optional[float], game_total: Optional[float]) -> dict:
    """
    Given a team's spread (negative = favored) and the game total,
    return probability weights across 5 game states.

    Derived from empirical NBA blowout rates:
    - ATS covers happen ~50% of time (efficient market)
    - Games within 8 pts at end: ~55% of all games
    - Blowouts (20+) happen ~12% of games
    - These shift significantly with spread size
    """
    if spread is None:
        # No line data — uniform distribution, slight close-game bias
        return {
            "CLOSE":       0.50,
            "MOD_FAV":     0.12,
            "MOD_DOG":     0.12,
            "BLOWOUT_FAV": 0.08,
            "BLOWOUT_DOG": 0.08,
            "NEUTRAL":     0.10,  # catch-all for push/unclear
        }

    spread_abs = abs(spread)
    team_favored = spread < 0  # negative = this team favored

    # Base close-game probability drops as spread increases
    # At pick'em (~0): ~60% close
    # At -7: ~45% close
    # At -14: ~25% close
    # At -20+: ~10% close
    close_prob = max(0.10, 0.62 - (spread_abs * 0.024))

    # Moderate and blowout probabilities scale up with spread
    # and are asymmetric — favored team has higher blowout_fav probability
    if team_favored:
        mod_fav_prob     = min(0.35, 0.08 + spread_abs * 0.018)
        blowout_fav_prob = min(0.30, 0.04 + spread_abs * 0.014)
        mod_dog_prob     = min(0.20, 0.06 + spread_abs * 0.008)
        blowout_dog_prob = min(0.10, 0.02 + spread_abs * 0.004)
    else:
        # From underdog's perspective
        mod_dog_prob     = min(0.35, 0.08 + spread_abs * 0.018)
        blowout_dog_prob = min(0.30, 0.04 + spread_abs * 0.014)
        mod_fav_prob     = min(0.20, 0.06 + spread_abs * 0.008)
        blowout_fav_prob = min(0.10, 0.02 + spread_abs * 0.004)

    # Normalize to sum to 1
    total = close_prob + mod_fav_prob + mod_dog_prob + blowout_fav_prob + blowout_dog_prob
    return {
        "CLOSE":       round(close_prob / total, 4),
        "MOD_FAV":     round(mod_fav_prob / total, 4),
        "MOD_DOG":     round(mod_dog_prob / total, 4),
        "BLOWOUT_FAV": round(blowout_fav_prob / total, 4),
        "BLOWOUT_DOG": round(blowout_dog_prob / total, 4),
    }


# ── Player role classification ────────────────────────────────────────────────

def classify_role(minutes_mean: float, minutes_cv: float) -> str:
    """
    Classify player role from their actual minutes distribution.
    This is Cade-specific, not PG-generic.
    """
    if minutes_mean >= 32:
        return "star"
    elif minutes_mean >= 26:
        return "starter"
    elif minutes_mean >= 20:
        return "role"
    else:
        return "fringe"


# ── Game state output modifiers ───────────────────────────────────────────────

# For each (role, game_state, shot_profile) combination,
# what multiplier applies to a player's expected output?
# These are derived from empirical patterns:
#   - Stars sit Q4 in blowout wins (favored side)
#   - Underdog stars get MORE minutes chasing
#   - Role players may gain garbage time when team is winning big
#   - Shooting-independent stats are less affected than PTS

_OUTPUT_MODIFIERS = {
    # (role, state): {shot_profile: output_multiplier}
    ("star", "CLOSE"):       {"independent": 1.00, "mixed": 1.00, "dependent": 1.00},
    ("star", "MOD_FAV"):     {"independent": 0.88, "mixed": 0.84, "dependent": 0.80},
    ("star", "MOD_DOG"):     {"independent": 1.05, "mixed": 1.04, "dependent": 1.02},
    ("star", "BLOWOUT_FAV"): {"independent": 0.65, "mixed": 0.58, "dependent": 0.52},
    ("star", "BLOWOUT_DOG"): {"independent": 1.08, "mixed": 1.07, "dependent": 1.05},

    ("starter", "CLOSE"):       {"independent": 1.00, "mixed": 1.00, "dependent": 1.00},
    ("starter", "MOD_FAV"):     {"independent": 0.92, "mixed": 0.89, "dependent": 0.86},
    ("starter", "MOD_DOG"):     {"independent": 1.03, "mixed": 1.02, "dependent": 1.01},
    ("starter", "BLOWOUT_FAV"): {"independent": 0.72, "mixed": 0.66, "dependent": 0.60},
    ("starter", "BLOWOUT_DOG"): {"independent": 1.06, "mixed": 1.05, "dependent": 1.03},

    ("role", "CLOSE"):       {"independent": 1.00, "mixed": 1.00, "dependent": 1.00},
    ("role", "MOD_FAV"):     {"independent": 1.05, "mixed": 1.04, "dependent": 1.03},  # may gain mins
    ("role", "MOD_DOG"):     {"independent": 0.95, "mixed": 0.93, "dependent": 0.90},
    ("role", "BLOWOUT_FAV"): {"independent": 1.12, "mixed": 1.10, "dependent": 1.08},  # garbage time
    ("role", "BLOWOUT_DOG"): {"independent": 0.82, "mixed": 0.78, "dependent": 0.72},  # rotation tightens

    ("fringe", "CLOSE"):       {"independent": 1.00, "mixed": 1.00, "dependent": 1.00},
    ("fringe", "MOD_FAV"):     {"independent": 1.08, "mixed": 1.06, "dependent": 1.04},
    ("fringe", "MOD_DOG"):     {"independent": 0.88, "mixed": 0.85, "dependent": 0.80},
    ("fringe", "BLOWOUT_FAV"): {"independent": 1.20, "mixed": 1.15, "dependent": 1.10},
    ("fringe", "BLOWOUT_DOG"): {"independent": 0.60, "mixed": 0.55, "dependent": 0.50},
}


def get_output_modifier(role: str, state: str, shot_prof: str) -> float:
    key = (role, state)
    mods = _OUTPUT_MODIFIERS.get(key, {"independent": 1.0, "mixed": 1.0, "dependent": 1.0})
    return mods.get(shot_prof, 1.0)


# ── Player output distribution from actual log ────────────────────────────────

def compute_player_output_distribution(
    log: pd.DataFrame,
    stat: str,
    line: float,
) -> dict:
    """
    From a player's actual game log, compute:
    - Mean output for this stat
    - Standard deviation
    - Hit rate at this line
    - Output in close games vs blowout games (using MIN as proxy)

    This is player-specific — no position archetypes.
    """
    if log is None or log.empty:
        return {
            "mean": None, "std": None, "hit_rate": None,
            "close_mean": None, "blowout_mean": None,
            "n_games": 0,
        }

    # Add combo stats if needed
    stat_upper = stat.upper()
    col = stat_upper

    df = log.copy()

    # Compute combo stat columns if missing
    if col not in df.columns:
        if col == "PRA" and all(c in df.columns for c in ["PTS","REB","AST"]):
            df["PRA"] = df["PTS"] + df["REB"] + df["AST"]
        elif col == "PR" and all(c in df.columns for c in ["PTS","REB"]):
            df["PR"] = df["PTS"] + df["REB"]
        elif col == "PA" and all(c in df.columns for c in ["PTS","AST"]):
            df["PA"] = df["PTS"] + df["AST"]
        elif col == "RA" and all(c in df.columns for c in ["REB","AST"]):
            df["RA"] = df["REB"] + df["AST"]

    if col not in df.columns:
        return {"mean": None, "std": None, "hit_rate": None,
                "close_mean": None, "blowout_mean": None, "n_games": 0}

    # Filter to games with real minutes (not DNP)
    if "MIN" in df.columns:
        played = df[df["MIN"] >= 10].copy()
    else:
        played = df.copy()

    vals = pd.to_numeric(played[col], errors="coerce").dropna()

    if len(vals) < 3:
        return {"mean": None, "std": None, "hit_rate": None,
                "close_mean": None, "blowout_mean": None, "n_games": len(vals)}

    mean_val = float(vals.mean())
    std_val  = float(vals.std()) if len(vals) > 1 else 0.0
    hit_rate = float((vals > line).mean())

    # Split by minutes as proxy for game state:
    # High minutes (>= player's own mean) → player was needed → close game
    # Low minutes (< mean - 5) → possibly blowout/garbage time
    mins_col = "MIN" if "MIN" in played.columns else None
    close_mean = blowout_mean = None

    if mins_col and len(played) >= 6:
        mins_vals = pd.to_numeric(played[mins_col], errors="coerce")
        mins_mean = float(mins_vals.mean())
        close_mask   = mins_vals >= (mins_mean - 2)
        blowout_mask = mins_vals < (mins_mean - 5)
        if close_mask.sum() >= 3:
            close_mean = float(vals[close_mask.values[:len(vals)]].mean()) if len(vals[close_mask.values[:len(vals)]]) >= 3 else mean_val
        if blowout_mask.sum() >= 3:
            blowout_mean = float(vals[blowout_mask.values[:len(vals)]].mean()) if len(vals[blowout_mask.values[:len(vals)]]) >= 3 else mean_val

    return {
        "mean":         round(mean_val, 2),
        "std":          round(std_val, 2),
        "hit_rate":     round(hit_rate, 4),
        "close_mean":   round(close_mean, 2) if close_mean is not None else round(mean_val, 2),
        "blowout_mean": round(blowout_mean, 2) if blowout_mean is not None else round(mean_val * 0.75, 2),
        "n_games":      len(vals),
    }


def compute_state_hit_probs(
    output_dist: dict,
    line: float,
    role: str,
    shot_prof: str,
    state_probs: dict,
) -> dict:
    """
    For each game state, compute the conditional hit probability
    using player's actual output distribution shifted by the
    state-specific output modifier.

    Uses normal distribution CDF with player's actual mean and std.
    """
    mean = output_dist.get("mean")
    std  = output_dist.get("std")

    if mean is None or std is None or std == 0:
        # Fall back to flat hit rate across all states
        hr = output_dist.get("hit_rate", 0.5) or 0.5
        return {state: hr for state in state_probs}

    hit_probs = {}
    for state in state_probs:
        modifier    = get_output_modifier(role, state, shot_prof)
        adj_mean    = mean * modifier

        # Use actual std but don't let modifier shrink it below baseline
        # (variance doesn't compress proportionally with mean in blowouts)
        adj_std = max(std * 0.85, std * modifier)

        if adj_std > 0:
            # P(X > line) using normal CDF
            z = (line - adj_mean) / adj_std
            hit_prob = 1.0 - scipy_stats.norm.cdf(z)
        else:
            hit_prob = 1.0 if adj_mean > line else 0.0

        hit_probs[state] = round(float(np.clip(hit_prob, 0.01, 0.99)), 4)

    return hit_probs


def compute_weighted_hit(state_probs: dict, hit_probs: dict) -> float:
    """Overall expected hit rate weighted across game states."""
    return round(sum(state_probs[s] * hit_probs.get(s, 0.5) for s in state_probs), 4)


# ── Full player game script profile ──────────────────────────────────────────

def build_game_script_profile(
    log: pd.DataFrame,
    stat: str,
    line: float,
    spread: Optional[float],
    game_total: Optional[float],
    minutes_mean: Optional[float] = None,
    minutes_cv: Optional[float] = None,
) -> dict:
    """
    Build the complete game script profile for a single player/stat/line.
    This is what gets attached to each prop card at pipeline time.
    """
    shot_prof   = shot_profile(stat)
    state_probs = compute_state_probs(spread, game_total)

    # Get player's actual minutes if not passed in
    if minutes_mean is None and log is not None and not log.empty:
        if "MIN" in log.columns:
            mins = pd.to_numeric(log["MIN"], errors="coerce").dropna()
            mins_played = mins[mins >= 5]
            minutes_mean = float(mins_played.mean()) if len(mins_played) > 0 else 0.0
            minutes_cv   = float(mins_played.std() / mins_played.mean()) if len(mins_played) > 1 and mins_played.mean() > 0 else 0.0
        else:
            minutes_mean = 0.0
            minutes_cv   = 0.0

    role         = classify_role(minutes_mean or 0, minutes_cv or 0)
    output_dist  = compute_player_output_distribution(log, stat, line)
    hit_probs    = compute_state_hit_probs(output_dist, line, role, shot_prof, state_probs)
    weighted_hit = compute_weighted_hit(state_probs, hit_probs)

    return {
        "state_probs":   state_probs,
        "hit_probs":     hit_probs,
        "weighted_hit":  weighted_hit,
        "output_dist":   output_dist,
        "role":          role,
        "shot_profile":  shot_prof,
        "minutes_mean":  round(minutes_mean or 0, 1),
        "minutes_cv":    round(minutes_cv or 0, 3),
        "spread":        spread,
        "game_total":    game_total,
    }


# ── Pairwise covariance engine ────────────────────────────────────────────────

def compute_pairwise_correlation(
    log_a: pd.DataFrame,
    stat_a: str,
    log_b: pd.DataFrame,
    stat_b: str,
    same_team: bool,
) -> Optional[float]:
    """
    Compute Pearson correlation between two players' outputs on a given stat,
    aligned by game_id (same game, same night).

    Same-team players: direct game_id join.
    Opposing players: game_id join across team logs.

    Returns None if insufficient shared games (< 5).
    """
    if log_a is None or log_b is None or log_a.empty or log_b.empty:
        return None

    if "game_id" not in log_a.columns or "game_id" not in log_b.columns:
        return None

    # Add combo stats to both logs
    def add_combos(df):
        df = df.copy()
        if "PRA" not in df.columns and all(c in df.columns for c in ["PTS","REB","AST"]):
            df["PRA"] = df["PTS"] + df["REB"] + df["AST"]
        if "PR" not in df.columns and all(c in df.columns for c in ["PTS","REB"]):
            df["PR"] = df["PTS"] + df["REB"]
        if "PA" not in df.columns and all(c in df.columns for c in ["PTS","AST"]):
            df["PA"] = df["PTS"] + df["AST"]
        if "RA" not in df.columns and all(c in df.columns for c in ["REB","AST"]):
            df["RA"] = df["REB"] + df["AST"]
        return df

    log_a = add_combos(log_a)
    log_b = add_combos(log_b)

    col_a = stat_a.upper()
    col_b = stat_b.upper()

    if col_a not in log_a.columns or col_b not in log_b.columns:
        return None

    # Filter to real minutes only
    min_thresh = 8
    played_a = log_a[pd.to_numeric(log_a.get("MIN", pd.Series([99]*len(log_a))), errors="coerce") >= min_thresh]
    played_b = log_b[pd.to_numeric(log_b.get("MIN", pd.Series([99]*len(log_b))), errors="coerce") >= min_thresh]

    # Align by game_id
    merged = played_a[["game_id", col_a]].merge(
        played_b[["game_id", col_b]],
        on="game_id",
        how="inner"
    )

    if len(merged) < 5:
        return None  # Not enough shared games for meaningful correlation

    vals_a = pd.to_numeric(merged[col_a], errors="coerce")
    vals_b = pd.to_numeric(merged[col_b], errors="coerce")
    valid  = vals_a.notna() & vals_b.notna()

    if valid.sum() < 5:
        return None

    try:
        corr, pval = scipy_stats.pearsonr(vals_a[valid], vals_b[valid])
        # Only return statistically meaningful correlations (p < 0.15)
        # With small samples (5-10 games) we can't be too strict
        if pval < 0.15:
            return round(float(corr), 4)
        else:
            # Weak/noisy — return a dampened version
            return round(float(corr) * 0.4, 4)
    except Exception:
        return None


def estimate_opposing_correlation(
    stat_a: str,
    stat_b: str,
    role_a: str,
    role_b: str,
) -> float:
    """
    For opposing-team players where shared game sample is too small,
    estimate correlation from stat type and role.

    Key insights:
    - PTS vs PTS across teams: slightly negative (competing for pace/possessions)
    - REB vs REB across teams: moderately negative (same boards)
    - AST vs AST across teams: near zero (not directly competing)
    - Stars dominate pace — when star_a goes nuclear, game pace increases,
      which slightly benefits star_b's counting stats
    - High-total games lift both sides (positive correlation on volume stats)
    """
    s_a = stat_a.upper()
    s_b = stat_b.upper()

    # Rebounds are contested — most negative cross-team correlation
    if s_a in ("REB","RA","PR") and s_b in ("REB","RA","PR"):
        return -0.18

    # Points: slight negative (possession competition) offset by pace correlation
    if s_a in ("PTS","PA","PR","PRA") and s_b in ("PTS","PA","PR","PRA"):
        return -0.08

    # Assists: near zero
    if s_a in ("AST","PA","RA") and s_b in ("AST","PA","RA"):
        return 0.02

    # Mixed combos — small negative
    return -0.05


# ── Parlay correlation matrix ─────────────────────────────────────────────────

def build_correlation_matrix(
    legs: list[dict],
    player_logs: dict,
) -> np.ndarray:
    """
    Build an n×n correlation matrix for n parlay legs.
    Diagonal = 1.0 (each leg perfectly correlated with itself).
    Off-diagonal = pairwise correlation from actual game logs or estimation.

    legs: list of prop card dicts
    player_logs: dict of player_name+team → DataFrame
    """
    n = len(legs)
    corr_matrix = np.eye(n)

    for i in range(n):
        for j in range(i+1, n):
            leg_a = legs[i]
            leg_b = legs[j]

            key_a = f"{leg_a.get('player_name','')}_{leg_a.get('team','')}"
            key_b = f"{leg_b.get('player_name','')}_{leg_b.get('team','')}"

            log_a = player_logs.get(key_a)
            log_b = player_logs.get(key_b)

            same_team    = leg_a.get("team") == leg_b.get("team")
            same_game    = (leg_a.get("team") == leg_b.get("team") or
                           leg_a.get("opponent") == leg_b.get("team") or
                           leg_a.get("team") == leg_b.get("opponent"))
            diff_game    = not same_game

            if diff_game:
                # Different games — independent (small slate-wide pace correlation)
                corr = 0.04
            elif same_team:
                # Try real correlation from shared game logs
                corr = compute_pairwise_correlation(
                    log_a, leg_a.get("stat","PTS"),
                    log_b, leg_b.get("stat","PTS"),
                    same_team=True
                )
                if corr is None:
                    # Fall back: same team, same game — moderately correlated
                    # Stars and role players are usage-linked
                    prof_a = leg_a.get("game_script_profile", {})
                    prof_b = leg_b.get("game_script_profile", {})
                    role_a = prof_a.get("role", "role")
                    role_b = prof_b.get("role", "role")
                    # Star + star: high correlation (same game script)
                    # Star + role: moderate (role player usage moves with star)
                    if role_a == "star" and role_b == "star":
                        corr = 0.55
                    elif "star" in (role_a, role_b):
                        corr = 0.35
                    else:
                        corr = 0.25
            else:
                # Opposing team — try real correlation first
                corr = compute_pairwise_correlation(
                    log_a, leg_a.get("stat","PTS"),
                    log_b, leg_b.get("stat","PTS"),
                    same_team=False
                )
                if corr is None:
                    prof_a = leg_a.get("game_script_profile", {})
                    prof_b = leg_b.get("game_script_profile", {})
                    corr = estimate_opposing_correlation(
                        leg_a.get("stat","PTS"),
                        leg_b.get("stat","PTS"),
                        prof_a.get("role","role"),
                        prof_b.get("role","role"),
                    )

            corr_matrix[i][j] = corr
            corr_matrix[j][i] = corr

    return corr_matrix


# ── Joint hit probability ─────────────────────────────────────────────────────

def compute_joint_hit_probability(
    hit_probs: list[float],
    corr_matrix: np.ndarray,
    n_simulations: int = 50000,
) -> float:
    """
    Compute the joint probability that ALL legs hit,
    accounting for the correlation structure.

    Uses Gaussian copula simulation:
    1. Map each marginal hit probability to a standard normal quantile
    2. Draw correlated normal samples using the correlation matrix
    3. For each simulation, check if all legs hit
    4. Joint probability = fraction of simulations where all legs hit

    This is the correct way to combine correlated binary outcomes.
    An independent multiplication would overestimate true joint probability
    when legs are positively correlated (same team) and underestimate
    when negatively correlated (opposing teams on same stat).
    """
    n = len(hit_probs)
    if n == 0:
        return 0.0
    if n == 1:
        return hit_probs[0]

    # Convert hit probabilities to standard normal thresholds.
    # We want P(Z > threshold) = hit_prob, so threshold = norm.ppf(1 - hit_prob).
    # Equivalently: a leg "hits" when its correlated normal draw exceeds this threshold.
    thresholds = np.array([scipy_stats.norm.ppf(1.0 - p) for p in hit_probs])

    # Ensure correlation matrix is positive semi-definite
    try:
        # Nearest PSD matrix if needed
        eigvals = np.linalg.eigvals(corr_matrix)
        if np.any(eigvals < 0):
            # Clip negative eigenvalues
            eigvals_c, eigvecs = np.linalg.eigh(corr_matrix)
            eigvals_c = np.maximum(eigvals_c, 1e-6)
            corr_matrix = eigvecs @ np.diag(eigvals_c) @ eigvecs.T
            # Re-normalize diagonal to 1
            d = np.sqrt(np.diag(corr_matrix))
            corr_matrix = corr_matrix / np.outer(d, d)
    except Exception:
        corr_matrix = np.eye(n)

    try:
        # Draw correlated standard normal samples
        rng = np.random.default_rng(42)  # deterministic seed for reproducibility
        samples = rng.multivariate_normal(
            mean=np.zeros(n),
            cov=corr_matrix,
            size=n_simulations
        )

        # Each leg hits if the sample exceeds its threshold
        # (since P(Z > threshold) = hit_prob by construction)
        hits = samples > thresholds  # shape: (n_simulations, n)
        all_hit = hits.all(axis=1)
        joint_prob = float(all_hit.mean())

    except Exception as e:
        logger.warning(f"Simulation failed: {e}, falling back to independence")
        joint_prob = float(np.prod(hit_probs))

    return round(joint_prob, 4)


# ── Independent baseline (for comparison) ────────────────────────────────────

def compute_independent_joint(hit_probs: list[float]) -> float:
    """Naive independent multiplication — used to show correlation impact."""
    if not hit_probs:
        return 0.0
    return round(float(np.prod(hit_probs)), 4)


# ── Parlay warnings ───────────────────────────────────────────────────────────

def generate_parlay_warnings(legs: list[dict], corr_matrix: np.ndarray) -> list[dict]:
    """
    Generate specific, player-aware warnings for a parlay.
    Not generic — references the actual players and stats involved.
    """
    warnings = []
    n = len(legs)

    if n < 2:
        return warnings

    # 1. High positive correlation pairs (same team, same game script risk)
    for i in range(n):
        for j in range(i+1, n):
            corr = corr_matrix[i][j]
            la, lb = legs[i], legs[j]
            name_a = la.get("player_name","?").split()[-1]
            name_b = lb.get("player_name","?").split()[-1]

            if corr >= 0.45:
                warnings.append({
                    "type": "danger",
                    "msg": f"🔴 {name_a} + {name_b} are highly correlated ({corr:.0%}). "
                           f"Same game script — if {la.get('team','')} blows this game, both legs fail together."
                })
            elif corr >= 0.28:
                warnings.append({
                    "type": "caution",
                    "msg": f"⚡ {name_a} + {name_b} share game script risk ({corr:.0%} corr). "
                           f"Partial correlation — not independent bets."
                })

    # 2. Shooting dependency concentration
    dep_legs = [l for l in legs if shot_profile(l.get("stat","")) == "dependent"]
    if len(dep_legs) >= 2:
        names = " + ".join(l.get("player_name","?").split()[-1] for l in dep_legs)
        warnings.append({
            "type": "danger",
            "msg": f"📉 {names}: {len(dep_legs)} shooting-dependent legs. "
                   f"A cold-shooting night for either kills multiple legs simultaneously."
        })
    elif len(dep_legs) == 1:
        name = dep_legs[0].get("player_name","?").split()[-1]
        stat = dep_legs[0].get("stat","")
        warnings.append({
            "type": "caution",
            "msg": f"📊 {name} {stat} is shooting-dependent. "
                   f"Consider an RA or AST line for the same player if available."
        })

    # 3. Role player blowout risk concentration
    for leg in legs:
        prof = leg.get("game_script_profile", {})
        role = prof.get("role","")
        spread = prof.get("spread")
        name = leg.get("player_name","?").split()[-1]
        stat = leg.get("stat","")
        if role == "fringe" and spread is not None and spread < -8:
            warnings.append({
                "type": "caution",
                "msg": f"⚠️ {name} {stat}: fringe player on a {abs(spread):.0f}-pt favorite. "
                       f"Minutes unpredictable — may not see enough floor time."
            })

    # 4. Regression risk legs
    for leg in legs:
        hook = leg.get("hook_level","")
        name = leg.get("player_name","?").split()[-1]
        stat = leg.get("stat","")
        if "REGRESSION" in str(hook) or "hot" in str(hook).lower():
            warnings.append({
                "type": "caution",
                "msg": f"📊 {name} {stat}: running hot — regression risk flagged by model."
            })

    # 5. Clean parlay
    if not warnings:
        all_locks = all(l.get("is_lock") for l in legs)
        warnings.append({
            "type": "good",
            "msg": ("✅ All legs are Locks with low correlation. Clean parlay structure." if all_locks
                    else "✅ No structural conflicts. Reasonable correlation profile.")
        })

    return warnings


# ── Main parlay analysis function ─────────────────────────────────────────────

def compute_parlay_profile(
    legs: list[dict],
    player_logs: dict,
) -> dict:
    """
    Full parlay analysis for selected legs.

    legs: list of prop card dicts (from allProps JSON, with game_script_profile attached)
    player_logs: dict of "player_name_team" → DataFrame

    Returns:
    {
        "joint_hit_prob":      float,   # true joint probability (corr-adjusted)
        "independent_prob":    float,   # naive independent multiplication
        "correlation_impact":  float,   # difference (negative = correlation hurts)
        "corr_matrix":         list,    # n×n for display
        "leg_hit_probs":       list,    # per-leg weighted hit probs
        "warnings":            list,    # player-specific warnings
        "shooting_indep_pct":  int,     # % of legs that are shot-independent
        "parlay_grade":        str,     # A/B/C/D/F
    }
    """
    if len(legs) < 2:
        return {"error": "Need at least 2 legs"}

    # Extract per-leg hit probabilities from game script profiles
    leg_hit_probs = []
    for leg in legs:
        prof = leg.get("game_script_profile")
        if prof and prof.get("weighted_hit") is not None:
            leg_hit_probs.append(prof["weighted_hit"])
        else:
            # Fall back to model's hit rate
            hrs = [leg.get("l5_hr"), leg.get("l10_hr"), leg.get("l20_hr")]
            valid = [h/100 for h in hrs if h is not None]
            leg_hit_probs.append(float(np.mean(valid)) if valid else 0.5)

    # Build correlation matrix
    corr_matrix = build_correlation_matrix(legs, player_logs)

    # Compute joint probability
    joint_prob       = compute_joint_hit_probability(leg_hit_probs, corr_matrix)
    independent_prob = compute_independent_joint(leg_hit_probs)
    corr_impact      = round(joint_prob - independent_prob, 4)

    # Shooting independence score
    ind_count  = sum(1 for l in legs if shot_profile(l.get("stat","")) == "independent")
    si_pct     = round((ind_count / len(legs)) * 100)

    # Parlay grade
    # Based on: joint hit prob, correlation structure, shot profile mix
    avg_corr = float(np.mean([corr_matrix[i][j]
                               for i in range(len(legs))
                               for j in range(i+1, len(legs))])) if len(legs) > 1 else 0

    dep_count = sum(1 for l in legs if shot_profile(l.get("stat","")) == "dependent")

    if joint_prob >= 0.45 and avg_corr <= 0.20 and dep_count == 0:
        grade = "A"
    elif joint_prob >= 0.35 and avg_corr <= 0.30 and dep_count <= 1:
        grade = "B"
    elif joint_prob >= 0.25 and avg_corr <= 0.40:
        grade = "C"
    elif joint_prob >= 0.15:
        grade = "D"
    else:
        grade = "F"

    # Generate warnings
    warnings = generate_parlay_warnings(legs, corr_matrix)

    return {
        "joint_hit_prob":      joint_prob,
        "independent_prob":    independent_prob,
        "correlation_impact":  corr_impact,
        "corr_matrix":         corr_matrix.tolist(),
        "leg_hit_probs":       leg_hit_probs,
        "warnings":            warnings,
        "shooting_indep_pct":  si_pct,
        "parlay_grade":        grade,
        "avg_correlation":     round(avg_corr, 3),
        "n_legs":              len(legs),
    }
