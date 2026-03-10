"""
context.py — Situational context metrics for player props research.

Computes factors beyond raw stat averages:
- Rest days & back-to-back flag
- Home vs away performance splits
- Recent trend (L5 vs L20 direction)
- Opponent defensive tendency (how many pts/reb/ast they allow)
- Revenge game flag (player facing former team)
- Role/minutes trend (is his role growing or shrinking?)
- Consistency score (how volatile is this player game-to-game?)
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ── Rest & schedule context ───────────────────────────────────────────────────

def compute_rest_context(
    player_log: pd.DataFrame,
    game_date: date,
) -> dict:
    """
    Given a player's game log and today's game date, compute:
    - days_rest: days since last game (0 = back-to-back)
    - is_back_to_back: True if days_rest == 0
    - games_last_7_days: workload indicator
    - games_last_14_days: workload indicator
    """
    if player_log.empty:
        return {
            "days_rest": None,
            "is_back_to_back": None,
            "games_last_7_days": None,
            "games_last_14_days": None,
        }

    dates = pd.to_datetime(player_log["GAME_DATE"]).dt.date
    past_dates = sorted([d for d in dates if d < game_date], reverse=True)

    if not past_dates:
        return {
            "days_rest": None,
            "is_back_to_back": None,
            "games_last_7_days": 0,
            "games_last_14_days": 0,
        }

    last_game = past_dates[0]
    days_rest = (game_date - last_game).days - 1  # 0 = back-to-back

    # Guard against negative values caused by date parsing anomalies
    # (e.g. game_date == last_game produces -1, not a real B2B)
    if days_rest < 0:
        days_rest = 0

    cutoff_7  = game_date - timedelta(days=7)
    cutoff_14 = game_date - timedelta(days=14)

    games_7  = sum(1 for d in past_dates if d >= cutoff_7)
    games_14 = sum(1 for d in past_dates if d >= cutoff_14)

    return {
        "days_rest": days_rest,
        "is_back_to_back": "🔴 YES" if days_rest == 0 else "No",
        "games_last_7_days": games_7,
        "games_last_14_days": games_14,
    }


# ── Home / Away splits ────────────────────────────────────────────────────────

def compute_home_away_splits(
    player_log: pd.DataFrame,
    stat: str = "PTS",
) -> dict:
    """
    Compute home vs away averages for a given stat.
    Requires a 'location' column ('Home' or 'Away') in the log.
    """
    if player_log.empty or "location" not in player_log.columns:
        return {}

    result = {}
    for loc in ["Home", "Away"]:
        subset = player_log[player_log["location"] == loc]
        if subset.empty or stat not in subset.columns:
            result[f"{stat}_avg_{loc.lower()}"] = None
            result[f"{stat}_games_{loc.lower()}"] = 0
            continue
        vals = pd.to_numeric(subset[stat], errors="coerce").dropna()
        result[f"{stat}_avg_{loc.lower()}"] = round(float(vals.mean()), 1) if len(vals) else None
        result[f"{stat}_games_{loc.lower()}"] = len(vals)

    return result


def get_all_home_away_splits(
    player_log: pd.DataFrame,
    stats: list[str] = None,
) -> dict:
    """Return home/away splits for all key stats."""
    if stats is None:
        stats = ["PTS", "REB", "AST", "FG3M", "BLK", "STL", "TOV"]
    result = {}
    for stat in stats:
        result.update(compute_home_away_splits(player_log, stat))
    return result


# ── Trend analysis ────────────────────────────────────────────────────────────

def compute_trend(
    player_log: pd.DataFrame,
    stat: str,
    short_window: int = 5,
    long_window: int = 20,
) -> dict:
    """
    Compare short-term vs long-term average to detect trend direction.

    Returns:
        trend_direction: "📈 Hot", "📉 Cold", "➡️ Neutral"
        short_avg: average over last short_window games
        long_avg: average over last long_window games
        pct_change: percentage change from long to short avg
    """
    if player_log.empty or stat not in player_log.columns:
        return {
            f"{stat}_trend": "—",
            f"{stat}_l{short_window}_avg": None,
            f"{stat}_l{long_window}_avg": None,
            f"{stat}_trend_pct": None,
        }

    vals_all  = pd.to_numeric(player_log[stat], errors="coerce").dropna()
    short_avg = float(vals_all.head(short_window).mean()) if len(vals_all) >= short_window else None
    long_avg  = float(vals_all.head(long_window).mean())  if len(vals_all) >= 3 else None

    if short_avg is None or long_avg is None or long_avg == 0:
        direction = "—"
        pct_change = None
    else:
        pct_change = round((short_avg - long_avg) / long_avg * 100, 1)
        if pct_change >= 10:
            direction = "📈 Hot"
        elif pct_change <= -10:
            direction = "📉 Cold"
        else:
            direction = "➡️ Neutral"

    return {
        f"{stat}_trend": direction,
        f"{stat}_l{short_window}_avg": round(short_avg, 1) if short_avg is not None else None,
        f"{stat}_l{long_window}_avg": round(long_avg, 1) if long_avg is not None else None,
        f"{stat}_trend_pct": pct_change,
    }


def compute_all_trends(player_log: pd.DataFrame) -> dict:
    """Compute trends for all key stats."""
    result = {}
    for stat in ["PTS", "REB", "AST", "FG3M", "BLK", "MIN"]:
        result.update(compute_trend(player_log, stat))
    return result


# ── Consistency score ─────────────────────────────────────────────────────────

def compute_consistency(
    player_log: pd.DataFrame,
    stat: str,
    n_games: int = 10,
) -> dict:
    """
    Coefficient of variation (std/mean) — lower = more consistent.
    Also returns a label: 🎯 Consistent (<15%), ⚡ Variable (15-30%), 🎲 Boom/Bust (>30%)
    """
    if player_log.empty or stat not in player_log.columns:
        return {f"{stat}_consistency": "—", f"{stat}_cv": None}

    vals = pd.to_numeric(player_log[stat], errors="coerce").dropna().head(n_games)
    if len(vals) < 3 or vals.mean() == 0:
        return {f"{stat}_consistency": "—", f"{stat}_cv": None}

    cv = round(float(vals.std(ddof=0) / vals.mean() * 100), 1)

    if cv < 20:
        label = "🎯 Consistent"
    elif cv < 35:
        label = "⚡ Variable"
    else:
        label = "🎲 Boom/Bust"

    return {f"{stat}_consistency": label, f"{stat}_cv": cv}


# ── Opponent defensive tendency ───────────────────────────────────────────────

def compute_opponent_defense(
    full_df: pd.DataFrame,
    opponent_abbr: str,
    stat: str = "PTS",
    position_filter: Optional[str] = None,
) -> dict:
    """
    How much does this opponent allow in a given stat?
    Looks at all games in the CSV where the opponent played defense.

    full_df: the complete game log CSV (all players)
    opponent_abbr: the team playing defense tonight
    stat: which stat to analyze
    position_filter: optional position filter (e.g. "G", "F", "C") - checks if Pos. contains this
    """
    if full_df.empty or "Opp" not in full_df.columns or stat not in full_df.columns:
        return {f"opp_{stat.lower()}_allowed_avg": None, f"opp_{stat.lower()}_allowed_rank": None}

    # Games where this team was the opponent (playing defense)
    opp_games = full_df[full_df["Opp"].str.upper() == opponent_abbr.upper()].copy()

    if position_filter and "position" in opp_games.columns:
        opp_games = opp_games[
            opp_games["position"].fillna("").str.contains(position_filter, case=False)
        ]

    if opp_games.empty:
        return {f"opp_{stat.lower()}_allowed_avg": None}

    vals = pd.to_numeric(opp_games[stat], errors="coerce").dropna()
    if vals.empty:
        return {f"opp_{stat.lower()}_allowed_avg": None}

    avg_allowed = round(float(vals.mean()), 1)

    # Rank vs all teams (percentile — higher = softer defense for this stat)
    all_team_avgs = {}
    for team in full_df["Opp"].dropna().unique():
        t_games = full_df[full_df["Opp"].str.upper() == team.upper()]
        t_vals  = pd.to_numeric(t_games[stat], errors="coerce").dropna()
        if len(t_vals) >= 5:
            all_team_avgs[team] = float(t_vals.mean())

    # Require at least 10 data points to produce a meaningful defensive rating.
    # With fewer games we can't reliably distinguish soft vs tough — surface as unknown.
    MIN_SAMPLE = 10
    if len(vals) < MIN_SAMPLE:
        return {
            f"opp_{stat.lower()}_allowed_avg": avg_allowed,
            f"opp_{stat.lower()}_matchup": "—",
        }

    if len(all_team_avgs) >= 5:
        sorted_avgs = sorted(all_team_avgs.values())
        n = len(sorted_avgs)
        # Find the index of the closest match using enumerate — avoids
        # list.index() which returns the FIRST occurrence and breaks when
        # multiple teams share the same rounded average.
        closest_idx = min(
            range(n),
            key=lambda i: abs(sorted_avgs[i] - avg_allowed)
        )
        rank_pct = round(closest_idx / (n - 1) * 100) if n > 1 else 50
        # Soft defense = allows a lot = high percentile = favorable for overs
        if rank_pct >= 70:
            matchup_label = "🟢 Soft D"
        elif rank_pct <= 30:
            matchup_label = "🔴 Tough D"
        else:
            matchup_label = "🟡 Mid D"
    else:
        matchup_label = "—"

    return {
        f"opp_{stat.lower()}_allowed_avg": avg_allowed,
        f"opp_{stat.lower()}_matchup": matchup_label,
    }


def get_positional_recent_lines(
    full_df: pd.DataFrame,
    opponent_abbr: str,
    stat: str,
    position_filter: str,
    line_val: float,
    n: int = 8,
) -> dict:
    """
    Return the last N individual game values for players at this position
    who faced this opponent, compared to line_val.

    Returns a dict with:
      - pos_lines: list of {player, value, hit} dicts (newest first)
      - pos_line_hit_count: int
      - pos_line_total: int
      - pos_line_hit_rate: float (0-100)
      - pos_line_label: e.g. "6/8 Gs vs MIA hit O3.5 (75%)"
    """
    empty = {
        "pos_lines": [],
        "pos_line_hit_count": None,
        "pos_line_total": None,
        "pos_line_hit_rate": None,
        "pos_line_label": None,
    }

    if full_df.empty or stat not in full_df.columns or "Opp" not in full_df.columns:
        return empty

    opp_games = full_df[full_df["Opp"].str.upper() == opponent_abbr.upper()].copy()

    if position_filter and "position" in opp_games.columns:
        opp_games = opp_games[
            opp_games["position"].fillna("").str.contains(position_filter, case=False)
        ]

    if opp_games.empty:
        return empty

    # Sort newest first — use GAME_DATE if available
    if "GAME_DATE" in opp_games.columns:
        opp_games = opp_games.sort_values("GAME_DATE", ascending=False)

    opp_games = opp_games.head(n)

    vals = pd.to_numeric(opp_games[stat], errors="coerce")
    player_col = "player_name" if "player_name" in opp_games.columns else None

    lines = []
    for idx, (val, row) in enumerate(zip(vals, opp_games.itertuples())):
        if pd.isna(val):
            continue
        pname = getattr(row, "player_name", "—") if player_col else "—"
        lines.append({
            "player": pname,
            "value": round(float(val), 1),
            "hit": bool(val > line_val),
        })

    if not lines:
        return empty

    hit_count = sum(1 for l in lines if l["hit"])
    total = len(lines)
    hit_rate = round(hit_count / total * 100, 1)

    pos_label = position_filter if position_filter else "players"
    label = f"{hit_count}/{total} {pos_label}s vs {opponent_abbr} hit O{line_val} ({hit_rate:.0f}%)"

    return {
        "pos_lines": lines,
        "pos_line_hit_count": hit_count,
        "pos_line_total": total,
        "pos_line_hit_rate": hit_rate,
        "pos_line_label": label,
    }




def is_revenge_game(
    player_log: pd.DataFrame,
    current_team: str,
    opponent_abbr: str,
) -> dict:
    """
    Check if a player previously appeared in games where opponent_abbr was
    their team (i.e. they used to play for the opponent).
    Uses the full CSV — if a player has rows where team == opponent_abbr,
    they're a former member.
    """
    if player_log.empty or "team" not in player_log.columns:
        return {"revenge_game": "—"}

    former_teams = player_log["team"].str.upper().unique().tolist()
    opp_upper = opponent_abbr.upper()
    curr_upper = current_team.upper()

    if opp_upper in former_teams and curr_upper != opp_upper:
        return {"revenge_game": "🔥 Revenge Game"}

    return {"revenge_game": "—"}


# ── Minutes / role trend ──────────────────────────────────────────────────────

def compute_minutes_trend(player_log: pd.DataFrame) -> dict:
    """
    Is this player's role expanding, stable, or shrinking?
    Compares L5 vs L15 minutes average.
    """
    if player_log.empty or "MIN" not in player_log.columns:
        return {"minutes_trend": "—", "minutes_l5_avg": None, "minutes_l15_avg": None}

    vals = pd.to_numeric(player_log["MIN"], errors="coerce").dropna()

    l5  = round(float(vals.head(5).mean()), 1)  if len(vals) >= 3 else None
    l15 = round(float(vals.head(15).mean()), 1) if len(vals) >= 5 else None

    if l5 is None or l15 is None or l15 == 0:
        return {"minutes_trend": "—", "minutes_l5_avg": l5, "minutes_l15_avg": l15}

    diff = l5 - l15
    if diff >= 3:
        label = "📈 More MPG"
    elif diff <= -3:
        label = "📉 Less MPG"
    else:
        label = "➡️ Stable"

    return {
        "minutes_trend": label,
        "minutes_l5_avg": l5,
        "minutes_l15_avg": l15,
        "minutes_diff": round(diff, 1),
    }


# ── Minutes stability (coefficient of variation) ──────────────────────────────

def compute_minutes_stability(
    player_log: pd.DataFrame,
    n_games: int = 10,
) -> dict:
    """
    How predictable is this player's playing time?

    Coefficient of variation (std/mean) on minutes over the last n_games.
    Low CV = clock-steady role. High CV = game-script dependent, DNP risk.

    This is separate from minutes_trend (direction) — a player can be
    trending upward in minutes but still be highly volatile game-to-game.

    Labels:
      🎯 Stable   : CV <= 0.25  (minutes very predictable)
      ⚡ Unstable : CV 0.26–0.45 (some variance, proceed with care)
      🎲 Volatile : CV > 0.45   (rotation risk — line may be a trap)
    """
    if player_log.empty or "MIN" not in player_log.columns:
        return {"minutes_cv": None, "minutes_stability": "—"}

    vals = pd.to_numeric(player_log["MIN"], errors="coerce").dropna().head(n_games)
    if len(vals) < 3 or vals.mean() == 0:
        return {"minutes_cv": None, "minutes_stability": "—"}

    cv = round(float(vals.std(ddof=0) / vals.mean()), 3)

    if cv > 0.45:
        label = "🎲 Volatile"
    elif cv > 0.25:
        label = "⚡ Unstable"
    else:
        label = "🎯 Stable"

    return {
        "minutes_cv":        cv,
        "minutes_stability": label,
        "minutes_min_l10":   round(float(vals.min()), 1),
        "minutes_max_l10":   round(float(vals.max()), 1),
    }


# ── Master context builder ────────────────────────────────────────────────────

def build_player_context(
    player_name: str,
    team_abbr: str,
    opponent_abbr: str,
    player_log: pd.DataFrame,
    full_df: pd.DataFrame,
    game_date: date,
    today_location: str = "—",  # "Home" or "Away" for tonight's game
    position: str = "",          # player position: G / F / C
) -> dict:
    """
    Build the complete context row for a player.
    Returns a flat dict ready to write to the Context sheet.
    """
    ctx: dict = {
        "player_name":     player_name,
        "team":            team_abbr,
        "opponent":        opponent_abbr,
        "tonight_location": today_location,
    }

    # Rest
    ctx.update(compute_rest_context(player_log, game_date))

    # Minutes trend
    ctx.update(compute_minutes_trend(player_log))

    # Minutes stability (coefficient of variation — rotation risk detection)
    ctx.update(compute_minutes_stability(player_log))

    # Trends for key stats
    ctx.update(compute_all_trends(player_log))

    # Consistency for key stats
    for stat in ["PTS", "REB", "AST"]:
        ctx.update(compute_consistency(player_log, stat, n_games=10))

    # Home/Away historical splits
    ctx.update(get_all_home_away_splits(player_log, stats=["PTS", "REB", "AST"]))

    # Opponent defense — aggregate and positional
    for stat in ["PTS", "REB", "AST", "BLK"]:
        ctx.update(compute_opponent_defense(full_df, opponent_abbr, stat))

    # Positional mismatch — how does this opponent defend THIS position?
    if position and position in ("G", "F", "C"):
        for stat in ["PTS", "REB", "AST"]:
            pos_def = compute_opponent_defense(full_df, opponent_abbr, stat, position_filter=position)
            matchup = pos_def.get(f"opp_{stat.lower()}_matchup", "")
            if matchup:
                ctx[f"opp_{stat.lower()}_pos_matchup"] = matchup
                # Flag if bottom-8 defense (Soft D) for this position/stat combo
                ctx[f"opp_{stat.lower()}_pos_weak"] = "Soft" in matchup

    # Revenge game
    ctx.update(is_revenge_game(player_log, team_abbr, opponent_abbr))

    # Defensive trend — how has opponent been defending recently vs season?
    for stat in ["PTS", "REB", "AST"]:
        ctx.update(compute_defensive_trend(full_df, opponent_abbr, stat, recent_n=10))

    return ctx


# ── Head-to-head player history vs opponent ──────────────────────────────────

def compute_h2h(
    player_log: pd.DataFrame,
    opponent_abbr: str,
    stat: str,
    line_val: float,
    n: int = 8,
) -> dict:
    """
    How has THIS player performed against THIS specific opponent?
    Returns avg, hit rate, and last N values vs that opponent.
    """
    empty = {"h2h_avg": None, "h2h_hit_rate": None, "h2h_total": None, "h2h_values": []}

    if player_log.empty or "Opp" not in player_log.columns or stat not in player_log.columns:
        return empty

    vs_opp = player_log[
        player_log["Opp"].str.upper() == opponent_abbr.upper()
    ].copy()

    if vs_opp.empty:
        return empty

    if "GAME_DATE" in vs_opp.columns:
        vs_opp = vs_opp.sort_values("GAME_DATE", ascending=False)

    vs_opp = vs_opp.head(n)
    vals = pd.to_numeric(vs_opp[stat], errors="coerce").dropna()

    if vals.empty:
        return empty

    avg = round(float(vals.mean()), 1)
    hits = int((vals > line_val).sum())
    total = len(vals)
    hit_rate = round(hits / total * 100, 1) if total else None

    return {
        "h2h_avg":      avg,
        "h2h_hit_rate": hit_rate,
        "h2h_total":    total,
        "h2h_values":   [round(float(v), 1) for v in vals.tolist()],
    }


# ── Opponent recent defensive trend ──────────────────────────────────────────

def compute_defensive_trend(
    full_df: pd.DataFrame,
    opponent_abbr: str,
    stat: str = "PTS",
    recent_n: int = 10,
) -> dict:
    """
    How is this opponent defending recently vs season average?
    Returns season avg allowed, L10 avg allowed, and direction.
    """
    empty = {
        f"opp_{stat.lower()}_allowed_season": None,
        f"opp_{stat.lower()}_allowed_l{recent_n}": None,
        f"opp_{stat.lower()}_def_trend": None,
    }

    if full_df.empty or "Opp" not in full_df.columns or stat not in full_df.columns:
        return empty

    opp_games = full_df[full_df["Opp"].str.upper() == opponent_abbr.upper()].copy()

    if opp_games.empty:
        return empty

    if "GAME_DATE" in opp_games.columns:
        opp_games = opp_games.sort_values("GAME_DATE", ascending=False)

    vals_all = pd.to_numeric(opp_games[stat], errors="coerce").dropna()
    if vals_all.empty:
        return empty

    season_avg = round(float(vals_all.mean()), 1)

    recent = opp_games.head(recent_n)
    vals_recent = pd.to_numeric(recent[stat], errors="coerce").dropna()
    if vals_recent.empty:
        return empty

    recent_avg = round(float(vals_recent.mean()), 1)
    delta = round(recent_avg - season_avg, 1)

    if delta >= 3:
        trend = f"↑ +{delta} vs season (getting softer)"
    elif delta <= -3:
        trend = f"↓ {delta} vs season (tightening up)"
    else:
        trend = f"→ {delta:+.1f} vs season (stable)"

    return {
        f"opp_{stat.lower()}_allowed_season": season_avg,
        f"opp_{stat.lower()}_allowed_l{recent_n}": recent_avg,
        f"opp_{stat.lower()}_def_trend": trend,
        f"opp_{stat.lower()}_def_delta": delta,
    }


# ── Usage share — FGA% and minutes rank within team ──────────────────────────

def compute_usage_share(
    player_name: str,
    player_log: pd.DataFrame,
    full_df: pd.DataFrame,
    team_abbr: str,
    n_games: int = 10,
) -> dict:
    """
    Compute this player's FGA share and minutes rank within their team.

    Returns:
      fga_share_l5     - player FGA% of team total last 5 games (float 0-100)
      fga_share_l10    - player FGA% of team total last 10 games
      fga_rank         - team rank by FGA (1=highest usage)
      min_rank         - team rank by minutes (1=most minutes)
      team_fga_l10     - team total FGA per game last 10 games
      usage_tier       - "STAR" / "CO-STAR" / "ROLE" / "BENCH"
    """
    empty = {
        "fga_share_l5": None,
        "fga_share_l10": None,
        "fga_rank": None,
        "min_rank": None,
        "team_fga_l10": None,
        "usage_tier": None,
    }

    if player_log.empty or "FGA" not in player_log.columns:
        return empty
    if full_df.empty or "FGA" not in full_df.columns or "team" not in full_df.columns:
        return empty

    # Filter full_df to this team's players
    team_df = full_df[full_df["team"].str.upper() == team_abbr.upper()].copy()
    if team_df.empty:
        return empty

    # Sort by date — use GAME_DATE if available
    if "GAME_DATE" in team_df.columns:
        team_df = team_df.sort_values("GAME_DATE", ascending=False)
    if "GAME_DATE" in player_log.columns:
        player_log = player_log.sort_values("GAME_DATE", ascending=False)

    # ── Player FGA per game ────────────────────────────────────────────────
    p_fga_l5  = pd.to_numeric(player_log["FGA"].head(5),  errors="coerce").dropna()
    p_fga_l10 = pd.to_numeric(player_log["FGA"].head(10), errors="coerce").dropna()
    p_min_l10 = pd.to_numeric(player_log["MIN"].head(10), errors="coerce").dropna()

    if p_fga_l10.empty:
        return empty

    # ── Team FGA per game (sum across all players per game, then avg) ──────
    # Group by GAME_DATE to get team totals per game
    if "GAME_DATE" in team_df.columns:
        # Get team FGA per game for last n_games unique dates
        dates = team_df["GAME_DATE"].dropna().unique()
        dates_sorted = sorted(dates, reverse=True)[:n_games]
        team_recent = team_df[team_df["GAME_DATE"].isin(dates_sorted)]
        team_fga_per_game = (
            pd.to_numeric(team_recent["FGA"], errors="coerce")
            .groupby(team_recent["GAME_DATE"])
            .sum()
        )
        team_fga_avg = float(team_fga_per_game.mean()) if len(team_fga_per_game) > 0 else None
    else:
        team_fga_avg = None

    # ── FGA share ─────────────────────────────────────────────────────────
    p_avg_fga_l5  = float(p_fga_l5.mean())  if len(p_fga_l5)  >= 3 else None
    p_avg_fga_l10 = float(p_fga_l10.mean()) if len(p_fga_l10) >= 5 else None

    fga_share_l5  = round(p_avg_fga_l5  / team_fga_avg * 100, 1) if (p_avg_fga_l5  and team_fga_avg) else None
    fga_share_l10 = round(p_avg_fga_l10 / team_fga_avg * 100, 1) if (p_avg_fga_l10 and team_fga_avg) else None

    # ── Team rank by FGA and minutes ──────────────────────────────────────
    # Get each player's L10 avg FGA on this team
    if "GAME_DATE" in team_df.columns and "player_name" in team_df.columns:
        team_recent_all = team_df[team_df["GAME_DATE"].isin(
            sorted(team_df["GAME_DATE"].dropna().unique(), reverse=True)[:n_games]
        )]
        player_avgs = (
            team_recent_all
            .assign(FGA_n=pd.to_numeric(team_recent_all["FGA"], errors="coerce"),
                    MIN_n=pd.to_numeric(team_recent_all["MIN"], errors="coerce"))
            .groupby("player_name")
            .agg(fga=("FGA_n", "mean"), mins=("MIN_n", "mean"), games=("FGA_n", "count"))
            .query("games >= 3")  # at least 3 games to count
            .sort_values("fga", ascending=False)
            .reset_index()
        )
        # Find this player's rank
        fga_rank = None
        min_rank = None
        if not player_avgs.empty:
            fga_matches = player_avgs[player_avgs["player_name"] == player_name]
            if not fga_matches.empty:
                fga_rank = int(player_avgs.index[player_avgs["player_name"] == player_name][0]) + 1

            # Min rank
            by_mins = player_avgs.sort_values("mins", ascending=False).reset_index(drop=True)
            min_matches = by_mins[by_mins["player_name"] == player_name]
            if not min_matches.empty:
                min_rank = int(by_mins.index[by_mins["player_name"] == player_name][0]) + 1
    else:
        fga_rank = None
        min_rank = None

    # ── Usage tier ────────────────────────────────────────────────────────
    tier = None
    if fga_share_l10 is not None:
        if fga_share_l10 >= 22:    tier = "STAR"
        elif fga_share_l10 >= 15:  tier = "CO-STAR"
        elif fga_share_l10 >= 9:   tier = "ROLE"
        else:                       tier = "BENCH"
    elif fga_rank == 1:
        tier = "STAR"

    return {
        "fga_share_l5":   fga_share_l5,
        "fga_share_l10":  fga_share_l10,
        "fga_rank":       fga_rank,
        "min_rank":       min_rank,
        "team_fga_l10":   round(team_fga_avg, 1) if team_fga_avg else None,
        "usage_tier":     tier,
    }
