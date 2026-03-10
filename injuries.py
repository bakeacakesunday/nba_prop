"""
injuries.py — Injury report + opportunity modeling.

For each player ruled out tonight:
  1. Pull their team's game logs
  2. Find historical games where they were absent (0 min or DNP)
  3. Compare every teammate's stats in those games vs games they played
  4. Rank teammates by how much their usage/stats increased
  5. Surface specific targets with opportunity scores

This is the real edge: not just knowing who's out, but knowing
exactly who absorbs the opportunity based on actual historical data.
"""
from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Minimum games needed to calculate opportunity effect
MIN_GAMES_WITH    = 4   # games with the injured player
MIN_GAMES_WITHOUT = 2   # games without (to find the pattern)

# Stats we track for opportunity analysis
OPPORTUNITY_STATS = ["MIN", "PTS", "REB", "AST", "FG3M", "STL", "BLK", "TOV"]


# ── Injury report ─────────────────────────────────────────────────────────────

def get_injury_report(team_abbrs: list[str]) -> list[dict]:
    """
    Pull tonight's injury report from balldontlie.
    Returns list of {player_name, team, status, description}
    """
    from bdl_client import get_client, BASE_V1
    client = get_client()

    # Build a team_id → abbreviation lookup
    all_teams  = client.get_all_teams()
    id_to_abbr = {t["id"]: t["abbreviation"] for t in all_teams}

    injuries = []
    try:
        # Correct endpoint: /v1/player_injuries (no /nba/ prefix)
        data = client._get_paginated("https://api.balldontlie.io/v1/player_injuries")
        logger.info(f"  Raw injury data: {len(data)} entries")

        for inj in data:
            player  = inj.get("player", {})
            team_id = player.get("team_id")
            abbr    = id_to_abbr.get(team_id, "")

            if team_abbrs and abbr not in team_abbrs:
                continue

            injuries.append({
                "player_name": f"{player.get('first_name','')} {player.get('last_name','')}".strip(),
                "player_id":   player.get("id"),
                "team":        abbr,
                "status":      inj.get("status", ""),
                "description": inj.get("description", ""),
                "return_date": inj.get("return_date", ""),
            })
    except Exception as e:
        logger.warning(f"Could not fetch injury report: {e}")
        return []

    logger.info(f"  Injury report: {len(injuries)} players listed")
    return injuries


def is_out_tonight(injury: dict) -> bool:
    """
    Return True only if the player is CONFIRMED out tonight.
    Questionable (~50% chance to play) and Doubtful (~25% chance) are
    excluded — treating them as OUT inflates opportunity scores for teammates
    when the player actually suits up, which is the majority case for those tags.
    """
    status = (injury.get("status") or "").lower()
    desc   = (injury.get("description") or "").lower()

    # Hard-out keywords only — do NOT include "questionable" or "doubtful"
    confirmed_out = ["out", "inactive", "dnp", "suspended", "will not play",
                     "ruled out", "did not play"]
    return any(k in status or k in desc for k in confirmed_out)


def is_game_time_decision(injury: dict) -> bool:
    """
    Return True if the player is questionable or doubtful — monitor but
    don't treat as OUT for opportunity modeling.
    """
    status = (injury.get("status") or "").lower()
    desc   = (injury.get("description") or "").lower()
    gtd_keywords = ["questionable", "doubtful", "gtd", "game-time", "game time"]
    return any(k in status or k in desc for k in gtd_keywords)


# ── Opportunity modeling ──────────────────────────────────────────────────────

def find_games_without_player(
    team_df: pd.DataFrame,
    player_name: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split team game log into:
    - games_with:    games where the player played (MIN > 5)
    - games_without: games where the player was absent (MIN == 0 or not in lineup)

    Returns (games_with_dates, games_without_dates) as sets of game_ids.
    """
    from utils import normalize_name
    norm_target = normalize_name(player_name)

    player_rows = team_df[team_df["player_name"].apply(
        lambda n: normalize_name(n) == norm_target
    )]

    if player_rows.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Games where player had meaningful minutes
    with_game_ids    = set(player_rows[player_rows["MIN"] > 5]["game_id"].tolist())
    # All games in team log
    all_game_ids     = set(team_df["game_id"].dropna().unique().tolist())
    # Games without = all games minus games with
    without_game_ids = all_game_ids - with_game_ids

    games_with    = team_df[team_df["game_id"].isin(with_game_ids)]
    games_without = team_df[team_df["game_id"].isin(without_game_ids)]

    return games_with, games_without


def compute_opportunity_effect(
    team_df: pd.DataFrame,
    absent_player: str,
) -> list[dict]:
    """
    For each teammate, compare their stats in games with vs without the absent player.
    Returns ranked list of teammates sorted by opportunity score.
    """
    games_with, games_without = find_games_without_player(team_df, absent_player)

    if games_with.empty or games_without.empty:
        logger.info(f"    Not enough data to model opportunity for {absent_player}")
        return []

    n_with    = games_with["game_id"].nunique()
    n_without = games_without["game_id"].nunique()

    if n_with < MIN_GAMES_WITH or n_without < MIN_GAMES_WITHOUT:
        logger.info(f"    {absent_player}: {n_with} games with, {n_without} without — need more data")
        return []

    logger.info(f"    {absent_player}: modeling on {n_without} absence games vs {n_with} presence games")

    # Get all teammates (not the absent player)
    from utils import normalize_name
    norm_absent = normalize_name(absent_player)
    teammates = team_df[team_df["player_name"].apply(
        lambda n: normalize_name(n) != norm_absent
    )]["player_name"].unique()

    results = []
    for teammate in teammates:
        from utils import normalize_name as nn
        tm_with    = games_with[games_with["player_name"].apply(lambda n: nn(n) == nn(teammate))]
        tm_without = games_without[games_without["player_name"].apply(lambda n: nn(n) == nn(teammate))]

        if len(tm_with) < MIN_GAMES_WITH or len(tm_without) < MIN_GAMES_WITHOUT:
            continue

        stat_changes = {}
        opportunity_score = 0.0

        for stat in OPPORTUNITY_STATS:
            if stat not in tm_with.columns:
                continue
            vals_with    = pd.to_numeric(tm_with[stat],    errors="coerce").dropna()
            vals_without = pd.to_numeric(tm_without[stat], errors="coerce").dropna()

            if vals_with.empty or vals_without.empty:
                continue

            avg_with    = float(vals_with.mean())
            avg_without = float(vals_without.mean())
            change      = avg_without - avg_with
            pct_change  = (change / avg_with * 100) if avg_with > 0 else 0

            stat_changes[stat] = {
                "avg_with":    round(avg_with, 1),
                "avg_without": round(avg_without, 1),
                "change":      round(change, 1),
                "pct_change":  round(pct_change, 1),
            }

            # Weight stats for opportunity score
            # Minutes is the primary signal, then scoring/usage stats
            weight = {"MIN": 3.0, "PTS": 2.0, "REB": 1.5, "AST": 1.5,
                      "FG3M": 1.0, "STL": 0.5, "BLK": 0.5, "TOV": -0.3}.get(stat, 1.0)
            opportunity_score += (pct_change / 100) * weight

        if not stat_changes:
            continue

        # Determine opportunity level
        min_change = stat_changes.get("MIN", {}).get("pct_change", 0)
        pts_change = stat_changes.get("PTS", {}).get("pct_change", 0)

        if opportunity_score >= 3.0 or min_change >= 25:
            opp_level = "🔥 MAJOR OPPORTUNITY"
            opp_color = "prime"
        elif opportunity_score >= 1.5 or min_change >= 12:
            opp_level = "✅ SOLID OPPORTUNITY"
            opp_color = "good"
        elif opportunity_score >= 0.5 or min_change >= 5:
            opp_level = "〰 MINOR BUMP"
            opp_color = "mild"
        else:
            continue  # No meaningful opportunity

        # Build summary of biggest changes
        top_changes = sorted(
            [(stat, d) for stat, d in stat_changes.items() if d["change"] > 0 and stat != "TOV"],
            key=lambda x: abs(x[1]["pct_change"]),
            reverse=True
        )[:4]

        summary_parts = [
            f"{stat}: {d['avg_without']} vs {d['avg_with']} ({'+' if d['change'] >= 0 else ''}{d['change']})"
            for stat, d in top_changes
        ]

        results.append({
            "teammate_name":   teammate,
            "opportunity_score": round(opportunity_score, 2),
            "opp_level":       opp_level,
            "opp_color":       opp_color,
            "min_change_pct":  round(min_change, 1),
            "pts_change_pct":  round(pts_change, 1),
            "stat_changes":    stat_changes,
            "summary":         " | ".join(summary_parts),
            "games_sampled":   n_without,
        })

    # Sort by opportunity score
    results.sort(key=lambda x: x["opportunity_score"], reverse=True)
    return results


# ── Full pipeline ─────────────────────────────────────────────────────────────

def build_injury_intelligence(
    team_abbrs: list[str],
    team_df_getter,  # callable: team_abbr → pd.DataFrame
) -> dict:
    """
    Main entry point. For all teams playing tonight:
    1. Get injury report
    2. For each player ruled out, model opportunity for teammates
    3. Return structured intelligence dict

    Returns:
        {
          "injuries":     [injury dicts with status],
          "opportunities":[{absent_player, team, teammate, opp_level, summary, ...}],
          "targets":      {player_name: [opp dicts]} — keyed by who to bet on
        }
    """
    injuries     = get_injury_report(team_abbrs)
    out_tonight  = [inj for inj in injuries if is_out_tonight(inj)]
    gtd_tonight  = [inj for inj in injuries if is_game_time_decision(inj)]

    if not out_tonight:
        logger.info("  No confirmed-out players found for tonight")
        return {
            "injuries":      injuries,
            "out_tonight":   out_tonight,
            "gtd_tonight":   gtd_tonight,
            "opportunities": [],
            "targets":       {},
        }

    logger.info(f"  {len(out_tonight)} players out tonight — modeling opportunities...")

    all_opportunities = []
    targets: dict[str, list[dict]] = {}

    for inj in out_tonight:
        team    = inj["team"]
        absent  = inj["player_name"]
        team_df = team_df_getter(team)

        if team_df.empty:
            continue

        opps = compute_opportunity_effect(team_df, absent)

        for opp in opps:
            entry = {
                **opp,
                "absent_player": absent,
                "absent_status": inj.get("status", ""),
                "absent_desc":   inj.get("description", ""),
                "team":          team,
            }
            all_opportunities.append(entry)

            # Index by teammate for fast lookup in main pipeline
            tname = opp["teammate_name"]
            if tname not in targets:
                targets[tname] = []
            targets[tname].append(entry)

            logger.info(
                f"    {opp['opp_level']}: {opp['teammate_name']} "
                f"(when {absent} out) — score {opp['opportunity_score']}"
            )

    # Sort all opportunities by score
    all_opportunities.sort(key=lambda x: x["opportunity_score"], reverse=True)

    logger.info(f"  Opportunity modeling complete: {len(all_opportunities)} teammate impacts found")
    return {
        "injuries":      out_tonight,
        "out_tonight":   out_tonight,
        "gtd_tonight":   gtd_tonight,
        "opportunities": all_opportunities,
        "targets":       targets,
    }


def format_opportunity_for_card(player_name: str, targets: dict) -> Optional[dict]:
    """Return the best opportunity entry for a player if they're a target."""
    opps = targets.get(player_name, [])
    if not opps:
        return None
    # Return highest-scored opportunity
    return max(opps, key=lambda x: x["opportunity_score"])
