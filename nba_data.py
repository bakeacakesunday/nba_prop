"""
nba_data.py — NBA data via balldontlie.io API.

Replaces the old CSV-based approach entirely.
All data is fetched automatically, cached in nba_cache.db,
and re-fetched only when stale.

No CSV downloads, no manual --teams flag needed.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
import numpy as np

from bdl_client import get_client

logger = logging.getLogger(__name__)

# How many days of game log history to pull
LOOKBACK_DAYS = 60


# ── Schedule ──────────────────────────────────────────────────────────────────

def get_todays_games(game_date: str = None) -> list[dict]:
    """
    Return tonight's NBA games as a list of dicts with:
    home_team_abbr, away_team_abbr, home_team_id, away_team_id, game_id, game_time
    """
    if game_date is None:
        game_date = date.today().isoformat()

    client = get_client()
    games  = client.get_games_for_date(game_date)

    if not games:
        logger.info(f"No games found for {game_date}")
        return []

    result = []
    for g in games:
        home = g.get("home_team", {})
        away = g.get("visitor_team", {})
        result.append({
            "game_id":        g["id"],
            "home_team_abbr": home.get("abbreviation", ""),
            "away_team_abbr": away.get("abbreviation", ""),
            "home_team_id":   home.get("id"),
            "away_team_id":   away.get("id"),
            "home_team_name": home.get("full_name", ""),
            "away_team_name": away.get("full_name", ""),
            "game_time":      g.get("status", ""),
            "status":         g.get("status", ""),
        })

    logger.info(f"Found {len(result)} games for {game_date}")
    return result


def get_team_ids_for_tonight(game_date: str = None) -> list[str]:
    """Return list of team abbreviations playing tonight."""
    games = get_todays_games(game_date)
    teams = []
    for g in games:
        teams.append(g["home_team_abbr"])
        teams.append(g["away_team_abbr"])
    return sorted(set(teams))


# ── Roster ────────────────────────────────────────────────────────────────────

def get_active_roster_for_team(
    team_abbr: str,
    season_end_year: int = None,
    n_lookback: int = 3,
    refresh: bool = False,
) -> list[dict]:
    """
    Return players who appeared (min > 0) in the last n_lookback games
    for this team, derived from the game log data.
    """
    df = get_team_game_log_df(team_abbr)
    if df.empty:
        logger.warning(f"No data found for team '{team_abbr}'")
        return []

    # Get last n_lookback unique game dates
    recent_dates = sorted(df["GAME_DATE"].dt.date.unique(), reverse=True)[:n_lookback]
    recent_df    = df[df["GAME_DATE"].dt.date.isin(recent_dates)]

    from utils import normalize_name
    active = {}
    for _, row in recent_df.iterrows():
        if (row.get("MIN") or 0) <= 0:
            continue
        name = row["player_name"]
        key  = normalize_name(name)
        if key not in active:
            active[key] = {
                "player_name": name,
                "team":        team_abbr.upper(),
                "position":    row.get("position", ""),
                "player_id":   row.get("player_id", 0),
            }

    result = list(active.values())
    logger.info(f"  {team_abbr}: {len(result)} active players (last {n_lookback} games)")
    return result


# ── Game log building ─────────────────────────────────────────────────────────

def _stats_to_rows(stats: list[dict]) -> list[dict]:
    """Convert raw BDL stats objects to flat row dicts."""
    rows = []
    for s in stats:
        player = s.get("player", {})
        team   = s.get("team", {})
        game   = s.get("game", {})

        # Parse minutes
        min_str = s.get("min", "0") or "0"
        if ":" in str(min_str):
            parts = str(min_str).split(":")
            try:
                minutes = float(parts[0]) + float(parts[1]) / 60
            except (ValueError, IndexError):
                minutes = 0.0
        else:
            try:
                minutes = float(min_str)
            except (ValueError, TypeError):
                minutes = 0.0

        # Determine home/away
        home_id = game.get("home_team_id")
        team_id = team.get("id")
        location = "Home" if team_id == home_id else "Away"

        # Opponent
        if team_id == home_id:
            opp_id = game.get("visitor_team_id")
        else:
            opp_id = game.get("home_team_id")

        rows.append({
            "player_id":   player.get("id"),
            "player_name": f"{player.get('first_name','')} {player.get('last_name','')}".strip(),
            "position":    player.get("position", ""),
            "team":        team.get("abbreviation", ""),
            "team_id":     team_id,
            "Opp_id":      opp_id,
            "GAME_DATE":   game.get("date", ""),
            "game_id":     game.get("id"),
            "location":    location,
            "MIN":         round(minutes, 1),
            "PTS":         float(s.get("pts") or 0),
            "REB":         float(s.get("reb") or 0),
            "AST":         float(s.get("ast") or 0),
            "FG3M":        float(s.get("fg3m") or 0),
            "STL":         float(s.get("stl") or 0),
            "BLK":         float(s.get("blk") or 0),
            "TOV":         float(s.get("turnover") or 0),
            "FG":          float(s.get("fgm") or 0),
            "FGA":         float(s.get("fga") or 0),
            "FT":          float(s.get("ftm") or 0),
            "FTA":         float(s.get("fta") or 0),
        })
    return rows


# Cache of built DataFrames (keyed by team abbreviation)
# Stores tuples of (DataFrame, build_datetime) so we can detect stale data.
# Without a TTL the in-memory cache serves stale data for the lifetime of the
# Flask process -- causing days_rest to be wrong whenever a game was played
# after the cache was last populated (BDL has ~12-24hr stat posting lag).
# TTL: 4 hours -- short enough to pick up last night's stats by morning.
_TEAM_DF_CACHE_TTL_HOURS = 4
_team_df_cache: dict = {}   # team -> (DataFrame, build_datetime)
# Opp abbreviation lookup: opp_id -> abbr
_opp_abbr_cache: dict[int, str] = {}


def _build_opp_abbr_lookup(all_teams: list[dict]) -> dict[int, str]:
    return {t["id"]: t["abbreviation"] for t in all_teams}


def get_team_game_log_df(team_abbr: str, refresh: bool = False) -> pd.DataFrame:
    """
    Build a DataFrame of all player game logs for a team over the last LOOKBACK_DAYS days.
    Fetches game-by-game stats from the API and caches the result.
    """
    global _opp_abbr_cache

    team_upper = team_abbr.upper()
    if not refresh and team_upper in _team_df_cache:
        cached_df, cached_at = _team_df_cache[team_upper]
        age_hours = (datetime.now() - cached_at).total_seconds() / 3600
        if age_hours < _TEAM_DF_CACHE_TTL_HOURS:
            return cached_df
        # Cache is stale — fall through and rebuild
        logger.info(f"  {team_abbr}: in-memory cache expired ({age_hours:.1f}h old) — refreshing")

    client = get_client()

    # Get team id from abbreviation
    all_teams = client.get_all_teams()
    team_obj  = next((t for t in all_teams if t["abbreviation"] == team_upper), None)

    if team_obj is None:
        logger.warning(f"Team '{team_abbr}' not found in balldontlie data.")
        return pd.DataFrame()

    if not _opp_abbr_cache:
        _opp_abbr_cache = _build_opp_abbr_lookup(all_teams)

    team_id = team_obj["id"]

    # Get recent games for this team
    games = client.get_recent_games_for_team(team_id, n_days=LOOKBACK_DAYS)
    if not games:
        logger.warning(f"No recent games found for {team_abbr}")
        return pd.DataFrame()

    logger.info(f"  {team_abbr}: fetching stats for {len(games)} games...")

    all_rows = []
    for i, game in enumerate(games):
        game_id = game["id"]
        stats   = client.get_stats_for_game(game_id)
        rows    = _stats_to_rows(stats)
        all_rows.extend(rows)

        if (i + 1) % 5 == 0:
            logger.info(f"    {team_abbr}: processed {i+1}/{len(games)} games")

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)

    # Add combo stat columns (PRA, PR, PA, RA)
    from metrics import add_combo_stats
    df = add_combo_stats(df)

    # Add opponent abbreviation
    df["Opp"] = df["Opp_id"].map(_opp_abbr_cache).fillna("???")

    # Parse dates
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce")
    df = df.dropna(subset=["GAME_DATE"])
    df = df.sort_values("GAME_DATE", ascending=False).reset_index(drop=True)

    _team_df_cache[team_upper] = (df, datetime.now())
    logger.info(f"  {team_abbr}: built {len(df)} player-game rows")
    return df


def get_full_df(teams: list[str], refresh: bool = False) -> pd.DataFrame:
    """
    Build a combined DataFrame for all teams playing tonight.
    Used by context.py for opponent defense calculations.
    """
    frames = []
    for team in teams:
        df = get_team_game_log_df(team, refresh=refresh)
        if not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True).drop_duplicates(
        subset=["player_id", "GAME_DATE"]
    )


def get_player_game_log(
    player_name: str,
    team_abbr: str,
    season_end_year: int = None,
    n_games: int = 25,
    refresh: bool = False,
) -> pd.DataFrame:
    """
    Return a player's game log sorted newest-first.
    Pulls from the team's cached DataFrame.
    """
    from utils import normalize_name

    df         = get_team_game_log_df(team_abbr, refresh=refresh)
    norm_target = normalize_name(player_name)

    if df.empty:
        # Try without team filter
        all_frames = [cached_df for cached_df, _ in _team_df_cache.values()]
        if all_frames:
            df = pd.concat(all_frames, ignore_index=True)

    if df.empty:
        logger.warning(f"No data available for {player_name} ({team_abbr})")
        return pd.DataFrame()

    matched = df[df["player_name"].apply(
        lambda n: normalize_name(n) == norm_target
    )]

    if matched.empty:
        logger.warning(f"No rows found for {player_name} ({team_abbr})")
        return pd.DataFrame()

    result = matched.sort_values("GAME_DATE", ascending=False).head(n_games)
    return result.reset_index(drop=True)


# ── Props fetching ────────────────────────────────────────────────────────────

# Prop type mapping: BDL name → our stat type
PROP_TYPE_MAP = {
    # Singles
    "points":                       "PTS",
    "rebounds":                     "REB",
    "assists":                      "AST",
    "threes":                       "FG3M",
    "steals":                       "STL",
    "blocks":                       "BLK",
    "turnovers":                    "TOV",
    # Combos
    "points_rebounds_assists":      "PRA",
    "points_rebounds":              "PR",
    "points_assists":               "PA",
    "rebounds_assists":             "RA",
    # Milestone (binary yes/no — market type = "milestone")
    "double_double":                "DD",
    "triple_double":                "TD",
    # First quarter
    "points_1q":                    "PTS_1Q",
    "rebounds_1q":                  "REB_1Q",
    "assists_1q":                   "AST_1Q",
    # First 3 minutes
    "points_first3min":             "PTS_F3",
    "rebounds_first3min":           "REB_F3",
    "assists_first3min":            "AST_F3",
    # Legacy fallback keys
    "three_pointers":               "FG3M",
    "pts_reb_ast":                  "PRA",
    "pts_reb":                      "PR",
    "pts_ast":                      "PA",
    "reb_ast":                      "RA",
}

# Milestone props are binary (achieved or not) — no traditional line value
MILESTONE_PROPS = {"DD", "TD"}

# First quarter / first 3 minute props — have lines but no historical quarter splits in game log
QUARTER_PROPS = {"PTS_1Q", "REB_1Q", "AST_1Q", "PTS_F3", "REB_F3", "AST_F3"}

# Preferred vendor priority order
VENDOR_PRIORITY = ["draftkings", "fanduel", "prizepicks", "betmgm", "caesars", "betrivers"]


def get_props_for_games(game_ids: list[int]) -> tuple[list[dict], dict[int, list[dict]], dict[str, int]]:
    """
    Fetch player props for tonight's games from balldontlie.

    Returns:
        props_out        : list of {player_name, team, stat_type, line, odds, vendor}
                           using consensus/best line — ready for Lines tab
        raw_props_by_game: {game_id: [all raw prop dicts]} — for line shopping
        player_id_lookup : {player_name: player_id} — for line shopping
    """
    from collections import defaultdict

    client    = get_client()
    all_teams = client.get_all_teams()

    # Build player lookup from cached team dfs
    player_lookup: dict[int, tuple[str, str]] = {}  # player_id → (name, team_abbr)
    player_id_lookup: dict[str, int] = {}            # player_name → player_id
    for cached_df, _ in _team_df_cache.values():
        df = cached_df
        if df.empty:
            continue
        for _, row in df[["player_id","player_name","team"]].drop_duplicates().iterrows():
            if row["player_id"]:
                pid = int(row["player_id"])
                player_lookup[pid] = (row["player_name"], row["team"])
                player_id_lookup[row["player_name"]] = pid

    props_out: list[dict] = []
    raw_props_by_game: dict[int, list[dict]] = {}

    for game_id in game_ids:
        logger.info(f"  Fetching props for game {game_id}...")
        raw_props = client.get_player_props(game_id)

        if not raw_props:
            logger.info(f"    No props available for game {game_id} (may need paid tier)")
            raw_props_by_game[game_id] = []
            continue

        # Store ALL raw props for line shopping
        raw_props_by_game[game_id] = raw_props

        # Group by player + stat type (all vendors)
        grouped: dict[tuple, list] = defaultdict(list)
        unmapped_types = set()
        for prop in raw_props:
            prop_type = prop.get("prop_type", "")
            stat_type = PROP_TYPE_MAP.get(prop_type)
            if not stat_type:
                unmapped_types.add(prop_type)
                continue
            market = prop.get("market", {})
            mtype  = market.get("type", "")
            # Milestone market only valid for actual binary milestone props
            if mtype == "milestone" and stat_type not in MILESTONE_PROPS:
                continue
            if mtype not in ("over_under", "milestone"):
                continue
            grouped[(prop["player_id"], stat_type, mtype)].append(prop)

        if unmapped_types:
            logger.info(f"    ALL unmapped prop_types for game {game_id}: {sorted(unmapped_types)}")
            combo_types = {t for t in unmapped_types if any(
                k in t for k in ["pts","reb","ast","point","rebound","assist"]
            )}
            if combo_types:
                logger.info(f"    Unmapped combo prop_types: {combo_types}")

        # Log what stat types we DID capture for this game
        captured = set(stat for (_, stat, _) in grouped.keys())
        logger.info(f"    Captured stat types: {sorted(captured)}")

        for (player_id, stat_type, mtype), prop_list in grouped.items():
            player_info = player_lookup.get(int(player_id))
            if not player_info:
                continue

            player_name, team_abbr = player_info
            is_milestone = (mtype == "milestone") or (stat_type in MILESTONE_PROPS)
            is_quarter   = stat_type in QUARTER_PROPS

            if is_milestone:
                # Milestone props are binary — line is 0.5 (over 0.5 = achieved)
                # Odds represent "yes" probability
                chosen = None
                for vendor in VENDOR_PRIORITY:
                    chosen = next((p for p in prop_list if p.get("vendor") == vendor), None)
                    if chosen:
                        break
                if not chosen:
                    chosen = prop_list[0]
                market = chosen.get("market", {})
                yes_odds = market.get("yes_odds") or market.get("over_odds") or market.get("odds")
                props_out.append({
                    "player_name":   player_name,
                    "team":          team_abbr,
                    "stat_type":     stat_type,
                    "line":          0.5,          # binary threshold
                    "odds":          yes_odds or "",
                    "vendor":        chosen.get("vendor", ""),
                    "market_type":   "milestone",
                    "is_quarter":    False,
                })
            else:
                # Standard over/under — use consensus line
                lines = [float(p.get("line_value", 0)) for p in prop_list]
                consensus_line = max(set(lines), key=lines.count)
                at_consensus = [p for p in prop_list if float(p.get("line_value", 0)) == consensus_line]
                chosen = None
                for vendor in VENDOR_PRIORITY:
                    chosen = next((p for p in at_consensus if p.get("vendor") == vendor), None)
                    if chosen:
                        break
                if not chosen:
                    chosen = at_consensus[0]
                market = chosen.get("market", {})
                props_out.append({
                    "player_name":   player_name,
                    "team":          team_abbr,
                    "stat_type":     stat_type,
                    "line":          consensus_line,
                    "odds":          market.get("over_odds", ""),
                    "vendor":        chosen.get("vendor", ""),
                    "market_type":   "over_under",
                    "is_quarter":    is_quarter,
                })

    logger.info(f"  Total props fetched: {len(props_out)} across {len(game_ids)} games")
    return props_out, raw_props_by_game, player_id_lookup


# ── Misc helpers ──────────────────────────────────────────────────────────────

def get_all_teams_in_csv() -> list[str]:
    """Compatibility shim — returns teams from cached data."""
    client    = get_client()
    all_teams = client.get_all_teams()
    return sorted(t["abbreviation"] for t in all_teams)


def get_current_season_end_year(for_date=None) -> int:
    d = for_date or date.today()
    return d.year + 1 if d.month >= 10 else d.year
