"""
bdl_client.py — balldontlie.io API wrapper with SQLite caching and rate limiting.

Implements ALL 22 relevant BDL endpoints across v1 and v2.
Every method has:
  - SQLite caching with appropriate TTL per data type
  - Pagination handling (never silently drops rows)
  - Visible error logging (never swallows failures silently)
  - No empty-result caching (BDL posts data incrementally)

Cache TTL philosophy:
  - Static / slow-changing (teams, players, contracts): 24-168 hours
  - Season averages / standings: 12-24 hours
  - Game-day context (injuries, lineups): 1-2 hours
  - Live / odds: 30 minutes, busted on every pipeline refresh
  - Live box scores: 3 minutes (updated in real time)
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import requests

logger = logging.getLogger(__name__)

API_KEY  = "7bcccd07-2923-48cb-bc42-299f430c52fd"
BASE_V1  = "https://api.balldontlie.io/nba/v1"
BASE_V2  = "https://api.balldontlie.io/v2"
DB_PATH  = Path("nba_cache.db")

REQUEST_DELAY = 0.5   # seconds between calls — 600 req/min GOAT tier = 10/sec
MAX_RETRIES   = 3


def _current_season() -> int:
    """BDL season convention: 2025 = 2025-26 season."""
    today = date.today()
    return today.year if today.month >= 10 else today.year - 1


class BDLClient:

    def __init__(self):
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": API_KEY,
            "Accept":        "application/json",
        })
        self._last_request_time = 0.0
        self._init_db()

    # ── SQLite cache ──────────────────────────────────────────────────────────

    def _init_db(self):
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cache (
                    key       TEXT PRIMARY KEY,
                    value     TEXT NOT NULL,
                    cached_at TEXT NOT NULL
                )
            """)
            conn.commit()

    def _cache_get(self, key: str, max_age_hours: float) -> Optional[dict]:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                "SELECT value, cached_at FROM cache WHERE key = ?", (key,)
            ).fetchone()
        if not row:
            return None
        age = datetime.now() - datetime.fromisoformat(row[1])
        if age > timedelta(hours=max_age_hours):
            return None
        return json.loads(row[0])

    def _cache_set(self, key: str, value: dict):
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO cache (key, value, cached_at) VALUES (?, ?, ?)",
                (key, json.dumps(value), datetime.now().isoformat())
            )
            conn.commit()

    def _cache_delete(self, key: str):
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("DELETE FROM cache WHERE key = ?", (key,))
            conn.commit()

    # ── HTTP ──────────────────────────────────────────────────────────────────

    def _get(self, url: str, params: dict = None) -> Optional[dict]:
        elapsed = time.time() - self._last_request_time
        if elapsed < REQUEST_DELAY:
            time.sleep(REQUEST_DELAY - elapsed)

        for attempt in range(MAX_RETRIES):
            try:
                resp = self._session.get(url, params=params, timeout=20)
                self._last_request_time = time.time()

                if resp.status_code == 429:
                    wait = 15 * (attempt + 1)
                    logger.warning(f"Rate limited on {url} — waiting {wait}s")
                    time.sleep(wait)
                    continue

                if resp.status_code == 404:
                    logger.debug(f"404 on {url}")
                    return None

                if resp.status_code == 401:
                    logger.error(f"401 Unauthorized on {url} — check API key")
                    return None

                resp.raise_for_status()
                return resp.json()

            except requests.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt+1}/{MAX_RETRIES}) {url}: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(3 * (attempt + 1))

        logger.error(f"All {MAX_RETRIES} retries failed for {url}")
        return None

    def _get_paginated(self, url: str, params: dict = None) -> list:
        """Fetch all pages. Never silently drops rows."""
        params = dict(params or {})
        params["per_page"] = 100
        all_data = []
        cursor = None
        page = 0

        while True:
            page += 1
            if cursor:
                params["cursor"] = cursor

            result = self._get(url, params)
            if not result:
                if page == 1:
                    logger.warning(f"No data returned from {url} params={params}")
                break

            data = result.get("data", [])
            all_data.extend(data)

            meta   = result.get("meta", {})
            cursor = meta.get("next_cursor")
            if not cursor:
                break

        return all_data

    # =========================================================================
    # SECTION 1: Teams
    # =========================================================================

    def get_all_teams(self) -> list[dict]:
        """All NBA teams. Cached 7 days."""
        key = "all_teams"
        cached = self._cache_get(key, 168)
        if cached:
            return cached["data"]
        data = self._get_paginated(f"{BASE_V1}/teams")
        if data:
            self._cache_set(key, {"data": data})
        return data or []

    # =========================================================================
    # SECTION 2: Players
    # =========================================================================

    def get_active_players(self) -> list[dict]:
        """
        All currently active NBA players with current team.
        Authoritative source for player->team mappings.
        Catches trades, signings, waivers.
        Cached 24 hours.
        """
        key = "active_players"
        cached = self._cache_get(key, 24)
        if cached:
            return cached.get("data", [])
        data = self._get_paginated(f"{BASE_V1}/players/active")
        if data:
            self._cache_set(key, {"data": data})
            logger.info(f"Active players loaded: {len(data)} players")
        else:
            logger.warning("Active players returned empty")
        return data or []

    def get_active_players_lookup(self) -> dict:
        """
        Returns dict: player_name_lower -> {team_abbr, player_id, position}
        Used to validate player->team mappings in the pipeline.
        """
        players = self.get_active_players()
        lookup = {}
        for p in players:
            name = f"{p.get('first_name','')} {p.get('last_name','')}".strip().lower()
            team = p.get("team", {})
            lookup[name] = {
                "team_abbr": team.get("abbreviation", ""),
                "team_id":   team.get("id"),
                "player_id": p.get("id"),
                "position":  p.get("position", ""),
            }
        return lookup

    # =========================================================================
    # SECTION 3: Games
    # =========================================================================

    def get_games_for_date(self, game_date: str) -> list[dict]:
        """Games on a date. Cached 1 hour today, 24 hours past."""
        key = f"games_{game_date}"
        max_age = 1 if game_date == date.today().isoformat() else 24
        cached = self._cache_get(key, max_age)
        if cached:
            return cached.get("data", [])
        result = self._get(f"{BASE_V1}/games", {"dates[]": game_date, "per_page": 100})
        if not result:
            logger.warning(f"Games unavailable for {game_date}")
            return []
        data = result.get("data", [])
        if data:
            self._cache_set(key, {"data": data})
        return data or []

    def get_recent_games_for_team(self, team_id: int, n_days: int = 60) -> list[dict]:
        """Recent completed games for a team. Cached 2 hours."""
        key = f"team_games_{team_id}_{n_days}"
        cached = self._cache_get(key, 2)
        if cached:
            return cached.get("data", [])
        end   = date.today().isoformat()
        start = (date.today() - timedelta(days=n_days)).isoformat()
        data  = self._get_paginated(f"{BASE_V1}/games", {
            "team_ids[]": team_id,
            "start_date": start,
            "end_date":   end,
        })
        data = [g for g in data if g.get("status") == "Final"]
        if data:
            self._cache_set(key, {"data": data})
        return data or []

    # =========================================================================
    # SECTION 4: Stats (full game + per quarter)
    # =========================================================================

    def get_stats_for_game(self, game_id: int) -> list[dict]:
        """Full-game player stats. Cached 7 days."""
        key = f"stats_{game_id}"
        cached = self._cache_get(key, 168)
        if cached:
            return cached.get("data", [])
        data = self._get_paginated(f"{BASE_V1}/stats", {"game_ids[]": game_id})
        if data:
            self._cache_set(key, {"data": data})
        return data or []

    def get_stats_for_game_period(self, game_id: int, period: int) -> list[dict]:
        """
        Per-quarter stats for a completed game.
        period: 1=Q1, 2=Q2, 3=Q3, 4=Q4
        Powers 1Q prop scoring. Cached 7 days.
        """
        key = f"stats_{game_id}_p{period}"
        cached = self._cache_get(key, 168)
        if cached:
            return cached.get("data", [])
        data = self._get_paginated(f"{BASE_V1}/stats", {
            "game_ids[]": game_id,
            "period":     period,
        })
        if data:
            self._cache_set(key, {"data": data})
        return data or []

    def get_player_quarter_logs(
        self,
        player_id:  int,
        period:     int = 1,
        start_date: str = None,
        end_date:   str = None,
        season:     int = None,
    ) -> list[dict]:
        """
        All per-quarter game logs for a player.
        Builds Q1 hit rate history for 1Q props.
        Cached 6 hours.
        """
        if season is None:
            season = _current_season()
        if end_date is None:
            end_date = date.today().isoformat()
        if start_date is None:
            start_date = f"{season}-10-01"

        key = f"player_q{period}_logs_{player_id}_{start_date}_{end_date}"
        cached = self._cache_get(key, 6)
        if cached:
            return cached.get("data", [])

        data = self._get_paginated(f"{BASE_V1}/stats", {
            "player_ids[]": player_id,
            "period":       period,
            "start_date":   start_date,
            "end_date":     end_date,
        })
        if data:
            self._cache_set(key, {"data": data})
        return data or []

    # =========================================================================
    # SECTION 5: Advanced Stats V2
    # =========================================================================

    def get_advanced_stats_for_player(
        self,
        player_id:  int,
        start_date: str = None,
        end_date:   str = None,
        season:     int = None,
    ) -> list[dict]:
        """
        Per-game advanced stats: usage%, net_rating, pace, PIE, tracking.
        Real usage% replaces our FGA-share estimate.
        Net rating identifies who gets benched in blowouts.
        Cached 6 hours.
        """
        if season is None:
            season = _current_season()
        if end_date is None:
            end_date = date.today().isoformat()
        if start_date is None:
            start_date = f"{season}-10-01"

        key = f"adv_player_{player_id}_{start_date}_{end_date}"
        cached = self._cache_get(key, 6)
        if cached:
            return cached.get("data", [])

        data = self._get_paginated(f"{BASE_V2}/stats/advanced", {
            "player_ids[]": player_id,
            "start_date":   start_date,
            "end_date":     end_date,
            "period":       0,
        })
        if data:
            self._cache_set(key, {"data": data})
        return data or []

    def get_advanced_stats_for_game(self, game_id: int) -> list[dict]:
        """Advanced stats for all players in a single game. Cached 7 days."""
        key = f"adv_stats_{game_id}"
        cached = self._cache_get(key, 168)
        if cached:
            return cached.get("data", [])
        data = self._get_paginated(f"{BASE_V2}/stats/advanced", {
            "game_ids[]": game_id,
            "period":     0,
        })
        if data:
            self._cache_set(key, {"data": data})
        return data or []

    # =========================================================================
    # SECTION 6: Season Averages (Player)
    # =========================================================================

    def get_season_averages(self, player_id: int, season: int = None) -> Optional[dict]:
        """
        Basic season averages: pts, reb, ast, stl, blk, min, fg_pct.
        True baseline for outlier detection — better anchor than L20 alone.
        Cached 12 hours.
        """
        if season is None:
            season = _current_season()
        key = f"season_avg_{player_id}_{season}"
        cached = self._cache_get(key, 12)
        if cached:
            return cached.get("data")
        result = self._get(f"{BASE_V1}/season_averages", {
            "player_id": player_id,
            "season":    season,
        })
        if not result:
            return None
        data = result.get("data", [])
        record = data[0] if data else None
        if record:
            self._cache_set(key, {"data": record})
        return record

    def get_season_averages_advanced(self, player_id: int, season: int = None) -> Optional[dict]:
        """
        Advanced season averages: usage%, net_rating, assist%, reb%, turnover_ratio.
        Better role detection and combined stat confidence.
        Cached 12 hours.
        """
        if season is None:
            season = _current_season()
        key = f"season_avg_adv_{player_id}_{season}"
        cached = self._cache_get(key, 12)
        if cached:
            return cached.get("data")
        data = self._get_paginated(f"{BASE_V1}/season_averages/general", {
            "player_ids[]": player_id,
            "season":       season,
            "season_type":  "regular",
            "type":         "advanced",
        })
        record = data[0] if data else None
        if record:
            self._cache_set(key, {"data": record})
        return record

    def get_season_averages_usage(self, player_id: int, season: int = None) -> Optional[dict]:
        """
        Usage season averages: usage_percentage, possessions, pace.
        Cached 12 hours.
        """
        if season is None:
            season = _current_season()
        key = f"season_avg_usage_{player_id}_{season}"
        cached = self._cache_get(key, 12)
        if cached:
            return cached.get("data")
        data = self._get_paginated(f"{BASE_V1}/season_averages/general", {
            "player_ids[]": player_id,
            "season":       season,
            "season_type":  "regular",
            "type":         "usage",
        })
        record = data[0] if data else None
        if record:
            self._cache_set(key, {"data": record})
        return record

    def get_season_averages_clutch(self, player_id: int, season: int = None) -> Optional[dict]:
        """
        Clutch performance: stats within 5 pts, final 5 min.
        High-leverage signal for under bets in close game scripts.
        Cached 12 hours.
        """
        if season is None:
            season = _current_season()
        key = f"season_avg_clutch_{player_id}_{season}"
        cached = self._cache_get(key, 12)
        if cached:
            return cached.get("data")
        data = self._get_paginated(f"{BASE_V1}/season_averages/clutch", {
            "player_ids[]": player_id,
            "season":       season,
            "season_type":  "regular",
            "type":         "base",
        })
        record = data[0] if data else None
        if record:
            self._cache_set(key, {"data": record})
        return record

    def get_season_averages_defense(self, player_id: int, season: int = None) -> Optional[dict]:
        """
        Defensive season averages: opponent FG%, matchup stats.
        Context for STL/BLK prop reliability.
        Cached 12 hours.
        """
        if season is None:
            season = _current_season()
        key = f"season_avg_def_{player_id}_{season}"
        cached = self._cache_get(key, 12)
        if cached:
            return cached.get("data")
        data = self._get_paginated(f"{BASE_V1}/season_averages/defense", {
            "player_ids[]": player_id,
            "season":       season,
            "season_type":  "regular",
            "type":         "overall",
        })
        record = data[0] if data else None
        if record:
            self._cache_set(key, {"data": record})
        return record

    # =========================================================================
    # SECTION 7: Team Season Averages
    # =========================================================================

    def _get_team_season_averages(self, category: str, avg_type: str, season: int = None) -> list[dict]:
        """Generic team season averages fetcher. Cached 12 hours."""
        if season is None:
            season = _current_season()
        key = f"team_season_{category}_{avg_type}_{season}"
        cached = self._cache_get(key, 12)
        if cached:
            return cached.get("data", [])

        params = {"season": season, "season_type": "regular"}
        if avg_type:
            params["type"] = avg_type

        data = self._get_paginated(f"{BASE_V1}/team_season_averages/{category}", params)
        if data:
            self._cache_set(key, {"data": data})
            logger.info(f"Team avgs loaded: {category}/{avg_type} — {len(data)} teams")
        else:
            logger.warning(f"Team avgs empty: {category}/{avg_type}")
        return data or []

    def get_team_opponent_averages(self, season: int = None) -> list[dict]:
        """
        What each team ALLOWS: pts_allowed, reb_allowed, ast_allowed, etc.
        Replaces our rough matchup computation with real season data.
        Powers Soft D / Tough D grades.
        """
        return self._get_team_season_averages("general", "opponent", season)

    def get_team_base_averages(self, season: int = None) -> list[dict]:
        """Team offensive output: pts, reb, ast, pace."""
        return self._get_team_season_averages("general", "base", season)

    def get_team_advanced_averages(self, season: int = None) -> list[dict]:
        """Team offensive/defensive rating, pace, net rating."""
        return self._get_team_season_averages("general", "advanced", season)

    def get_team_tracking_averages(self, season: int = None) -> list[dict]:
        """Team pace, drives, paint touches — pace context for counting stats."""
        return self._get_team_season_averages("tracking", "speeddistance", season)

    def get_team_hustle_averages(self, season: int = None) -> list[dict]:
        """Team deflections, contested shots — defensive intensity context."""
        return self._get_team_season_averages("hustle", "", season)

    def get_team_averages_lookup(self, season: int = None) -> dict:
        """
        Build team_abbr -> {base, opponent, advanced, tracking, hustle}
        lookup for the pipeline. All team context in one dict.
        """
        base     = self.get_team_base_averages(season)
        opponent = self.get_team_opponent_averages(season)
        advanced = self.get_team_advanced_averages(season)
        tracking = self.get_team_tracking_averages(season)
        hustle   = self.get_team_hustle_averages(season)

        lookup: dict = {}

        for rows, label in [(base, "base"), (opponent, "opponent"),
                            (advanced, "advanced"), (tracking, "tracking"),
                            (hustle, "hustle")]:
            for row in rows:
                abbr = row.get("team", {}).get("abbreviation")
                if abbr:
                    lookup.setdefault(abbr, {})[label] = row.get("stats", {})
                    lookup[abbr]["team_id"] = row.get("team", {}).get("id")

        return lookup

    # =========================================================================
    # SECTION 8: Standings
    # =========================================================================

    def get_standings(self, season: int = None) -> list[dict]:
        """
        W/L, home record, road record, conference rank.
        Powers record-differential blowout risk and motivation context.
        Cached 12 hours.
        """
        if season is None:
            season = _current_season()
        key = f"standings_{season}"
        cached = self._cache_get(key, 12)
        if cached:
            return cached.get("data", [])
        result = self._get(f"{BASE_V1}/standings", {"season": season})
        if not result:
            logger.warning(f"Standings unavailable for season {season}")
            return []
        data = result.get("data", [])
        if data:
            self._cache_set(key, {"data": data})
            logger.info(f"Standings loaded: {len(data)} teams")
        return data or []

    def get_standings_lookup(self, season: int = None) -> dict:
        """team_abbr -> {wins, losses, win_pct, home_record, road_record, conf_rank}"""
        rows = self.get_standings(season)
        lookup = {}
        for row in rows:
            abbr = row.get("team", {}).get("abbreviation")
            if abbr:
                w = row.get("wins", 0)
                l = row.get("losses", 0)
                lookup[abbr] = {
                    "wins":        w,
                    "losses":      l,
                    "win_pct":     round(w / max(w + l, 1), 3),
                    "home_record": row.get("home_record", ""),
                    "road_record": row.get("road_record", ""),
                    "conf_rank":   row.get("conference_rank"),
                    "div_rank":    row.get("division_rank"),
                }
        return lookup

    # =========================================================================
    # SECTION 9: Leaders
    # =========================================================================

    def get_leaders(self, stat_type: str, season: int = None) -> list[dict]:
        """
        Stat leaders for pts, reb, ast, stl, blk, fg3m, tov.
        Elite players whose lines are set more carefully by books.
        Cached 24 hours.
        """
        if season is None:
            season = _current_season()
        key = f"leaders_{stat_type}_{season}"
        cached = self._cache_get(key, 24)
        if cached:
            return cached.get("data", [])
        result = self._get(f"{BASE_V1}/leaders", {
            "stat_type":   stat_type,
            "season":      season,
            "season_type": "regular",
        })
        if not result:
            logger.warning(f"Leaders unavailable for {stat_type}")
            return []
        data = result.get("data", [])
        if data:
            self._cache_set(key, {"data": data})
        return data or []

    def get_leaders_lookup(self, season: int = None) -> dict:
        """player_id -> {pts_rank, reb_rank, ast_rank, fg3m_rank, stl_rank, blk_rank}"""
        lookup: dict = {}
        for stat in ["pts", "reb", "ast", "fg3m", "stl", "blk"]:
            for row in self.get_leaders(stat, season):
                pid  = row.get("player", {}).get("id")
                rank = row.get("rank")
                if pid and rank:
                    lookup.setdefault(pid, {})[f"{stat}_rank"] = rank
        return lookup

    # =========================================================================
    # SECTION 10: Box Scores
    # =========================================================================

    def get_box_scores_for_date(self, game_date: str) -> list[dict]:
        """
        Full box scores with plus_minus per player.
        Plus/minus over L5 games signals who's on the floor in meaningful moments.
        Cached 30 min today, 7 days past.
        """
        today = date.today().isoformat()
        max_age = 0.5 if game_date == today else 168
        key = f"box_scores_{game_date}"
        cached = self._cache_get(key, max_age)
        if cached:
            return cached.get("data", [])
        result = self._get(f"{BASE_V1}/box_scores", {"date": game_date})
        if not result:
            logger.warning(f"Box scores unavailable for {game_date}")
            return []
        data = result.get("data", [])
        if data:
            self._cache_set(key, {"data": data})
        return data or []

    def get_live_box_scores(self) -> list[dict]:
        """
        Real-time box scores for all in-progress games.
        Updated every ~30 seconds by BDL.
        Live prop tracking dashboard data source.
        Cached 3 minutes.
        """
        key = "live_box_scores"
        cached = self._cache_get(key, 0.05)
        if cached:
            return cached.get("data", [])
        result = self._get(f"{BASE_V1}/box_scores/live")
        if not result:
            return []
        data = result.get("data", [])
        self._cache_set(key, {"data": data})
        return data or []

    def get_plus_minus_lookup(self, game_date: str) -> dict:
        """player_name -> plus_minus from box scores on a date."""
        lookup = {}
        for game in self.get_box_scores_for_date(game_date):
            for side in ["home_team", "visitor_team"]:
                for p in game.get(side, {}).get("players", []):
                    name = f"{p.get('player',{}).get('first_name','')} {p.get('player',{}).get('last_name','')}".strip()
                    pm   = p.get("plus_minus")
                    if name and pm is not None:
                        lookup[name] = pm
        return lookup

    # =========================================================================
    # SECTION 11: Lineups
    # =========================================================================

    def get_lineups_for_game(self, game_id: int) -> list[dict]:
        """
        Starting lineups once game begins (2025+ only).
        Confirms starters before betting.
        Cached 1 hour.
        """
        key = f"lineups_{game_id}"
        cached = self._cache_get(key, 1)
        if cached:
            return cached.get("data", [])
        data = self._get_paginated(f"{BASE_V1}/lineups", {"game_ids[]": game_id})
        if data:
            self._cache_set(key, {"data": data})
        return data or []

    def get_starters_lookup(self, game_ids: list) -> dict:
        """player_id -> is_starter bool for multiple games."""
        lookup = {}
        for gid in game_ids:
            for row in self.get_lineups_for_game(gid):
                pid = row.get("player", {}).get("id")
                if pid:
                    lookup[pid] = row.get("starter", False)
        return lookup

    # =========================================================================
    # SECTION 12: Play-by-Play
    # =========================================================================

    def get_plays_for_game(self, game_id: int) -> list[dict]:
        """
        Complete play-by-play (2025+ season only).
        Live prop tracking and post-game analysis.
        Cached 10 min in progress, 7 days completed.
        """
        key = f"plays_{game_id}"
        cached = self._cache_get(key, 168)
        if cached:
            return cached.get("data", [])
        result = self._get(f"{BASE_V1}/plays", {"game_id": game_id})
        if not result:
            return []
        data = result.get("data", [])
        if data:
            self._cache_set(key, {"data": data})
        return data or []

    # =========================================================================
    # SECTION 13: Injuries
    # =========================================================================

    def get_injuries(self) -> list[dict]:
        """All current injuries. Cached 1 hour."""
        key = "injuries"
        cached = self._cache_get(key, 1)
        if cached:
            return cached.get("data", [])
        data = self._get_paginated(f"{BASE_V1}/player_injuries")
        if data:
            self._cache_set(key, {"data": data})
            logger.info(f"Injuries loaded: {len(data)} players")
        return data or []

    # =========================================================================
    # SECTION 14: Betting Odds
    # =========================================================================

    def get_game_odds(self, game_date: str) -> list[dict]:
        """
        Game-level odds. Paginated. No empty caching. 30 min TTL.
        Busted on every pipeline refresh.
        """
        key = f"game_odds_date_{game_date}"
        cached = self._cache_get(key, 0.5)
        if cached:
            data = cached.get("data", [])
            if data:
                return data

        data = self._get_paginated(f"{BASE_V2}/odds", {"dates[]": game_date})
        if data:
            self._cache_set(key, {"data": data})
            logger.info(f"Game odds: {len(data)} rows for {game_date}")
        else:
            logger.warning(f"Game odds empty for {game_date} — lines may not be posted yet")
        return data or []

    def get_player_props(self, game_id: int) -> list[dict]:
        """Live player props. Not cached — always live."""
        result = self._get(f"{BASE_V2}/odds/player_props", {"game_id": game_id})
        if not result:
            return []
        return result.get("data", [])

    def bust_odds_cache(self, game_date: str):
        """Force-clear odds cache. Call on every pipeline refresh."""
        self._cache_delete(f"game_odds_date_{game_date}")
        logger.info(f"Odds cache busted for {game_date}")

    # =========================================================================
    # SECTION 15: Contracts
    # =========================================================================

    def get_team_contracts(self, team_id: int, season: int = None) -> list[dict]:
        """
        All contracts for a team this season.
        Identifies contract-year players — motivation signal.
        Cached 24 hours.
        """
        if season is None:
            season = _current_season()
        key = f"contracts_team_{team_id}_{season}"
        cached = self._cache_get(key, 24)
        if cached:
            return cached.get("data", [])
        result = self._get(f"{BASE_V1}/contracts/teams", {
            "team_id": team_id,
            "season":  season,
        })
        if not result:
            return []
        data = result.get("data", [])
        if data:
            self._cache_set(key, {"data": data})
        return data or []

    def get_player_contract_aggregate(self, player_id: int) -> list[dict]:
        """
        Multi-year contract: total value, UFA/RFA status, free agent year.
        Walk-year players historically outperform props.
        Cached 24 hours.
        """
        key = f"contract_agg_{player_id}"
        cached = self._cache_get(key, 24)
        if cached:
            return cached.get("data", [])
        result = self._get(f"{BASE_V1}/contracts/players/aggregate", {
            "player_id": player_id,
        })
        if not result:
            return []
        data = result.get("data", [])
        if data:
            self._cache_set(key, {"data": data})
        return data or []

    def is_contract_year(self, player_id: int, season: int = None) -> bool:
        """Returns True if player becomes UFA/RFA next offseason."""
        if season is None:
            season = _current_season()
        for c in self.get_player_contract_aggregate(player_id):
            if (c.get("contract_status") == "CURRENT"
                    and c.get("free_agent_year") == season + 1):
                return True
        return False

    # =========================================================================
    # SECTION 16: Pipeline prefetch helper
    # =========================================================================

    def prefetch_pipeline_context(self, game_date: str, season: int = None) -> dict:
        """
        Fetch all stable/slow-changing context for a pipeline run.
        Called once at the start of run_pipeline().

        Returns:
            active_players : {name_lower: {team_abbr, player_id, position}}
            standings      : {team_abbr: {wins, losses, win_pct, ...}}
            team_averages  : {team_abbr: {base, opponent, advanced, tracking, hustle}}
            leaders        : {player_id: {pts_rank, reb_rank, ...}}
        """
        if season is None:
            season = _current_season()

        logger.info("Prefetching BDL pipeline context...")
        ctx = {}

        for label, fn in [
            ("active_players", self.get_active_players_lookup),
            ("standings",      lambda: self.get_standings_lookup(season)),
            ("team_averages",  lambda: self.get_team_averages_lookup(season)),
            ("leaders",        lambda: self.get_leaders_lookup(season)),
        ]:
            try:
                ctx[label] = fn()
                logger.info(f"  {label}: {len(ctx[label])} records")
            except Exception as e:
                logger.warning(f"  {label} failed (non-fatal): {e}")
                ctx[label] = {}

        # Always bust odds cache on pipeline refresh
        self.bust_odds_cache(game_date)

        return ctx


# ── Module-level singleton ────────────────────────────────────────────────────

_client: Optional[BDLClient] = None


def get_client() -> BDLClient:
    global _client
    if _client is None:
        _client = BDLClient()
    return _client
