"""
bdl_client.py — balldontlie.io API wrapper with SQLite caching and rate limiting.

Handles:
- Authentication via API key
- Rate limiting (polite delays between requests)
- SQLite caching so we don't re-fetch data we already have
- Automatic retries on failure
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

API_KEY   = "7bcccd07-2923-48cb-bc42-299f430c52fd"
BASE_V1   = "https://api.balldontlie.io/nba/v1"
BASE_V2   = "https://api.balldontlie.io/v2"
DB_PATH   = Path("nba_cache.db")

# Delay between API calls (seconds) — keeps us well under rate limits
REQUEST_DELAY = 1.0
MAX_RETRIES   = 3


class BDLClient:
    def __init__(self):
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": API_KEY,
            "Accept": "application/json",
        })
        self._last_request_time = 0.0
        self._init_db()

    # ── SQLite cache ─────────────────────────────────────────────────────────

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

    def _cache_get(self, key: str, max_age_hours: float = 6) -> Optional[dict]:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                "SELECT value, cached_at FROM cache WHERE key = ?", (key,)
            ).fetchone()
        if not row:
            return None
        cached_at = datetime.fromisoformat(row[1])
        if datetime.now() - cached_at > timedelta(hours=max_age_hours):
            return None
        return json.loads(row[0])

    def _cache_set(self, key: str, value: dict):
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO cache (key, value, cached_at) VALUES (?, ?, ?)",
                (key, json.dumps(value), datetime.now().isoformat())
            )
            conn.commit()

    # ── HTTP ──────────────────────────────────────────────────────────────────

    def _get(self, url: str, params: dict = None) -> Optional[dict]:
        # Rate limiting
        elapsed = time.time() - self._last_request_time
        if elapsed < REQUEST_DELAY:
            time.sleep(REQUEST_DELAY - elapsed)

        for attempt in range(MAX_RETRIES):
            try:
                resp = self._session.get(url, params=params, timeout=15)
                self._last_request_time = time.time()

                if resp.status_code == 429:
                    wait = 10 * (attempt + 1)
                    logger.warning(f"Rate limited — waiting {wait}s...")
                    time.sleep(wait)
                    continue

                if resp.status_code == 404:
                    return None  # endpoint doesn't exist — don't retry

                resp.raise_for_status()
                return resp.json()

            except requests.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt+1}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(3 * (attempt + 1))

        logger.error(f"All retries failed for {url}")
        return None

    def _get_paginated(self, url: str, params: dict = None) -> list:
        """Fetch all pages of a paginated endpoint."""
        params = params or {}
        params["per_page"] = 100
        all_data = []
        cursor = None

        while True:
            if cursor:
                params["cursor"] = cursor

            result = self._get(url, params)
            if not result:
                break

            data = result.get("data", [])
            all_data.extend(data)

            meta = result.get("meta", {})
            cursor = meta.get("next_cursor")
            if not cursor:
                break

        return all_data

    # ── Public API ────────────────────────────────────────────────────────────

    def get_games_for_date(self, game_date: str) -> list[dict]:
        """Get all NBA games for a specific date (YYYY-MM-DD)."""
        cache_key = f"games_{game_date}"
        # Don't cache today's games for long (they may not be scheduled yet)
        max_age = 1 if game_date == date.today().isoformat() else 24
        cached = self._cache_get(cache_key, max_age_hours=max_age)
        if cached:
            return cached["data"]

        result = self._get(f"{BASE_V1}/games", {"dates[]": game_date, "per_page": 100})
        if not result:
            return []

        self._cache_set(cache_key, result)
        return result.get("data", [])

    def get_recent_games_for_team(self, team_id: int, n_days: int = 60) -> list[dict]:
        """Get recent completed games for a team."""
        cache_key = f"team_games_{team_id}_{n_days}"
        cached = self._cache_get(cache_key, max_age_hours=2)
        if cached:
            return cached["data"]

        end_date   = date.today().isoformat()
        start_date = (date.today() - timedelta(days=n_days)).isoformat()

        data = self._get_paginated(f"{BASE_V1}/games", {
            "team_ids[]":   team_id,
            "start_date":   start_date,
            "end_date":     end_date,
            "per_page":     100,
        })

        # Only completed games
        data = [g for g in data if g.get("status") == "Final"]
        self._cache_set(cache_key, {"data": data})
        return data

    def get_stats_for_game(self, game_id: int) -> list[dict]:
        """Get all player stats for a specific game."""
        cache_key = f"stats_{game_id}"
        cached = self._cache_get(cache_key, max_age_hours=168)  # cache for a week
        if cached:
            return cached["data"]

        data = self._get_paginated(f"{BASE_V1}/stats", {"game_ids[]": game_id})
        if data:
            self._cache_set(cache_key, {"data": data})
        return data

    def get_player_props(self, game_id: int) -> list[dict]:
        """Get live player props for a game (requires paid tier for some vendors)."""
        result = self._get(f"{BASE_V2}/odds/player_props", {"game_id": game_id})
        if not result:
            return []
        return result.get("data", [])

    def get_game_odds(self, game_date: str) -> list[dict]:
        """
        Get game-level odds for all games on a date.
        Endpoint: GET /v2/odds?dates[]=YYYY-MM-DD
        Returns list of odds rows — multiple vendors per game.
        """
        cache_key = f"game_odds_date_{game_date}"
        cached = self._cache_get(cache_key, max_age_hours=2)
        if cached:
            return cached.get("data", [])

        result = self._get(f"{BASE_V2}/odds", {"dates[]": game_date})
        if not result:
            return []

        data = result.get("data", [])
        self._cache_set(cache_key, {"data": data})
        return data

    def get_all_teams(self) -> list[dict]:
        """Get all NBA teams."""
        cache_key = "all_teams"
        cached = self._cache_get(cache_key, max_age_hours=168)
        if cached:
            return cached["data"]

        data = self._get_paginated(f"{BASE_V1}/teams")
        if data:
            self._cache_set(cache_key, {"data": data})
        return data


# Module-level singleton
_client: Optional[BDLClient] = None

def get_client() -> BDLClient:
    global _client
    if _client is None:
        _client = BDLClient()
    return _client
