"""
app.py — NBA Props Research Web App
====================================
Run: python3 app.py
Then open: http://localhost:5000

Serves the React frontend and exposes JSON API endpoints.
Uses SQLite to store flags and notes locally.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from thresholds import (
    L20_VETO_FLOOR, L20_EDGE_MIN, L20_PARLAY_MIN,
    l20_threshold_for_stat, is_l20_below_threshold, normalize_rate,
    _L20_WEAK_THRESHOLDS_PARLAY,  # backward compat alias
)
import sys
import threading
from datetime import date, datetime
from pathlib import Path
from typing import Optional
from flask import Flask, jsonify, request, send_from_directory

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder="static", template_folder="templates")

DB_PATH    = Path("app_data.db")
CACHE_PATH = Path("pipeline_cache.json")


# ── Database ──────────────────────────────────────────────────────────────────

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS flags (
                key       TEXT PRIMARY KEY,
                flagged   INTEGER NOT NULL DEFAULT 0,
                flag_type TEXT DEFAULT 'watch',
                note      TEXT DEFAULT '',
                updated_at TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_status (
                id        INTEGER PRIMARY KEY,
                status    TEXT,
                message   TEXT,
                started   TEXT,
                finished  TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS prop_snapshots (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_key    TEXT UNIQUE,        -- game_date|player|team|stat|line
                game_date       TEXT NOT NULL,
                player_name     TEXT NOT NULL,
                team            TEXT NOT NULL,
                opponent        TEXT NOT NULL,
                stat            TEXT NOT NULL,
                line            REAL NOT NULL,
                -- Model signals at snapshot time
                score           REAL,
                is_lock         INTEGER,
                is_hammer       INTEGER,
                is_lock_under   INTEGER,
                median_gap      REAL,
                true_over_pct   REAL,
                near_miss_pct   REAL,
                l5_hr           REAL,
                l10_hr          REAL,
                l20_hr          REAL,
                l5_values       TEXT,               -- JSON array
                days_rest       INTEGER,
                minutes_mean    REAL,
                minutes_cv      REAL,
                implied_total   REAL,
                spread          REAL,
                game_total      REAL,
                blowout_level   TEXT,
                ghost_rate      REAL,
                opportunity     TEXT,               -- JSON
                shot_profile    TEXT,
                weighted_hit    REAL,               -- from game_script_profile
                parlay_ready    INTEGER,            -- passed PARLAY READY filter
                parlay_consider INTEGER,            -- passed PARLAY CONSIDER filter
                odds            INTEGER,
                created_at      TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS prop_outcomes (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_key    TEXT NOT NULL,      -- FK to prop_snapshots
                game_date       TEXT NOT NULL,
                player_name     TEXT NOT NULL,
                team            TEXT NOT NULL,
                stat            TEXT NOT NULL,
                line            REAL NOT NULL,
                actual_value    REAL,               -- what the player actually did
                hit             INTEGER,            -- 1=over, 0=under, NULL=unknown
                fetch_source    TEXT DEFAULT 'bdl', -- where outcome came from
                fetched_at      TEXT,
                UNIQUE(snapshot_key)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_date ON prop_snapshots(game_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_outcomes_date  ON prop_outcomes(game_date)")

        # ── Schema migrations — safely add columns added after initial deploy ──
        # Each ALTER TABLE is wrapped in try/except so it's a no-op if column exists.
        migrations = [
            "ALTER TABLE prop_snapshots ADD COLUMN is_lock         INTEGER DEFAULT 0",
            "ALTER TABLE prop_snapshots ADD COLUMN is_hammer       INTEGER DEFAULT 0",
            "ALTER TABLE prop_snapshots ADD COLUMN is_lock_under   INTEGER DEFAULT 0",
            "ALTER TABLE prop_snapshots ADD COLUMN median_gap      REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN true_over_pct   REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN near_miss_pct   REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN l5_hr           REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN l10_hr          REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN l20_hr          REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN l5_values       TEXT",
            "ALTER TABLE prop_snapshots ADD COLUMN days_rest       INTEGER",
            "ALTER TABLE prop_snapshots ADD COLUMN minutes_mean    REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN minutes_cv      REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN implied_total   REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN spread          REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN game_total      REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN blowout_level   TEXT",
            "ALTER TABLE prop_snapshots ADD COLUMN ghost_rate      REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN opportunity     TEXT",
            "ALTER TABLE prop_snapshots ADD COLUMN shot_profile    TEXT",
            "ALTER TABLE prop_snapshots ADD COLUMN weighted_hit    REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN parlay_ready    INTEGER DEFAULT 0",
            "ALTER TABLE prop_snapshots ADD COLUMN parlay_consider INTEGER DEFAULT 0",
            "ALTER TABLE prop_snapshots ADD COLUMN odds            INTEGER",
            "ALTER TABLE prop_snapshots ADD COLUMN edge_score      REAL",
            # DQ / dist profile columns — added after initial schema
            "ALTER TABLE prop_snapshots ADD COLUMN parlay_disqualified      INTEGER DEFAULT 0",
            "ALTER TABLE prop_snapshots ADD COLUMN parlay_disqualify_reason TEXT DEFAULT ''",
            "ALTER TABLE prop_snapshots ADD COLUMN dist_profile             TEXT DEFAULT ''",
            "ALTER TABLE prop_snapshots ADD COLUMN dist_cv                  REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN regression_risk          INTEGER DEFAULT 0",
            # Session 4 fields — BDL-sourced player data
            "ALTER TABLE prop_snapshots ADD COLUMN usage_pct           REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN net_rating_l10      REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN season_avg_vs_line  REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN contract_year       INTEGER DEFAULT 0",
            "ALTER TABLE prop_snapshots ADD COLUMN game_pace           REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN opp_pts_allowed     REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN opp_def_rating      REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN team_wins           INTEGER",
            "ALTER TABLE prop_snapshots ADD COLUMN opp_wins            INTEGER",
            "ALTER TABLE prop_snapshots ADD COLUMN trade_alert         INTEGER DEFAULT 0",
            "ALTER TABLE prop_snapshots ADD COLUMN no_brainer_tier     TEXT",
            "ALTER TABLE prop_snapshots ADD COLUMN edge_score_full     REAL",
            # CLV tracking columns
            "ALTER TABLE prop_snapshots ADD COLUMN model_prob        REAL",  # our estimated true prob at snapshot time
            "ALTER TABLE prop_snapshots ADD COLUMN closing_line      REAL",  # line at game time
            "ALTER TABLE prop_snapshots ADD COLUMN closing_odds      INTEGER",  # closing odds (american)
            "ALTER TABLE prop_snapshots ADD COLUMN closing_prob      REAL",  # no-vig implied prob from closing odds
            "ALTER TABLE prop_snapshots ADD COLUMN clv               REAL",  # model_prob - closing_prob (positive = beat the market)
            "ALTER TABLE prop_snapshots ADD COLUMN closing_fetched_at TEXT", # when we fetched closing line
            # Percentile profile columns
            "ALTER TABLE prop_snapshots ADD COLUMN pct_p10           REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN pct_p25           REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN pct_p50           REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN pct_p75           REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN pct_p90           REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN prob_over         REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN prob_over_plus1   REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN prob_over_plus2   REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN prob_under_minus2 REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN tail_risk_low     REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN spike_ratio       REAL",
            "ALTER TABLE prop_snapshots ADD COLUMN consistency_score REAL",
        ]
        for sql in migrations:
            try:
                conn.execute(sql)
            except Exception:
                pass  # column already exists — safe to ignore

        conn.commit()


def get_flag(key: str) -> dict:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT flagged, flag_type, note FROM flags WHERE key = ?", (key,)
        ).fetchone()
    if not row:
        return {"flagged": False, "flag_type": None, "note": ""}
    return {"flagged": bool(row[0]), "flag_type": row[1], "note": row[2] or ""}


def set_flag(key: str, flagged: bool, flag_type: str, note: str):
    from datetime import datetime
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """INSERT OR REPLACE INTO flags (key, flagged, flag_type, note, updated_at)
               VALUES (?, ?, ?, ?, ?)""",
            (key, int(flagged), flag_type, note, datetime.now().isoformat())
        )
        conn.commit()


def set_pipeline_status(status: str, message: str, started: str = None, finished: str = None):
    from datetime import datetime
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM pipeline_status")
        conn.execute(
            "INSERT INTO pipeline_status (status, message, started, finished) VALUES (?, ?, ?, ?)",
            (status, message, started or datetime.now().isoformat(), finished)
        )
        conn.commit()


def get_pipeline_status() -> dict:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT status, message, started, finished FROM pipeline_status"
        ).fetchone()
    if not row:
        return {"status": "idle", "message": "Not run yet"}
    return {"status": row[0], "message": row[1], "started": row[2], "finished": row[3]}


# ── Pipeline runner ───────────────────────────────────────────────────────────

_pipeline_lock = threading.Lock()
_player_logs_cache: dict = {}   # "player_name_team" → DataFrame, populated each pipeline run


def run_pipeline(game_date: str = None) -> dict:
    """Run the full data pipeline and return structured results."""
    import pandas as pd
    import nba_data
    import metrics
    import context as ctx_module
    from distribution import build_distribution_profile
    from line_shopping import build_line_shopping_rows
    from game_script import build_game_script_profile

    if game_date is None:
        game_date = date.today().isoformat()

    logger.info(f"Pipeline starting for {game_date}...")
    result = {
        "date":   game_date,
        "games":  [],
        "props":  [],
        "errors": [],
    }

    # 1. Schedule
    games = nba_data.get_todays_games(game_date)
    if not games:
        result["errors"].append(f"No games found for {game_date}")
        return result

    result["games"] = games
    team_list = []
    for g in games:
        team_list.append(g["away_team_abbr"])
        team_list.append(g["home_team_abbr"])

    # ── BDL context prefetch (all stable/slow-changing data) ─────────────────
    # Fetched once per pipeline run — standings, team averages, active players,
    # leaders. All downstream scoring and gate logic reads from this dict.
    from bdl_client import get_client as _get_bdl_client
    _bdl_ctx = {}
    try:
        _bdl = _get_bdl_client()
        _bdl_ctx = _bdl.prefetch_pipeline_context(game_date)
    except Exception as _bdl_err:
        logger.warning(f"BDL context prefetch failed (non-fatal): {_bdl_err}")

    # Convenience accessors with safe fallbacks
    _active_players_lkp : dict = _bdl_ctx.get("active_players", {})   # name_lower -> team info
    _standings_lkp      : dict = _bdl_ctx.get("standings", {})        # abbr -> wins/losses/records
    _team_avgs_lkp      : dict = _bdl_ctx.get("team_averages", {})    # abbr -> base/opponent/advanced
    _leaders_lkp        : dict = _bdl_ctx.get("leaders", {})          # player_id -> stat ranks

    # 2. Game logs
    for team in team_list:
        nba_data.get_team_game_log_df(team)

    # 3. Rosters
    roster_all  = []
    player_logs = {}
    for team in team_list:
        players = nba_data.get_active_roster_for_team(team, n_lookback=3)
        roster_all.extend(players)
        for p in players:
            key = f"{p['player_name']}|{p['team']}"
            player_logs[key] = nba_data.get_player_game_log(p["player_name"], p["team"])

    # Expose player logs globally for the parlay analysis endpoint
    global _player_logs_cache
    _player_logs_cache = player_logs

    # ── Historical player hit rates (from app_data.db) ────────────────────────
    # Build two lookup tables:
    #   _hist_player      : player_name → {hit_rate, sample, tier}  (all stats combined)
    #   _hist_player_stat : (player_name, stat) → {hit_rate, sample, tier}  (specific combos)
    # Tiers: PROVEN (≥75%, ≥20 graded), TRENDING (≥70%, ≥10), WATCH (≥65%, ≥8)
    # RECENCY FIX: filter to last 30 days so traded players (Trae Young etc)
    # reflect their new team context, not stale data from previous team.
    _hist_player: dict = {}
    _hist_player_stat: dict = {}
    _hist_cutoff = (date.today() - __import__('datetime').timedelta(days=30)).isoformat()
    try:
        with sqlite3.connect(DB_PATH) as _hconn:
            # All-stat player hit rates (min 10 graded in last 30 days)
            _hrows = _hconn.execute("""
                SELECT s.player_name, COUNT(*) as n, SUM(o.hit) as hits,
                       ROUND(100.0*SUM(o.hit)/COUNT(*), 1) as hit_rate
                FROM prop_snapshots s
                JOIN prop_outcomes o ON s.snapshot_key = o.snapshot_key
                WHERE o.hit IS NOT NULL
                  AND s.game_date >= ?
                GROUP BY s.player_name
                HAVING COUNT(*) >= 10
                ORDER BY hit_rate DESC
            """, (_hist_cutoff,)).fetchall()
            for row in _hrows:
                name, n, hits, hr = row
                tier = (
                    "PROVEN"   if hr >= 75 and n >= 15 else
                    "TRENDING" if hr >= 70 and n >= 10 else
                    "WATCH"    if hr >= 65 and n >= 8  else None
                )
                if tier:
                    _hist_player[name] = {"hit_rate": hr, "sample": n, "tier": tier}

            # Player+stat combos (min 6 graded in last 30 days)
            _hsrows = _hconn.execute("""
                SELECT s.player_name, s.stat, COUNT(*) as n, SUM(o.hit) as hits,
                       ROUND(100.0*SUM(o.hit)/COUNT(*), 1) as hit_rate
                FROM prop_snapshots s
                JOIN prop_outcomes o ON s.snapshot_key = o.snapshot_key
                WHERE o.hit IS NOT NULL
                  AND s.game_date >= ?
                GROUP BY s.player_name, s.stat
                HAVING COUNT(*) >= 6
                ORDER BY hit_rate DESC
            """, (_hist_cutoff,)).fetchall()
            for row in _hsrows:
                name, stat_key, n, hits, hr = row
                tier = (
                    "PROVEN"   if hr >= 80 and n >= 8 else
                    "TRENDING" if hr >= 70 and n >= 6 else None
                )
                if tier:
                    _hist_player_stat[(name, stat_key)] = {"hit_rate": hr, "sample": n, "tier": tier}
    except Exception as _he:
        logger.warning(f"Historical hit rate lookup failed (non-fatal): {_he}")
        _hist_player = {}
        _hist_player_stat = {}

    # 4. Props + game odds
    game_ids = [g["game_id"] for g in games if g.get("game_id")]
    auto_props, raw_props_by_game, player_id_lookup = nba_data.get_props_for_games(game_ids)

    # Fetch game-level odds (spread, total) for blowout risk — graceful fallback
    from bdl_client import get_client as _get_bdl
    _bdl = _get_bdl()
    game_odds_by_id: dict[int, dict] = {}

    # ── Lineup confirmation (once games begin) ────────────────────────────────
    # Only available after tip-off. Returns {} before games start — safe fallback.
    # player_id → True (starter) / False (bench confirmed)
    _starters_lkp: dict = {}
    try:
        _starters_lkp = _bdl.get_starters_lookup(game_ids)
        if _starters_lkp:
            starters_count = sum(1 for v in _starters_lkp.values() if v)
            logger.info(f"Lineups loaded: {starters_count} starters confirmed")
    except Exception as _le:
        logger.debug(f"Lineup fetch failed (non-fatal, likely pre-game): {_le}")
    try:
        # Bust stale odds cache before fetching — ensures lines are fresh
        # especially for late-tipping games that had no odds when pipeline first ran
        _bdl.bust_odds_cache(game_date)
        raw_odds = _bdl.get_game_odds(game_date)
        # BDL returns multiple vendor rows per game — build consensus per game_id
        # using preferred vendor priority: draftkings > fanduel > caesars > betmgm
        odds_by_game: dict[int, list] = {}
        for row in raw_odds:
            gid = row.get("game_id")
            if gid:
                odds_by_game.setdefault(gid, []).append(row)

        for gid, rows in odds_by_game.items():
            # Pick best vendor
            chosen = None
            for vendor in ["draftkings", "fanduel", "caesars", "betmgm"]:
                chosen = next((r for r in rows if r.get("vendor") == vendor), None)
                if chosen:
                    break
            if not chosen:
                chosen = rows[0]

            home_spread = chosen.get("spread_home_value")
            game_total  = chosen.get("total_value")
            home_ml     = chosen.get("moneyline_home_odds")
            away_ml     = chosen.get("moneyline_away_odds")

            game_odds_by_id[gid] = {
                "home_spread": float(home_spread) if home_spread is not None else None,
                "game_total":  float(game_total)  if game_total  is not None else None,
                "home_ml":     int(home_ml)        if home_ml     is not None else None,
                "away_ml":     int(away_ml)        if away_ml     is not None else None,
            }
        if game_odds_by_id:
            logger.info(f"Game odds loaded for {len(game_odds_by_id)} games")
        else:
            logger.warning(f"Game odds empty for {game_date} — blowout risk will be UNKNOWN for all games")
    except Exception as e:
        logger.warning(f"Game odds fetch failed: {e} — blowout risk will be UNKNOWN")

    # Build team → (game_id, home_spread, game_total, is_home, implied_total) lookup
    team_game_odds: dict[str, dict] = {}
    for g in games:
        gid = g.get("game_id")
        if not gid:
            continue
        odds = game_odds_by_id.get(gid, {})
        home_spread = odds.get("home_spread")
        game_total  = odds.get("game_total")
        home_abbr   = g.get("home_team_abbr", "")
        away_abbr   = g.get("away_team_abbr", "")

        # Implied team total = market's projection for how many points THIS team scores.
        # Formula: home_implied = (game_total - home_spread) / 2
        #   e.g. total=224, home_spread=-6 → home_implied = (224+6)/2 = 115
        #   e.g. total=224, home_spread=+6 → away_implied = (224+6)/2 = 115
        home_implied = None
        away_implied = None
        if game_total is not None and home_spread is not None:
            home_implied = round((game_total - home_spread) / 2, 1)
            away_implied = round((game_total + home_spread) / 2, 1)

        if home_abbr:
            team_game_odds[home_abbr] = {
                "game_id":       gid,
                "spread":        home_spread,       # negative = home favored
                "game_total":    game_total,
                "implied_total": home_implied,      # market's pts projection for this team
                "is_home":       True,
                "opponent":      away_abbr,
            }
        if away_abbr:
            away_spread = -home_spread if home_spread is not None else None
            team_game_odds[away_abbr] = {
                "game_id":       gid,
                "spread":        away_spread,       # positive = away underdog
                "game_total":    game_total,
                "implied_total": away_implied,
                "is_home":       False,
                "opponent":      home_abbr,
            }

    # 5. Match lines — also capture correct team from props data
    import unicodedata, re
    def norm(s):
        s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
        return re.sub(r"[^a-z0-9]","",s.lower())

    norm_to_canon = {norm(p["player_name"]): p["player_name"] for p in roster_all}
    # Build canon → team from roster (but props data overrides this below)
    canon_to_team = {p["player_name"]: p["team"] for p in roster_all}
    # Build canon → simplified position (G / F / C) for positional mismatch
    def _simplify_position(pos: str) -> str:
        p = (pos or "").upper()
        if "C" in p:   return "C"
        if "F" in p:   return "F"
        if "G" in p:   return "G"
        return ""
    canon_to_position = {p["player_name"]: _simplify_position(p.get("position","")) for p in roster_all}
    canonical_names = [p["player_name"] for p in roster_all]

    matched_lines:    dict[str, dict[str, list[float]]] = {}
    matched_odds:     dict[str, Optional[float]] = {}
    matched_metadata: dict[str, dict] = {}   # key → {market_type, is_quarter}

    for entry in auto_props:
        raw_name    = entry.get("player_name","").strip()
        stat        = entry.get("stat_type","").strip().upper()
        line_val    = float(entry.get("line") or 0)
        odds_val    = entry.get("odds")
        prop_team   = entry.get("team", "").strip().upper()
        market_type = entry.get("market_type", "over_under")
        is_quarter  = entry.get("is_quarter", False)
        if not raw_name or not stat:
            continue
        n     = norm(raw_name)
        canon = norm_to_canon.get(n)
        if canon is None:
            for cname in canonical_names:
                if norm(cname) == n or n in norm(cname):
                    canon = cname
                    break
        if canon is None:
            continue
        if prop_team:
            canon_to_team[canon] = prop_team
        matched_lines.setdefault(canon, {}).setdefault(stat, []).append(line_val)
        prop_key = f"{canon}|{stat}|{line_val}"
        matched_odds[prop_key]     = odds_val
        matched_metadata[prop_key] = {"market_type": market_type, "is_quarter": is_quarter}

    # 6. Metrics
    windows = {"Last5": 5, "Last10": 10, "Last20": 20}
    window_metrics_by_player = {}
    for p in roster_all:
        key  = f"{p['player_name']}|{p['team']}"
        log  = player_logs.get(key, pd.DataFrame())
        pname = p["player_name"]
        line_map = matched_lines.get(pname, {})
        window_metrics_by_player[pname] = {}
        for wname, n_games in windows.items():
            if log.empty:
                row = {"player_name": pname, "team": p["team"], "games_count": 0}
            else:
                row = metrics.compute_metrics(log, n_games=n_games, line_map=line_map)
                row["player_name"] = pname
                row["team"] = p["team"]
            window_metrics_by_player[pname][wname] = row

    # 7. Context
    full_df = nba_data.get_full_df(team_list)
    team_to_opponent = {}
    team_to_location = {}
    for g in games:
        h, a = g.get("home_team_abbr",""), g.get("away_team_abbr","")
        if h and a:
            team_to_opponent[h] = a
            team_to_opponent[a] = h
            team_to_location[h] = "Home"
            team_to_location[a] = "Away"

    context_by_player = {}
    for p in roster_all:
        key      = f"{p['player_name']}|{p['team']}"
        log      = player_logs.get(key, pd.DataFrame())
        opponent = team_to_opponent.get(p["team"], "—")
        location = team_to_location.get(p["team"], "—")
        position = canon_to_position.get(p["player_name"], "")
        c = ctx_module.build_player_context(
            player_name   = p["player_name"],
            team_abbr     = p["team"],
            opponent_abbr = opponent,
            player_log    = log,
            full_df       = full_df,
            game_date     = date.fromisoformat(game_date),
            today_location = location,
            position      = position,
        )
        context_by_player[p["player_name"]] = c

    # 7. Injury intelligence
    from injuries import build_injury_intelligence, format_opportunity_for_card
    logger.info("Building injury intelligence...")
    injury_intel = build_injury_intelligence(
        team_abbrs     = team_list,
        team_df_getter = nba_data.get_team_game_log_df,
    )
    opp_targets  = injury_intel.get("targets", {})
    result["injuries"]     = injury_intel.get("out_tonight", injury_intel.get("injuries", []))
    result["gtd_tonight"]  = injury_intel.get("gtd_tonight", [])
    result["opportunities"] = injury_intel.get("opportunities", [])

    # Pre-load today's line movements for use inside card loop
    _line_movements: dict = {}
    try:
        from line_movement import get_all_movements
        _line_movements = get_all_movements(game_date)
    except Exception:
        pass

    # 8. Build prop cards
    prop_cards = []
    for pname, stat_lines in matched_lines.items():
        team     = canon_to_team.get(pname, "")
        opponent = team_to_opponent.get(team, "—")
        location = team_to_location.get(team, "—")
        ctx  = context_by_player.get(pname, {})
        key  = f"{pname}|{team}"
        log  = player_logs.get(key, pd.DataFrame())
        # Enrich log with combo stats once — all downstream consumers use this
        if not log.empty:
            log = metrics.add_combo_stats(log)

        # ── Per-player BDL fetch (season averages + advanced stats) ───────────
        # Fetched once per player, used across all stat/line combos below.
        # Cached 12h for season avgs, 6h for advanced stats — fast after first run.
        _pid            = player_id_lookup.get(pname)
        _season_avg     = {}   # basic season avgs: pts, reb, ast, min, fg_pct etc
        _season_adv     = {}   # advanced: usage%, net_rating, assist%, reb%
        _season_usage   = {}   # usage%, possessions, pace
        _adv_game_logs  = []   # per-game advanced stats (last N games)
        _pm_rolling     = None # rolling plus/minus average (L10)

        if _pid:
            try:
                _sa = _bdl.get_season_averages(_pid)
                _season_avg = _sa or {}
            except Exception as _e:
                logger.debug(f"season_avg failed for {pname}: {_e}")

            try:
                _sa2 = _bdl.get_season_averages_advanced(_pid)
                _season_adv = (_sa2.get("stats") or {}) if _sa2 else {}
            except Exception as _e:
                logger.debug(f"season_avg_adv failed for {pname}: {_e}")

            try:
                _su = _bdl.get_season_averages_usage(_pid)
                _season_usage = (_su.get("stats") or {}) if _su else {}
            except Exception as _e:
                logger.debug(f"season_avg_usage failed for {pname}: {_e}")

            try:
                # L20 advanced game logs for rolling usage% and net_rating
                from datetime import date as _d, timedelta as _td
                _start = (_d.today() - _td(days=60)).isoformat()
                _adv_game_logs = _bdl.get_advanced_stats_for_player(
                    _pid, start_date=_start
                )
            except Exception as _e:
                logger.debug(f"adv_stats failed for {pname}: {_e}")

            # Rolling plus/minus from box scores (last 10 games)
            try:
                _pm_lookup = _bdl.get_plus_minus_lookup(game_date)
                # Note: this is today's box score — used post-game
                # Rolling L10 pm comes from adv_game_logs
                if _adv_game_logs:
                    _recent_pm = [
                        g.get("plus_minus") for g in _adv_game_logs[:10]
                        if g.get("plus_minus") is not None
                    ]
                    _pm_rolling = round(sum(_recent_pm) / len(_recent_pm), 1) if _recent_pm else None
            except Exception as _e:
                logger.debug(f"plus_minus failed for {pname}: {_e}")

        # Derived signals from season data
        _real_usage_pct = (
            _season_adv.get("usage_percentage") or
            _season_usage.get("usage_percentage")
        )
        _season_net_rating = _season_adv.get("net_rating")
        _season_pie        = _season_adv.get("pie")

        # Rolling usage% and net_rating from recent advanced game logs (L10)
        _l10_usage = _l10_net_rating = None
        if _adv_game_logs:
            _l10_usage_vals = [g.get("usage_percentage") for g in _adv_game_logs[:10]
                               if g.get("usage_percentage") is not None]
            _l10_nr_vals    = [g.get("net_rating") for g in _adv_game_logs[:10]
                               if g.get("net_rating") is not None]
            _l10_usage      = round(sum(_l10_usage_vals) / len(_l10_usage_vals), 3) if _l10_usage_vals else None
            _l10_net_rating = round(sum(_l10_nr_vals)    / len(_l10_nr_vals),    1) if _l10_nr_vals    else None

        # Contract year detection
        _is_contract_year = False
        if _pid:
            try:
                _is_contract_year = _bdl.is_contract_year(_pid)
            except Exception:
                pass

        for stat, line_vals in stat_lines.items():
            for line_val in line_vals:
                odds_key = f"{pname}|{stat}|{line_val}"
                odds     = matched_odds.get(odds_key)

                # Hit rates
                l5_hr = l10_hr = l20_hr = None
                for wname, n_label in [("Last5","l5"),("Last10","l10"),("Last20","l20")]:
                    w = window_metrics_by_player.get(pname,{}).get(wname,{})
                    # Try unsuffixed key first (single line for this stat)
                    hr = w.get(f"{stat}_hit_rate")
                    # If None, there are multiple lines — find the one matching line_val
                    if hr is None:
                        i = 1
                        while True:
                            candidate_line = w.get(f"{stat}_line{i}")
                            if candidate_line is None:
                                break
                            if abs(float(candidate_line) - line_val) < 0.01:
                                hr = w.get(f"{stat}_hit_rate{i}")
                                break
                            i += 1
                    if wname == "Last5":  l5_hr  = hr
                    if wname == "Last10": l10_hr = hr
                    if wname == "Last20": l20_hr = hr

                # Averages
                l5_avg  = window_metrics_by_player.get(pname,{}).get("Last5",{}).get(f"{stat}_avg")
                l10_avg = window_metrics_by_player.get(pname,{}).get("Last10",{}).get(f"{stat}_avg")

                # EV - compute both directions; pick relevant one for display later
                impl_prob = ev = over_ev = under_ev = None
                if odds:
                    try:
                        from line_shopping import american_to_implied_prob, calculate_ev
                        impl_prob = american_to_implied_prob(float(odds))
                        if l10_hr is not None:
                            over_ev  = calculate_ev(l10_hr,       float(odds))
                            under_ev = calculate_ev(1.0 - l10_hr, float(odds))
                            ev = over_ev  # used for scoring
                    except Exception:
                        pass

                # Distribution — milestone props get special binary analysis
                dist = {}
                from nba_data import MILESTONE_PROPS, QUARTER_PROPS
                is_milestone = stat in MILESTONE_PROPS
                is_quarter   = stat in QUARTER_PROPS

                if is_milestone:
                    from distribution import compute_milestone_profile
                    dist = compute_milestone_profile(log, stat)
                elif is_quarter:
                    # ── Real quarter stats from BDL ───────────────────────────
                    # Previously a placeholder (hook_score=0, completely blind).
                    # Now fetch actual Q1 game logs and compute real hit rates.
                    _qperiod = 1  # always Q1 for now (points_1q, rebounds_1q, assists_1q)
                    _qstat_map = {
                        "points_1q":   ("PTS", 1), "rebounds_1q": ("REB", 1),
                        "assists_1q":  ("AST", 1),
                        "points_first3min": ("PTS", 1), "rebounds_first3min": ("REB", 1),
                        "assists_first3min": ("AST", 1),
                    }
                    _qstat_base, _qperiod_num = _qstat_map.get(stat.lower(), ("PTS", 1))
                    _q_l5_hr = _q_l10_hr = _q_l20_hr = None
                    _q_values = []

                    if _pid:
                        try:
                            from datetime import date as _qd, timedelta as _qtd
                            _q_start = (_qd.today() - _qtd(days=90)).isoformat()
                            _q_logs = _bdl.get_player_quarter_logs(
                                _pid, period=_qperiod_num, start_date=_q_start
                            )
                            # Extract the relevant stat from each game
                            _q_values = []
                            for _qg in _q_logs:
                                _qv = _qg.get(_qstat_base.lower())
                                if _qv is not None:
                                    _q_values.append(float(_qv))
                            # Compute hit rates
                            if _q_values:
                                def _qhr(vals, n):
                                    if len(vals) < n: return None
                                    return round(sum(1 for v in vals[:n] if v > line_val) / n * 100, 1)
                                _q_l5_hr  = _qhr(_q_values, 5)
                                _q_l10_hr = _qhr(_q_values, 10)
                                _q_l20_hr = _qhr(_q_values, 20)
                        except Exception as _qe:
                            logger.debug(f"Q1 stats failed for {pname} {stat}: {_qe}")

                    # Build dist with real Q1 data if available, else placeholder
                    if _q_values and _q_l10_hr is not None:
                        _q_median = sorted(_q_values[:10])[len(_q_values[:10])//2] if len(_q_values) >= 10 else None
                        _q_gap    = (_q_median - line_val) if _q_median else 0
                        _q_hook   = (1 if _q_gap > 0.5 else (-1 if _q_gap < -0.5 else 0))
                        dist = {
                            "stat": stat, "line": line_val,
                            "hook_level":   "🔥 Q1 PRIME OVER" if _q_gap > 1 else ("🔴 Q1 UNDER" if _q_gap < -1 else "🟡 Q1 NEUTRAL"),
                            "hook_warning": f"Q1 {_qstat_base} median {_q_median:.1f} vs line {line_val}" if _q_median else "",
                            "hook_score":   _q_hook,
                            "avoid":        False,
                            "line_quality": "—",
                            "is_quarter":   True,
                            "q1_l5_hr":     _q_l5_hr,
                            "q1_l10_hr":    _q_l10_hr,
                            "q1_l20_hr":    _q_l20_hr,
                            "q1_values":    _q_values[:20],
                            "median_l10":   _q_median,
                        }
                        # Override l5/l10/l20 hit rates with Q1 data
                        if _q_l5_hr  is not None: l5_hr  = _q_l5_hr
                        if _q_l10_hr is not None: l10_hr = _q_l10_hr
                        if _q_l20_hr is not None: l20_hr = _q_l20_hr
                    else:
                        dist = {
                            "stat": stat, "line": line_val,
                            "hook_level": "⚪ Q1 Prop", "hook_warning": "",
                            "hook_score": 0, "avoid": False, "line_quality": "—",
                            "is_quarter": True,
                        }
                elif not log.empty and stat in log.columns:
                    dist = build_distribution_profile(log, stat, line_val)

                # Ghost game profile — detect players who play real minutes
                # but produce near-zero output at an anomalous rate
                from distribution import compute_ghost_profile
                ghost = compute_ghost_profile(log, stat) if not log.empty else {}

                # Line shopping
                all_raw = []
                for props in raw_props_by_game.values():
                    all_raw.extend(props)
                pid = player_id_lookup.get(pname)
                shop = None
                if pid:
                    from line_shopping import shop_lines
                    shop = shop_lines(all_raw, pid, stat, l10_hr)

                # Scoring
                card_key  = f"{pname}|{stat}|{line_val}"
                flag_data = get_flag(card_key)
                opp_data  = format_opportunity_for_card(pname, opp_targets)
                meta      = matched_metadata.get(card_key, {})

                # Game-level blowout risk for this player's team.
                # Now enhanced with standings (record differential) and
                # team averages (opponent defensive quality).
                game_info    = team_game_odds.get(team, {})
                opp_team     = game_info.get("opponent", "")
                player_mins  = ctx.get("minutes_l5_avg")

                # Standings context — record differential, playoff push, tanking, home/road
                _team_standing = _standings_lkp.get(team, {})
                _opp_standing  = _standings_lkp.get(opp_team, {})
                _record_diff   = None
                if _team_standing and _opp_standing:
                    _record_diff = round(
                        _team_standing.get("win_pct", 0.5) -
                        _opp_standing.get("win_pct", 0.5), 3
                    )

                # ── Playoff / tanking context ─────────────────────────────────
                _team_conf_rank = _team_standing.get("conf_rank")
                _team_wins      = _team_standing.get("wins", 0)
                _team_losses    = _team_standing.get("losses", 0)
                _total_games    = _team_wins + _team_losses
                _playoff_push   = bool(_team_conf_rank and _total_games >= 50 and _team_conf_rank <= 8)
                _tanking        = bool(_team_conf_rank and _total_games >= 50 and _team_conf_rank >= 14)

                # ── Home/road splits ──────────────────────────────────────────
                def _parse_record(rec_str):
                    try:
                        w, l = rec_str.split("-")
                        total = int(w) + int(l)
                        return round(int(w) / total, 3) if total > 0 else None
                    except Exception:
                        return None

                _is_home_game     = game_info.get("is_home", False)
                _home_win_pct     = _parse_record(_team_standing.get("home_record", ""))
                _road_win_pct     = _parse_record(_team_standing.get("road_record", ""))
                _location_win_pct = _home_win_pct if _is_home_game else _road_win_pct

                # ── Improved pace estimate (both teams averaged) ──────────────
                _team_adv_s  = _team_avgs_lkp.get(team, {}).get("advanced", {})
                _opp_adv_s   = _team_avgs_lkp.get(opp_team, {}).get("advanced", {})
                _team_pace_v = _team_adv_s.get("pace")
                _opp_pace_v  = _opp_adv_s.get("pace")
                if _team_pace_v and _opp_pace_v:
                    _opp_pace = round((_team_pace_v + _opp_pace_v) / 2, 1)
                elif _team_pace_v:
                    _opp_pace = _team_pace_v

                blowout = _compute_blowout_risk(
                    game_info.get("spread"),
                    game_info.get("game_total"),
                    mins=player_mins,
                    record_diff=_record_diff,
                    team_wins=_team_standing.get("wins"),
                    opp_wins=_opp_standing.get("wins"),
                )
                # Attach implied team total and standings context to blowout dict
                blowout["implied_team_total"] = game_info.get("implied_total")
                blowout["team_wins"]          = _team_standing.get("wins")
                blowout["opp_wins"]           = _opp_standing.get("wins")
                blowout["record_diff"]        = _record_diff

                # Team matchup context from BDL opponent averages
                # What does this opponent allow per game? Real data, not estimate.
                _opp_avgs = _team_avgs_lkp.get(opp_team, {}).get("opponent", {})
                _team_adv = _team_avgs_lkp.get(opp_team, {}).get("advanced", {})
                _opp_pace = _team_adv.get("pace") or _team_avgs_lkp.get(team, {}).get("advanced", {}).get("pace")

                # Build game-level blowout profile
                gs_profile = build_game_script_profile(
                    log        = log,
                    stat       = stat,
                    line       = line_val,
                    spread     = game_info.get("spread"),
                    game_total = game_info.get("game_total"),
                    minutes_mean = ctx.get("minutes_l5_avg"),
                    minutes_cv   = ctx.get("minutes_cv"),
                )

                # Recompute hit rates from actual game log values FIRST —
                # guarantees score, hammer, lock, and displayed bars all use identical data.
                # NOTE: use explicit None check, not `or`, so 0.0 (0/5 L5) doesn't
                # fall back to the stale value.
                def _hr_from_log(n):
                    if log.empty or stat not in log.columns:
                        return None
                    vals = pd.to_numeric(log[stat], errors="coerce").dropna().head(n)
                    if vals.empty:
                        return None
                    return round(float((vals > line_val).mean()), 3)

                computed_l5  = _hr_from_log(5)
                computed_l10 = _hr_from_log(10)
                computed_l20 = _hr_from_log(20)
                if computed_l5  is not None: l5_hr  = computed_l5
                if computed_l10 is not None: l10_hr = computed_l10
                if computed_l20 is not None: l20_hr = computed_l20

                # Score computed AFTER hit rate sync
                # Compute positional recent lines first so hit rate feeds into score
                _pos_lines_data = ctx_module.get_positional_recent_lines(
                    full_df         = full_df,
                    opponent_abbr   = opponent,
                    stat            = stat,
                    position_filter = canon_to_position.get(pname, ""),
                    line_val        = line_val,
                    n               = 8,
                )
                # Line movement for this specific prop
                _mv_key = f"{pname}|{stat}|{line_val}"
                _line_mv = _line_movements.get(_mv_key, {"available": False})

                score = _compute_score(l5_hr, l10_hr, l20_hr, ev, dist, ctx, stat,
                                       opp_data, blowout=blowout, ghost=ghost,
                                       pos_line_hit_rate=_pos_lines_data.get("pos_line_hit_rate"),
                                       line_movement=_line_mv)

                # Quarter props have no historical column data so score stays near 0 —
                # nudge to LEAN so they surface; EV from odds is the real signal here
                if is_quarter and abs(score) < 0.8:
                    score = 0.8

                # Milestone props (DD/TD) also have no game log column —
                # use achievement rate from distribution to set a real score
                if is_milestone:
                    achievement = dist.get("true_over_rate_l10") or dist.get("true_over_rate_l20")
                    if achievement is not None:
                        if achievement >= 0.60:   score = 3.0
                        elif achievement >= 0.45: score = 2.0
                        elif achievement >= 0.30: score = 1.0
                        else:                     score = 0.5
                    elif abs(score) < 0.8:
                        score = 0.8  # no data but still surface it

                # Pick the direction-appropriate EV for display and cap at ±30%
                def cap_ev(v):
                    if v is None: return None
                    return max(-30.0, min(30.0, round(v * 100, 1)))
                display_ev = cap_ev(under_ev if score < 0 and under_ev is not None else over_ev)

                # Hammer/lock computed AFTER hit rate sync so badge matches bars
                hammer_data = _compute_hammer(l5_hr, l10_hr, l20_hr, ev, dist, ctx=ctx)
                lock       = _compute_lock(l5_hr, l10_hr, l20_hr, dist, ctx=ctx, blowout=blowout, stat=stat, ghost=ghost)
                lock_under = _compute_lock_under(l5_hr, l10_hr, l20_hr, dist,
                                                 ctx=ctx, blowout=blowout, stat=stat)
                units = _compute_units(score, hammer_data.get("hammer", False), lock, lock_under)

                # Kelly Criterion bet sizing
                # Use a conservative blended probability rather than raw L10.
                # Raw L10 overstates edge because it captures recent hot streaks.
                # Blend: 40% L10 + 30% L20 + 30% regression toward 55% (market's fair line).
                # This shrinks Kelly sizes meaningfully when L10 is inflated by a hot streak.
                kelly_data = {}
                if odds and l10_hr is not None:
                    try:
                        from kelly import recommended_bet
                        _l10_w  = l10_hr
                        _l20_w  = l20_hr if l20_hr is not None else l10_hr
                        # Shrink toward the empirical market baseline (~37%), not 55%.
                        # Sportsbooks set lines near each player's median output, so the
                        # average L20 hit rate across the full slate is ~37% by design.
                        # Using 0.55 as the prior was inflating every prop's Kelly size
                        # by ~5-6pp. 0.37 is the correct regression target.
                        _regress = 0.37
                        _kelly_p = _l10_w * 0.40 + _l20_w * 0.30 + _regress * 0.30
                        kelly_data = recommended_bet(_kelly_p, float(odds))
                        kelly_data["blended_p"] = round(_kelly_p * 100, 1)  # expose for debugging
                    except Exception:
                        pass

                # ── Edge score (mispricedness-anchored composite) ─────────────
                try:
                    from scoring import score_from_vrow as _score_from_vrow
                    _vrow_for_scoring = {
                        "stat_type":         stat,
                        "line":              line_val,
                        "player_name":       pname,
                        "team":              team,
                        "distribution":      dist,
                        "last5_hit_rate":    l5_hr,
                        "last10_hit_rate":   l10_hr,
                        "last20_hit_rate":   l20_hr,
                        "opportunity":       opp_data.get("title", "") if isinstance(opp_data, dict) else "",
                        # New signals wired into scorer
                        "line_movement":     _line_mv,
                        "regression_soft":   hammer_data.get("regression_soft", False),
                        "outlier_inflated":  _outlier_data.get("outlier_inflated", False),
                        "dist_profile":      _dist_prof.get("dist_profile"),
                        # Historical hit rate signals
                        "hist_tier":         _hist_player_stat.get((pname, stat), {}).get("tier") or _hist_player.get(pname, {}).get("tier"),
                        "hist_stat_tier":    _hist_player_stat.get((pname, stat), {}).get("tier"),
                        # Market price — no-vig implied probability (feeds EV signal in value layer)
                        "implied_prob":      round(impl_prob * 100, 1) if impl_prob is not None else None,
                    }
                    # Also inject pos_line_hit_rate into ctx for score_from_vrow to pick up
                    _ctx_for_scoring = dict(ctx)
                    _ctx_for_scoring["pos_line_hit_rate"] = _pos_lines_data.get("pos_line_hit_rate")
                    _ctx_for_scoring["blowout_level"]     = blowout.get("level", "UNKNOWN")
                    # New BDL context signals
                    _ctx_for_scoring["opp_pts_allowed"]   = _opp_avgs.get("pts")
                    _ctx_for_scoring["opp_reb_allowed"]   = _opp_avgs.get("reb")
                    _ctx_for_scoring["opp_ast_allowed"]   = _opp_avgs.get("ast")
                    _ctx_for_scoring["opp_fg3m_allowed"]  = _opp_avgs.get("fg3m")
                    _ctx_for_scoring["game_pace"]         = _opp_pace
                    _ctx_for_scoring["opp_def_rating"]    = _team_avgs_lkp.get(opp_team, {}).get("advanced", {}).get("defensive_rating")
                    _ctx_for_scoring["record_diff"]       = blowout.get("record_diff")
                    _ctx_for_scoring["stat_leader_rank"]  = _leaders_lkp.get(
                        player_id_lookup.get(pname), {}
                    ).get(f"{stat.lower()}_rank") if player_id_lookup.get(pname) else None
                    # Season avg and advanced stats for scoring
                    _ctx_for_scoring["season_avg_vs_line"]  = _outlier_data.get("season_avg_vs_line")
                    _ctx_for_scoring["real_usage_pct"]      = _real_usage_pct
                    _ctx_for_scoring["net_rating_l10"]      = _l10_net_rating
                    _ctx_for_scoring["contract_year"]       = _is_contract_year
                    _ctx_for_scoring["playoff_push"]        = _playoff_push
                    _ctx_for_scoring["tanking"]             = _tanking
                    _ctx_for_scoring["location_win_pct"]    = _location_win_pct
                    _ctx_for_scoring["is_home_game"]        = _is_home_game
                    # Percentile profile signals for scoring
                    _ctx_for_scoring["spike_ratio"]         = _pct_profile.get("spike_ratio")
                    _ctx_for_scoring["tail_risk_low"]       = _pct_profile.get("tail_risk_low")
                    _ctx_for_scoring["prob_over_plus1"]     = _pct_profile.get("prob_over_plus1")
                    _ctx_for_scoring["consistency_score"]   = _pct_profile.get("consistency_score")
                    _edge_result = _score_from_vrow(_vrow_for_scoring, _ctx_for_scoring)
                except Exception as _e:
                    import logging as _lg
                    _lg.getLogger(__name__).debug(f"Edge score failed for {pname}|{stat}: {_e}")
                    _edge_result = {}

                # Distribution profile — CV, floor rate, modal gap, parlay DQ
                _l5_raw = (
                    [round(v, 1) for v in
                     pd.to_numeric(log[stat], errors="coerce").dropna().head(5).tolist()]
                    if not log.empty and stat in log.columns else []
                )
                _regression_risk = hammer_data.get("regression_risk", False)
                _regression_soft = hammer_data.get("regression_soft", False)
                _regression_gap  = hammer_data.get("regression_gap", 0)
                _is_b2b          = ctx.get("is_back_to_back", "") == "🔴 YES"
                _dist_prof = _compute_dist_profile(
                    l5_values       = _l5_raw,
                    line            = line_val,
                    stat            = stat,
                    hook_level      = dist.get("hook_level", ""),
                    regression_risk = bool(_regression_risk),   # hard DQ only (gap>=45)
                    is_b2b          = _is_b2b,
                    l20_hr          = l20_hr,
                )

                # ── Outlier inflation detection ───────────────────────────────────
                # Pass season average as the true baseline where available.
                # If a player averages 8 pts/game on the season but L5 shows 18,
                # that's a stronger signal than comparing L5 to L20 alone.
                _stat_season_avg = None
                if _season_avg:
                    _stat_map = {
                        "PTS": "pts", "REB": "reb", "AST": "ast",
                        "STL": "stl", "BLK": "blk", "FG3M": "fg3m",
                    }
                    _stat_key = _stat_map.get(stat.upper())
                    if _stat_key:
                        _stat_season_avg = _season_avg.get(_stat_key)

                _outlier_data = _compute_outlier_inflation(
                    _l5_raw, line_val, stat,
                    season_avg=_stat_season_avg,
                )

                # ── Percentile profile — full empirical distribution ──────────────
                # Use L20 values for the widest stable sample.
                # Falls back to L10 if L20 is too short.
                _l20_raw = (
                    [round(v, 1) for v in
                     pd.to_numeric(log[stat], errors="coerce").dropna().head(20).tolist()]
                    if not log.empty and stat in log.columns else []
                )
                _l10_raw = (
                    [round(v, 1) for v in
                     pd.to_numeric(log[stat], errors="coerce").dropna().head(10).tolist()]
                    if not log.empty and stat in log.columns else []
                )
                _pct_values = _l20_raw if len(_l20_raw) >= 8 else _l10_raw
                _pct_profile = _compute_percentile_profile(_pct_values, line_val)

                _nb_median_gap = (dist.get("median_l10") or 0) - line_val

                card = {
                    "key":         card_key,
                    "player_name": pname,
                    "team":        team,
                    "opponent":    opponent,
                    "location":    location,
                    "game_id":     game_info.get("game_id"),
                    "stat":        stat,
                    "line":        line_val,
                    "odds":        odds,
                    # Hit rates
                    "l5_hr":  round(l5_hr * 100, 1)  if l5_hr  is not None else None,
                    "l10_hr": round(l10_hr * 100, 1) if l10_hr is not None else None,
                    "l20_hr": round(l20_hr * 100, 1) if l20_hr is not None else None,
                    # Averages
                    "l5_avg":  round(l5_avg, 1)  if l5_avg  is not None else None,
                    "l10_avg": round(l10_avg, 1) if l10_avg is not None else None,
                    # EV (direction-aware, capped)
                    "implied_prob": round(impl_prob * 100, 1) if impl_prob is not None else None,
                    # model_prob: our best estimate of true probability at snapshot time
                    # Blend: 60% weighted hit rate (empirical) + 40% true_over_rate_l10 (distributional)
                    # This is what we compare against closing implied prob to compute CLV
                    "model_prob": round(
                        (
                            (_weighted_l10_l20_hr := (
                                (l10_hr * 0.60 + l20_hr * 0.40) if l10_hr is not None and l20_hr is not None
                                else (l10_hr if l10_hr is not None else l20_hr)
                            )) * 0.60 +
                            (dist.get("true_over_rate_l10") or _weighted_l10_l20_hr or 0.5) * 0.40
                        ) * 100, 1
                    ) if (l10_hr is not None or l20_hr is not None) else None,
                    "ev":           display_ev,
                    # Distribution
                    "median":        dist.get("median_l10"),
                    "modal_outcome": dist.get("modal_outcome"),
                    "hook_level":    dist.get("hook_level", "⚪ Neutral"),
                    "hook_warning":  dist.get("hook_warning", ""),
                    "true_over_pct": round(dist["true_over_rate_l10"] * 100, 1) if dist.get("true_over_rate_l10") is not None else None,
                    "near_miss_pct": dist.get("near_miss_pct"),
                    "line_quality":  dist.get("line_quality", "—"),
                    "top_outcomes":  dist.get("top_outcomes", []),
                    # Line shopping
                    "num_books":       shop["num_books"]       if shop else 0,
                    "best_over_line":  shop["best_over_line"]  if shop else None,
                    "best_over_book":  shop["best_over_book"]  if shop else None,
                    "best_over_odds":  shop["best_over_odds"]  if shop else None,
                    "line_spread":     shop["line_spread"]     if shop else 0,
                    "shopping_opp":    shop["shopping_opportunity"] if shop else False,
                    "all_books":       shop["all_books"]       if shop else [],
                    # Context
                    "days_rest":       ctx.get("days_rest"),
                    "is_b2b":          ctx.get("is_back_to_back",""),
                    "minutes_l5_avg":  ctx.get("minutes_l5_avg"),
                    "games_last_7":    ctx.get("games_last_7_days"),
                    "pts_trend":       ctx.get("PTS_trend",""),
                    "reb_trend":       ctx.get("REB_trend",""),
                    "ast_trend":       ctx.get("AST_trend",""),
                    "stat_trend":      ctx.get(f"{stat}_trend",""),
                    "matchup":         ctx.get(f"opp_{stat.lower()}_matchup", ctx.get("opp_pts_matchup","")),
                    "pos_matchup":     ctx.get(f"opp_{stat.lower()}_pos_matchup", ""),
                    "pos_mismatch":    ctx.get(f"opp_{stat.lower()}_pos_weak", False),
                    "player_position": canon_to_position.get(pname, ""),
                    # Positional recent lines — precomputed above for score, reused here
                    **_pos_lines_data,
                    # Head-to-head: this player's own history vs tonight's opponent
                    **ctx_module.compute_h2h(
                        player_log    = log,
                        opponent_abbr = opponent,
                        stat          = stat,
                        line_val      = line_val,
                        n             = 8,
                    ),
                    # Usage share — FGA%, team rank, role tier
                    **ctx_module.compute_usage_share(
                        player_name = pname,
                        player_log  = log,
                        full_df     = full_df,
                        team_abbr   = team,
                        n_games     = 10,
                    ),
                    # Defensive trend — is opponent getting easier/harder to score against?
                    "def_trend":        ctx.get(f"opp_{stat.lower()}_def_trend", ""),
                    "def_allowed_l10":  ctx.get(f"opp_{stat.lower()}_allowed_l10"),
                    "def_allowed_season": ctx.get(f"opp_{stat.lower()}_allowed_season"),
                    "def_delta":        ctx.get(f"opp_{stat.lower()}_def_delta"),
                    # Distribution detail for card display
                    "median_gap":      round(dist.get("median_l10", 0) - line_val, 2) if dist.get("median_l10") is not None else None,
                    "modal_vs_line":   round(dist.get("modal_outcome", 0) - line_val, 2) if dist.get("modal_outcome") is not None else None,
                    # Prop type metadata
                    "market_type":     meta.get("market_type", "over_under"),
                    "is_milestone":    is_milestone,
                    "is_quarter":      is_quarter,
                    # Milestone-specific rates (same field names for card compat)
                    "milestone_l5_rate":  round(dist.get("milestone_l5_rate", 0) * 100, 1) if dist.get("milestone_l5_rate") is not None else None,
                    "milestone_l10_rate": round(dist.get("milestone_l10_rate", 0) * 100, 1) if dist.get("milestone_l10_rate") is not None else None,
                    "milestone_l20_rate": round(dist.get("milestone_l20_rate", 0) * 100, 1) if dist.get("milestone_l20_rate") is not None else None,
                    # Blowout risk
                    "blowout_level":    blowout.get("level", "UNKNOWN"),
                    "blowout_label":    blowout.get("label", ""),
                    "blowout_spread":   blowout.get("spread_abs"),
                    "blowout_context":  blowout.get("blowout_context", ""),
                    "blowout_side":     blowout.get("side", ""),
                    "blowout_tier":     blowout.get("tier", ""),
                    "team_favored":     blowout.get("team_favored"),
                    "game_total":       blowout.get("game_total"),
                    "implied_total":    blowout.get("implied_team_total"),
                    # User flags
                    "flagged":      flag_data["flagged"],
                    "flag_type":    flag_data["flag_type"],
                    "note":         flag_data["note"],
                    # Opportunity
                    "opportunity":  opp_data,
                    # Last 5 raw game values for this stat (newest first)
                    # Used for the color-coded game strip on the card
                    "l5_values": (
                        [round(v, 1) for v in
                         pd.to_numeric(log[stat], errors="coerce").dropna().head(5).tolist()]
                        if not log.empty and stat in log.columns else []
                    ),
                    # Ghost game risk
                    "ghost_rate":   round(ghost.get("ghost_rate", 0) * 100, 1) if ghost.get("ghost_rate") is not None else None,
                    "floor_rate":   round(ghost.get("floor_rate", 0) * 100, 1) if ghost.get("floor_rate") is not None else None,
                    "ghost_flag":   ghost.get("ghost_flag", "—"),
                    "ghost_label":  ghost.get("ghost_label", "—"),
                    # Minutes stability — CV and label from context
                    "minutes_cv":        ctx.get("minutes_cv"),
                    "minutes_stability": ctx.get("minutes_stability", "—"),
                    "minutes_min_l10":   ctx.get("minutes_min_l10"),
                    "minutes_max_l10":   ctx.get("minutes_max_l10"),
                    # Line credibility: line / std_l10 — <1.0 means line is inside one std dev (trap)
                    "line_credibility": (
                        round(line_val / dist["std_l10"], 2)
                        if dist.get("std_l10") and dist["std_l10"] > 0 else None
                    ),
                    # Hammer + regression
                    **hammer_data,
                    # Lock + unit sizing
                    "lock":       lock == "over",
                    "lock_under": lock_under,
                    "units": units,
                    # Edge score — mispricedness-anchored composite (0-100)
                    "edge_score":        _edge_result.get("edge_score"),
                    "direction":         _edge_result.get("direction"),
                    "misprice_score":    _edge_result.get("misprice_score"),
                    "misprice_label":    _edge_result.get("misprice_label"),
                    "confidence_score":  _edge_result.get("confidence_score"),
                    "context_score":     _edge_result.get("context_score"),
                    "role_score":        _edge_result.get("role_score"),
                    "is_parlay_ready":   _edge_result.get("is_parlay_ready", False),
                    "is_hammer":         _edge_result.get("is_hammer", False),
                    "is_lock":           _edge_result.get("is_lock", False),
                    # Final score (legacy — kept for backward compat)
                    "score": score,
                    # Game script profile — player-specific, powers parlay engine
                    "game_script_profile": gs_profile,
                    # Kelly Criterion bet sizing
                    "kelly": kelly_data,
                    # Line movement (populated after pipeline, placeholder here)
                    "line_movement": _line_mv,
                    # Distribution profile — CV, floor rate, modal gap, parlay DQ
                    "dist_profile":             _dist_prof.get("dist_profile", "UNKNOWN"),
                    "dist_cv":                  _dist_prof.get("dist_cv"),
                    "dist_floor_rate":          _dist_prof.get("dist_floor_rate"),
                    "dist_modal_gap":           _dist_prof.get("modal_gap"),
                    "parlay_disqualified":      _dist_prof.get("parlay_disqualified", False),
                    "parlay_disqualify_reason": _dist_prof.get("parlay_disqualify_reason", ""),
                    "is_shot_dependent":        _dist_prof.get("is_shot_dependent", False),
                    "is_no_brainer":   False,   # assigned in post-processing pass
                    "no_brainer_tier": None,     # assigned in post-processing pass
                    # Outlier inflation — recent monster game masking weak baseline
                    "outlier_inflated":  _outlier_data.get("outlier_inflated", False),
                    "outlier_note":      _outlier_data.get("outlier_note", ""),
                    "outlier_game_val":  _outlier_data.get("outlier_game_val"),
                    "true_l5_hr":        _outlier_data.get("true_l5_hr"),
                    # Percentile profile — full empirical distribution structure
                    "pct_p10":              _pct_profile.get("p10"),
                    "pct_p25":              _pct_profile.get("p25"),
                    "pct_p50":              _pct_profile.get("p50"),
                    "pct_p75":              _pct_profile.get("p75"),
                    "pct_p90":              _pct_profile.get("p90"),
                    "prob_over":            _pct_profile.get("prob_over"),
                    "prob_over_plus1":      _pct_profile.get("prob_over_plus1"),
                    "prob_over_plus2":      _pct_profile.get("prob_over_plus2"),
                    "prob_under_minus2":    _pct_profile.get("prob_under_minus2"),
                    "tail_risk_low":        _pct_profile.get("tail_risk_low"),
                    "tail_risk_high":       _pct_profile.get("tail_risk_high"),
                    "spike_ratio":          _pct_profile.get("spike_ratio"),
                    "consistency_score":    _pct_profile.get("consistency_score"),
                    # GTD flag — player is game-time decision tonight
                    "gtd": any(
                        inj.get("player_name", "").lower() == pname.lower()
                        for inj in injury_intel.get("gtd_tonight", [])
                    ),
                    # Historical hit rate — from app_data.db graded outcomes
                    # hist_tier: PROVEN (≥75% on 20+ props) / TRENDING (≥70% on 10+) / WATCH (≥65% on 8+)
                    "hist_tier":       (_hist_player_stat.get((pname, stat), {}).get("tier") or
                                        _hist_player.get(pname, {}).get("tier")),
                    "hist_hit_rate":   (_hist_player_stat.get((pname, stat), {}).get("hit_rate") or
                                        _hist_player.get(pname, {}).get("hit_rate")),
                    "hist_sample":     (_hist_player_stat.get((pname, stat), {}).get("sample") or
                                        _hist_player.get(pname, {}).get("sample")),
                    "hist_stat_tier":  _hist_player_stat.get((pname, stat), {}).get("tier"),
                    "hist_stat_hr":    _hist_player_stat.get((pname, stat), {}).get("hit_rate"),
                    "hist_stat_n":     _hist_player_stat.get((pname, stat), {}).get("sample"),
                    # ── NEW: BDL context fields ───────────────────────────────
                    # Standings — competitive context
                    "team_wins":       blowout.get("team_wins"),
                    "opp_wins":        blowout.get("opp_wins"),
                    "record_diff":     blowout.get("record_diff"),
                    "team_win_pct":    _standings_lkp.get(team, {}).get("win_pct"),
                    "opp_win_pct":     _standings_lkp.get(opp_team, {}).get("win_pct"),
                    "team_conf_rank":  _standings_lkp.get(team, {}).get("conf_rank"),
                    "opp_conf_rank":   _standings_lkp.get(opp_team, {}).get("conf_rank"),
                    # Opponent defensive averages — what this defense ALLOWS
                    "opp_pts_allowed": _opp_avgs.get("pts"),
                    "opp_reb_allowed": _opp_avgs.get("reb"),
                    "opp_ast_allowed": _opp_avgs.get("ast"),
                    "opp_fg3m_allowed":_opp_avgs.get("fg3m"),
                    "opp_stl_allowed": _opp_avgs.get("stl"),
                    "opp_blk_allowed": _opp_avgs.get("blk"),
                    # Team pace — affects counting stat expectations
                    "game_pace":       _opp_pace,
                    # Opponent offensive/defensive rating
                    "opp_def_rating":  _team_avgs_lkp.get(opp_team, {}).get("advanced", {}).get("defensive_rating"),
                    "opp_off_rating":  _team_avgs_lkp.get(opp_team, {}).get("advanced", {}).get("offensive_rating"),
                    # League leader rank for this player+stat
                    "stat_leader_rank": _leaders_lkp.get(
                        player_id_lookup.get(pname), {}
                    ).get(f"{stat.lower()}_rank") if player_id_lookup.get(pname) else None,
                    # Trade alert — player's team doesn't match active roster
                    "trade_alert": (
                        _active_players_lkp.get(pname.lower(), {}).get("team_abbr", "").upper() not in ("", team.upper())
                        and bool(_active_players_lkp.get(pname.lower()))
                    ),
                    "current_team_bdl": _active_players_lkp.get(pname.lower(), {}).get("team_abbr", ""),
                    # ── Lineup confirmation ────────────────────────────────────
                    # confirmed_starter: True = confirmed starter, False = confirmed bench
                    # None = lineup not posted yet (pre-game)
                    "confirmed_starter": (
                        _starters_lkp.get(_pid)
                        if _pid and _pid in _starters_lkp else None
                    ),
                    # ── Playoff / tanking / home-road context ──────────────────
                    "playoff_push":      _playoff_push,
                    "tanking":           _tanking,
                    "team_conf_rank":    _team_conf_rank,
                    "home_win_pct":      _home_win_pct,
                    "road_win_pct":      _road_win_pct,
                    "location_win_pct":  _location_win_pct,
                    "is_home_game":      _is_home_game,
                    # ── Season averages (BDL) ──────────────────────────────────
                    "season_avg_pts":      _season_avg.get("pts"),
                    "season_avg_reb":      _season_avg.get("reb"),
                    "season_avg_ast":      _season_avg.get("ast"),
                    "season_avg_min":      _season_avg.get("min"),
                    "season_avg_fg3m":     _season_avg.get("fg3m"),
                    "season_avg_stl":      _season_avg.get("stl"),
                    "season_avg_blk":      _season_avg.get("blk"),
                    "season_games_played": _season_avg.get("games_played"),
                    "season_avg_vs_line":  _outlier_data.get("season_avg_vs_line"),
                    # ── Advanced stats (BDL) ───────────────────────────────────
                    "usage_pct":        _real_usage_pct,
                    "usage_pct_l10":    _l10_usage,
                    "net_rating":       _season_net_rating,
                    "net_rating_l10":   _l10_net_rating,
                    "pie":              _season_pie,
                    "pm_rolling_l10":   _pm_rolling,
                    "contract_year":    _is_contract_year,
                    # ── Quarter prop data ──────────────────────────────────────
                    "q1_l5_hr":   dist.get("q1_l5_hr") if is_quarter else None,
                    "q1_l10_hr":  dist.get("q1_l10_hr") if is_quarter else None,
                    "q1_l20_hr":  dist.get("q1_l20_hr") if is_quarter else None,
                    "q1_values":  dist.get("q1_values", []) if is_quarter else [],
                }

                prop_cards.append(card)

    # Sort by edge_score descending (falls back to legacy score if missing)
    prop_cards.sort(key=lambda c: c.get("edge_score") if c.get("edge_score") is not None else abs(c.get("score", 0)) * 10, reverse=True)

    # ── No-brainer tier assignment (post-processing) ──────────────────────────
    # Tiers are assigned AFTER the full card is built so _parlay_is_clean has
    # access to every field. This is the single authoritative gate — no duplicate
    # checks needed here because _parlay_is_clean owns all the hard blocks.
    #
    # Tier definitions (applied only to clean props):
    #   PRIME  — elite consistency: cv≤0.22, zero floor, L5≥80%, L10≥70%,
    #             L20≥60%, median gap≥+1.5, shot-independent, CONSISTENT/MODERATE dist
    #   STRONG — high confidence: cv≤0.35, floor≤0.20, L5≥70%, L10≥65%,
    #             L20≥55%, median gap≥+0.5, CONSISTENT/MODERATE dist
    #   SOLID  — parlay-eligible: L5≥70%, L10≥65%, L20≥55% (or unknown),
    #             floor≤0.40, any dist profile (catches no-dist-data props)
    for card in prop_cards:
        # Step 1: must pass every hard gate
        if not _parlay_is_clean(card):
            card["no_brainer_tier"] = None
            card["is_no_brainer"]   = False
            continue

        cv      = card.get("dist_cv")
        floor   = card.get("dist_floor_rate")
        profile = card.get("dist_profile", "UNKNOWN")
        shot    = card.get("is_shot_dependent", False)
        l5      = (card.get("l5_hr")  or 0) / 100
        l10     = (card.get("l10_hr") or 0) / 100
        l20     = card.get("l20_hr")   # may be None for no-data props
        l20v    = (l20 or 0) / 100
        mgap    = (card.get("median") or 0) - (card.get("line") or 0)

        is_prime = (
            profile in ("CONSISTENT", "MODERATE")
            and cv is not None and cv <= 0.22
            and floor is not None and floor == 0.0
            and l5  >= 0.80
            and l10 >= 0.70
            and l20v >= 0.60
            and mgap >= 1.5
            and not shot
            and (card.get("edge_score") or 0) >= 68   # must have real edge score to be PRIME
        )
        is_strong = (
            not is_prime
            and profile in ("CONSISTENT", "MODERATE")
            and (cv is None or cv <= 0.35)
            and (floor is None or floor <= 0.20)
            and l5  >= 0.70
            and l10 >= 0.65
            and l20v >= 0.55
            and mgap >= 0.5
            and (card.get("edge_score") or 0) >= 55   # edge 50-54 is borderline — require 55+
        )
        is_solid = (
            not is_prime and not is_strong
            and profile in ("CONSISTENT", "MODERATE", "UNKNOWN")
            and l5  >= 0.70
            and l10 >= 0.65
            and (l20 is None or l20v >= 0.55)
            and (floor is None or floor <= 0.40)
            and (card.get("edge_score") or 0) >= 48   # require at least some edge signal
        )

        tier = "PRIME" if is_prime else "STRONG" if is_strong else "SOLID" if is_solid else None
        card["no_brainer_tier"] = tier
        card["is_no_brainer"]   = (tier == "PRIME")  # legacy compat

    # ── Stat component tagging ────────────────────────────────────────────────
    # Each combined stat is decomposed into primitive components so the parlay
    # builder can detect correlated legs (e.g. PA+AST for same player both
    # contain the AST primitive — parlaying them is effectively a duplicate).
    _STAT_COMPONENTS: dict = {
        "PTS": frozenset(["PTS"]),
        "REB": frozenset(["REB"]),
        "AST": frozenset(["AST"]),
        "BLK": frozenset(["BLK"]),
        "STL": frozenset(["STL"]),
        "FG3M": frozenset(["FG3M"]),
        "TOV": frozenset(["TOV"]),
        "RA":  frozenset(["REB", "AST"]),
        "PR":  frozenset(["PTS", "REB"]),
        "PA":  frozenset(["PTS", "AST"]),
        "PRA": frozenset(["PTS", "REB", "AST"]),
        "DD":  frozenset(["DD"]),
        "TD":  frozenset(["TD"]),
    }
    for card in prop_cards:
        stat_key = (card.get("stat") or "").upper()
        card["stat_components"] = sorted(_STAT_COMPONENTS.get(stat_key, frozenset([stat_key])))

    # ── One prop per player deduplication ────────────────────────────────────
    # Keep only the highest edge_score prop per player. When multiple props exist
    # for the same player, prefer: (a) no component overlap with already-selected
    # props, (b) higher tier, (c) higher edge_score.
    # Suppressed props are still included but tagged so the UI can hide them.
    _seen_players: set = set()
    for card in prop_cards:
        pname = card.get("player_name", "")
        if pname not in _seen_players:
            _seen_players.add(pname)
            card["best_prop_for_player"] = True
        else:
            card["best_prop_for_player"] = False

    result["props"] = prop_cards

    # ── Data quality validation ───────────────────────────────────────────────
    # Runs automatically after every pipeline. Surfaces warnings in the UI
    # so you know when signals are missing BEFORE you bet.
    _dq_warnings = []
    _total_props = len(prop_cards)

    if _total_props > 0:
        # Check 1: Spread data coverage
        _unknown_blowout = [p for p in prop_cards if p.get("blowout_level") == "UNKNOWN"]
        _unknown_pct = len(_unknown_blowout) / _total_props * 100
        if _unknown_pct > 40:
            _dq_warnings.append({
                "level": "error",
                "code":  "SPREAD_DATA_MISSING",
                "msg":   f"{_unknown_pct:.0f}% of props missing spread data — blowout risk unreliable. "
                         f"Lines may not be posted yet for late games. Refresh after noon.",
            })
        elif _unknown_pct > 20:
            _dq_warnings.append({
                "level": "warn",
                "code":  "SPREAD_DATA_PARTIAL",
                "msg":   f"{_unknown_pct:.0f}% of props missing spread data. Some late games may not have lines yet.",
            })

        # Check 2: Role players with unknown blowout
        _role_unknown = [
            p for p in prop_cards
            if p.get("blowout_level") == "UNKNOWN"
            and p.get("usage_tier") in ("ROLE", "BENCH")
            and (p.get("min_rank") or 99) >= 4
            and (p.get("minutes_l5_avg") or 99) < 28
        ]
        if _role_unknown:
            _role_names = list({p["player_name"] for p in _role_unknown})[:4]
            _dq_warnings.append({
                "level": "warn",
                "code":  "ROLE_UNKNOWN_BLOWOUT",
                "msg":   f"{len(_role_unknown)} role player props blocked (UNKNOWN blowout): "
                         + ", ".join(_role_names) + (" + more" if len(_role_unknown) > 4 else ""),
            })

        # Check 3: Tiered props — any failing gates?
        _tiered = [p for p in prop_cards if p.get("no_brainer_tier")]
        _tiered_bad = []
        for p in _tiered:
            from app import _parlay_is_clean
            if not _parlay_is_clean(p):
                _tiered_bad.append(p["player_name"])
        if _tiered_bad:
            _dq_warnings.append({
                "level": "error",
                "code":  "TIER_GATE_MISMATCH",
                "msg":   f"BUG: {len(_tiered_bad)} tiered props failing parlay gate: "
                         + ", ".join(_tiered_bad[:3]),
            })

        # Check 4: Edge scores suspiciously high on low-hit-rate props
        _suspicious = [
            p for p in prop_cards
            if (p.get("edge_score") or 0) >= 65
            and (p.get("l10_hr") or 0) < 40
        ]
        if _suspicious:
            _dq_warnings.append({
                "level": "warn",
                "code":  "HIGH_EDGE_LOW_HR",
                "msg":   f"{len(_suspicious)} props with edge≥65 but L10<40% — likely under bets or check scoring.",
            })

        # Check 5: hist_tier field presence (new field — confirms new app.py deployed)
        _has_hist = any("hist_tier" in p for p in prop_cards[:5])
        if not _has_hist:
            _dq_warnings.append({
                "level": "warn",
                "code":  "HIST_TIER_MISSING",
                "msg":   "hist_tier field missing from props — historical edge badges won't show. Re-deploy app.py.",
            })

    result["data_quality"] = {
        "warnings":       _dq_warnings,
        "has_errors":     any(w["level"] == "error" for w in _dq_warnings),
        "has_warnings":   any(w["level"] == "warn" for w in _dq_warnings),
        "spread_coverage": round(
            100 * sum(1 for p in prop_cards if p.get("blowout_level") != "UNKNOWN") / max(_total_props, 1), 1
        ),
        "tiered_count":   sum(1 for p in prop_cards if p.get("no_brainer_tier")),
        "clean_count":    sum(1 for p in prop_cards if _parlay_is_clean(p)),
    }
    try:
        from line_movement import record_lines
        record_lines(prop_cards, game_date)
    except Exception as e:
        logger.warning(f"Line movement snapshot failed (non-fatal): {e}")

    # Build game headers — one per game with environment summary for the frontend
    game_headers = {}
    for g in games:
        gid = g.get("game_id")
        if not gid:
            continue
        home = g.get("home_team_abbr", "")
        away = g.get("away_team_abbr", "")
        odds_h = team_game_odds.get(home, {})
        odds_a = team_game_odds.get(away, {})

        def_trend_vs_home = ctx_module.compute_defensive_trend(full_df, home, "PTS", recent_n=10)
        def_trend_vs_away = ctx_module.compute_defensive_trend(full_df, away, "PTS", recent_n=10)

        injuries = []
        for card in prop_cards:
            if card.get("game_id") == gid and card.get("opportunity"):
                opp = card["opportunity"]
                out_name = opp.get("out_player")
                if out_name and out_name not in injuries:
                    injuries.append(out_name)

        game_headers[str(gid)] = {
            "game_id":      gid,
            "home":         home,
            "away":         away,
            "home_name":    g.get("home_team_name", home),
            "away_name":    g.get("away_team_name", away),
            "game_time":    g.get("game_time", ""),
            "spread":       odds_h.get("spread"),
            "game_total":   odds_h.get("game_total"),
            "home_implied": odds_h.get("implied_total"),
            "away_implied": odds_a.get("implied_total"),
            "home_def_allowed_l10":    def_trend_vs_home.get("opp_pts_allowed_l10"),
            "home_def_allowed_season": def_trend_vs_home.get("opp_pts_allowed_season"),
            "home_def_trend":          def_trend_vs_home.get("opp_pts_def_trend"),
            "home_def_delta":          def_trend_vs_home.get("opp_pts_def_delta"),
            "away_def_allowed_l10":    def_trend_vs_away.get("opp_pts_allowed_l10"),
            "away_def_allowed_season": def_trend_vs_away.get("opp_pts_allowed_season"),
            "away_def_trend":          def_trend_vs_away.get("opp_pts_def_trend"),
            "away_def_delta":          def_trend_vs_away.get("opp_pts_def_delta"),
            "injuries":     injuries,
        }

    result["game_headers"] = game_headers
    logger.info(f"Pipeline complete: {len(prop_cards)} prop cards built")

    # Persist snapshots to DB for historical hit rate tracking
    try:
        _save_prop_snapshots(prop_cards, game_date)
        # Attempt to fetch outcomes for previous days now that we have fresh data
        threading.Thread(target=_auto_fetch_yesterday_outcomes, daemon=True).start()
    except Exception as e:
        logger.warning(f"Snapshot save failed (non-fatal): {e}")

    return result


def _is_parlay_ready(card: dict) -> bool:
    SI = {'REB','AST','RA','BLK','STL','TOV'}
    if (card.get('stat','').upper()) not in SI:          return False
    if not card.get('lock'):                              return False
    gap = card.get('median_gap')
    if gap is None or gap < 2.0:                         return False
    if (card.get('true_over_pct') or 0) < 80:            return False
    if (card.get('near_miss_pct') or 0) > 0:             return False
    l5 = card.get('l5_values') or []
    if any(v <= card.get('line', 999) for v in l5):      return False
    if (card.get('days_rest') or 99) < 2:                return False
    hook = (card.get('hook_level') or '').lower()
    if any(h in hook for h in ['regression','severe','warning','hot']): return False
    return True


def _is_parlay_consider(card: dict) -> bool:
    SI = {'REB','AST','RA','BLK','STL','TOV'}
    if (card.get('stat','').upper()) not in SI:          return False
    if not card.get('lock') and not card.get('hammer'):  return False
    gap = card.get('median_gap')
    if gap is None or gap < 1.5:                         return False
    if (card.get('true_over_pct') or 0) < 70:            return False
    l5 = card.get('l5_values') or []
    misses = sum(1 for v in l5 if v <= card.get('line', 999))
    if misses > 1:                                        return False
    hook = (card.get('hook_level') or '').lower()
    if any(h in hook for h in ['regression','severe','warning']): return False
    return True


# ── Stat component map (shared by parlay builder and parlay_analyze) ─────────

_STAT_COMPONENTS: dict = {
    "PTS": frozenset(["PTS"]),
    "REB": frozenset(["REB"]),
    "AST": frozenset(["AST"]),
    "BLK": frozenset(["BLK"]),
    "STL": frozenset(["STL"]),
    "FG3M": frozenset(["FG3M"]),
    "TOV":  frozenset(["TOV"]),
    "RA":   frozenset(["REB", "AST"]),
    "PR":   frozenset(["PTS", "REB"]),
    "PA":   frozenset(["PTS", "AST"]),
    "PRA":  frozenset(["PTS", "REB", "AST"]),
    "DD":   frozenset(["DD"]),
    "TD":   frozenset(["TD"]),
}


_HARD_DQ_CATS = frozenset({"VOLATILE-FLOOR", "COMBINED STAT CV too high", "SHOT-DEPENDENT", "REGRESSION RISK"})


def _parlay_is_clean(card: dict) -> bool:
    """Return True if this prop passes all hard DQ gates for parlay inclusion."""
    # Hard block: regression risk (gap >=45% L5 vs L20) — no exceptions
    if card.get("regression_risk"):
        return False

    # Hard block: outlier inflation detected
    if card.get("outlier_inflated"):
        return False

    # Hard block: extreme volatility — cv > 0.50 means the player is wildly inconsistent
    cv = card.get("dist_cv")
    if cv is not None and cv > 0.50:
        return False

    # Hard block: DQ category
    dq_cat = (card.get("parlay_disqualify_reason") or "").split(":")[0].strip()
    if dq_cat in _HARD_DQ_CATS:
        return False

    # Hard block: WEAK L20 label (belt-and-suspenders — the threshold check below also catches it)
    if dq_cat == "WEAK L20":
        return False

    # Hard block: L20 below minimum threshold
    l20 = card.get("l20_hr")
    if l20 is not None:
        thresh = _L20_WEAK_THRESHOLDS_PARLAY.get((card.get("stat") or "").upper(), 0.55)
        if (l20 / 100.0) < thresh:
            return False

    # Hard block: B2B
    if (card.get("is_b2b") or "").upper().startswith("🔴"):
        return False

    # Hard block: GTD — do not parlay a player who may not play
    if card.get("gtd"):
        return False

    # Hard block: confirmed bench player (lineup posted, player not starting)
    # confirmed_starter=False means lineup IS posted and player is NOT starting.
    # confirmed_starter=None means lineup not posted yet — don't block.
    if card.get("confirmed_starter") is False:
        return False

    # Hard block: ROLE/BENCH player with UNKNOWN blowout data
    # When spread data is unavailable we can't assess garbage-time risk.
    # A ROLE player ranked 4th+ in minutes on a team with a franchise player
    # is exactly the Champagnie scenario — unknown blowout = unknown benching risk.
    if (card.get("blowout_level") == "UNKNOWN"
            and card.get("usage_tier") in ("ROLE", "BENCH")
            and (card.get("min_rank") or 99) >= 4
            and (card.get("minutes_l5_avg") or 99) < 28):
        return False

    return True


def _stat_overlap(a: dict, b: dict) -> frozenset:
    """Return the set of primitive stat components shared by two prop cards."""
    ca = frozenset(_STAT_COMPONENTS.get((a.get("stat") or "").upper(), frozenset()))
    cb = frozenset(_STAT_COMPONENTS.get((b.get("stat") or "").upper(), frozenset()))
    return ca & cb


def _game_key(card: dict) -> str:
    """Canonical game key (order-independent team pair)."""
    return "|".join(sorted([card.get("team", ""), card.get("opponent", "")]))


def build_parlay_suggestions(props: list, max_legs: int = 5) -> dict:
    """
    Auto-build parlay suggestions from the prop pool.

    Algorithm:
      1. Filter to no_brainer_tier props (PRIME > STRONG > SOLID), clean DQ.
         If the pool is thin (<4 eligible), drop to B-tier (L5>=70, L10>=60, clean).
      2. One prop per player — pick the highest-tier / highest-edge leg.
         For same player, skip legs whose stat components fully overlap with an
         already-chosen leg (e.g. if PRA is chosen, skip PR/PA/RA for that player).
      3. Max 2 legs per game (correlation guard).
      4. Build all valid 3-leg, 4-leg, and 5-leg parlays from the pool,
         ranked by joint L10 hit probability (product of individual L10 rates).
      5. Return the best parlay at each leg count plus the full eligible pool.

    Returns dict with keys:
      pool        – ordered list of eligible prop cards
      mode        – "A-tier" | "B-tier"
      parlays     – {3: {...}, 4: {...}, 5: {...}}  each with legs + joint_prob + label
    """
    _TIER_RANK = {"PRIME": 0, "STRONG": 1, "SOLID": 2, None: 9}

    def _tier(c):
        return c.get("no_brainer_tier")

    def _edge(c):
        return c.get("edge_score") or 0

    def _l10(c):
        l10 = c.get("l10_hr") or 0
        return l10 / 100.0 if l10 > 1.0 else l10

    # Step 1 — build candidate pool from no_brainer_tier props
    candidates = [
        c for c in props
        if _parlay_is_clean(c) and _tier(c) is not None
    ]

    b_tier_mode = False
    if len(candidates) < 4:
        # Not enough PRIME/STRONG/SOLID props — drop to B-tier
        b_tier_mode = True
        candidates = [
            c for c in props
            if _parlay_is_clean(c)
            and (c.get("l5_hr") or 0) >= 70
            and (c.get("l10_hr") or 0) >= 60
        ]

    # Sort: tier rank first, then edge_score descending
    candidates.sort(key=lambda c: (_TIER_RANK.get(_tier(c), 9), -_edge(c)))

    # Step 2 — one prop per player, component-overlap-aware
    seen_players: dict = {}   # player_name → frozenset of claimed components
    pool: list = []
    game_counts: dict = {}    # game_key → count

    for card in candidates:
        pname = card.get("player_name", "")
        gkey  = _game_key(card)
        stat  = (card.get("stat") or "").upper()
        comps = _STAT_COMPONENTS.get(stat, frozenset([stat]))

        # Max 2 legs per game
        if game_counts.get(gkey, 0) >= 2:
            continue

        if pname in seen_players:
            # Player already has a leg — only allow if zero component overlap
            if seen_players[pname] & comps:
                continue   # overlapping components — skip
            # Non-overlapping stat for same player — allowed (rare: e.g. BLK + AST)
            seen_players[pname] = seen_players[pname] | comps
        else:
            seen_players[pname] = comps

        game_counts[gkey] = game_counts.get(gkey, 0) + 1
        pool.append(card)

    # Step 3 — build best parlay at each leg count
    import itertools
    parlays: dict = {}

    for n_legs in range(3, min(max_legs + 1, len(pool) + 1)):
        best = None
        best_prob = -1.0

        for combo in itertools.combinations(pool, n_legs):
            # Re-check max-2-per-game within this combo
            gc: dict = {}
            valid = True
            for leg in combo:
                gk = _game_key(leg)
                gc[gk] = gc.get(gk, 0) + 1
                if gc[gk] > 2:
                    valid = False
                    break
            if not valid:
                continue

            # Joint probability = product of individual L10 rates
            joint = 1.0
            for leg in combo:
                joint *= max(0.01, _l10(leg))

            if joint > best_prob:
                best_prob = joint
                best      = combo

        if best is not None:
            avg_odds  = sum(leg.get("odds") or -110 for leg in best) / len(best)
            tiers_str = "/".join((_tier(l) or "B") for l in best)
            parlays[n_legs] = {
                "legs":       [l.get("key") for l in best],
                "leg_cards":  list(best),
                "joint_l10":  round(best_prob * 100, 1),
                "label":      f"{n_legs}-leg ({tiers_str}) — {round(best_prob*100,1)}% joint L10",
                "avg_odds":   round(avg_odds),
                "mode":       "B-tier" if b_tier_mode else "A-tier",
            }

    return {
        "pool":    pool,
        "mode":    "B-tier" if b_tier_mode else "A-tier",
        "parlays": parlays,
    }


def _save_prop_snapshots(cards: list, game_date: str):
    """Persist today's prop cards to prop_snapshots table."""
    from datetime import datetime
    rows = []
    for c in cards:
        key = f"{game_date}|{c.get('player_name','')}|{c.get('team','')}|{c.get('stat','')}|{c.get('line','')}"
        gs  = c.get('game_script_profile') or {}
        rows.append((
            key,
            game_date,
            c.get('player_name',''),
            c.get('team',''),
            c.get('opponent',''),
            c.get('stat',''),
            c.get('line'),
            c.get('score'),
            int(bool(c.get('lock'))),
            int(bool(c.get('hammer'))),
            int(bool(c.get('lock_under'))),
            c.get('median_gap'),
            c.get('true_over_pct'),
            c.get('near_miss_pct'),
            c.get('l5_hr'),
            c.get('l10_hr'),
            c.get('l20_hr'),
            json.dumps(c.get('l5_values') or []),
            c.get('days_rest'),
            gs.get('minutes_mean'),
            gs.get('minutes_cv'),
            c.get('implied_total'),
            c.get('blowout_spread'),
            c.get('game_total'),
            c.get('blowout_level',''),
            c.get('ghost_rate'),
            json.dumps(c.get('opportunity') or {}),
            gs.get('shot_profile',''),
            gs.get('weighted_hit'),
            int(_is_parlay_ready(c)),
            int(_is_parlay_consider(c)),
            c.get('odds'),
            c.get('edge_score'),
            int(bool(c.get('parlay_disqualified', False))),
            c.get('parlay_disqualify_reason', '') or '',
            c.get('dist_profile', '') or '',
            c.get('dist_cv'),
            int(bool(c.get('regression_risk', False))),
            # New Session 4/5 fields
            c.get('usage_pct'),
            c.get('net_rating_l10'),
            c.get('season_avg_vs_line'),
            int(bool(c.get('contract_year', False))),
            c.get('game_pace'),
            c.get('opp_pts_allowed'),
            c.get('opp_def_rating'),
            c.get('team_wins'),
            c.get('opp_wins'),
            int(bool(c.get('trade_alert', False))),
            c.get('no_brainer_tier'),
            c.get('edge_score'),   # edge_score_full
            c.get('model_prob'),   # CLV: our estimated true probability at snapshot time
            # Percentile profile
            c.get('pct_p10'),
            c.get('pct_p25'),
            c.get('pct_p50'),
            c.get('pct_p75'),
            c.get('pct_p90'),
            c.get('prob_over'),
            c.get('prob_over_plus1'),
            c.get('prob_over_plus2'),
            c.get('prob_under_minus2'),
            c.get('tail_risk_low'),
            c.get('spike_ratio'),
            c.get('consistency_score'),
            datetime.now().isoformat(),
        ))

    with sqlite3.connect(DB_PATH) as conn:
        conn.executemany("""
            INSERT OR IGNORE INTO prop_snapshots (
                snapshot_key, game_date, player_name, team, opponent, stat, line,
                score, is_lock, is_hammer, is_lock_under,
                median_gap, true_over_pct, near_miss_pct,
                l5_hr, l10_hr, l20_hr, l5_values,
                days_rest, minutes_mean, minutes_cv,
                implied_total, spread, game_total, blowout_level,
                ghost_rate, opportunity, shot_profile, weighted_hit,
                parlay_ready, parlay_consider, odds, edge_score,
                parlay_disqualified, parlay_disqualify_reason,
                dist_profile, dist_cv, regression_risk,
                usage_pct, net_rating_l10, season_avg_vs_line,
                contract_year, game_pace, opp_pts_allowed, opp_def_rating,
                team_wins, opp_wins, trade_alert, no_brainer_tier, edge_score_full,
                model_prob,
                pct_p10, pct_p25, pct_p50, pct_p75, pct_p90,
                prob_over, prob_over_plus1, prob_over_plus2, prob_under_minus2,
                tail_risk_low, spike_ratio, consistency_score,
                created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, rows)
        conn.commit()
    logger.info(f"Saved {len(rows)} prop snapshots for {game_date}")


def fetch_and_store_outcomes(game_date: str) -> dict:
    """
    Auto-fetch final box scores for game_date and match against snapshots.
    Called automatically each morning for the previous day's games.
    Returns summary of outcomes recorded.
    """
    import nba_data
    from datetime import datetime
    from bdl_client import get_client

    logger.info(f"Fetching outcomes for {game_date}...")

    # Get all snapshots for this date that don't have outcomes yet
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("""
            SELECT s.snapshot_key, s.player_name, s.team, s.stat, s.line
            FROM prop_snapshots s
            LEFT JOIN prop_outcomes o ON s.snapshot_key = o.snapshot_key
            WHERE s.game_date = ? AND o.snapshot_key IS NULL
        """, (game_date,)).fetchall()

    if not rows:
        return {"date": game_date, "recorded": 0, "message": "No pending snapshots"}

    # Get games for that date to find game_ids
    client = get_client()
    games  = client.get_games_for_date(game_date)
    if not games:
        return {"date": game_date, "recorded": 0, "message": "No games found"}

    # Check if games are actually final
    final_games = [g for g in games if str(g.get('status','')).upper() in ('FINAL','FINAL/OT','FINAL/2OT','FINAL/3OT') or str(g.get('status','')).startswith('Final')]
    if not final_games:
        return {"date": game_date, "recorded": 0, "message": "Games not yet final"}

    # Build player → actual stats lookup from box scores
    player_stats: dict = {}   # normalize(player_name) → {stat: value}
    from utils import normalize_name

    for game in final_games:
        game_id = game.get('id')
        if not game_id:
            continue
        try:
            stats = client.get_stats_for_game(game_id)
        except Exception as e:
            logger.warning(f"Could not fetch stats for game {game_id}: {e}")
            continue

        for s in stats:
            player = s.get('player', {})
            raw_name = f"{player.get('first_name','')} {player.get('last_name','')}".strip()
            if not raw_name:
                continue
            key = normalize_name(raw_name)

            # Parse minutes
            min_str = s.get('min','0') or '0'
            if ':' in str(min_str):
                parts = str(min_str).split(':')
                try:    minutes = float(parts[0]) + float(parts[1])/60
                except: minutes = 0.0
            else:
                try:    minutes = float(min_str)
                except: minutes = 0.0

            pts  = float(s.get('pts') or 0)
            reb  = float(s.get('reb') or 0)
            ast  = float(s.get('ast') or 0)
            fg3m = float(s.get('fg3m') or 0)
            stl  = float(s.get('stl') or 0)
            blk  = float(s.get('blk') or 0)
            tov  = float(s.get('turnover') or 0)

            player_stats[key] = {
                'PTS':  pts,
                'REB':  reb,
                'AST':  ast,
                'FG3M': fg3m,
                'STL':  stl,
                'BLK':  blk,
                'TOV':  tov,
                'PRA':  pts + reb + ast,
                'PR':   pts + reb,
                'PA':   pts + ast,
                'RA':   reb + ast,
                'MIN':  minutes,
            }

    if not player_stats:
        return {"date": game_date, "recorded": 0, "message": "No player stats found"}

    # Match snapshots to actual outcomes
    outcomes = []
    now = datetime.now().isoformat()
    for (snap_key, player_name, team, stat, line) in rows:
        norm = normalize_name(player_name)
        pstats = player_stats.get(norm)
        if not pstats:
            continue
        actual = pstats.get(stat.upper())
        if actual is None:
            continue
        # DNP — skip, don't record as miss
        if pstats.get('MIN', 0) < 5:
            continue
        hit = 1 if actual > line else 0
        outcomes.append((snap_key, game_date, player_name, team, stat, line, actual, hit, 'bdl', now))

    if outcomes:
        with sqlite3.connect(DB_PATH) as conn:
            conn.executemany("""
                INSERT OR IGNORE INTO prop_outcomes
                (snapshot_key, game_date, player_name, team, stat, line,
                 actual_value, hit, fetch_source, fetched_at)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, outcomes)
            conn.commit()
        logger.info(f"Recorded {len(outcomes)} outcomes for {game_date}")

    return {
        "date":       game_date,
        "recorded":   len(outcomes),
        "pending":    len(rows) - len(outcomes),
        "message":    f"Recorded {len(outcomes)} of {len(rows)} snapshots"
    }


def _auto_fetch_yesterday_outcomes():
    """
    Called on app startup and after each pipeline run.
    Attempts to fetch outcomes for yesterday (and up to 3 days back)
    in case games weren't final when last checked.
    """
    from datetime import date, timedelta
    for days_ago in range(1, 4):
        target = (date.today() - timedelta(days=days_ago)).isoformat()
        try:
            # Only attempt if we have snapshots for that date
            with sqlite3.connect(DB_PATH) as conn:
                count = conn.execute(
                    "SELECT COUNT(*) FROM prop_snapshots WHERE game_date=?", (target,)
                ).fetchone()[0]
            if count > 0:
                result = fetch_and_store_outcomes(target)
                logger.info(f"Auto-fetch outcomes {target}: {result.get('message','')}")
        except Exception as e:
            logger.warning(f"Auto-fetch outcomes failed for {target}: {e}")


def _compute_percentile_profile(values: list, line: float) -> dict:
    """
    Compute full empirical percentile structure from raw game log values.

    Takes the last N game values and computes:
      - p10, p25, p50, p75, p90  (full percentile spine)
      - prob_over      : empirical P(outcome > line)
      - prob_push      : empirical P(outcome == line)  [rare but real]
      - prob_over_plus1: empirical P(outcome > line+1)  — "comfortable over"
      - prob_over_plus2: empirical P(outcome > line+2)  — "strong over"
      - prob_under_minus2: empirical P(outcome < line-2) — "blowup risk"
      - tail_risk_low  : P(outcome <= p10)  — floor tail (catastrophic unders)
      - tail_risk_high : P(outcome >= p90)  — ceiling spike (outlier games)
      - spike_ratio    : (p90 - p50) / max(p50 - p10, 1) — asymmetry measure
                         >1.5 = player spikes up more than they drop
                         <0.7 = player drops more than they spike (floor risk)
      - consistency_score: 0-100, how tight the distribution is around the median
                           100 = every game within 10% of median, 0 = wildly volatile

    This separates:
      - Consistent scorers: p25-p75 range is narrow, high consistency_score
      - Spike players: spike_ratio > 1.5, hit rate driven by outlier games
      - Floor players: tail_risk_low high, prob_under_minus2 high
    """
    result = {
        "p10": None, "p25": None, "p50": None, "p75": None, "p90": None,
        "prob_over":       None,
        "prob_over_plus1": None,
        "prob_over_plus2": None,
        "prob_under_minus2": None,
        "tail_risk_low":   None,
        "tail_risk_high":  None,
        "spike_ratio":     None,
        "consistency_score": None,
        "n_games_pct":     None,
    }

    vals = [v for v in (values or []) if v is not None and v >= 0]
    n = len(vals)
    if n < 4 or line is None or line < 0:
        return result

    sorted_vals = sorted(vals)

    def percentile(p):
        """Linear interpolation percentile."""
        if not sorted_vals:
            return None
        idx = (p / 100.0) * (n - 1)
        lo, hi = int(idx), min(int(idx) + 1, n - 1)
        frac = idx - lo
        return round(sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac, 2)

    p10 = percentile(10)
    p25 = percentile(25)
    p50 = percentile(50)
    p75 = percentile(75)
    p90 = percentile(90)

    result["p10"] = p10
    result["p25"] = p25
    result["p50"] = p50
    result["p75"] = p75
    result["p90"] = p90
    result["n_games_pct"] = n

    # Empirical probabilities — direct counts, no distribution assumption
    result["prob_over"]        = round(sum(1 for v in vals if v >  line)           / n, 3)
    result["prob_push"]        = round(sum(1 for v in vals if v == line)           / n, 3)
    result["prob_over_plus1"]  = round(sum(1 for v in vals if v >  line + 1.0)    / n, 3)
    result["prob_over_plus2"]  = round(sum(1 for v in vals if v >  line + 2.0)    / n, 3)
    result["prob_under_minus2"]= round(sum(1 for v in vals if v <  line - 2.0)    / n, 3)

    # Tail risk — how often does player land in the extreme tails?
    result["tail_risk_low"]    = round(sum(1 for v in vals if v <= p10)            / n, 3) if p10 is not None else None
    result["tail_risk_high"]   = round(sum(1 for v in vals if v >= p90)            / n, 3) if p90 is not None else None

    # Spike asymmetry — does this player spike up or crash down?
    if p50 is not None and p10 is not None and p90 is not None:
        upside   = p90 - p50
        downside = max(p50 - p10, 0.5)  # floor to avoid div by zero
        result["spike_ratio"] = round(upside / downside, 2)

    # Consistency score — how tight is the interquartile range relative to the median?
    # IQR / median gives a normalized spread measure.
    # Score 0-100: 100 = very tight (IQR < 5% of median), 0 = very loose (IQR > 100% of median)
    if p25 is not None and p75 is not None and p50 is not None and p50 > 0:
        iqr = p75 - p25
        iqr_pct = iqr / p50  # normalized by median
        consistency = max(0.0, min(100.0, (1.0 - iqr_pct) * 100.0))
        result["consistency_score"] = round(consistency, 1)

    return result


def _compute_outlier_inflation(
    l5_values: list,
    line: float,
    stat: str,
    season_avg: Optional[float] = None,
) -> dict:
    """
    Detect when recent outlier game(s) are artificially inflating L5 hit rate.
    Enhanced with season_avg as a true baseline — if L5 is running 50%+ above
    season average, that's a stronger signal than comparing L5 to L20 alone.

    A prop is 'outlier inflated' when:
      1. The most recent game value is >= 2.5x the line (a clear outlier)
         AND it is >= 1.8x the median of the other 4 L5 games
      2. OR: the top 1 value in L5 is > 2x the median of the remaining 4,
         AND removing it drops the L5 hit rate by >= 30 percentage points.
      3. OR (new): L5 average is >= 1.5x season average for this stat,
         and the line is set above season average (book pricing the hot streak).

    Returns dict with:
      outlier_inflated : bool
      outlier_note     : str
      outlier_game_val : float | None
      inflated_l5_hr   : float | None
      true_l5_hr       : float | None
      season_avg_vs_line : float | None  (season_avg minus line — positive = over edge)
    """
    result = {
        "outlier_inflated":    False,
        "outlier_note":        "",
        "outlier_game_val":    None,
        "inflated_l5_hr":      None,
        "true_l5_hr":          None,
        "season_avg_vs_line":  round(season_avg - line, 2) if season_avg and line else None,
    }

    vals = [v for v in (l5_values or []) if v is not None and v >= 0]
    if len(vals) < 4 or line <= 0:
        return result

    # Most recent game is index 0
    recent = vals[0]
    rest   = vals[1:]

    rest_median = sorted(rest)[len(rest) // 2]
    all_median  = sorted(vals)[len(vals) // 2]

    # Current L5 hit rate (with outlier)
    inflated_hr = sum(1 for v in vals if v > line) / len(vals)

    # Outlier check: recent game is a monster relative to line AND rest of L5
    recent_is_outlier = (
        recent >= line * 2.5
        and rest_median > 0
        and recent >= rest_median * 1.8
    )

    # Alternative: any single game in L5 is a massive outlier
    max_val = max(vals)
    max_idx = vals.index(max_val)
    without_max = [v for i, v in enumerate(vals) if i != max_idx]
    without_max_median = sorted(without_max)[len(without_max) // 2] if without_max else 0
    without_max_hr = sum(1 for v in without_max if v > line) / len(without_max) if without_max else 0

    max_is_outlier = (
        max_val >= line * 2.5
        and without_max_median > 0
        and max_val >= without_max_median * 2.0
        and (inflated_hr - without_max_hr) >= 0.25  # drops hit rate by 25pp+
    )

    if recent_is_outlier or max_is_outlier:
        outlier_val = recent if recent_is_outlier else max_val
        true_hr     = sum(1 for v in (without_max if max_is_outlier else rest) if v > line)
        true_hr_pct = true_hr / (len(vals) - 1)
        drop_pp     = round((inflated_hr - true_hr_pct) * 100)

        result["outlier_inflated"]  = True
        result["outlier_game_val"]  = outlier_val
        result["inflated_l5_hr"]    = round(inflated_hr * 100, 1)
        result["true_l5_hr"]        = round(true_hr_pct * 100, 1)
        result["outlier_note"] = (
            f"Recent outlier game ({outlier_val:.0f} vs line {line}) inflating L5 by ~{drop_pp}pp. "
            f"True L5 without outlier: {true_hr_pct*100:.0f}%. Book has likely adjusted line up — fade risk."
        )

    # ── Season average hot streak check ──────────────────────────────────────
    # If player's L5 average is 50%+ above their season average AND the book has
    # moved the line above the season average, this is a regression risk.
    # Mark outlier_inflated even if no single monster game is present.
    if not result["outlier_inflated"] and season_avg and season_avg > 0 and len(vals) >= 4:
        l5_avg = sum(vals) / len(vals)
        if l5_avg >= season_avg * 1.5 and line > season_avg:
            true_hr_pct = sum(1 for v in vals if v > season_avg) / len(vals)
            result["outlier_inflated"]  = True
            result["inflated_l5_hr"]    = round(inflated_hr * 100, 1)
            result["true_l5_hr"]        = round(true_hr_pct * 100, 1)
            result["outlier_note"] = (
                f"L5 avg ({l5_avg:.1f}) is {((l5_avg/season_avg)-1)*100:.0f}% above season avg ({season_avg:.1f}). "
                f"Line ({line}) set above season avg — book pricing hot streak. Regression likely."
            )

    return result


def _compute_dist_profile(l5_values: list, line: float, stat: str, hook_level: str = "",
                          regression_risk: bool = False, is_b2b: bool = False,
                          l20_hr=None) -> dict:
    """
    Compute distribution profile fields for a prop card:
      - dist_cv         : coefficient of variation (std/mean) — measures consistency
      - dist_floor_rate : fraction of L5 games at or below the line (floor risk)
      - dist_profile    : CONSISTENT / MODERATE / VOLATILE / VOLATILE-FLOOR
      - modal_gap       : modal outcome minus line
      - parlay_disqualified     : True if this leg should be excluded from parlays
      - parlay_disqualify_reason: human-readable reason
      - is_shot_dependent: True if stat contains a shooting component

    Hard DQ rules (learned from 2026-03-11 slate):
      1. B2B — hard disqualification, not just a warning
      2. L20 < 55% — insufficient long-run sample to trust
      3. VOLATILE-FLOOR profile
      4. Regression risk / running hot
      5. Pure PTS shot-dependent
      6. Combined stats (PR/PRA/RA) require CV <= 0.25 — stricter threshold
    """
    import statistics

    _SHOT_DEP_STATS = {"PTS", "FG3M", "FGM", "FGA", "FG3A"}
    _COMBINED_STATS = {"PR", "PRA", "RA", "PA"}
    stat_up = stat.upper()
    is_shot_dep = stat_up in _SHOT_DEP_STATS or stat_up.startswith("Q")
    is_combined = stat_up in _COMBINED_STATS

    # Defaults
    result = {
        "dist_cv": None,
        "dist_floor_rate": None,
        "dist_profile": "UNKNOWN",
        "modal_gap": None,
        "parlay_disqualified": False,
        "parlay_disqualify_reason": "",
        "is_shot_dependent": is_shot_dep,
    }

    vals = [v for v in (l5_values or []) if v is not None and v >= 0]
    if len(vals) < 3:
        return result

    mean = statistics.mean(vals)
    if mean == 0:
        return result

    std = statistics.pstdev(vals)
    cv = round(std / mean, 3)

    floor_count = sum(1 for v in vals if v <= line)
    floor_rate = round(floor_count / len(vals), 3)

    # Modal outcome: most-common value (round to nearest integer bucket)
    from collections import Counter
    buckets = [round(v) for v in vals]
    modal = Counter(buckets).most_common(1)[0][0]
    modal_gap = round(modal - line, 2)

    # Classify profile
    if cv <= 0.15 and floor_rate == 0:
        profile = "CONSISTENT"
    elif cv <= 0.25 and floor_rate <= 0.20:
        profile = "MODERATE"
    elif floor_rate >= 0.40:
        profile = "VOLATILE-FLOOR"
    elif cv > 0.35:
        profile = "VOLATILE"
    else:
        profile = "MODERATE"

    # ── Parlay disqualification rules ─────────────────────────────────────────
    # Ordered by severity. First match wins.
    dq = False
    dq_reason = ""

    # Per-stat L20 "genuinely weak" thresholds (fraction scale, 0-1).
    # Derived from empirical L20 distribution: threshold = max(mean - 1.5 * sd, 0.10)
    # Sportsbooks set lines near each player's median output, so the average L20
    # across the board is ~37% by design — a hardcoded 55% gate eliminates 70% of
    # the board incorrectly. These thresholds only flag genuine outliers on the low end.
    _l20_thresh = _L20_VETO_FLOOR.get(stat_up, 0.12)

    # Rule 1: B2B — hard DQ, not a warning (learned 2026-03-11)
    if is_b2b:
        dq = True
        dq_reason = "B2B: back-to-back game, rest/DNP risk too high"

    # Rule 2: Genuinely weak L20 — below the stat-specific floor (mean - 1.5 SD).
    # Only fires when the player is a true outlier on the low side for their stat type.
    # If l20_hr is None (insufficient 20-game sample), skip this gate.
    elif l20_hr is not None and l20_hr < _l20_thresh:
        dq = True
        dq_reason = f"WEAK L20: only {round((l20_hr or 0)*100)}% hit rate over last 20 games (floor for {stat_up}: {round(_l20_thresh*100)}%)"

    # Rule 3: Volatile floor — too many misses in L5
    elif profile == "VOLATILE-FLOOR":
        dq = True
        dq_reason = f"VOLATILE-FLOOR: {int(floor_rate*100)}% of L5 games at/below line"

    # Rule 4: Regression risk / running hot
    elif regression_risk or (hook_level and any(k in hook_level.upper() for k in ("REGRESSION", "HOT", "SEVERE", "WARNING"))):
        dq = True
        dq_reason = "REGRESSION RISK: running hot, regression flagged"

    # Rule 5: Combined stats require tighter CV (more moving parts = more ways to miss)
    elif is_combined and cv > 0.25:
        dq = True
        dq_reason = f"COMBINED STAT CV too high: {cv} > 0.25 threshold for {stat_up}"

    # Rule 6: Pure shot-dependent PTS prop
    elif is_shot_dep and stat_up == "PTS":
        dq = True
        dq_reason = "SHOT-DEPENDENT: pure PTS props excluded from safe parlays"

    result.update({
        "dist_cv": cv,
        "dist_floor_rate": floor_rate,
        "dist_profile": profile,
        "modal_gap": modal_gap,
        "parlay_disqualified": dq,
        "parlay_disqualify_reason": dq_reason,
        "is_shot_dependent": is_shot_dep,
    })
    return result


def _compute_blowout_risk(
    spread: Optional[float],
    game_total: Optional[float],
    mins: Optional[float] = None,
    record_diff: Optional[float] = None,
    team_wins: Optional[int] = None,
    opp_wins: Optional[int] = None,
) -> dict:
    """
    Asymmetric blowout risk based on spread direction AND player minute tier.
    Enhanced with standings: a 60-win team vs a 10-win team is EXTREME
    regardless of what the nightly spread says.

    The old model applied abs(spread) equally to both teams — wrong in opposite
    directions. A 12-point favorite's stars get sat in Q4 (bad). A 12-point
    underdog's stars stay on the floor longer to chase the game (neutral/good).
    5 players are always on the court — minutes lost by stars go somewhere.

    spread convention (as stored in team_game_odds):
      negative → this team is FAVORED  (e.g. -12 = favored by 12)
      positive → this team is UNDERDOG (e.g. +12 = underdog by 12)

    Player minute tiers:
      Star:        32+ min  — heavy Q4 usage, big resting risk when favored
      Starter:     25–31 min — moderate resting risk when favored
      Role player: 20–24 min — may GAIN garbage minutes when favored
      Fringe:      <20 min  — unpredictable both directions

    Returns a penalty score where:
      positive penalty → subtract from prop score (bad for the over)
      negative penalty → adds to prop score (good for the over)
    Also returns detailed label for card display.
    """
    if spread is None:
        return {
            "level": "UNKNOWN", "spread_abs": None, "penalty": 0.0,
            "label": "—", "should_avoid_role_players": False,
            "game_total": game_total, "side": "unknown",
            "blowout_context": "—",
        }

    spread_abs   = abs(spread)
    team_favored = spread < 0   # negative spread = this team is favored

    # ── Game-level severity ───────────────────────────────────────────────────
    if spread_abs >= 13:
        level = "EXTREME"
        base_label = f"🚨 {'FAV' if team_favored else 'DOG'} by {spread_abs:.0f}"
    elif spread_abs >= 10:
        level = "HIGH"
        base_label = f"⚠️ {'FAV' if team_favored else 'DOG'} by {spread_abs:.0f}"
    elif spread_abs >= 7:
        level = "MODERATE"
        base_label = f"🟡 {'FAV' if team_favored else 'DOG'} by {spread_abs:.0f}"
    else:
        level = "LOW"
        base_label = f"🟢 Tight game ({spread_abs:.0f} pt spread)"

    # ── Standings override ────────────────────────────────────────────────────
    # A historically dominant mismatch (40+ win differential) is EXTREME
    # even if the nightly spread is smaller than usual (rest game, injury, etc).
    # Also: a large record differential can elevate a MODERATE to HIGH.
    if team_wins is not None and opp_wins is not None:
        win_diff = abs(team_wins - opp_wins)
        if win_diff >= 35 and level in ("LOW", "MODERATE"):
            level = "HIGH"
            base_label += f" [standings: {win_diff}W diff]"
        elif win_diff >= 45 and level == "HIGH":
            level = "EXTREME"
            base_label += f" [standings: {win_diff}W diff]"
    elif record_diff is not None:
        # Fallback: use win_pct diff if raw wins not available
        if abs(record_diff) >= 0.25 and level in ("LOW", "MODERATE"):
            level = "HIGH"
            base_label += f" [standings: {record_diff:+.0%} WP diff]"

    # ── Player tier ───────────────────────────────────────────────────────────
    if mins is None or mins == 0:
        tier = "unknown"
    elif mins >= 32:
        tier = "star"       # sits in Q4 blowouts when favored
    elif mins >= 25:
        tier = "starter"    # partial Q4 reduction
    elif mins >= 20:
        tier = "role"       # may inherit garbage minutes when favored
    else:
        tier = "fringe"     # unpredictable, small role

    # ── Asymmetric penalty matrix ─────────────────────────────────────────────
    #
    # FAVORED team blowout logic:
    #   Stars get sat → big penalty
    #   Starters get partial rest → moderate penalty
    #   Role players may GAIN garbage minutes → neutral to slight positive
    #   Fringe players also may gain → slight positive (but fragile)
    #
    # UNDERDOG team blowout logic:
    #   Stars stay on longer to chase → neutral (no penalty)
    #   Starters also kept in → slight penalty (some rotation tightening)
    #   Role players: rotation tightens when chasing → moderate penalty
    #   Fringe: coach plays starters only → heavy penalty
    #
    #                   EXTREME   HIGH    MOD    LOW
    penalty_matrix = {
        # Favored side
        ("favored", "star"):    { "EXTREME": 3.0,  "HIGH": 2.0,  "MODERATE": 1.0,  "LOW": 0.0 },
        ("favored", "starter"): { "EXTREME": 1.75, "HIGH": 1.0,  "MODERATE": 0.5,  "LOW": 0.0 },
        ("favored", "role"):    { "EXTREME":-0.5,  "HIGH":-0.25, "MODERATE": 0.0,  "LOW": 0.0 },
        ("favored", "fringe"):  { "EXTREME": 0.25, "HIGH": 0.0,  "MODERATE": 0.0,  "LOW": 0.0 },
        # Underdog side
        ("underdog", "star"):   { "EXTREME": 0.0,  "HIGH": 0.0,  "MODERATE": 0.0,  "LOW": 0.0 },
        ("underdog", "starter"):{ "EXTREME": 0.75, "HIGH": 0.5,  "MODERATE": 0.25, "LOW": 0.0 },
        ("underdog", "role"):   { "EXTREME": 1.5,  "HIGH": 1.0,  "MODERATE": 0.5,  "LOW": 0.0 },
        ("underdog", "fringe"): { "EXTREME": 2.0,  "HIGH": 1.5,  "MODERATE": 0.75, "LOW": 0.0 },
        ("unknown",  "unknown"):{ "EXTREME": 1.5,  "HIGH": 1.0,  "MODERATE": 0.5,  "LOW": 0.0 },
    }
    side_key = "favored" if team_favored else "underdog"
    row = penalty_matrix.get((side_key, tier),
          penalty_matrix.get(("unknown", "unknown"), {}))
    penalty = row.get(level, 0.0)

    # ── Human-readable context string ─────────────────────────────────────────
    if level == "LOW":
        blowout_context = "Competitive game — blowout risk minimal"
    elif team_favored:
        if tier == "star":
            blowout_context = "Star on big favorite — Q4 sit risk is real"
        elif tier == "starter":
            blowout_context = "Starter on big favorite — partial Q4 reduction likely"
        elif tier == "role":
            blowout_context = "Role player on big favorite — may inherit garbage minutes"
        else:
            blowout_context = "Fringe player on big favorite — minutes unpredictable"
    else:  # underdog
        if tier == "star":
            blowout_context = "Star on underdog — stays on court to chase, neutral"
        elif tier == "starter":
            blowout_context = "Starter on underdog — minor rotation tightening risk"
        elif tier == "role":
            blowout_context = "Role player on underdog — coach tightens rotation chasing"
        else:
            blowout_context = "Fringe player on underdog — likely benched in chase mode"

    label = f"{base_label} · {blowout_context}"

    # ── Slow pace adds to suppression regardless of side ─────────────────────
    total_penalty = 0.0
    if game_total is not None and game_total < 215:
        total_penalty = 0.5
        label += f" + slow pace ({game_total} total)"

    # Role players on big FAVORITES get benched in garbage time (Champagnie scenario).
    # Role players on big underdogs get MORE minutes chasing — different risk profile.
    # Stars on big favorites get rested in Q4 — also avoid.
    should_avoid = (
        level in ("HIGH", "EXTREME")
        and not (not team_favored and tier in ("role", "fringe"))  # underdog role players may benefit
    )

    return {
        "level":            level,
        "spread_abs":       round(spread_abs, 1),
        "penalty":          round(penalty + total_penalty, 2),
        "label":            label,
        "game_total":       game_total,
        "should_avoid_role_players": should_avoid,
        "side":             side_key,
        "tier":             tier,
        "team_favored":     team_favored,
        "blowout_context":  blowout_context,
    }


def _compute_score(l5_hr, l10_hr, l20_hr, ev, dist, ctx, stat, opportunity=None,
                   blowout=None, ghost=None, pos_line_hit_rate=None,
                   line_movement=None) -> float:
    """Compute a sortable score for each prop card."""
    score = 0.0

    # ── Stat category: does this stat depend on shooting percentage? ──────────
    # Shooting-dependent stats (PTS, FG3M) swing wildly with shooting luck.
    # Shooting-independent stats (REB, AST, RA, BLK, STL, TOV) are insulated
    # from the single biggest source of variance in basketball.
    # A player who shoots 4-for-18 still gets 9 assists and 12 rebounds.
    # This distinction affects how we weight credibility, implied totals,
    # and blowout risk throughout the score.
    _SHOOTING_DEPENDENT   = {"PTS", "FG3M", "PA"}         # line tied to shooting luck
    _SHOOTING_INDEPENDENT = {"REB", "AST", "RA", "BLK", "STL", "TOV"}  # decoupled
    _MIXED                = {"PRA", "PR"}                  # partially insulated
    stat_upper = stat.upper()
    shooting_dependent   = stat_upper in _SHOOTING_DEPENDENT
    shooting_independent = stat_upper in _SHOOTING_INDEPENDENT
    shooting_mixed       = stat_upper in _MIXED

    # ── 1. Hit rate (recency-weighted) ────────────────────────────────────────
    hrs = [(l5_hr, 0.45), (l10_hr, 0.35), (l20_hr, 0.20)]
    avail = [(hr, w) for hr, w in hrs if hr is not None]
    if avail:
        total_w = sum(w for _, w in avail)
        whr = sum(hr * w for hr, w in avail) / total_w
        score += (whr - 0.5) * 6

    # ── 2. Median position — the most reliable structural signal ─────────────
    median = dist.get("median_l10")
    line   = dist.get("line")
    if median is not None and line is not None:
        gap = median - line
        if gap >= 2.5:    score += 2.0
        elif gap >= 1.5:  score += 1.5
        elif gap >= 0.5:  score += 1.0
        elif gap >= 0.0:  score += 0.5
        elif gap >= -0.5: score -= 0.25
        elif gap >= -1.5: score -= 0.75
        else:             score -= 1.5

    # ── 3. Modal outcome ─────────────────────────────────────────────────────
    modal = dist.get("modal_outcome")
    if modal is not None and line is not None:
        modal_gap = modal - line
        if modal_gap >= 1.5:  score += 1.0
        elif modal_gap >= 0:  score += 0.5
        elif modal_gap >= -1: score -= 0.5
        else:                 score -= 1.0

    # ── 4. True over rate ────────────────────────────────────────────────────
    tor = dist.get("true_over_rate_l10")
    if tor is not None:
        if tor >= 0.70:   score += 1.5
        elif tor >= 0.65: score += 1.0
        elif tor >= 0.60: score += 0.5
        elif tor < 0.40:  score -= 0.5

    # ── 5. Hook penalty/bonus ─────────────────────────────────────────────────
    hook_level = dist.get("hook_level", "")
    if "SEVERE" in hook_level:           score -= 3
    elif "WARNING" in hook_level:        score -= 1.5
    elif "MILD" in hook_level:           score -= 0.5
    elif "PRIME" in hook_level:          score += 2
    elif "UNDER FRIENDLY" in hook_level: score -= 1

    # ── 5b. Shooting independence bonus ──────────────────────────────────────
    # REB/AST/RA lines are structurally more reliable than PTS lines because
    # they decouple from shooting variance — the single biggest unpredictable
    # factor in basketball. A cold-shooting night doesn't suppress rebounds
    # or assists. This makes these lines more predictable over time.
    # Insight: Cade shot 25% tonight but still cleared his RA line easily.
    if shooting_independent:
        score += 0.75   # structural reliability bonus
    elif shooting_mixed:
        score += 0.35   # partial benefit — some insulation from shooting luck
    elif shooting_dependent:
        score -= 0.25   # structural fragility penalty — one bad shooting night kills it

    # ── 6. EV — tiered contribution based on edge size ───────────────────────
    if ev is not None:
        if ev >= 0.15:    score += 1.5   # large positive edge
        elif ev >= 0.10:  score += 1.0
        elif ev >= 0.05:  score += 0.5
        elif ev >= 0.0:   score += 0.0   # neutral
        elif ev >= -0.05: score -= 0.25
        else:             score -= 0.75  # book has the edge

    # ── 7. Line credibility — is this line meaningful given player's variance?
    # Ratio = line / std_dev. If line < 1 std dev, one bad game = zero/very low.
    # This is the core Jenkins problem: 1.5 REB with std=2.5 → ratio=0.60 → trap.
    # Shooting-independent stats (REB, AST, RA) get a softer credibility penalty
    # because their variance is driven by role/minutes, not shooting luck — their
    # std_dev is more stable and predictable than a PTS line std_dev.
    std_l10 = dist.get("std_l10")
    if std_l10 is not None and line is not None and std_l10 > 0:
        credibility = line / std_l10
        penalty_scale = 0.65 if shooting_independent else (0.80 if shooting_mixed else 1.0)
        if credibility < 0.75:
            score -= 2.5 * penalty_scale
        elif credibility < 1.0:
            score -= 1.5 * penalty_scale
        elif credibility < 1.25:
            score -= 0.5 * penalty_scale

    # ── 8. Minimum meaningful lines per stat ─────────────────────────────────
    # Lines this low are inherently unreliable regardless of hit rate.
    # The book is pricing playing time uncertainty, not statistical output.
    _MIN_LINES = {"REB": 2.5, "AST": 2.5, "PTS": 10.5, "FG3M": 1.5,
                  "STL": 0.5, "BLK": 0.5, "TOV": 0.5}
    min_line = _MIN_LINES.get(stat.upper())
    if min_line is not None and line is not None and line < min_line:
        score -= 1.5

    # ── 9. Matchup quality ────────────────────────────────────────────────────
    # Increased from ±0.75 — a tough matchup is a real signal, not a footnote.
    # Uses stat-specific matchup where available, falls back to PTS matchup.
    matchup = ctx.get(f"opp_{stat.lower()}_matchup", ctx.get("opp_pts_matchup", ""))
    if "🟢" in str(matchup):   score += 1.5
    elif "🔴" in str(matchup): score -= 2.0

    # ── 9b. Positional recent lines hit rate ─────────────────────────────────
    # The most direct signal: how did the same-position players actually perform
    # against this opponent recently? This is a stronger, more specific signal
    # than the aggregate matchup grade because it filters by position and uses
    # actual individual game lines rather than season averages.
    # Weight it meaningfully — if 7/8 Gs vs MIA hit O3.5 recently, that matters.
    if pos_line_hit_rate is not None:
        plhr = pos_line_hit_rate / 100.0  # convert to 0-1
        if plhr >= 0.875:   score += 2.0   # 7/8 or 8/8 — very strong positional trend
        elif plhr >= 0.75:  score += 1.25  # 6/8 — solid
        elif plhr >= 0.625: score += 0.5   # 5/8 — mild positive
        elif plhr <= 0.25:  score -= 2.0   # 2/8 or worse — fade signal
        elif plhr <= 0.375: score -= 1.25  # 3/8 — weak

    # ── 10. Implied team total — what the market thinks THIS team will score ──
    # The spread and game total already exist; implied total converts them to
    # a per-team scoring projection. Low implied total suppresses all counting stats.
    itt = (blowout or {}).get("implied_team_total")
    if itt is not None:
        if itt < 105:    score -= 2.5   # team expected to barely score
        elif itt < 109:  score -= 1.5
        elif itt < 113:  score -= 0.5
        elif itt > 120:  score += 1.0
        elif itt > 116:  score += 0.5

    # ── 11. Trend ─────────────────────────────────────────────────────────────
    trend = ctx.get(f"{stat}_trend", "")
    if "📈" in str(trend): score += 0.5
    if "📉" in str(trend): score -= 0.5

    # ── 12. B2B — weighted by workload (minutes), not flat ────────────────────
    mins = ctx.get("minutes_l5_avg") or 0
    if ctx.get("is_back_to_back") == "🔴 YES":
        if mins >= 35:   score -= 2.0
        elif mins >= 28: score -= 1.25
        else:            score -= 0.5
        games_7 = ctx.get("games_last_7_days") or 0
        if games_7 >= 4:
            score -= 0.5
    elif ctx.get("days_rest") and ctx.get("days_rest", 0) >= 2:
        days = ctx.get("days_rest", 0)
        if days >= 5:    score += 0.75  # well rested — volume stats trend up
        elif days >= 3:  score += 0.50  # good rest
        else:            score += 0.25  # normal rest

    # ── 13. Minutes as a validity check ───────────────────────────────────────
    if mins and mins > 0:
        if mins < 15:   score -= 2.0
        elif mins < 20: score -= 1.0
        elif mins < 25: score -= 0.25

    # ── 14. Minutes stability — is this player's role predictable? ────────────
    # High CV means minutes vary wildly game-to-game → line could be a trap.
    mins_cv = ctx.get("minutes_cv")
    if mins_cv is not None:
        if mins_cv > 0.45:   score -= 2.0   # highly volatile — rotation risk
        elif mins_cv > 0.30: score -= 0.75  # unstable role

    # ── 15. Blowout risk ──────────────────────────────────────────────────────
    # The penalty is already asymmetric and player-tier-aware from _compute_blowout_risk.
    # Favored-side role players may have negative penalty (score boost).
    # Underdog-side stars have zero penalty. Just apply directly.
    if blowout:
        bl_penalty = blowout.get("penalty", 0.0)
        if bl_penalty != 0.0:
            score -= bl_penalty

    # ── 16. Ghost game risk ────────────────────────────────────────────────────
    # Players who play real minutes and still produce zero at an anomalous rate.
    # This is separate from variance — a high hit rate built on games where they
    # played has a hidden asterisk if 1-in-4 of those games they disappeared.
    # Ghost rate uses the primary stat specifically (not cross-stat).
    if ghost:
        ghost_rate = ghost.get("ghost_rate")
        floor_rate = ghost.get("floor_rate")
        if ghost_rate is not None:
            if ghost_rate >= 0.30:
                score -= 3.0   # systemic — nearly 1 in 3 real-minute games = zero
            elif ghost_rate >= 0.20:
                score -= 2.0   # significant — 1 in 4-5 games they disappear
            elif ghost_rate >= 0.10:
                score -= 1.0   # elevated — worth flagging
        # Floor rate is a softer signal — catches near-zeros too
        if floor_rate is not None and floor_rate >= 0.35:
            score -= 0.5   # regularly produces tiny fractions of their normal output

    # ── 17. Injury opportunity ────────────────────────────────────────────────
    if opportunity:
        if "MAJOR" in opportunity.get("opp_level", ""):   score += 2.0
        elif "SOLID" in opportunity.get("opp_level", ""): score += 1.0
        elif "MINOR" in opportunity.get("opp_level", ""): score += 0.3

    # ── 18. Line movement — steam and sharp money signals ────────────────────
    # Line movement is a forward-looking signal that reflects where sharp money
    # is going. A steam DOWN on the over means books are adjusting for under
    # action (or sharp under bettors); steam UP means over action.
    # We apply modest adjustments — movement confirms or contradicts our model.
    if line_movement and line_movement.get("available"):
        movement  = line_movement.get("movement", 0) or 0
        sharp     = line_movement.get("sharp_move", False)
        steam     = line_movement.get("steam_move", False)
        direction = line_movement.get("direction", "FLAT")
        if sharp:
            # Sharp money: meaningful signal — agrees with our over = boost, contradicts = penalty
            if direction == "DOWN":   score -= 1.0  # sharp money on under
            elif direction == "UP":   score += 0.75  # sharp money on over
        elif steam:
            if direction == "DOWN":   score -= 0.5
            elif direction == "UP":   score += 0.4

    return round(score, 3)


def _compute_lock(l5_hr, l10_hr, l20_hr, dist, ctx=None, blowout=None,
                  stat: str = "", ghost: dict = None) -> bool:
    """
    LOCK: all structural signals pointing the same direction simultaneously.
    Every gate must pass — one failure disqualifies.

    New gates added (alongside existing ones):
    - Matchup gate: tough D requires stricter hit rate thresholds
    - Line credibility: line must be meaningful relative to player's variance
    - Minimum lines per stat: very low lines are trap lines, never lock
    - Minutes stability: volatile minutes = rotation risk = no lock
    - Implied team total: team expected to score very little = no lock
    """
    # Gate 1: True over rate L10 >= 65%
    tor = dist.get("true_over_rate_l10")
    if tor is None or tor < 0.65:
        return False

    # Gate 2: Every available window >= 60%
    for hr in [l5_hr, l10_hr, l20_hr]:
        if hr is not None and hr < 0.60:
            return False

    # Gate 3: No bad hook
    hook = dist.get("hook_level", "")
    if any(x in hook for x in ["SEVERE", "WARNING", "MILD", "UNDER FRIENDLY"]):
        return False

    # Gate 4: Median genuinely above the line
    median = dist.get("median_l10")
    line   = dist.get("line")
    if median is None or line is None or median < line:
        return False

    # Gate 5: Modal outcome at or above the line
    modal = dist.get("modal_outcome")
    if modal is not None and line is not None and modal < line:
        return False

    # Gate 6: Player must have enough minutes to reliably hit the line
    mins = (ctx or {}).get("minutes_l5_avg") or 0
    if mins > 0 and mins < 20:
        return False

    # Gate 7: Asymmetric blowout risk gate.
    # The old gate blocked all non-stars in any blowout — wrong for underdog stars
    # (they stay on court) and wrong for favored role players (they may gain minutes).
    # Now uses the tier and side already computed in _compute_blowout_risk.
    if blowout and blowout.get("level") in ("HIGH", "EXTREME"):
        side = blowout.get("side", "unknown")
        tier = blowout.get("tier", "unknown")
        # Favored stars: Q4 sit risk is real — no lock
        if side == "favored" and tier == "star":
            return False
        # Favored starters: meaningful Q4 reduction — no lock
        if side == "favored" and tier == "starter":
            return False
        # Underdog fringe: rotation tightens hard when chasing — no lock
        if side == "underdog" and tier == "fringe":
            return False
        # Underdog role players: moderate tightening risk — no lock
        if side == "underdog" and tier == "role":
            return False
        # Favored role players: may gain garbage time — lock allowed if other gates pass
        # Underdog stars: stays on court to chase — lock allowed if other gates pass

    # Gate 8: Matchup — tough defensive matchup requires stricter thresholds.
    # A 🔴 Tough D means the opponent actively suppresses this stat; the
    # player's historical hit rate was built against the full slate of opponents,
    # not this specific defense. Raise the bar.
    matchup = (ctx or {}).get(f"opp_{stat.lower()}_matchup",
                               (ctx or {}).get("opp_pts_matchup", ""))
    if "🔴" in str(matchup):
        # Tough matchup: all available windows must be >= 70% (not just 60%)
        for hr in [l5_hr, l10_hr, l20_hr]:
            if hr is not None and hr < 0.70:
                return False
        # True over rate must also be >= 70%
        if tor is not None and tor < 0.70:
            return False

    # Gate 9: Line credibility — the line must be meaningful relative to variance.
    # If line < 1.0× std_dev, one bad game hits zero. That's not a lock, it's a trap.
    # Jenkins 1.5 REB with std=2.5 → ratio=0.60 → fails here.
    std_l10 = dist.get("std_l10")
    if std_l10 is not None and line is not None and std_l10 > 0:
        credibility = line / std_l10
        if credibility < 1.0:
            return False

    # Gate 10: Minimum meaningful lines per stat.
    # Lines this low are pricing playing time uncertainty, not statistical output.
    # Even 80% hit rates on REB 1.5 don't mean much — one DNP-adjacent night = loss.
    _MIN_LOCK_LINES = {
        "PTS":  10.5,
        "REB":  2.5,
        "AST":  2.5,
        "FG3M": 1.5,
        "STL":  0.5,
        "BLK":  0.5,
        "TOV":  0.5,
    }
    min_line = _MIN_LOCK_LINES.get(stat.upper())
    if min_line is not None and line is not None and line < min_line:
        return False

    # Gate 11: Minutes stability — if a player's minutes are highly volatile,
    # their prop lines are fragile regardless of hit rate. The hit rate was built
    # on nights he played. Volatile CV means there are DNP-adjacent nights hiding.
    mins_cv = (ctx or {}).get("minutes_cv")
    if mins_cv is not None and mins_cv > 0.45:
        return False

    # Gate 12: Implied team total — if the market projects this team to score
    # very little tonight, counting stats are suppressed across the board.
    # This is the market's forward-looking matchup signal; trust it.
    itt = (blowout or {}).get("implied_team_total")
    if itt is not None and itt < 107:
        return False

    # Gate 13: Ghost game risk — a player who regularly plays real minutes
    # and produces zero cannot be a Lock regardless of historical hit rate.
    # Their hit rate has a hidden asterisk: it was built on nights they showed up.
    # A ghost rate >= 20% means 1 in 4-5 real-minute games they disappear entirely.
    if ghost:
        ghost_rate = ghost.get("ghost_rate")
        if ghost_rate is not None and ghost_rate >= 0.20:
            return False

    return "over"


def _compute_lock_under(l5_hr, l10_hr, l20_hr, dist, ctx=None, blowout=None,
                        stat: str = "") -> bool:
    """
    LOCK UNDER: all structural signals point toward the under.

    Two pathways:
    A) Statistical: low hit rates + median well below line + modal below line
    B) Situational: star-load player on combo prop in a blowout game
       (Wemby PR scenario — large spread kills 4th quarter accumulation)
    """
    mins = (ctx or {}).get("minutes_l5_avg") or 0

    # ── Pathway B: Blowout situational under ─────────────────────────────────
    # Star player + combo stat + high blowout risk = skip or under
    COMBO_STATS = {"PR", "PRA", "PA", "RA"}
    if (blowout and blowout.get("level") in ("HIGH", "EXTREME")
            and stat.upper() in COMBO_STATS
            and mins >= 30):
        # Blowout kills 4th quarter accumulation for combo stats
        # Even if the player is good, the game script suppresses the ceiling
        return True

    # ── Pathway A: Statistical under lock ────────────────────────────────────
    # Gate 1: True over rate must be genuinely low
    tor = dist.get("true_over_rate_l10")
    if tor is None or tor > 0.38:
        return False

    # Gate 2: All available windows must be low-hit-rate for the over
    for hr in [l5_hr, l10_hr, l20_hr]:
        if hr is not None and hr > 0.42:
            return False

    # Gate 3: Median must be genuinely below the line
    median = dist.get("median_l10")
    line   = dist.get("line")
    if median is None or line is None or median >= line:
        return False

    # Gate 4: Modal below line (most common outcome is an under)
    modal = dist.get("modal_outcome")
    if modal is not None and line is not None and modal >= line:
        return False

    # Gate 5: Enough minutes to be meaningful
    if mins > 0 and mins < 15:
        return False  # line too low to trust either direction

    # Gate 6: Minutes stability — volatile players can't be lock unders either
    mins_cv = (ctx or {}).get("minutes_cv")
    if mins_cv is not None and mins_cv > 0.45:
        return False

    return True


def _compute_units(score: float, hammer: bool, lock, lock_under: bool = False) -> float:
    """Recommend unit size — symmetric for overs and unders."""
    if lock == "over": return 3.0
    if lock_under:     return 3.0
    if hammer:         return 2.0
    if score >= 4:     return 2.0
    if score >= 2:     return 1.0
    if score >= 0.8:   return 0.5
    if score <= -4:    return 2.0   # strong under = same as strong over
    if score <= -2:    return 1.0
    if score <= -0.8:  return 0.5
    return 0.0


def _compute_hammer(l5_hr, l10_hr, l20_hr, ev, dist, ctx=None) -> dict:
    """
    Determine if a prop qualifies as a HAMMER pick and detect regression risk.

    HAMMER criteria (all must pass):
    1. 70%+ hit rate on L5, L10, AND L20
    2. No hook warning including UNDER FRIENDLY
    3. Positive EV
    4. No regression risk
    5. Median above line
    6. Minimum minutes threshold — low-minute players can't be HAMMERs
    """
    result = {
        "hammer":           False,
        "hammer_reasons":   [],
        "regression_risk":  False,   # True only for hard-DQ (gap >=45)
        "regression_soft":  False,   # True for moderate gaps (30-44%) — score penalty only
        "regression_gap":   0,       # raw L5-L20 gap in percentage points
        "regression_note":  "",
        "running_hot":      False,
    }

    hrs = {"l5": l5_hr, "l10": l10_hr, "l20": l20_hr}
    # Convert decimals (0-1) to percentages (0-100) if needed
    available = {k: (v * 100 if v is not None and v <= 1.0 else v) for k, v in hrs.items() if v is not None}

    if not available:
        return result

    # Regression risk: L5 >> L20 means running hot
    # Normalize to percentages first
    l5_pct  = (l5_hr  * 100 if l5_hr  is not None and l5_hr  <= 1.0 else l5_hr)
    l20_pct = (l20_hr * 100 if l20_hr is not None and l20_hr <= 1.0 else l20_hr)
    if l5_pct is not None and l20_pct is not None:
        hot_gap = l5_pct - l20_pct
        result["regression_gap"] = round(hot_gap, 0)
        if hot_gap >= 45:
            # Hard DQ — severe mean-reversion risk; L5 is >2x L20
            result["regression_risk"] = True
            result["running_hot"]     = True
            result["regression_note"] = (
                f"Running hot: {l5_pct:.0f}% L5 vs {l20_pct:.0f}% L20 "
                f"(+{hot_gap:.0f}% gap) — hard regression DQ"
            )
        elif hot_gap >= 30:
            # Soft DQ — notable gap; penalise score but keep in parlay pool
            result["regression_soft"] = True
            result["running_hot"]     = True
            result["regression_note"] = (
                f"Running hot: {l5_pct:.0f}% L5 vs {l20_pct:.0f}% L20 "
                f"(+{hot_gap:.0f}% gap) — regression risk"
            )
        elif hot_gap >= 20:
            result["running_hot"]     = True
            result["regression_note"] = (
                f"Slightly hot: {l5_pct:.0f}% L5 vs {l20_pct:.0f}% L20"
            )

    # HAMMER checks
    fails = []

    # 1. Hit rate 70%+ across all available windows
    for window, hr in available.items():
        if hr < 70:
            fails.append(f"{window.upper()} hit rate {hr:.0f}% < 70%")

    # 2. No bad hook — UNDER FRIENDLY also disqualifies (HAMMER = over signal)
    hook = dist.get("hook_level", "")
    if "SEVERE" in hook or "WARNING" in hook:
        fails.append(f"Hook risk: {hook}")
    elif "UNDER FRIENDLY" in hook:
        fails.append("Under-friendly setup contradicts over signal")

    # 3. Median must be above the line
    median = dist.get("median_l10")
    line   = dist.get("line")
    if median is not None and line is not None and median < line:
        fails.append(f"Median {median} below line {line} — structural under signal")

    # 4. Positive EV (only check if we have odds data)
    if ev is not None and ev < 0:
        fails.append(f"Negative EV ({ev:.1f}%)")

    # 5. No regression risk
    if result["regression_risk"]:
        fails.append(result["regression_note"])

    # 6. Minutes gate — low-minute players can't be HAMMERs (line is a trap)
    mins = (ctx or {}).get("minutes_l5_avg") or 0
    if mins > 0 and mins < 18:
        fails.append(f"Low minutes ({mins:.0f} mpg) — line reliability too low")

    # 7. Minutes stability — volatile rotation = line is a trap
    mins_cv = (ctx or {}).get("minutes_cv")
    if mins_cv is not None and mins_cv > 0.45:
        fails.append(f"Volatile minutes (CV={mins_cv:.2f}) — rotation risk")

    # 8. Line credibility — line must be meaningful relative to player's variance
    std_l10 = dist.get("std_l10")
    line_val = dist.get("line")
    if std_l10 is not None and line_val is not None and std_l10 > 0:
        credibility = line_val / std_l10
        if credibility < 1.0:
            fails.append(f"Low line credibility (line={line_val} < 1 std dev={std_l10:.1f})")

    if not fails:
        result["hammer"] = True
        result["hammer_reasons"] = [
            f"✓ {v:.0f}% {k.upper()}" for k, v in available.items()
        ] + (
            [f"✓ EV +{ev:.1f}%"] if ev is not None else ["✓ Clean line"]
        )
    else:
        result["hammer_reasons"] = fails

    return result


# ── API routes ────────────────────────────────────────────────────────────────

_pipeline_data: dict = {}


@app.route("/")
def index():
    return send_from_directory("templates", "index.html")


@app.route("/api/slate")
def api_slate():
    game_date = request.args.get("date", date.today().isoformat())

    # Return cached data if available and same date
    if _pipeline_data.get("date") == game_date and _pipeline_data.get("props"):
        return jsonify({"status": "ok", "cached": True, **_pipeline_data})

    return jsonify({"status": "empty", "date": game_date, "games": [], "props": []})


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    if not _pipeline_lock.acquire(blocking=False):
        return jsonify({"status": "running", "message": "Pipeline already running"})

    game_date = request.json.get("date", date.today().isoformat()) if request.json else date.today().isoformat()

    def run():
        global _pipeline_data
        try:
            set_pipeline_status("running", "Fetching data...")
            data = run_pipeline(game_date)
            _pipeline_data = data
            # Persist to disk
            with open(CACHE_PATH, "w") as f:
                json.dump(data, f, default=str)
            set_pipeline_status("done", f"Loaded {len(data.get('props',[]))} props", finished=date.today().isoformat())
            # Auto-generate AI export alongside the main cache
            try:
                from export_for_ai import export as _ai_export
                _ai_export(
                    input_path  = str(CACHE_PATH),
                    output_path = "pipeline_cache_ai.json",
                    clean_only  = True,
                )
                logger.info("AI export written to pipeline_cache_ai.json")
            except Exception as e:
                logger.warning(f"AI export failed (non-fatal): {e}")
        except Exception as e:
            logger.error(f"Pipeline error: {e}", exc_info=True)
            set_pipeline_status("error", str(e))
        finally:
            _pipeline_lock.release()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return jsonify({"status": "started"})


@app.route("/api/status")
def api_status():
    return jsonify(get_pipeline_status())


@app.route("/api/flag", methods=["POST"])
def api_flag():
    data      = request.json or {}
    key       = data.get("key", "")
    flagged   = data.get("flagged", False)
    flag_type = data.get("flag_type", "watch")
    note      = data.get("note", "")
    if not key:
        return jsonify({"error": "key required"}), 400
    set_flag(key, flagged, flag_type, note)
    return jsonify({"status": "ok", "key": key, "flagged": flagged})


@app.route("/api/note", methods=["POST"])
def api_note():
    data = request.json or {}
    key  = data.get("key", "")
    note = data.get("note", "")
    if not key:
        return jsonify({"error": "key required"}), 400
    existing = get_flag(key)
    set_flag(key, existing["flagged"], existing["flag_type"] or "watch", note)
    # Update in-memory cache
    if _pipeline_data.get("props"):
        for card in _pipeline_data["props"]:
            if card["key"] == key:
                card["note"] = note
                break
    return jsonify({"status": "ok"})


@app.route("/api/grade", methods=["POST"])
def api_grade():
    """Grade yesterday's props against actual results."""
    from grading import grade_props, init_results_db, run_loss_audit
    init_results_db()
    yesterday = request.json.get("date") if request.json else None
    try:
        graded = grade_props(Path("pipeline_cache.json"), DB_PATH, yesterday)
        # Auto-run loss audit after grading — categorizes every loss automatically
        try:
            audit = run_loss_audit(days_back=7)
            audit_summary = {
                "total_audited": len(audit),
                "gate_miss":     sum(1 for r in audit if r["fail_category"] == "GATE_MISS"),
                "data_miss":     sum(1 for r in audit if r["fail_category"] == "DATA_MISS"),
                "scoring_error": sum(1 for r in audit if r["fail_category"] == "SCORING_ERROR"),
                "variance":      sum(1 for r in audit if r["fail_category"] == "VARIANCE"),
            }
        except Exception as ae:
            logger.warning(f"Loss audit failed (non-fatal): {ae}")
            audit_summary = {}
        return jsonify({"status": "ok", "graded": len(graded), "results": graded,
                        "loss_audit": audit_summary})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/loss_audit")
def api_loss_audit():
    """Get loss audit breakdown by failure category."""
    from grading import get_loss_audit_summary, run_loss_audit
    days_back = int(request.args.get("days", 30))
    refresh   = request.args.get("refresh", "false").lower() == "true"
    if refresh:
        run_loss_audit(days_back=days_back)
    summary = get_loss_audit_summary(days_back=days_back)
    return jsonify(summary)


@app.route("/api/results")
def api_results():
    """Get graded results for a date."""
    from grading import get_results_for_date, get_track_record
    game_date = request.args.get("date", (date.today() - __import__('datetime').timedelta(days=1)).isoformat())
    results   = get_results_for_date(game_date)
    track     = get_track_record()
    return jsonify({"date": game_date, "results": results, "track_record": track})


@app.route("/api/track_record")
def api_track_record():
    from grading import get_track_record
    return jsonify(get_track_record())


@app.route("/api/games")
def api_games():
    game_date = request.args.get("date", date.today().isoformat())
    import nba_data
    games = nba_data.get_todays_games(game_date)
    return jsonify({"games": games})


@app.route("/api/chat", methods=["POST"])
def api_chat():
    """AI chat endpoint — passes props data + user message to Claude Haiku."""
    import os
    import urllib.request as urlreq

    body         = request.get_json(force=True)
    user_message = body.get("message", "").strip()
    history      = body.get("history", [])

    if not user_message:
        return jsonify({"error": "No message provided"}), 400

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return jsonify({"error": "ANTHROPIC_API_KEY not set. Run: export ANTHROPIC_API_KEY=your-key"}), 500

    props      = _pipeline_data.get("props", [])      if _pipeline_data else []
    slate_date = _pipeline_data.get("date", "unknown") if _pipeline_data else "unknown"

    def summarize(p):
        opp  = p.get("opportunity") or {}
        opp_str = f" OPP:{opp.get('opp_level','')}" if opp else ""
        return (
            f"{p['player_name']} ({p['team']} vs {p['opponent']}) "
            f"{p['stat']} O{p['line']} | "
            f"L5:{p.get('l5_hr','?')}% L10:{p.get('l10_hr','?')}% L20:{p.get('l20_hr','?')}% | "
            f"Med:{p.get('median','?')} Hook:{p.get('hook_level','')} | "
            f"Call:{p.get('final_call') or p.get('hook_level','')} | "
            f"Score:{p.get('score',0)}{opp_str}"
        )

    top_props  = sorted(props, key=lambda x: x.get("score", 0), reverse=True)
    props_text = "\n".join(summarize(p) for p in top_props)

    system_prompt = f"""You are an expert NBA props analyst assistant built into a research tool called PropsDesk.
Tonight's date: {slate_date}
You have all {len(top_props)} props from tonight's slate sorted by model score:

{props_text}

Format: Player (Team vs Opponent) Stat Line | L5/L10/L20 hit rates | Median Hook | Final Call | Score

RULES when building parlays:
- High hit rates across ALL windows (L5, L10, L20)
- No SEVERE or WARNING hooks
- Legs from different games to avoid correlated risk
- For unders: RED/low median vs line, low hit rates on the over
- For overs: high hit rates, PRIME OVER or UNDER FRIENDLY hook

Be concise and direct. Only reference players shown above — never invent data."""

    messages = []
    for h in history[-6:]:
        if h.get("role") in ("user", "assistant"):
            messages.append({"role": h["role"], "content": h["content"]})
    messages.append({"role": "user", "content": user_message})

    payload = json.dumps({
        "model":      "claude-haiku-4-5-20251001",
        "max_tokens": 1024,
        "system":     system_prompt,
        "messages":   messages,
    }).encode()

    req = urlreq.Request(
        "https://api.anthropic.com/v1/messages",
        data=payload,
        headers={
            "Content-Type":      "application/json",
            "x-api-key":         api_key,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )
    try:
        with urlreq.urlopen(req, timeout=30) as resp:
            data  = json.loads(resp.read())
            reply = data["content"][0]["text"]
            return jsonify({"reply": reply})
    except Exception as e:
        logger.error(f"Chat API error: {e}")
        return jsonify({"error": str(e)}), 500


# ── Startup ───────────────────────────────────────────────────────────────────

def load_cached_pipeline():
    """Load last pipeline result from disk on startup."""
    global _pipeline_data
    if CACHE_PATH.exists():
        try:
            with open(CACHE_PATH) as f:
                _pipeline_data = json.load(f)
            cached_date = _pipeline_data.get("date","")
            n_props     = len(_pipeline_data.get("props",[]))
            logger.info(f"Loaded cached pipeline data: {n_props} props from {cached_date}")
        except Exception as e:
            logger.warning(f"Could not load cache: {e}")



@app.route("/api/debug/player_log")
def api_debug_player_log():
    """
    Debug endpoint — shows what game dates are in the cache for a player.
    Usage: /api/debug/player_log?player=LaMelo+Ball&team=CHA

    Returns the actual game dates stored in the in-memory cache so you can
    verify whether a recent game is missing (data freshness issue) or whether
    the rest days calculation itself is wrong.
    """
    import nba_data
    from datetime import date as _date

    player = request.args.get("player", "").strip()
    team   = request.args.get("team", "").strip().upper()

    if not player or not team:
        return jsonify({"error": "Provide ?player=Name&team=ABBR"}), 400

    log = nba_data.get_player_game_log(player, team)

    if log.empty:
        return jsonify({
            "player": player, "team": team,
            "error": "No data found — check spelling or team abbreviation",
        })

    # Pull game dates and sort newest-first
    dates = sorted(
        log["GAME_DATE"].dt.date.unique().tolist(),
        reverse=True
    )

    today      = _date.today()
    most_recent = dates[0] if dates else None

    # Compute rest days the same way compute_rest_context does
    if most_recent:
        days_rest = (today - most_recent).days - 1
        is_b2b    = days_rest == 0
    else:
        days_rest = None
        is_b2b    = None

    # Flag if the most recent game looks suspiciously old
    stale_flag = None
    if most_recent and (today - most_recent).days > 5:
        stale_flag = (
            f"WARNING: most recent game is {(today - most_recent).days} days ago. "
            "BDL may not have posted recent game stats yet — pipeline will self-correct '"
            "within 2h as the SQLite game list cache expires."
        )

    return jsonify({
        "player":       player,
        "team":         team,
        "today":        today.isoformat(),
        "game_dates":   [d.isoformat() for d in dates[:15]],  # last 15 games
        "most_recent":  most_recent.isoformat() if most_recent else None,
        "days_rest":    days_rest,
        "is_b2b":       is_b2b,
        "total_games":  len(dates),
        "stale_warning": stale_flag,
    })


@app.route("/api/parlay_suggest")
def api_parlay_suggest():
    """
    Auto-build tonight's best parlay options from the prop pool.

    Query params:
      date        – game date (default: today)
      max_legs    – max legs per parlay (default: 5)
      min_tier    – minimum tier: PRIME | STRONG | SOLID (default: SOLID)

    Returns:
    {
      mode:     "A-tier" | "B-tier",
      pool:     [prop cards eligible for tonight's parlay],
      parlays:  { "3": {...}, "4": {...}, "5": {...} }
        each parlay:
          legs, leg_cards, joint_l10, label, avg_odds, mode
    }
    """
    game_date = request.args.get("date", date.today().isoformat())
    max_legs  = int(request.args.get("max_legs", 5))

    if _pipeline_data.get("date") != game_date or not _pipeline_data.get("props"):
        return jsonify({"error": "No pipeline data for this date. Run /api/refresh first."}), 404

    props = _pipeline_data.get("props", [])
    result = build_parlay_suggestions(props, max_legs=max_legs)

    # Slim down leg_cards to avoid bloated JSON — keep only the fields the UI needs
    _CARD_FIELDS = [
        "key", "player_name", "team", "opponent", "stat", "line", "odds",
        "l5_hr", "l10_hr", "l20_hr", "edge_score", "no_brainer_tier",
        "lock", "hammer", "regression_soft", "regression_gap",
        "dist_profile", "dist_cv", "regression_note", "hook_level",
        "stat_components", "parlay_disqualify_reason",
        "outlier_inflated", "outlier_note", "outlier_game_val", "true_l5_hr",
    ]
    for parlay in result.get("parlays", {}).values():
        parlay["leg_cards"] = [
            {k: leg.get(k) for k in _CARD_FIELDS}
            for leg in parlay.get("leg_cards", [])
        ]

    pool_slim = [
        {k: c.get(k) for k in _CARD_FIELDS}
        for c in result.get("pool", [])
    ]
    result["pool"] = pool_slim

    return jsonify(result)


@app.route("/api/parlay_analyze", methods=["POST"])
def api_parlay_analyze():
    """
    Analyze a set of selected parlay legs using the game script + covariance engine.

    POST body: {"legs": [<prop card dicts>]}
    Each card dict must include game_script_profile (attached at pipeline time).

    Returns:
    {
        "joint_hit_prob":      float,
        "independent_prob":    float,
        "correlation_impact":  float,
        "leg_hit_probs":       [float],
        "warnings":            [{type, msg}],
        "shooting_indep_pct":  int,
        "parlay_grade":        str,
        "avg_correlation":     float,
        "corr_pairs":          [{name_a, name_b, corr, source}],
    }
    """
    from game_script import compute_parlay_profile

    try:
        body = request.get_json(force=True)
        legs = body.get("legs", [])
    except Exception:
        return jsonify({"error": "Invalid JSON body"}), 400

    if len(legs) < 2:
        return jsonify({"error": "Need at least 2 legs for parlay analysis"}), 400

    if len(legs) > 12:
        return jsonify({"error": "Maximum 12 legs supported"}), 400

    try:
        result = compute_parlay_profile(legs, _player_logs_cache)
        # Add human-readable correlation pairs for display
        corr_matrix = result.get("corr_matrix", [])
        pairs = []
        for i in range(len(legs)):
            for j in range(i+1, len(legs)):
                if corr_matrix and i < len(corr_matrix) and j < len(corr_matrix[i]):
                    corr_val = corr_matrix[i][j]
                    pairs.append({
                        "name_a": legs[i].get("player_name",""),
                        "stat_a": legs[i].get("stat",""),
                        "name_b": legs[j].get("player_name",""),
                        "stat_b": legs[j].get("stat",""),
                        "corr":   round(corr_val, 3),
                        "same_team": legs[i].get("team") == legs[j].get("team"),
                    })
        result["corr_pairs"] = pairs
        result.pop("corr_matrix", None)

        # ── Stat-overlap warnings ─────────────────────────────────────────────
        # Flag pairs whose stat components share primitives — these legs move
        # together by construction (e.g. PRA and AST for the same player both
        # count the same assists, so a great AST game wins both at once but a
        # bad AST game tanks both — it's correlated risk, not diversification).
        overlap_warnings = []
        for i in range(len(legs)):
            for j in range(i+1, len(legs)):
                shared = _stat_overlap(legs[i], legs[j])
                if not shared:
                    continue
                same_player = (legs[i].get("player_name","") == legs[j].get("player_name",""))
                severity = "high" if same_player else "medium"
                who = (f"{legs[i].get('player_name','')} {legs[i].get('stat','')} "
                       f"+ {legs[j].get('player_name','')} {legs[j].get('stat','')}")
                msg = (
                    f"⚠️ OVERLAP: {who} share "
                    f"{'/'.join(sorted(shared))} — "
                    + ("same player combined stats are correlated." if same_player
                       else "same-game teammate stats are correlated.")
                )
                overlap_warnings.append({
                    "type":       "stat_overlap",
                    "severity":   severity,
                    "shared":     sorted(shared),
                    "same_player": same_player,
                    "leg_i":      i,
                    "leg_j":      j,
                    "msg":        msg,
                })
        if overlap_warnings:
            existing = result.get("warnings", [])
            result["warnings"] = overlap_warnings + existing
            result["has_overlap"] = True
        else:
            result["has_overlap"] = False

        # Add Kelly parlay sizing with correlation adjustment
        try:
            from kelly import size_parlay, adjusted_parlay_probability
            # Build minimal leg dicts for kelly module
            kelly_legs = []
            for leg in legs:
                hr = leg.get("l10_hr")
                if hr is not None:
                    hr = hr / 100.0 if hr > 1.0 else hr
                else:
                    hr = result.get("joint_hit_prob", 0.5)
                kelly_legs.append({
                    "player_name":  leg.get("player_name", ""),
                    "stat":         leg.get("stat", ""),
                    "team":         leg.get("team", ""),
                    "game_id":      str(leg.get("game_id", "")),
                    "hit_rate":     hr,
                    "american_odds": leg.get("odds"),
                    "line":         leg.get("line"),
                })
            corr_analysis = adjusted_parlay_probability(kelly_legs)
            result["kelly"] = {
                "adjusted_prob":  corr_analysis.get("adjusted_prob"),
                "standard_prob":  corr_analysis.get("standard_prob"),
                "corr_warnings":  corr_analysis.get("warnings", []),
                "legs_analysis":  corr_analysis.get("legs_analysis", []),
                "recommendation": corr_analysis.get("recommendation", ""),
                "ev_adjusted":    corr_analysis.get("ev_adjusted"),
            }
        except Exception as ke:
            logger.debug(f"Kelly parlay sizing skipped: {ke}")

        return jsonify(result)
    except Exception as e:
        logger.error(f"Parlay analysis error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500



@app.route("/api/history")
def api_history():
    """
    Return historical hit rates broken down by model signal tier.
    Used by the RESULTS panel to show calibration data over time.
    """
    days = int(request.args.get("days", 30))
    from datetime import date, timedelta
    cutoff = (date.today() - timedelta(days=days)).isoformat()

    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("""
            SELECT
                s.game_date, s.player_name, s.team, s.stat, s.line,
                s.score, s.is_lock, s.is_hammer, s.is_lock_under,
                s.median_gap, s.true_over_pct, s.parlay_ready, s.parlay_consider,
                s.weighted_hit, s.shot_profile,
                o.actual_value, o.hit
            FROM prop_snapshots s
            LEFT JOIN prop_outcomes o ON s.snapshot_key = o.snapshot_key
            WHERE s.game_date >= ?
            ORDER BY s.game_date DESC, s.score DESC
        """, (cutoff,)).fetchall()

    cols = ['game_date','player_name','team','stat','line','score',
            'is_lock','is_hammer','is_lock_under','median_gap','true_over_pct',
            'parlay_ready','parlay_consider','weighted_hit','shot_profile',
            'actual_value','hit']
    records = [dict(zip(cols, r)) for r in rows]

    # Compute tier hit rates
    def hit_rate(subset):
        settled = [r for r in subset if r['hit'] is not None]
        if not settled: return None
        return round(sum(r['hit'] for r in settled) / len(settled) * 100, 1)

    def tier_stats(subset):
        settled = [r for r in subset if r['hit'] is not None]
        return {
            "total":    len(subset),
            "settled":  len(settled),
            "hit_rate": hit_rate(subset),
        }

    locks          = [r for r in records if r['is_lock']]
    hammers        = [r for r in records if r['is_hammer'] and not r['is_lock']]
    parlay_ready   = [r for r in records if r['parlay_ready']]
    parlay_consider= [r for r in records if r['parlay_consider'] and not r['parlay_ready']]
    shot_indep     = [r for r in records if r['shot_profile'] == 'independent']
    shot_dep       = [r for r in records if r['shot_profile'] == 'dependent']

    return jsonify({
        "days":       days,
        "total_props": len(records),
        "tiers": {
            "lock":            tier_stats(locks),
            "hammer":          tier_stats(hammers),
            "parlay_ready":    tier_stats(parlay_ready),
            "parlay_consider": tier_stats(parlay_consider),
            "shot_independent":tier_stats(shot_indep),
            "shot_dependent":  tier_stats(shot_dep),
            "all":             tier_stats(records),
        },
        "records": records[:200],  # most recent 200
    })


@app.route("/api/outcomes/fetch", methods=["POST"])
def api_fetch_outcomes():
    """Manually trigger outcome fetch for a specific date."""
    body = request.get_json(force=True) or {}
    target_date = body.get("date")
    if not target_date:
        from datetime import date, timedelta
        target_date = (date.today() - timedelta(days=1)).isoformat()
    result = fetch_and_store_outcomes(target_date)
    return jsonify(result)


@app.route("/api/line_movement")
def api_line_movement():
    """Return all line movement data for tonight."""
    game_date = request.args.get("date", date.today().isoformat())
    try:
        from line_movement import get_all_movements, get_steam_moves
        movements = get_all_movements(game_date)
        steam     = get_steam_moves(game_date)
        return jsonify({
            "date":      game_date,
            "movements": movements,
            "steam":     steam,
            "n_steam":   len(steam),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/steam")
def api_steam():
    """Return only steam/sharp moves for tonight — used by alert panel."""
    game_date = request.args.get("date", date.today().isoformat())
    try:
        from line_movement import get_steam_moves
        steam = get_steam_moves(game_date)
        return jsonify({"date": game_date, "steam_moves": steam})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/kelly", methods=["POST"])
def api_kelly():
    """
    Compute Kelly bet sizing for one or more props.
    POST body: {"legs": [{"hit_rate": 0.7, "american_odds": -110, ...}]}
    For a single leg, returns single bet recommendation.
    For multiple legs, returns parlay sizing with correlation adjustment.
    """
    body = request.get_json(force=True) or {}
    legs = body.get("legs", [])
    bankroll = float(body.get("bankroll", 1000))

    if not legs:
        return jsonify({"error": "legs required"}), 400

    try:
        from kelly import recommended_bet, size_parlay

        if len(legs) == 1:
            leg = legs[0]
            result = recommended_bet(
                hit_rate      = float(leg.get("hit_rate", 0.5)),
                american_odds = float(leg.get("american_odds", -110)),
                bankroll      = bankroll,
            )
        else:
            # Enrich legs with game_id from current pipeline data if available
            if _pipeline_data.get("props"):
                prop_map = {
                    f"{p['player_name']}|{p['stat']}|{p['line']}": p
                    for p in _pipeline_data["props"]
                }
                for leg in legs:
                    k = f"{leg.get('player_name','')}|{leg.get('stat','')}|{leg.get('line','')}"
                    if k in prop_map:
                        leg.setdefault("game_id",   prop_map[k].get("game_id"))
                        leg.setdefault("team",      prop_map[k].get("team"))
                        leg.setdefault("hit_rate",  (prop_map[k].get("l10_hr") or 50) / 100)
                        leg.setdefault("american_odds", prop_map[k].get("odds"))
            result = size_parlay(legs, bankroll=bankroll)

        return jsonify(result)
    except Exception as e:
        logger.error(f"Kelly error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/regrade", methods=["POST"])
def api_regrade():
    """
    Re-attempt grading for any NO DATA results within the last N days.
    Fixes the BDL stat posting lag problem — run this the next morning
    after pipeline to catch any results that weren't final yet.
    POST body: {"days_back": 3}
    """
    body      = request.get_json(force=True) or {}
    days_back = int(body.get("days_back", 3))
    try:
        from grading import regrade_stale_results
        summary = regrade_stale_results(days_back=days_back)
        total_fixed = sum(v.get("regraded", 0) for v in summary.values())
        return jsonify({
            "status":      "ok",
            "total_fixed": total_fixed,
            "by_date":     summary,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/gtd")
def api_gtd():
    """Return game-time decision players for tonight."""
    if _pipeline_data:
        return jsonify({
            "date":        _pipeline_data.get("date"),
            "gtd_tonight": _pipeline_data.get("gtd_tonight", []),
            "out_tonight": _pipeline_data.get("injuries", []),
        })
    return jsonify({"gtd_tonight": [], "out_tonight": []})


@app.route("/backtest")
def backtest():
    return send_from_directory("templates", "backtest.html")


@app.route("/live")
def live_dashboard():
    return send_from_directory("templates", "live.html")


@app.route("/api/live")
def api_live():
    """
    Returns live box score data merged with today's prop lines.
    Polls BDL /box_scores/live every 3 minutes (cached).
    Used by the live dashboard to track in-progress props.
    """
    try:
        from bdl_client import get_client as _bdl_get
        _bdl = _bdl_get()

        live_scores = _bdl.get_live_box_scores()
        props = _pipeline_data.get("props", [])

        # Build player -> prop lines lookup from today's pipeline
        prop_lines: dict = {}
        for p in props:
            name = p.get("player_name", "")
            stat = (p.get("stat") or "").upper()
            line = p.get("line")
            if name and stat and line is not None:
                prop_lines.setdefault(name, {})[stat] = {
                    "line":       line,
                    "odds":       p.get("odds"),
                    "edge_score": p.get("edge_score"),
                    "no_brainer_tier": p.get("no_brainer_tier"),
                    "direction":  p.get("direction"),
                }

        # Stat name mapping from BDL box score fields to our stat keys
        _stat_map = {
            "PTS": "pts", "REB": "reb", "AST": "ast",
            "STL": "stl", "BLK": "blk", "FG3M": "fg3m",
        }
        _combo_map = {
            "RA":  lambda p: (p.get("reb") or 0) + (p.get("ast") or 0),
            "PR":  lambda p: (p.get("pts") or 0) + (p.get("reb") or 0),
            "PA":  lambda p: (p.get("pts") or 0) + (p.get("ast") or 0),
            "PRA": lambda p: (p.get("pts") or 0) + (p.get("reb") or 0) + (p.get("ast") or 0),
        }

        games_live = []
        for game in live_scores:
            game_entry = {
                "status":            game.get("status"),
                "period":            game.get("period"),
                "time":              game.get("time"),
                "home_team_score":   game.get("home_team_score"),
                "visitor_team_score": game.get("visitor_team_score"),
                "home_team":         game.get("home_team", {}).get("abbreviation"),
                "visitor_team":      game.get("visitor_team", {}).get("abbreviation"),
                "players":           [],
            }

            for side in ["home_team", "visitor_team"]:
                team_data = game.get(side, {})
                for player in team_data.get("players", []):
                    pname = f"{player.get('player',{}).get('first_name','')} {player.get('player',{}).get('last_name','')}".strip()
                    if not pname:
                        continue

                    player_props = prop_lines.get(pname, {})
                    tracked = []

                    for stat_key, raw_field in _stat_map.items():
                        if stat_key in player_props:
                            current = player.get(raw_field) or 0
                            line    = player_props[stat_key]["line"]
                            tracked.append({
                                "stat":      stat_key,
                                "line":      line,
                                "current":   current,
                                "pct":       round(current / line * 100) if line else 0,
                                "hit":       current > line,
                                "remaining": round(line - current, 1),
                                "edge_score": player_props[stat_key].get("edge_score"),
                                "tier":      player_props[stat_key].get("no_brainer_tier"),
                            })

                    for stat_key, fn in _combo_map.items():
                        if stat_key in player_props:
                            current = fn(player)
                            line    = player_props[stat_key]["line"]
                            tracked.append({
                                "stat":      stat_key,
                                "line":      line,
                                "current":   current,
                                "pct":       round(current / line * 100) if line else 0,
                                "hit":       current > line,
                                "remaining": round(line - current, 1),
                                "edge_score": player_props[stat_key].get("edge_score"),
                                "tier":      player_props[stat_key].get("no_brainer_tier"),
                            })

                    if tracked or pname in prop_lines:
                        game_entry["players"].append({
                            "name":    pname,
                            "team":    team_data.get("abbreviation", side[:4].upper()),
                            "min":     player.get("min", "0"),
                            "pts":     player.get("pts"),
                            "reb":     player.get("reb"),
                            "ast":     player.get("ast"),
                            "stl":     player.get("stl"),
                            "blk":     player.get("blk"),
                            "fg3m":    player.get("fg3m"),
                            "plus_minus": player.get("plus_minus"),
                            "props":   tracked,
                        })

            games_live.append(game_entry)

        return jsonify({
            "status":      "ok",
            "games":       games_live,
            "prop_count":  len(props),
            "updated_at":  datetime.now().isoformat(),
        })

    except Exception as e:
        logger.error(f"Live API error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


def _get_loss_audit_for_backtest(conn) -> dict:
    """Pull loss audit summary for backtest dashboard."""
    try:
        rows = conn.execute("""
            SELECT fail_category, COUNT(*) as cnt
            FROM loss_audit
            GROUP BY fail_category
        """).fetchall()
        by_cat = {r[0]: r[1] for r in rows}
        total  = sum(by_cat.values())
        recent = conn.execute("""
            SELECT game_date, player_name, stat, line, actual_value,
                   fail_category, fail_reason, edge_score
            FROM loss_audit
            ORDER BY game_date DESC, edge_score DESC
            LIMIT 20
        """).fetchall()
        recent_list = [dict(zip(
            ["game_date","player_name","stat","line","actual_value",
             "fail_category","fail_reason","edge_score"], r
        )) for r in recent]
        return {
            "total":         total,
            "by_category":   by_cat,
            "fixable":       by_cat.get("GATE_MISS",0) + by_cat.get("DATA_MISS",0) + by_cat.get("SCORING_ERROR",0),
            "recent":        recent_list,
        }
    except Exception:
        return {"total": 0, "by_category": {}, "fixable": 0, "recent": []}


@app.route("/api/backtest")
def api_backtest():
    """Query app_data.db and return all data needed for the backtest dashboard."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row

        def q(sql, params=()):
            return [dict(r) for r in conn.execute(sql, params).fetchall()]

        # Summary
        summary = conn.execute("""
            SELECT COUNT(*) as total, SUM(o.hit) as hits,
                   ROUND(100.0*SUM(o.hit)/COUNT(*),1) as hit_rate,
                   COUNT(DISTINCT s.game_date) as days,
                   MIN(s.game_date) as date_from,
                   MAX(s.game_date) as date_to
            FROM prop_snapshots s
            JOIN prop_outcomes o ON s.snapshot_key = o.snapshot_key
            WHERE o.hit IS NOT NULL
        """).fetchone()

        # By date
        by_date = q("""
            SELECT s.game_date, COUNT(*) as total, SUM(o.hit) as hits,
                   ROUND(100.0*SUM(o.hit)/COUNT(*),1) as hit_rate
            FROM prop_snapshots s
            JOIN prop_outcomes o ON s.snapshot_key = o.snapshot_key
            WHERE o.hit IS NOT NULL
            GROUP BY s.game_date ORDER BY s.game_date
        """)

        # By L20 bucket
        by_l20 = q("""
            SELECT
                CASE
                    WHEN CAST(l20_hr AS REAL) >= 70 THEN '70%+'
                    WHEN CAST(l20_hr AS REAL) >= 60 THEN '60-69%'
                    WHEN CAST(l20_hr AS REAL) >= 55 THEN '55-59%'
                    WHEN CAST(l20_hr AS REAL) >= 45 THEN '45-54%'
                    WHEN CAST(l20_hr AS REAL) >= 30 THEN '30-44%'
                    ELSE '<30%'
                END as bucket,
                MIN(CAST(l20_hr AS REAL)) as sort_key,
                COUNT(*) as total, SUM(o.hit) as hits,
                ROUND(100.0*SUM(o.hit)/COUNT(*),1) as hit_rate
            FROM prop_snapshots s
            JOIN prop_outcomes o ON s.snapshot_key = o.snapshot_key
            WHERE l20_hr IS NOT NULL AND o.hit IS NOT NULL
            GROUP BY bucket ORDER BY sort_key DESC
        """)

        # By stat
        by_stat = q("""
            SELECT s.stat, COUNT(*) as total, SUM(o.hit) as hits,
                   ROUND(100.0*SUM(o.hit)/COUNT(*),1) as hit_rate
            FROM prop_snapshots s
            JOIN prop_outcomes o ON s.snapshot_key = o.snapshot_key
            WHERE o.hit IS NOT NULL
            GROUP BY s.stat ORDER BY hit_rate DESC
        """)

        # ── Tier calibration — the three numbers that prove model validity ──────
        # Reviewer's exact target: LOCK ~60-65%, STRONG ~56-60%, BET ~52-56%
        by_tier_calibration = q("""
            SELECT
                COALESCE(no_brainer_tier, 'UNTIERED') as tier,
                CASE no_brainer_tier
                    WHEN 'PRIME'  THEN 1
                    WHEN 'STRONG' THEN 2
                    WHEN 'SOLID'  THEN 3
                    ELSE 4
                END as sort_key,
                COUNT(*) as total,
                SUM(o.hit) as hits,
                ROUND(100.0*SUM(o.hit)/COUNT(*),1) as hit_rate,
                ROUND(AVG(s.edge_score),1) as avg_edge,
                ROUND(AVG(CASE WHEN s.clv IS NOT NULL THEN s.clv END),2) as avg_clv
            FROM prop_snapshots s
            JOIN prop_outcomes o ON s.snapshot_key = o.snapshot_key
            WHERE o.hit IS NOT NULL
            GROUP BY tier
            HAVING COUNT(*) >= 5
            ORDER BY sort_key
        """)

        # ── Edge score bucket — finer-grained than tiers ──────────────────────
        by_edge = q("""
            SELECT
                CASE
                    WHEN CAST(edge_score AS REAL) >= 75 THEN '75+  LOCK'
                    WHEN CAST(edge_score AS REAL) >= 62 THEN '62-74 STRONG'
                    WHEN CAST(edge_score AS REAL) >= 50 THEN '50-61 BET'
                    WHEN CAST(edge_score AS REAL) >= 38 THEN '38-49 LEAN'
                    WHEN CAST(edge_score AS REAL) > 0   THEN '1-37 SKIP'
                    ELSE '0 No Score'
                END as bucket,
                MIN(CAST(edge_score AS REAL)) as sort_key,
                COUNT(*) as total, SUM(o.hit) as hits,
                ROUND(100.0*SUM(o.hit)/COUNT(*),1) as hit_rate,
                ROUND(AVG(CASE WHEN s.clv IS NOT NULL THEN s.clv END),2) as avg_clv
            FROM prop_snapshots s
            JOIN prop_outcomes o ON s.snapshot_key = o.snapshot_key
            WHERE edge_score IS NOT NULL AND o.hit IS NOT NULL
            GROUP BY bucket
            HAVING COUNT(*) >= 5
            ORDER BY sort_key DESC
        """)

        # ── Veto accuracy — vetoed props should hit WORSE than clean props ────
        # This is the proof that the hard vetoes are actually filtering traps.
        by_veto = q("""
            SELECT
                CASE
                    WHEN s.parlay_disqualified = 1 AND s.parlay_disqualify_reason LIKE 'B2B%'
                        THEN 'B2B (veto)'
                    WHEN s.parlay_disqualified = 1 AND s.parlay_disqualify_reason LIKE 'REGRESSION%'
                        THEN 'REGRESSION (veto)'
                    WHEN s.parlay_disqualified = 1 AND s.parlay_disqualify_reason LIKE 'VOLATILE-FLOOR%'
                        THEN 'VOLATILE-FLOOR (veto)'
                    WHEN s.parlay_disqualified = 1 AND s.parlay_disqualify_reason LIKE 'COMBINED STAT CV%'
                        THEN 'HIGH CV (veto)'
                    WHEN s.parlay_disqualified = 1 AND s.parlay_disqualify_reason LIKE 'WEAK L20%'
                        THEN 'WEAK L20 (veto)'
                    WHEN s.parlay_disqualified = 1
                        THEN 'OTHER (veto)'
                    ELSE 'CLEAN (no veto)'
                END as veto_type,
                COUNT(*) as total,
                SUM(o.hit) as hits,
                ROUND(100.0*SUM(o.hit)/COUNT(*),1) as hit_rate
            FROM prop_snapshots s
            JOIN prop_outcomes o ON s.snapshot_key = o.snapshot_key
            WHERE o.hit IS NOT NULL
            GROUP BY veto_type
            HAVING COUNT(*) >= 10
            ORDER BY hit_rate DESC
        """)

        # ── EV per prop — model prob vs implied prob ──────────────────────────
        # Reviewer's suggestion: EV = model_probability - implied_probability
        # This identifies whether we're finding real pricing errors
        by_ev_bucket = q("""
            SELECT
                CASE
                    WHEN (s.model_prob - 50.0) >= 10  THEN '+10%+ EV'
                    WHEN (s.model_prob - 50.0) >= 5   THEN '+5-9% EV'
                    WHEN (s.model_prob - 50.0) >= 2   THEN '+2-4% EV'
                    WHEN (s.model_prob - 50.0) >= 0   THEN '0-1% EV'
                    ELSE 'Negative EV'
                END as ev_bucket,
                COUNT(*) as total,
                SUM(o.hit) as hits,
                ROUND(100.0*SUM(o.hit)/COUNT(*),1) as hit_rate,
                ROUND(AVG(s.model_prob - 50.0),1) as avg_ev
            FROM prop_snapshots s
            JOIN prop_outcomes o ON s.snapshot_key = o.snapshot_key
            WHERE o.hit IS NOT NULL AND s.model_prob IS NOT NULL
            GROUP BY ev_bucket
            HAVING COUNT(*) >= 10
            ORDER BY avg_ev DESC
        """)

        # By DQ (keep for backward compat)
        by_dq = q("""
            SELECT parlay_disqualified, COUNT(*) as total, SUM(o.hit) as hits,
                   ROUND(100.0*SUM(o.hit)/COUNT(*),1) as hit_rate
            FROM prop_snapshots s
            JOIN prop_outcomes o ON s.snapshot_key = o.snapshot_key
            WHERE o.hit IS NOT NULL
            GROUP BY parlay_disqualified
        """)

        # By regression risk
        by_regr = q("""
            SELECT regression_risk, COUNT(*) as total, SUM(o.hit) as hits,
                   ROUND(100.0*SUM(o.hit)/COUNT(*),1) as hit_rate
            FROM prop_snapshots s
            JOIN prop_outcomes o ON s.snapshot_key = o.snapshot_key
            WHERE o.hit IS NOT NULL
            GROUP BY regression_risk
        """)

        # By signal (lock/hammer/neither)
        by_signal = q("""
            SELECT
                CASE WHEN is_lock=1 THEN 'Lock'
                     WHEN is_hammer=1 THEN 'Hammer'
                     ELSE 'Neither' END as signal,
                COUNT(*) as total, SUM(o.hit) as hits,
                ROUND(100.0*SUM(o.hit)/COUNT(*),1) as hit_rate
            FROM prop_snapshots s
            JOIN prop_outcomes o ON s.snapshot_key = o.snapshot_key
            WHERE o.hit IS NOT NULL
            GROUP BY signal ORDER BY hit_rate DESC
        """)

        # By dist_cv bucket (new field — limited data)
        by_cv = q("""
            SELECT
                CASE
                    WHEN CAST(dist_cv AS REAL) <= 0.25 THEN '<=0.25 Consistent'
                    WHEN CAST(dist_cv AS REAL) <= 0.35 THEN '0.26-0.35 Moderate'
                    WHEN CAST(dist_cv AS REAL) <= 0.50 THEN '0.36-0.50 Volatile'
                    ELSE '>0.50 Blocked'
                END as bucket,
                MIN(CAST(dist_cv AS REAL)) as sort_key,
                COUNT(*) as total, SUM(o.hit) as hits,
                ROUND(100.0*SUM(o.hit)/COUNT(*),1) as hit_rate
            FROM prop_snapshots s
            JOIN prop_outcomes o ON s.snapshot_key = o.snapshot_key
            WHERE dist_cv IS NOT NULL AND o.hit IS NOT NULL
            GROUP BY bucket ORDER BY sort_key
        """)

        # Top players by hit rate (min 10 graded)
        by_player = q("""
            SELECT s.player_name, COUNT(*) as total, SUM(o.hit) as hits,
                   ROUND(100.0*SUM(o.hit)/COUNT(*),1) as hit_rate
            FROM prop_snapshots s
            JOIN prop_outcomes o ON s.snapshot_key = o.snapshot_key
            WHERE o.hit IS NOT NULL
            GROUP BY s.player_name
            HAVING COUNT(*) >= 10
            ORDER BY hit_rate DESC
            LIMIT 25
        """)

        # Worst players (min 10 graded)
        worst_player = q("""
            SELECT s.player_name, COUNT(*) as total, SUM(o.hit) as hits,
                   ROUND(100.0*SUM(o.hit)/COUNT(*),1) as hit_rate
            FROM prop_snapshots s
            JOIN prop_outcomes o ON s.snapshot_key = o.snapshot_key
            WHERE o.hit IS NOT NULL
            GROUP BY s.player_name
            HAVING COUNT(*) >= 10
            ORDER BY hit_rate ASC
            LIMIT 15
        """)

        # Best stat+player combos (min 8 graded)
        by_player_stat = q("""
            SELECT s.player_name, s.stat, COUNT(*) as total, SUM(o.hit) as hits,
                   ROUND(100.0*SUM(o.hit)/COUNT(*),1) as hit_rate
            FROM prop_snapshots s
            JOIN prop_outcomes o ON s.snapshot_key = o.snapshot_key
            WHERE o.hit IS NOT NULL
            GROUP BY s.player_name, s.stat
            HAVING COUNT(*) >= 8
            ORDER BY hit_rate DESC
            LIMIT 20
        """)

        conn.close()
        return jsonify({
            "summary":            dict(summary),
            "by_date":            by_date,
            "by_l20":             by_l20,
            "by_stat":            by_stat,
            "by_edge":            by_edge,
            "by_tier_calibration": by_tier_calibration,
            "by_veto":            by_veto,
            "by_ev_bucket":       by_ev_bucket,
            "by_dq":              by_dq,
            "by_regr":            by_regr,
            "by_signal":          by_signal,
            "by_cv":              by_cv,
            "by_player":          by_player,
            "worst_player":       worst_player,
            "by_player_stat":     by_player_stat,
            "loss_audit":         _get_loss_audit_for_backtest(conn),
        })
    except Exception as e:
        logger.error(f"Backtest API error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


def _american_to_no_vig_prob(over_odds: float, under_odds: float) -> tuple:
    """
    Convert american odds pair to no-vig implied probabilities.
    Removes the sportsbook's juice so we compare apples to apples.

    Returns (over_prob, under_prob) as 0.0–1.0 floats.
    """
    def to_decimal(american):
        if american > 0:
            return (american / 100.0) + 1.0
        else:
            return (100.0 / abs(american)) + 1.0

    def to_raw_prob(american):
        if american > 0:
            return 100.0 / (american + 100.0)
        else:
            return abs(american) / (abs(american) + 100.0)

    raw_over  = to_raw_prob(over_odds)
    raw_under = to_raw_prob(under_odds)
    total_vig = raw_over + raw_under

    if total_vig <= 0:
        return 0.5, 0.5

    # Remove vig by normalizing
    no_vig_over  = round(raw_over  / total_vig, 4)
    no_vig_under = round(raw_under / total_vig, 4)
    return no_vig_over, no_vig_under


def _fetch_closing_lines(game_date: str) -> dict:
    """
    Fetch final player prop lines from BDL for a completed game date.
    Returns {player_name|stat|line_str: {closing_line, closing_odds, closing_prob}} lookup.

    Called automatically at 6:30pm each evening (lines go final ~1-2 hours before tip).
    Also callable manually via /api/clv?refresh=true.
    """
    from bdl_client import get_client as _get_bdl
    _bdl = _get_bdl()

    closing = {}
    try:
        # Fetch final odds snapshot — don't bust cache, we want the settled line
        raw_odds = _bdl.get_game_odds(game_date)
        if not raw_odds:
            logger.warning(f"CLV: No odds data for {game_date}")
            return {}

        # BDL returns player prop odds per game_id
        # Group by player+stat to find the final (closing) line
        for row in raw_odds:
            player_name = row.get("player_name", "") or row.get("player", {}).get("name", "")
            stat        = (row.get("stat_type") or row.get("type", "")).upper()
            line        = row.get("line") or row.get("value")
            over_odds   = row.get("over_odds") or row.get("over")
            under_odds  = row.get("under_odds") or row.get("under")

            if not all([player_name, stat, line, over_odds, under_odds]):
                continue

            try:
                line       = float(line)
                over_odds  = float(over_odds)
                under_odds = float(under_odds)
            except (ValueError, TypeError):
                continue

            no_vig_over, no_vig_under = _american_to_no_vig_prob(over_odds, under_odds)

            key = f"{player_name}|{stat}|{line}"
            closing[key] = {
                "closing_line":  line,
                "closing_odds":  int(over_odds),
                "closing_prob":  round(no_vig_over * 100, 1),  # store as 0-100
            }

        logger.info(f"CLV: Fetched {len(closing)} closing lines for {game_date}")

    except Exception as e:
        logger.error(f"CLV: Closing line fetch failed for {game_date}: {e}")

    return closing


def _compute_and_store_clv(game_date: str) -> dict:
    """
    Match closing lines against today's snapshots and compute CLV.

    CLV = model_prob - closing_prob
      Positive: we estimated the line was more likely to hit than the market implied.
               This is the proof-of-edge signal.
      Negative: market was more confident than we were. We may have been chasing.
      Zero/None: no closing line available or no model_prob at snapshot time.

    Updates prop_snapshots with closing_line, closing_prob, clv.
    Returns summary dict for the morning report.
    """
    closing = _fetch_closing_lines(game_date)
    if not closing:
        return {"status": "no_closing_data", "updated": 0}

    updated = 0
    clv_values = []

    try:
        with sqlite3.connect(DB_PATH) as conn:
            rows = conn.execute("""
                SELECT snapshot_key, player_name, stat, line, model_prob, direction
                FROM prop_snapshots
                WHERE game_date = ?
                  AND model_prob IS NOT NULL
                  AND clv IS NULL
            """, (game_date,)).fetchall()

            for (snap_key, player_name, stat, line, model_prob, direction) in rows:
                key = f"{player_name}|{stat}|{line}"
                cl  = closing.get(key)
                if not cl:
                    # Try fuzzy match on line (books sometimes move 0.5)
                    for ckey, cval in closing.items():
                        parts = ckey.split("|")
                        if (len(parts) == 3
                                and parts[0] == player_name
                                and parts[1] == stat.upper()
                                and abs(float(parts[2]) - line) <= 0.5):
                            cl = cval
                            break

                if not cl:
                    continue

                closing_prob = cl["closing_prob"]
                # For under bets, flip closing_prob (closing_prob is always the over side)
                if direction == "UNDER":
                    closing_prob = round(100.0 - closing_prob, 1)

                clv = round(model_prob - closing_prob, 2)
                clv_values.append(clv)

                conn.execute("""
                    UPDATE prop_snapshots
                    SET closing_line      = ?,
                        closing_odds      = ?,
                        closing_prob      = ?,
                        clv               = ?,
                        closing_fetched_at = ?
                    WHERE snapshot_key = ?
                """, (
                    cl["closing_line"],
                    cl["closing_odds"],
                    closing_prob,
                    clv,
                    datetime.now().isoformat(),
                    snap_key,
                ))
                updated += 1

            conn.commit()

    except Exception as e:
        logger.error(f"CLV: Store failed for {game_date}: {e}")
        return {"status": "error", "message": str(e)}

    if not clv_values:
        return {"status": "ok", "updated": 0, "avg_clv": None}

    avg_clv      = round(sum(clv_values) / len(clv_values), 2)
    positive_clv = sum(1 for v in clv_values if v > 0)
    beat_pct     = round(positive_clv / len(clv_values) * 100, 1)

    logger.info(
        f"CLV {game_date}: updated={updated}, avg_clv={avg_clv:+.2f}%, "
        f"beat_closing={beat_pct}% ({positive_clv}/{len(clv_values)})"
    )

    return {
        "status":       "ok",
        "updated":      updated,
        "avg_clv":      avg_clv,
        "positive_clv": positive_clv,
        "total_with_clv": len(clv_values),
        "beat_closing_pct": beat_pct,
    }


@app.route("/api/clv")
def api_clv():
    """
    CLV summary for a date. ?refresh=true re-fetches closing lines.
    ?days=30 returns rolling CLV summary.
    """
    game_date = request.args.get("date", date.today().isoformat())
    refresh   = request.args.get("refresh", "false").lower() == "true"
    days_back = int(request.args.get("days", 30))

    if refresh:
        result = _compute_and_store_clv(game_date)
        return jsonify(result)

    try:
        with sqlite3.connect(DB_PATH) as conn:
            # Rolling CLV summary
            rolling = conn.execute("""
                SELECT
                    COUNT(*)                        as total,
                    ROUND(AVG(clv), 2)              as avg_clv,
                    SUM(CASE WHEN clv > 0 THEN 1 ELSE 0 END) as positive_count,
                    ROUND(100.0 * SUM(CASE WHEN clv > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as beat_pct,
                    MIN(clv)                        as worst_clv,
                    MAX(clv)                        as best_clv
                FROM prop_snapshots
                WHERE game_date >= date('now', ?)
                  AND clv IS NOT NULL
            """, (f"-{days_back} days",)).fetchone()

            # By-day CLV
            by_day = conn.execute("""
                SELECT
                    game_date,
                    COUNT(*)                        as total,
                    ROUND(AVG(clv), 2)              as avg_clv,
                    ROUND(100.0 * SUM(CASE WHEN clv > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as beat_pct
                FROM prop_snapshots
                WHERE game_date >= date('now', ?)
                  AND clv IS NOT NULL
                GROUP BY game_date
                ORDER BY game_date DESC
            """, (f"-{days_back} days",)).fetchall()

            # By-tier CLV
            by_tier = conn.execute("""
                SELECT
                    COALESCE(no_brainer_tier, 'UNTIERED') as tier,
                    COUNT(*)                        as total,
                    ROUND(AVG(clv), 2)              as avg_clv,
                    ROUND(100.0 * SUM(CASE WHEN clv > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as beat_pct
                FROM prop_snapshots
                WHERE game_date >= date('now', ?)
                  AND clv IS NOT NULL
                GROUP BY tier
                ORDER BY avg_clv DESC
            """, (f"-{days_back} days",)).fetchall()

            # By-stat CLV
            by_stat = conn.execute("""
                SELECT
                    stat,
                    COUNT(*)                        as total,
                    ROUND(AVG(clv), 2)              as avg_clv,
                    ROUND(100.0 * SUM(CASE WHEN clv > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as beat_pct
                FROM prop_snapshots
                WHERE game_date >= date('now', ?)
                  AND clv IS NOT NULL
                GROUP BY stat
                HAVING COUNT(*) >= 10
                ORDER BY avg_clv DESC
            """, (f"-{days_back} days",)).fetchall()

        summary = dict(zip(
            ["total", "avg_clv", "positive_count", "beat_pct", "worst_clv", "best_clv"],
            rolling
        )) if rolling else {}

        return jsonify({
            "days_back": days_back,
            "summary":   summary,
            "by_day":    [dict(zip(["date","total","avg_clv","beat_pct"], r)) for r in by_day],
            "by_tier":   [dict(zip(["tier","total","avg_clv","beat_pct"], r)) for r in by_tier],
            "by_stat":   [dict(zip(["stat","total","avg_clv","beat_pct"], r)) for r in by_stat],
        })

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


def _run_morning_report():
    """
    Full automated morning workflow. Called at 8am daily.
    1. Grade yesterday's props
    2. Run loss audit (categorize every loss)
    3. Run model tuning (surface signal calibration issues)
    4. Store report in DB for dashboard
    """
    from datetime import date, timedelta, datetime
    yesterday = (date.today() - timedelta(days=1)).isoformat()

    report = {
        "date":         yesterday,
        "generated_at": datetime.now().isoformat(),
        "grading":      {},
        "loss_audit":   {},
        "model_tuning": {},
        "errors":       [],
    }

    # Step 1: Grade yesterday
    try:
        from grading import grade_props, init_results_db
        init_results_db()
        graded = grade_props(Path("pipeline_cache.json"), DB_PATH, yesterday)
        hits   = sum(1 for g in graded if g.get("result") == "HIT")
        misses = sum(1 for g in graded if g.get("result") == "MISS")
        report["grading"] = {
            "total":    len(graded),
            "hits":     hits,
            "misses":   misses,
            "hit_rate": round(hits / len(graded) * 100, 1) if graded else None,
        }
        logger.info(f"Morning report grading: {hits}/{len(graded)} HIT ({report['grading'].get('hit_rate')}%)")
    except Exception as e:
        report["errors"].append(f"grading: {e}")
        logger.warning(f"Morning report grading failed: {e}")

    # Step 2: Loss audit
    try:
        from grading import run_loss_audit, get_loss_audit_summary
        run_loss_audit(days_back=7)
        summary = get_loss_audit_summary(days_back=7)
        report["loss_audit"] = summary
        fixable_pct = summary.get("fixable_pct", 0)
        logger.info(f"Morning report loss audit: {summary.get('total_losses',0)} losses, {fixable_pct}% fixable")
    except Exception as e:
        report["errors"].append(f"loss_audit: {e}")
        logger.warning(f"Morning report loss audit failed: {e}")

    # Step 2b: CLV — fetch closing lines for yesterday and compute CLV
    try:
        clv_result = _compute_and_store_clv(yesterday)
        report["clv"] = clv_result
        if clv_result.get("avg_clv") is not None:
            logger.info(f"Morning report CLV: avg={clv_result['avg_clv']:+.2f}%, beat_closing={clv_result.get('beat_closing_pct')}%")
    except Exception as e:
        report["errors"].append(f"clv: {e}")
        logger.warning(f"Morning report CLV failed: {e}")

    # Step 3: Model tuning
    try:
        report["model_tuning"] = _run_model_tuning()
        logger.info(f"Morning report model tuning: {len(report['model_tuning'].get('signals',[]))} signals analyzed")
    except Exception as e:
        report["errors"].append(f"model_tuning: {e}")
        logger.warning(f"Morning report model tuning failed: {e}")

    # Step 4: Store report in DB
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS morning_reports (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    report_date  TEXT UNIQUE,
                    report_json  TEXT NOT NULL,
                    generated_at TEXT NOT NULL
                )
            """)
            conn.execute("""
                INSERT OR REPLACE INTO morning_reports (report_date, report_json, generated_at)
                VALUES (?,?,?)
            """, (yesterday, json.dumps(report), report["generated_at"]))
            conn.commit()
        logger.info(f"Morning report stored for {yesterday}")
    except Exception as e:
        logger.warning(f"Morning report storage failed: {e}")

    return report


def _run_model_tuning() -> dict:
    """
    Analyze last 30 days of graded outcomes to surface scoring calibration issues.
    Compares signal presence vs hit rates to identify over/under-weighted signals.
    Returns a dict of signal calibration findings.
    """
    signals = []

    try:
        with sqlite3.connect(DB_PATH) as conn:
            # Edge score bucket performance
            edge_rows = conn.execute("""
                SELECT
                    CASE
                        WHEN CAST(s.edge_score AS REAL) >= 70 THEN '70+'
                        WHEN CAST(s.edge_score AS REAL) >= 65 THEN '65-69'
                        WHEN CAST(s.edge_score AS REAL) >= 60 THEN '60-64'
                        WHEN CAST(s.edge_score AS REAL) >= 55 THEN '55-59'
                        WHEN CAST(s.edge_score AS REAL) >= 50 THEN '50-54'
                        ELSE '<50'
                    END as bucket,
                    COUNT(*) as total,
                    SUM(o.hit) as hits,
                    ROUND(100.0*SUM(o.hit)/COUNT(*),1) as hit_rate
                FROM prop_snapshots s
                JOIN prop_outcomes o ON s.snapshot_key = o.snapshot_key
                WHERE o.hit IS NOT NULL
                  AND s.game_date >= date('now', '-30 days')
                  AND s.edge_score IS NOT NULL
                GROUP BY bucket
                HAVING COUNT(*) >= 15
                ORDER BY s.edge_score DESC
            """).fetchall()

            edge_buckets = [
                {"bucket": r[0], "total": r[1], "hits": r[2], "hit_rate": r[3]}
                for r in edge_rows
            ]

            # Signal calibration: do high-signal props actually hit more?
            for signal, col in [
                ("regression_risk", "regression_risk"),
                ("parlay_disqualified", "parlay_disqualified"),
            ]:
                try:
                    rows = conn.execute(f"""
                        SELECT {col}, COUNT(*) as total, SUM(o.hit) as hits,
                               ROUND(100.0*SUM(o.hit)/COUNT(*),1) as hit_rate
                        FROM prop_snapshots s
                        JOIN prop_outcomes o ON s.snapshot_key = o.snapshot_key
                        WHERE o.hit IS NOT NULL
                          AND s.game_date >= date('now', '-30 days')
                        GROUP BY {col}
                        HAVING COUNT(*) >= 20
                    """).fetchall()
                    for r in rows:
                        signals.append({
                            "signal":   signal,
                            "value":    bool(r[0]),
                            "total":    r[1],
                            "hit_rate": r[3],
                        })
                except Exception:
                    pass

            # Dist profile performance
            dist_rows = conn.execute("""
                SELECT dist_profile, COUNT(*) as total, SUM(o.hit) as hits,
                       ROUND(100.0*SUM(o.hit)/COUNT(*),1) as hit_rate
                FROM prop_snapshots s
                JOIN prop_outcomes o ON s.snapshot_key = o.snapshot_key
                WHERE o.hit IS NOT NULL
                  AND s.game_date >= date('now', '-30 days')
                  AND dist_profile IS NOT NULL AND dist_profile != ''
                GROUP BY dist_profile
                HAVING COUNT(*) >= 20
            """).fetchall()

            dist_performance = [
                {"profile": r[0], "total": r[1], "hit_rate": r[3]}
                for r in dist_rows
            ]

            # Identify miscalibrated buckets
            findings = []
            for b in edge_buckets:
                if b["bucket"] in ("65-69", "70+") and b["hit_rate"] < 52:
                    findings.append(f"⚠ Edge {b['bucket']} underperforming: {b['hit_rate']}% on {b['total']} props — scoring overcredits signals in this range")
                if b["bucket"] in ("50-54",) and b["hit_rate"] > 55:
                    findings.append(f"✅ Edge 50-54 hitting {b['hit_rate']}% — tier threshold could be lowered")

            # BLK validation
            blk_row = conn.execute("""
                SELECT COUNT(*) as total, SUM(o.hit) as hits,
                       ROUND(100.0*SUM(o.hit)/COUNT(*),1) as hit_rate
                FROM prop_snapshots s
                JOIN prop_outcomes o ON s.snapshot_key = o.snapshot_key
                WHERE o.hit IS NOT NULL
                  AND s.stat = 'BLK'
                  AND s.game_date >= date('now', '-30 days')
            """).fetchone()
            if blk_row and blk_row[0] >= 20:
                if blk_row[2] < 40:
                    findings.append(f"🚫 BLK confirmed trap: {blk_row[2]}% on {blk_row[0]} props — avoid in parlays")

    except Exception as e:
        findings = [f"Model tuning query failed: {e}"]
        edge_buckets = []
        dist_performance = []
        signals = []

    return {
        "edge_buckets":      edge_buckets,
        "dist_performance":  dist_performance,
        "signals":           signals,
        "findings":          findings,
    }


@app.route("/api/morning_report")
def api_morning_report():
    """Get the latest morning report, or trigger a fresh one."""
    refresh = request.args.get("refresh", "false").lower() == "true"
    if refresh:
        report = _run_morning_report()
        return jsonify(report)
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute("""
                SELECT report_json FROM morning_reports
                ORDER BY generated_at DESC LIMIT 1
            """).fetchone()
        if row:
            return jsonify(json.loads(row[0]))
        return jsonify({"status": "no_report", "message": "No morning report yet. Run with ?refresh=true"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    init_db()
    load_cached_pipeline()
    threading.Thread(target=_auto_fetch_yesterday_outcomes, daemon=True).start()

    # ── Startup regrade: fix stale NO DATA results from last 3 days ──────────
    def _startup_regrade():
        try:
            from grading import regrade_stale_results
            summary = regrade_stale_results(days_back=3)
            fixed = sum(v.get("regraded", 0) for v in summary.values())
            if fixed:
                logger.info(f"Startup regrade: fixed {fixed} stale NO DATA results")
        except Exception as e:
            logger.debug(f"Startup regrade skipped: {e}")
    threading.Thread(target=_startup_regrade, daemon=True).start()

    # ── Morning report scheduler: runs daily at 8am ───────────────────────────
    def _morning_report_scheduler():
        import time as _time
        from datetime import datetime as _dt, timedelta as _td
        while True:
            try:
                now = _dt.now()
                next_run = now.replace(hour=8, minute=0, second=0, microsecond=0)
                if now >= next_run:
                    next_run += _td(days=1)
                sleep_secs = (next_run - now).total_seconds()
                logger.info(f"Morning report scheduled for {next_run.strftime('%Y-%m-%d %H:%M')}")
                _time.sleep(sleep_secs)
                logger.info("=== MORNING REPORT STARTING ===")
                _run_morning_report()
                logger.info("=== MORNING REPORT COMPLETE ===")
            except Exception as e:
                logger.error(f"Morning report scheduler error: {e}")
                _time.sleep(3600)
    threading.Thread(target=_morning_report_scheduler, daemon=True).start()

    # ── Closing line scheduler: runs daily at 6:30pm ──────────────────────────
    # Fetches final prop lines from BDL after the market has settled (~1-2hr before tip).
    # Computes CLV = model_prob - closing_prob for every prop scored today.
    def _closing_line_scheduler():
        import time as _time
        from datetime import datetime as _dt, timedelta as _td
        while True:
            try:
                now = _dt.now()
                next_run = now.replace(hour=18, minute=30, second=0, microsecond=0)
                if now >= next_run:
                    next_run += _td(days=1)
                _time.sleep((next_run - now).total_seconds())
                today = date.today().isoformat()
                logger.info(f"=== CLOSING LINE FETCH STARTING ({today}) ===")
                result = _compute_and_store_clv(today)
                logger.info(f"=== CLOSING LINE FETCH COMPLETE: {result} ===")
            except Exception as e:
                logger.error(f"Closing line scheduler error: {e}")
                _time.sleep(3600)
    threading.Thread(target=_closing_line_scheduler, daemon=True).start()

    print("\n" + "="*50)
    print("  NBA Props Research App")
    print("  Open:     http://localhost:5000")
    print("  Live:     http://localhost:5000/live")
    print("  Backtest: http://localhost:5000/backtest")
    print("="*50 + "\n")
    app.run(debug=False, host="0.0.0.0", port=5000, use_reloader=False)
