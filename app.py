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
import sys
import threading
from datetime import date
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
        "date": game_date,
        "games": [],
        "props": [],
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

    # 4. Props + game odds
    game_ids = [g["game_id"] for g in games if g.get("game_id")]
    auto_props, raw_props_by_game, player_id_lookup = nba_data.get_props_for_games(game_ids)

    # Fetch game-level odds (spread, total) for blowout risk — graceful fallback
    from bdl_client import get_client as _get_bdl
    _bdl = _get_bdl()
    game_odds_by_id: dict[int, dict] = {}
    try:
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
    except Exception as e:
        logger.debug(f"Game odds unavailable: {e}")

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
                # Pass minutes so the asymmetric penalty matrix knows the player tier.
                game_info = team_game_odds.get(team, {})
                player_mins = ctx.get("minutes_l5_avg")
                blowout   = _compute_blowout_risk(
                    game_info.get("spread"),
                    game_info.get("game_total"),
                    mins=player_mins,
                )
                # Attach implied team total so score/lock functions can use it
                blowout["implied_team_total"] = game_info.get("implied_total")

                # Build player-specific game script profile
                # This powers the parlay covariance engine — graded as this player,
                # not as a position archetype.
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
                kelly_data = {}
                if odds and l10_hr is not None:
                    try:
                        from kelly import recommended_bet
                        kelly_data = recommended_bet(l10_hr, float(odds))
                    except Exception:
                        pass

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
                    # Final score
                    "score": score,
                    # Game script profile — player-specific, powers parlay engine
                    "game_script_profile": gs_profile,
                    # Kelly Criterion bet sizing
                    "kelly": kelly_data,
                    # Line movement (populated after pipeline, placeholder here)
                    "line_movement": _line_mv,
                    # GTD flag — player is game-time decision tonight
                    "gtd": any(
                        inj.get("player_name", "").lower() == pname.lower()
                        for inj in injury_intel.get("gtd_tonight", [])
                    ),
                }
                prop_cards.append(card)

    # Sort by score descending
    prop_cards.sort(key=lambda c: c["score"], reverse=True)
    result["props"] = prop_cards

    # ── Record line snapshots for movement tracking ───────────────────────────
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
                parlay_ready, parlay_consider, odds, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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


def _compute_blowout_risk(
    spread: Optional[float],
    game_total: Optional[float],
    mins: Optional[float] = None,
) -> dict:
    """
    Asymmetric blowout risk based on spread direction AND player minute tier.

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

    # Role players on favored teams in blowouts should avoid = False
    # (they may benefit). Everyone else at HIGH/EXTREME = avoid
    should_avoid = (
        level in ("HIGH", "EXTREME")
        and not (team_favored and tier in ("role", "fringe"))
        and not (not team_favored and tier == "star")
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
        "regression_risk":  False,
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
        if hot_gap >= 30:
            result["regression_risk"] = True
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
    from grading import grade_props, init_results_db
    init_results_db()
    yesterday = request.json.get("date") if request.json else None
    try:
        graded = grade_props(Path("pipeline_cache.json"), DB_PATH, yesterday)
        return jsonify({"status": "ok", "graded": len(graded), "results": graded})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


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
        # Remove raw matrix from response (too verbose)
        result.pop("corr_matrix", None)

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


if __name__ == "__main__":
    init_db()
    load_cached_pipeline()
    threading.Thread(target=_auto_fetch_yesterday_outcomes, daemon=True).start()
    # Re-grade any NO DATA results from the last 3 days (BDL posting lag fix)
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
    print("\n" + "="*50)
    print("  NBA Props Research App")
    print("  Open: http://localhost:5000")
    print("="*50 + "\n")
    app.run(debug=False, host="0.0.0.0", port=5000, use_reloader=False)
