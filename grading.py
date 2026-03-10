"""
grading.py — Grade last night's props against actual box score results.

Pulls yesterday's games, fetches box scores, and compares every prop
that was in the pipeline against the player's actual stat line.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

DB_PATH = Path("app_data.db")


def init_results_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS prop_results (
                key           TEXT PRIMARY KEY,
                game_date     TEXT,
                player_name   TEXT,
                team          TEXT,
                stat          TEXT,
                line          REAL,
                actual        REAL,
                result        TEXT,   -- HIT / MISS / PUSH
                flagged       INTEGER,
                flag_type     TEXT,
                note          TEXT,
                hammer        INTEGER,
                is_lock       INTEGER DEFAULT 0,
                parlay_ready  INTEGER DEFAULT 0,
                graded_at     TEXT
            )
        """)
        # Migration: add columns if they don't exist yet
        for col, typedef in [("is_lock", "INTEGER DEFAULT 0"), ("parlay_ready", "INTEGER DEFAULT 0")]:
            try:
                conn.execute(f"ALTER TABLE prop_results ADD COLUMN {col} {typedef}")
            except Exception:
                pass
        conn.commit()


def get_yesterdays_results(yesterday: str = None) -> dict:
    """
    Fetch all box scores from yesterday and return a lookup:
    {player_name_normalized: {stat: actual_value}}
    """
    import unicodedata, re
    from bdl_client import get_client

    if yesterday is None:
        yesterday = (date.today() - timedelta(days=1)).isoformat()

    client  = get_client()
    games   = client.get_games_for_date(yesterday)

    if not games:
        logger.info(f"  No games found for {yesterday}")
        return {}

    logger.info(f"  Grading: found {len(games)} games from {yesterday}")

    def norm(s):
        s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
        return re.sub(r"[^a-z0-9]","",s.lower())

    results = {}
    for game in games:
        game_id = game["id"]
        stats   = client.get_stats_for_game(game_id)
        for s in stats:
            player = s.get("player", {})
            name   = f"{player.get('first_name','')} {player.get('last_name','')}".strip()
            key    = norm(name)

            # Parse minutes — skip DNPs
            min_str = str(s.get("min") or "0")
            try:
                if ":" in min_str:
                    parts = min_str.split(":")
                    minutes = float(parts[0]) + float(parts[1])/60
                else:
                    minutes = float(min_str)
            except (ValueError, TypeError):
                minutes = 0.0

            if minutes < 3:
                continue

            pts  = float(s.get("pts")      or 0)
            reb  = float(s.get("reb")      or 0)
            ast  = float(s.get("ast")      or 0)
            fg3m = float(s.get("fg3m")     or 0)
            stl  = float(s.get("stl")      or 0)
            blk  = float(s.get("blk")      or 0)
            tov  = float(s.get("turnover") or 0)

            results[key] = {
                "player_name": name,
                "PTS":  pts,
                "REB":  reb,
                "AST":  ast,
                "FG3M": fg3m,
                "STL":  stl,
                "BLK":  blk,
                "TOV":  tov,
                "PRA":  pts + reb + ast,
                "PR":   pts + reb,
                "PA":   pts + ast,
                "RA":   reb + ast,
                "MIN":  round(minutes, 1),
            }

    logger.info(f"  Grading: loaded actual stats for {len(results)} players")
    return results


def grade_props(pipeline_cache_path: Path, flags_db_path: Path, yesterday: str = None) -> list[dict]:
    """
    Grade all props from yesterday's pipeline cache against actual results.
    Falls back to the prop_snapshots DB if pipeline_cache.json doesn't exist.
    Returns list of graded result dicts.
    """
    import unicodedata, re

    def norm(s):
        s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
        return re.sub(r"[^a-z0-9]","",s.lower())

    if yesterday is None:
        yesterday = (date.today() - timedelta(days=1)).isoformat()

    # ── Load props: try pipeline_cache.json first, then snapshot DB ──────────
    props = []

    cache_file = Path("pipeline_cache.json")
    if cache_file.exists():
        try:
            with open(cache_file) as f:
                cache = json.load(f)
            cached_date = cache.get("date", "")
            if cached_date == yesterday:
                props = cache.get("props", [])
                logger.info(f"  Loaded {len(props)} props from pipeline_cache.json")
            else:
                logger.info(f"  Cache date {cached_date} != {yesterday} — trying snapshot DB")
        except Exception as e:
            logger.warning(f"  Could not read pipeline_cache.json: {e}")

    # Fallback: load from prop_snapshots table
    if not props:
        try:
            with sqlite3.connect(flags_db_path) as conn:
                rows = conn.execute("""
                    SELECT player_name, team, stat, line, score, is_lock, is_hammer,
                           l5_hr, l10_hr, l20_hr, l5_values, days_rest,
                           blowout_level, ghost_rate
                    FROM prop_snapshots WHERE game_date = ?
                """, (yesterday,)).fetchall()
            cols = ["player_name","team","stat","line","score","lock","hammer",
                    "l5_hr","l10_hr","l20_hr","l5_values","days_rest",
                    "blowout_level","ghost_rate"]
            for r in rows:
                d = dict(zip(cols, r))
                # Reconstruct key for flag lookup
                d["key"] = f"{d['player_name']}|{d['stat']}|{d['line']}"
                d["hammer"] = bool(d.get("hammer"))
                # Parse l5_values JSON if present
                try:
                    d["l5_values"] = json.loads(d["l5_values"] or "[]")
                except Exception:
                    d["l5_values"] = []
                props.append(d)
            if props:
                logger.info(f"  Loaded {len(props)} props from snapshot DB for {yesterday}")
        except Exception as e:
            logger.warning(f"  Could not load from snapshot DB: {e}")

    if not props:
        logger.warning(f"  No props found for {yesterday} — nothing to grade")
        return []

    # Get flags from DB
    flags = {}
    try:
        with sqlite3.connect(flags_db_path) as conn:
            rows = conn.execute("SELECT key, flagged, flag_type, note FROM flags").fetchall()
            flags = {r[0]: {"flagged": bool(r[1]), "flag_type": r[2], "note": r[3]} for r in rows}
    except Exception:
        pass

    # Fetch actual results
    actuals = get_yesterdays_results(yesterday)

    graded = []
    for prop in props:
        pname  = prop.get("player_name","")
        stat   = prop.get("stat","")
        line   = prop.get("line")
        team   = prop.get("team","")
        key    = prop.get("key","")
        hammer = prop.get("hammer", False)

        if line is None:
            continue

        actual_data = actuals.get(norm(pname))
        if not actual_data:
            continue

        actual = actual_data.get(stat)
        if actual is None:
            continue

        # Grade
        if actual > line:
            result = "HIT"
        elif actual == line:
            result = "PUSH"
        else:
            result = "MISS"

        flag_data = flags.get(key, {})

        graded.append({
            "key":         key,
            "game_date":   yesterday,
            "player_name": pname,
            "team":        team,
            "stat":        stat,
            "line":        line,
            "actual":      actual,
            "result":      result,
            "flagged":     flag_data.get("flagged", False),
            "flag_type":   flag_data.get("flag_type",""),
            "note":        flag_data.get("note",""),
            "hammer":      hammer,
            "is_lock":     prop.get("lock", False),
            "parlay_ready": prop.get("parlay_ready", False),
            # Extra context for display
            "l5_hr":       prop.get("l5_hr"),
            "l10_hr":      prop.get("l10_hr"),
            "l20_hr":      prop.get("l20_hr"),
            "hook_level":  prop.get("hook_level",""),
            "score":       prop.get("score", 0),
        })

    # Save to DB
    save_results(graded)

    logger.info(f"  Graded {len(graded)} props from {yesterday}: "
                f"{sum(1 for g in graded if g['result']=='HIT')} HIT / "
                f"{sum(1 for g in graded if g['result']=='MISS')} MISS")
    return graded


def regrade_stale_results(days_back: int = 3) -> dict:
    """
    Re-attempt grading for any results marked NO DATA within the last `days_back` days.
    BDL posts box scores 12-24 hours after games, so a result graded at 11am
    for last night may have had no data — this cleans those up.

    Returns a summary: {date: {regraded: N, still_missing: N}}
    """
    from bdl_client import get_client
    from utils import normalize_name

    summary = {}
    client  = get_client()

    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("""
            SELECT key, game_date, player_name, stat, line, hammer
            FROM prop_results
            WHERE result = 'NO DATA'
              AND game_date >= date('now', ?)
        """, (f"-{days_back} days",)).fetchall()

    if not rows:
        logger.info("  No stale NO DATA results to re-grade")
        return {}

    # Group by date for efficient BDL fetching
    by_date: dict[str, list] = {}
    for row in rows:
        by_date.setdefault(row[1], []).append(row)

    for gdate, date_rows in by_date.items():
        summary[gdate] = {"regraded": 0, "still_missing": 0}
        actuals = get_yesterdays_results(gdate)
        if not actuals:
            summary[gdate]["still_missing"] = len(date_rows)
            continue

        for (key, game_date, player_name, stat, line, hammer) in date_rows:
            actual_data = actuals.get(normalize_name(player_name))
            if not actual_data:
                summary[gdate]["still_missing"] += 1
                continue
            actual = actual_data.get(stat)
            if actual is None:
                summary[gdate]["still_missing"] += 1
                continue

            if actual > line:
                result = "HIT"
            elif actual == line:
                result = "PUSH"
            else:
                result = "MISS"

            from datetime import datetime
            with sqlite3.connect(DB_PATH) as conn:
                conn.execute("""
                    UPDATE prop_results
                    SET actual = ?, result = ?, graded_at = ?
                    WHERE key = ?
                """, (actual, result, datetime.now().isoformat(), key))
                conn.commit()
            summary[gdate]["regraded"] += 1

        logger.info(f"  Re-grade {gdate}: {summary[gdate]['regraded']} fixed, "
                    f"{summary[gdate]['still_missing']} still missing")

    return summary



    from datetime import datetime
    with sqlite3.connect(DB_PATH) as conn:
        for g in graded:
            conn.execute("""
                INSERT OR REPLACE INTO prop_results
                (key, game_date, player_name, team, stat, line, actual, result,
                 flagged, flag_type, note, hammer, is_lock, parlay_ready, graded_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                g["key"], g["game_date"], g["player_name"], g["team"],
                g["stat"], g["line"], g["actual"], g["result"],
                int(g.get("flagged",False)), g.get("flag_type",""),
                g.get("note",""), int(g.get("hammer",False)),
                int(g.get("is_lock",False)), int(g.get("parlay_ready",False)),
                datetime.now().isoformat()
            ))
        conn.commit()


def get_results_for_date(game_date: str) -> list[dict]:
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("""
            SELECT key, game_date, player_name, team, stat, line, actual, result,
                   flagged, flag_type, note, hammer
            FROM prop_results WHERE game_date = ?
            ORDER BY result, player_name
        """, (game_date,)).fetchall()

    cols = ["key","game_date","player_name","team","stat","line","actual","result",
            "flagged","flag_type","note","hammer"]
    return [dict(zip(cols, r)) for r in rows]


def get_track_record() -> dict:
    """Aggregate hit rates by category for the model's track record."""
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("""
            SELECT result, hammer, flagged, stat, is_lock, parlay_ready, COUNT(*) as cnt
            FROM prop_results
            GROUP BY result, hammer, flagged, stat, is_lock, parlay_ready
        """).fetchall()

    total = hits = 0
    hammer_total = hammer_hits = 0
    lock_total = lock_hits = 0
    parlay_ready_total = parlay_ready_hits = 0
    flagged_total = flagged_hits = 0
    by_stat = {}

    for result, hammer, flagged, stat, is_lock, parlay_ready, cnt in rows:
        total += cnt
        if result == "HIT": hits += cnt
        if hammer:
            hammer_total += cnt
            if result == "HIT": hammer_hits += cnt
        if is_lock:
            lock_total += cnt
            if result == "HIT": lock_hits += cnt
        if parlay_ready:
            parlay_ready_total += cnt
            if result == "HIT": parlay_ready_hits += cnt
        if flagged:
            flagged_total += cnt
            if result == "HIT": flagged_hits += cnt
        if stat not in by_stat:
            by_stat[stat] = {"total": 0, "hits": 0}
        by_stat[stat]["total"] += cnt
        if result == "HIT":
            by_stat[stat]["hits"] += cnt

    return {
        "overall_hit_rate":       round(hits/total*100, 1) if total else None,
        "overall_total":          total,
        "hammer_hit_rate":        round(hammer_hits/hammer_total*100, 1) if hammer_total else None,
        "hammer_total":           hammer_total,
        "lock_hit_rate":          round(lock_hits/lock_total*100, 1) if lock_total else None,
        "lock_total":             lock_total,
        "parlay_ready_hit_rate":  round(parlay_ready_hits/parlay_ready_total*100, 1) if parlay_ready_total else None,
        "parlay_ready_total":     parlay_ready_total,
        "flagged_hit_rate":       round(flagged_hits/flagged_total*100, 1) if flagged_total else None,
        "flagged_total":          flagged_total,
        "by_stat": {
            s: {"hit_rate": round(d["hits"]/d["total"]*100,1), "total": d["total"]}
            for s, d in by_stat.items() if d["total"] > 0
        }
    }
