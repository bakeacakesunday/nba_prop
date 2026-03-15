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
        for col, typedef in [
            ("is_lock",             "INTEGER DEFAULT 0"),
            ("parlay_ready",        "INTEGER DEFAULT 0"),
            ("parlay_disqualified", "INTEGER DEFAULT 0"),
            ("dist_profile",        "TEXT DEFAULT ''"),
            ("edge_score",          "REAL"),
        ]:
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
                           blowout_level, ghost_rate,
                           parlay_disqualified, parlay_disqualify_reason,
                           dist_profile, regression_risk
                    FROM prop_snapshots WHERE game_date = ?
                """, (yesterday,)).fetchall()
            cols = ["player_name","team","stat","line","score","lock","hammer",
                    "l5_hr","l10_hr","l20_hr","l5_values","days_rest",
                    "blowout_level","ghost_rate",
                    "parlay_disqualified","parlay_disqualify_reason",
                    "dist_profile","regression_risk"]
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
            "parlay_disqualified": prop.get("parlay_disqualified", False),
            "dist_profile": prop.get("dist_profile", ""),
            "edge_score":  prop.get("edge_score"),
            # Extra context for display
            "l5_hr":       prop.get("l5_hr"),
            "l10_hr":      prop.get("l10_hr"),
            "l20_hr":      prop.get("l20_hr"),
            "hook_level":  prop.get("hook_level",""),
            "score":       prop.get("score", 0),
        })

    # Save to DB
    _save_results(graded)

    logger.info(f"  Graded {len(graded)} props from {yesterday}: "
                f"{sum(1 for g in graded if g['result']=='HIT')} HIT / "
                f"{sum(1 for g in graded if g['result']=='MISS')} MISS")
    return graded


def _save_results(graded: list[dict]) -> None:
    """Persist a list of graded prop dicts to prop_results."""
    from datetime import datetime
    with sqlite3.connect(DB_PATH) as conn:
        for g in graded:
            conn.execute("""
                INSERT OR REPLACE INTO prop_results
                (key, game_date, player_name, team, stat, line, actual, result,
                 flagged, flag_type, note, hammer, is_lock, parlay_ready,
                 parlay_disqualified, dist_profile, edge_score, graded_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                g["key"], g["game_date"], g["player_name"], g["team"],
                g["stat"], g["line"], g["actual"], g["result"],
                int(g.get("flagged", False)), g.get("flag_type", ""),
                g.get("note", ""), int(g.get("hammer", False)),
                int(g.get("is_lock", False)), int(g.get("parlay_ready", False)),
                int(g.get("parlay_disqualified", False)),
                g.get("dist_profile", ""),
                g.get("edge_score"),
                datetime.now().isoformat()
            ))
        conn.commit()


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
            SELECT result, hammer, flagged, stat, is_lock, parlay_ready,
                   parlay_disqualified, dist_profile, COUNT(*) as cnt
            FROM prop_results
            GROUP BY result, hammer, flagged, stat, is_lock, parlay_ready,
                     parlay_disqualified, dist_profile
        """).fetchall()

    total = hits = 0
    hammer_total = hammer_hits = 0
    lock_total = lock_hits = 0
    parlay_ready_total = parlay_ready_hits = 0
    flagged_total = flagged_hits = 0
    dq_total = dq_hits = 0
    clean_total = clean_hits = 0
    by_stat = {}
    by_dist_profile = {}

    for result, hammer, flagged, stat, is_lock, parlay_ready, parlay_dq, dist_prof, cnt in rows:
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
        # DQ vs clean breakdown — the key feedback loop
        if parlay_dq:
            dq_total += cnt
            if result == "HIT": dq_hits += cnt
        else:
            clean_total += cnt
            if result == "HIT": clean_hits += cnt
        if stat not in by_stat:
            by_stat[stat] = {"total": 0, "hits": 0}
        by_stat[stat]["total"] += cnt
        if result == "HIT":
            by_stat[stat]["hits"] += cnt
        # Distribution profile breakdown
        prof = dist_prof or "UNKNOWN"
        if prof not in by_dist_profile:
            by_dist_profile[prof] = {"total": 0, "hits": 0}
        by_dist_profile[prof]["total"] += cnt
        if result == "HIT":
            by_dist_profile[prof]["hits"] += cnt

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
        # DQ vs clean — validates the parlay filter
        "dq_hit_rate":            round(dq_hits/dq_total*100, 1) if dq_total else None,
        "dq_total":               dq_total,
        "clean_hit_rate":         round(clean_hits/clean_total*100, 1) if clean_total else None,
        "clean_total":            clean_total,
        # Per distribution profile — validates CONSISTENT/MODERATE gate
        "by_dist_profile": {
            p: {"hit_rate": round(d["hits"]/d["total"]*100, 1), "total": d["total"]}
            for p, d in by_dist_profile.items() if d["total"] > 0
        },
        "by_stat": {
            s: {"hit_rate": round(d["hits"]/d["total"]*100, 1), "total": d["total"]}
            for s, d in by_stat.items() if d["total"] > 0
        }
    }


# ─────────────────────────────────────────────────────────────────────────────
# LOSS AUDIT SYSTEM
# ─────────────────────────────────────────────────────────────────────────────

# Failure categories — every loss gets one of these labels
FAIL_GATE_MISS      = "GATE_MISS"      # model said clean, but a gate should have blocked it
FAIL_DATA_MISS      = "DATA_MISS"      # missing signal data (blowout UNKNOWN, no spread, etc)
FAIL_SCORING_ERROR  = "SCORING_ERROR"  # edge score was high but hit rate was low — miscalibration
FAIL_VARIANCE       = "VARIANCE"       # model correctly flagged risk, loss is acceptable
FAIL_CORRECT_FADE   = "CORRECT_FADE"   # model said fade (under), player overperformed — acceptable

# Thresholds for audit classification
_HIGH_EDGE_THRESHOLD     = 60    # props above this should hit more often
_LOW_HIT_RATE_THRESHOLD  = 40    # if model said OVER but L10 was < 40%, that's a scoring error
_BLOWOUT_UNKNOWN_PENALTY = True  # UNKNOWN blowout on a ROLE player = data miss


def run_loss_audit(days_back: int = 7) -> list[dict]:
    """
    Retrospective audit of every loss in the last N days.
    Categorizes each loss into one of 5 failure modes.
    Stores results in loss_audit table and returns the full list.

    Run automatically after nightly grading, or call manually.

    Returns list of audit records, newest first.
    """
    _init_audit_table()

    end_date   = date.today().isoformat()
    start_date = (date.today() - timedelta(days=days_back)).isoformat()

    # Pull all MISS outcomes joined with snapshot signals
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("""
            SELECT
                s.snapshot_key, s.game_date, s.player_name, s.team, s.stat, s.line,
                o.actual_value,
                s.edge_score, s.l5_hr, s.l10_hr, s.l20_hr,
                s.blowout_level, s.parlay_disqualified, s.parlay_disqualify_reason,
                s.dist_profile, s.dist_cv, s.regression_risk,
                s.is_lock, s.is_hammer,
                s.ghost_rate, s.spread, s.implied_total
            FROM prop_snapshots s
            JOIN prop_outcomes o ON s.snapshot_key = o.snapshot_key
            WHERE o.hit = 0
              AND s.game_date >= ?
              AND s.game_date <= ?
            ORDER BY s.game_date DESC
        """, (start_date, end_date)).fetchall()

    cols = [
        "snapshot_key", "game_date", "player_name", "team", "stat", "line",
        "actual_value",
        "edge_score", "l5_hr", "l10_hr", "l20_hr",
        "blowout_level", "parlay_disqualified", "parlay_disqualify_reason",
        "dist_profile", "dist_cv", "regression_risk",
        "is_lock", "is_hammer",
        "ghost_rate", "spread", "implied_total",
    ]

    audit_records = []
    for row in rows:
        r = dict(zip(cols, row))
        category, reason = _classify_loss(r)
        r["fail_category"] = category
        r["fail_reason"]   = reason
        audit_records.append(r)

    # Save to DB
    _save_audit_records(audit_records)

    logger.info(
        f"Loss audit: {len(audit_records)} losses over last {days_back} days — "
        + " | ".join(
            f"{cat}:{sum(1 for r in audit_records if r['fail_category']==cat)}"
            for cat in [FAIL_GATE_MISS, FAIL_DATA_MISS, FAIL_SCORING_ERROR,
                        FAIL_VARIANCE, FAIL_CORRECT_FADE]
        )
    )
    return audit_records


def _classify_loss(r: dict) -> tuple:
    """
    Classify a single loss into a failure category.
    Returns (category, human-readable reason).
    """
    edge   = r.get("edge_score") or 0
    l10_hr = r.get("l10_hr") or 0
    l5_hr  = r.get("l5_hr") or 0
    blowout = r.get("blowout_level") or "UNKNOWN"
    dq      = bool(r.get("parlay_disqualified"))
    dq_reason = r.get("parlay_disqualify_reason") or ""
    dist    = r.get("dist_profile") or ""
    cv      = r.get("dist_cv")
    regr    = bool(r.get("regression_risk"))
    ghost   = r.get("ghost_rate") or 0
    stat    = (r.get("stat") or "").upper()

    # 1. GATE_MISS — a hard gate should have blocked this but didn't
    if regr:
        return FAIL_GATE_MISS, "regression_risk=True passed the gate — hard block missed"
    if cv and cv > 0.50 and not dq:
        return FAIL_GATE_MISS, f"dist_cv={cv:.2f} > 0.50 but not DQ'd — CV gate missed"
    if dist == "VOLATILE-FLOOR" and not dq:
        return FAIL_GATE_MISS, "VOLATILE-FLOOR dist_profile not DQ'd — gate missed"
    if l20_hr := r.get("l20_hr"):
        _thresholds = {"AST":60,"REB":60,"RA":60,"PTS":55,"PR":55,"PRA":55,"PA":55,"FG3M":55,"BLK":50}
        thresh = _thresholds.get(stat, 55)
        if l20_hr < thresh and not dq:
            return FAIL_GATE_MISS, f"L20={l20_hr:.0f}% < threshold {thresh}% for {stat} but not DQ'd"

    # 2. DATA_MISS — we were flying blind on a critical signal
    if blowout == "UNKNOWN":
        # Check if player is a role player — if so, this is a meaningful data gap
        return FAIL_DATA_MISS, f"blowout_level=UNKNOWN — spread data missing, garbage-time risk unassessed"
    if not r.get("spread") and not r.get("implied_total"):
        return FAIL_DATA_MISS, "No spread or implied total — game odds feed failed"

    # 3. SCORING_ERROR — edge score was high but underlying hit rates don't support it
    if edge >= _HIGH_EDGE_THRESHOLD and l10_hr < _LOW_HIT_RATE_THRESHOLD:
        return FAIL_SCORING_ERROR, (
            f"edge_score={edge:.0f} but L10={l10_hr:.0f}% — model overcredited signals, "
            f"hit rate doesn't justify the score"
        )
    if edge >= 65 and l5_hr < 40:
        return FAIL_SCORING_ERROR, (
            f"edge_score={edge:.0f} but L5={l5_hr:.0f}% — extreme score/hit-rate mismatch"
        )

    # 4. VARIANCE — model had correct signals but player underperformed
    if ghost and ghost > 0.20:
        return FAIL_VARIANCE, f"ghost_rate={ghost*100:.0f}% — player has documented 0-output risk"
    if dist in ("VOLATILE", "VOLATILE-FLOOR"):
        return FAIL_VARIANCE, f"dist_profile={dist} — volatile output was known, loss is acceptable"
    if edge < 45:
        return FAIL_VARIANCE, f"edge_score={edge:.0f} — low-confidence prop, loss is acceptable"

    # 5. Default: model said over, player underperformed — variance
    return FAIL_VARIANCE, (
        f"edge={edge:.0f}, L10={l10_hr:.0f}%, blowout={blowout} — "
        f"no systemic failure found, loss attributed to variance"
    )


def _init_audit_table():
    """Create loss_audit table if it doesn't exist."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS loss_audit (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_key    TEXT,
                game_date       TEXT,
                player_name     TEXT,
                team            TEXT,
                stat            TEXT,
                line            REAL,
                actual_value    REAL,
                edge_score      REAL,
                l10_hr          REAL,
                blowout_level   TEXT,
                fail_category   TEXT,
                fail_reason     TEXT,
                audited_at      TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_date ON loss_audit(game_date)")
        conn.commit()


def _save_audit_records(records: list[dict]):
    """Persist audit records to loss_audit table."""
    from datetime import datetime
    now = datetime.now().isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        for r in records:
            conn.execute("""
                INSERT OR REPLACE INTO loss_audit
                (snapshot_key, game_date, player_name, team, stat, line,
                 actual_value, edge_score, l10_hr, blowout_level,
                 fail_category, fail_reason, audited_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                r.get("snapshot_key"), r.get("game_date"), r.get("player_name"),
                r.get("team"), r.get("stat"), r.get("line"),
                r.get("actual_value"), r.get("edge_score"), r.get("l10_hr"),
                r.get("blowout_level"), r.get("fail_category"),
                r.get("fail_reason"), now,
            ))
        conn.commit()


def get_loss_audit_summary(days_back: int = 30) -> dict:
    """
    Aggregate loss audit results by category.
    Used by the backtest dashboard to show failure mode breakdown.
    """
    _init_audit_table()
    start_date = (date.today() - timedelta(days=days_back)).isoformat()

    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("""
            SELECT fail_category, stat, COUNT(*) as cnt
            FROM loss_audit
            WHERE game_date >= ?
            GROUP BY fail_category, stat
        """, (start_date,)).fetchall()

        total_losses = conn.execute("""
            SELECT COUNT(*) FROM loss_audit WHERE game_date >= ?
        """, (start_date,)).fetchone()[0]

    by_category: dict = {}
    by_stat_and_category: dict = {}

    for cat, stat, cnt in rows:
        by_category[cat] = by_category.get(cat, 0) + cnt
        key = f"{stat}_{cat}"
        by_stat_and_category[key] = cnt

    # Top actionable losses (GATE_MISS + DATA_MISS + SCORING_ERROR = fixable)
    fixable = (
        by_category.get(FAIL_GATE_MISS, 0) +
        by_category.get(FAIL_DATA_MISS, 0) +
        by_category.get(FAIL_SCORING_ERROR, 0)
    )

    return {
        "total_losses":      total_losses,
        "fixable_losses":    fixable,
        "fixable_pct":       round(fixable / total_losses * 100, 1) if total_losses else 0,
        "by_category":       by_category,
        "breakdown": {
            FAIL_GATE_MISS:     by_category.get(FAIL_GATE_MISS, 0),
            FAIL_DATA_MISS:     by_category.get(FAIL_DATA_MISS, 0),
            FAIL_SCORING_ERROR: by_category.get(FAIL_SCORING_ERROR, 0),
            FAIL_VARIANCE:      by_category.get(FAIL_VARIANCE, 0),
            FAIL_CORRECT_FADE:  by_category.get(FAIL_CORRECT_FADE, 0),
        },
        "days_back": days_back,
    }
