"""
tracker.py — Pick history, grading, and performance tracking.

Workflow:
  1. Each time you run the tool, tonight's Value tab predictions are
     saved to the "Picks History" tab with a date stamp and PENDING status.

  2. Next day when you run the tool with a fresh CSV, it finds all
     PENDING picks from previous dates, looks up what the player actually
     did in the CSV, grades them HIT or MISS, and updates the row.

  3. The "Track Record" tab aggregates results so you can see:
     - Overall hit rate by signal type
     - Hit rate by stat type
     - Which context factors are actually predictive
     - Model accuracy over time

Grading logic:
  - OVER call: HIT if player's actual stat > line
  - UNDER call: HIT if player's actual stat <= line
  - PENDING: game hasn't happened yet (not in CSV)
  - NO DATA: player didn't appear in the CSV for that date
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Optional

import pandas as pd
import gspread

logger = logging.getLogger(__name__)

# Columns stored in Picks History tab
_HISTORY_HEADERS = [
    "Pick ID",          # date|player|stat|line — unique key
    "Date",
    "Player",
    "Team",
    "Stat",
    "Line",
    "Odds",
    "Final Call",       # what the model said
    "HR Signal",        # hit rate signal
    "Context Signal",   # context label
    "L5 Hit%",
    "L10 Hit%",
    "L20 Hit%",
    "Trend",
    "Matchup",
    "Rest",
    "Consistency",
    # Grading (filled in next day)
    "Actual",           # what the player actually did
    "Result",           # HIT / MISS / PUSH / NO DATA / PENDING
    "Correct?",         # 1 / 0 / — (for averaging)
]

_DIRECTION_MAP = {
    "🔥🔥 STRONG OVER": "OVER",
    "✅ BET OVER":      "OVER",
    "〰 Lean Over":     "OVER",
    "🔥🔥 STRONG UNDER": "UNDER",
    "✅ BET UNDER":     "UNDER",
    "〰 Lean Under":    "UNDER",
    "⚪ Skip":          "SKIP",
}


# ── Save tonight's picks ──────────────────────────────────────────────────────

def save_picks(
    ws: gspread.Worksheet,
    value_rows: list[dict],
    game_date: str,
) -> int:
    """
    Append tonight's value rows to Picks History tab.
    Skips rows already saved for this date (idempotent).
    Returns count of new rows added.
    """
    existing = ws.get_all_values()
    existing_ids = set()

    if existing:
        headers = existing[0]
        try:
            id_col = headers.index("Pick ID")
            existing_ids = {row[id_col] for row in existing[1:] if row}
        except ValueError:
            pass

    if not existing or existing[0] != _HISTORY_HEADERS:
        if not existing:
            ws.update(range_name="A1", values=[_HISTORY_HEADERS])

    new_rows = []
    for r in value_rows:
        final_call = r.get("final_call", r.get("FINAL CALL", "⚪ Skip"))
        if final_call == "⚪ Skip":
            continue  # don't track skips

        player = r.get("player_name", "")
        stat   = r.get("stat_type", "")
        line   = r.get("line", "")
        pick_id = f"{game_date}|{player}|{stat}|{line}"

        if pick_id in existing_ids:
            continue  # already saved

        ctx = r.get("context", {})
        trend    = ctx.get(f"{stat}_trend", ctx.get("PTS_trend", "—")) or "—"
        matchup  = ctx.get(f"opp_{stat.lower()}_matchup", ctx.get("opp_pts_matchup", "—")) or "—"
        is_b2b   = ctx.get("is_back_to_back", "")
        days_rest = ctx.get("days_rest")
        consistency = ctx.get(f"{stat}_consistency", ctx.get("PTS_consistency", "—")) or "—"

        rest_display = "—"
        if str(is_b2b) == "🔴 YES":
            rest_display = "🔴 B2B"
        elif days_rest is not None:
            rest_display = f"{days_rest}d rest"

        new_rows.append([
            pick_id,
            game_date,
            player,
            r.get("team", ""),
            stat,
            str(line),
            str(r.get("odds", "")),
            final_call,
            r.get("hr_signal", r.get("Hit Rate Signal", "—")),
            r.get("ctx_label", r.get("Context", "—")),
            r.get("last5_hit_rate", "—"),
            r.get("last10_hit_rate", "—"),
            r.get("last20_hit_rate", "—"),
            trend,
            matchup,
            rest_display,
            consistency,
            "",         # Actual — filled in next day
            "PENDING",  # Result
            "—",        # Correct?
        ])

    if new_rows:
        ws.append_rows(new_rows, value_input_option="USER_ENTERED")
        logger.info(f"  Saved {len(new_rows)} picks to history")
    else:
        logger.info("  No new picks to save (already saved or all skips)")

    return len(new_rows)


# ── Grade pending picks ───────────────────────────────────────────────────────

def grade_pending_picks(
    ws: gspread.Worksheet,
    full_df: pd.DataFrame,
    today: str,
) -> int:
    """
    Find all PENDING picks for dates before today, look up actual results,
    and grade them.

    IMPORTANT: full_df only covers tonight's teams, so any pick from a previous
    night involving teams NOT on tonight's slate will return NO DATA if we rely
    solely on full_df. Instead, we first check the DB-backed prop_outcomes table
    (populated by app.py's _auto_fetch_yesterday_outcomes), then fall back to a
    fresh BDL box-score fetch for any remaining pending picks, and finally fall
    back to full_df for same-night teams as a last resort.
    """
    all_vals = ws.get_all_values()
    if len(all_vals) < 2:
        return 0

    headers = all_vals[0]
    try:
        col = {h: i for i, h in enumerate(headers)}
        id_col       = col["Pick ID"]
        date_col     = col["Date"]
        player_col   = col["Player"]
        stat_col     = col["Stat"]
        line_col     = col["Line"]
        call_col     = col["Final Call"]
        actual_col   = col["Actual"]
        result_col   = col["Result"]
        correct_col  = col["Correct?"]
    except KeyError as e:
        logger.warning(f"Picks History tab missing column: {e}")
        return 0

    combo_defs = {
        "PRA": ["PTS", "REB", "AST"],
        "PR":  ["PTS", "REB"],
        "PA":  ["PTS", "AST"],
        "RA":  ["REB", "AST"],
    }

    # Collect pending picks grouped by date so we can batch BDL fetches
    pending: list[tuple[int, str, str, str, str, str]] = []
    for i, row in enumerate(all_vals[1:], start=2):
        if len(row) <= result_col:
            continue
        result   = row[result_col] if len(row) > result_col else "PENDING"
        pick_date = row[date_col] if len(row) > date_col else ""
        if result != "PENDING" or not pick_date or pick_date >= today:
            continue
        player    = row[player_col]
        stat      = row[stat_col]
        line_str  = row[line_col]
        final_call = row[call_col]
        pending.append((i, pick_date, player, stat, line_str, final_call))

    if not pending:
        return 0

    # ── Step 1: try the prop_outcomes table (populated by app.py) ────────────
    from pathlib import Path
    import sqlite3, json
    DB_PATH = Path("app_data.db")
    outcomes_db: dict[str, dict[str, float]] = {}  # normalize(player)|date → {stat: actual}
    try:
        from utils import normalize_name
        with sqlite3.connect(DB_PATH) as conn:
            rows_db = conn.execute(
                "SELECT player_name, game_date, stat, actual_value FROM prop_outcomes"
            ).fetchall()
        for pname, gdate, stat, actual in rows_db:
            k = f"{normalize_name(pname)}|{gdate}"
            outcomes_db.setdefault(k, {})[stat.upper()] = actual
    except Exception:
        pass

    # ── Step 2: BDL fetch for dates not covered by outcomes DB ───────────────
    dates_needed = set(p[1] for p in pending)
    dates_covered = set()
    for k in outcomes_db:
        dates_covered.add(k.split("|")[1])
    dates_missing = dates_needed - dates_covered

    bdl_actuals: dict[str, dict[str, float]] = {}  # normalize(player)|date → {stat: actual}
    if dates_missing:
        try:
            from bdl_client import get_client
            from utils import normalize_name
            client = get_client()
            for target_date in sorted(dates_missing):
                games = client.get_games_for_date(target_date)
                final_games = [
                    g for g in games
                    if str(g.get("status", "")).upper().startswith("FINAL")
                    or str(g.get("status", "")) == "Final"
                ]
                for game in final_games:
                    gid = game.get("id")
                    if not gid:
                        continue
                    try:
                        stats = client.get_stats_for_game(gid)
                    except Exception:
                        continue
                    for s in stats:
                        player = s.get("player", {})
                        raw_name = f"{player.get('first_name','')} {player.get('last_name','')}".strip()
                        if not raw_name:
                            continue
                        # Parse minutes — skip DNPs
                        min_str = s.get("min", "0") or "0"
                        try:
                            if ":" in str(min_str):
                                parts = str(min_str).split(":")
                                minutes = float(parts[0]) + float(parts[1]) / 60
                            else:
                                minutes = float(min_str)
                        except Exception:
                            minutes = 0.0
                        if minutes < 5:
                            continue
                        pts  = float(s.get("pts")      or 0)
                        reb  = float(s.get("reb")      or 0)
                        ast  = float(s.get("ast")      or 0)
                        fg3m = float(s.get("fg3m")     or 0)
                        stl  = float(s.get("stl")      or 0)
                        blk  = float(s.get("blk")      or 0)
                        tov  = float(s.get("turnover") or 0)
                        key = f"{normalize_name(raw_name)}|{target_date}"
                        bdl_actuals[key] = {
                            "PTS": pts, "REB": reb, "AST": ast, "FG3M": fg3m,
                            "STL": stl, "BLK": blk, "TOV": tov,
                            "PRA": pts + reb + ast,
                            "PR":  pts + reb,
                            "PA":  pts + ast,
                            "RA":  reb + ast,
                        }
        except Exception as e:
            logger.warning(f"BDL fetch for grading failed: {e}")

    # ── Step 3: full_df as last-resort for same-night teams ─────────────────
    def get_actual_from_df(player_name: str, game_date: str, stat: str) -> float | None:
        mask = (
            (full_df["player_name"].str.lower() == player_name.lower()) &
            (full_df["GAME_DATE"].dt.strftime("%Y-%m-%d") == game_date)
        )
        rows = full_df[mask]
        if rows.empty:
            return None
        stat_upper = stat.upper()
        if stat_upper in combo_defs:
            total = 0.0
            for c in combo_defs[stat_upper]:
                if c not in rows.columns:
                    return None
                val = pd.to_numeric(rows.iloc[0][c], errors="coerce")
                if pd.isna(val):
                    return None
                total += val
            return total
        elif stat_upper in rows.columns:
            val = pd.to_numeric(rows.iloc[0][stat_upper], errors="coerce")
            return None if pd.isna(val) else float(val)
        return None

    def lookup_actual(player_name: str, pick_date: str, stat: str) -> float | None:
        from utils import normalize_name
        stat_upper = stat.upper()
        key = f"{normalize_name(player_name)}|{pick_date}"
        # Check outcomes DB first
        if key in outcomes_db and stat_upper in outcomes_db[key]:
            return outcomes_db[key][stat_upper]
        # Check BDL fetch
        if key in bdl_actuals and stat_upper in bdl_actuals[key]:
            return bdl_actuals[key][stat_upper]
        # Fallback: full_df
        return get_actual_from_df(player_name, pick_date, stat)

    # ── Grade ─────────────────────────────────────────────────────────────────
    graded = 0
    updates = []

    for (row_idx, pick_date, player, stat, line_str, final_call) in pending:
        try:
            line_val = float(line_str)
        except ValueError:
            continue

        direction = _DIRECTION_MAP.get(final_call, "SKIP")
        if direction == "SKIP":
            updates.append((row_idx, actual_col + 1, "N/A"))
            updates.append((row_idx, result_col + 1, "SKIPPED"))
            updates.append((row_idx, correct_col + 1, "—"))
            continue

        actual = lookup_actual(player, pick_date, stat)

        if actual is None:
            updates.append((row_idx, actual_col + 1, "—"))
            updates.append((row_idx, result_col + 1, "NO DATA"))
            updates.append((row_idx, correct_col + 1, "—"))
            continue

        actual_rounded = round(actual, 1)
        if actual == line_val:
            result_str, correct = "PUSH", "—"
        elif direction == "OVER":
            result_str = "HIT" if actual > line_val else "MISS"
            correct    = "1"  if actual > line_val else "0"
        else:
            result_str = "HIT" if actual <= line_val else "MISS"
            correct    = "1"  if actual <= line_val else "0"

        updates.append((row_idx, actual_col + 1, str(actual_rounded)))
        updates.append((row_idx, result_col + 1, result_str))
        updates.append((row_idx, correct_col + 1, correct))
        graded += 1

    if updates:
        for (row_idx, col_idx, value) in updates:
            ws.update_cell(row_idx, col_idx, value)
        logger.info(f"  Graded {graded} picks ({len(pending) - graded} still pending)")

    return graded


# ── Build Track Record ────────────────────────────────────────────────────────

def build_track_record(
    history_ws: gspread.Worksheet,
    record_ws: gspread.Worksheet,
) -> None:
    """
    Read Picks History and write summary stats to Track Record tab.
    Shows: overall accuracy, by signal, by stat, by context factor.
    """
    all_vals = history_ws.get_all_values()
    if len(all_vals) < 2:
        record_ws.clear()
        record_ws.update(range_name="A1", values=[["No pick history yet. Run the tool for a few days first."]])
        return

    headers = all_vals[0]
    rows = [dict(zip(headers, row)) for row in all_vals[1:] if any(row)]

    # Only grade-eligible rows
    graded = [r for r in rows if r.get("Result") in ("HIT", "MISS")]
    if not graded:
        record_ws.clear()
        record_ws.update(range_name="A1", values=[["No graded picks yet. Results appear the day after games."]])
        return

    def hit_rate(subset):
        hits = sum(1 for r in subset if r.get("Result") == "HIT")
        return f"{hits}/{len(subset)} ({hits/len(subset):.0%})" if subset else "—"

    def section(title, groups):
        out = [[title, "Record", "Hit Rate"]]
        for label, subset in groups:
            out.append([label, f"{sum(1 for r in subset if r.get('Result')=='HIT')}/{len(subset)}", hit_rate(subset)])
        return out

    output = []

    # Overall
    output.append(["📊 TRACK RECORD", f"As of {date.today().isoformat()}", ""])
    output.append(["", "", ""])
    output.append(["Overall", f"{len(graded)} picks graded", hit_rate(graded)])
    output.append(["", "", ""])

    # By final call signal
    signals = sorted(set(r.get("Final Call","") for r in graded))
    sig_groups = [(s, [r for r in graded if r.get("Final Call") == s]) for s in signals]
    output += section("📣 By Signal", sig_groups)
    output.append(["", "", ""])

    # By stat
    stats = sorted(set(r.get("Stat","") for r in graded))
    stat_groups = [(s, [r for r in graded if r.get("Stat") == s]) for s in stats]
    output += section("📈 By Stat Type", stat_groups)
    output.append(["", "", ""])

    # By direction
    over_picks  = [r for r in graded if "OVER"  in _DIRECTION_MAP.get(r.get("Final Call",""), "")]
    under_picks = [r for r in graded if "UNDER" in _DIRECTION_MAP.get(r.get("Final Call",""), "")]
    output += section("↕️ By Direction", [("OVER calls", over_picks), ("UNDER calls", under_picks)])
    output.append(["", "", ""])

    # By context factors
    b2b_picks     = [r for r in graded if "B2B"      in r.get("Rest","")]
    fresh_picks   = [r for r in graded if "rest"     in r.get("Rest","") and "B2B" not in r.get("Rest","")]
    soft_d_picks  = [r for r in graded if "🟢 Soft"  in r.get("Matchup","")]
    tough_d_picks = [r for r in graded if "🔴 Tough" in r.get("Matchup","")]
    hot_picks     = [r for r in graded if "📈 Hot"   in r.get("Trend","")]
    cold_picks    = [r for r in graded if "📉 Cold"  in r.get("Trend","")]
    consistent    = [r for r in graded if "🎯"       in r.get("Consistency","")]
    boombus       = [r for r in graded if "🎲"       in r.get("Consistency","")]

    output += section("🔍 Context Factor Analysis", [
        ("Back-to-Back games",   b2b_picks),
        ("Well rested (3+ days)", fresh_picks),
        ("Soft matchup 🟢",      soft_d_picks),
        ("Tough matchup 🔴",     tough_d_picks),
        ("Player trending 📈",   hot_picks),
        ("Player trending 📉",   cold_picks),
        ("Consistent player 🎯", consistent),
        ("Boom/Bust player 🎲",  boombus),
    ])
    output.append(["", "", ""])

    # Recent 10 picks
    recent = sorted(graded, key=lambda r: r.get("Date",""), reverse=True)[:10]
    output.append(["🕒 Last 10 Graded Picks", "", ""])
    output.append(["Date", "Player", "Stat", "Line", "Call", "Actual", "Result"])
    for r in recent:
        output.append([
            r.get("Date",""), r.get("Player",""), r.get("Stat",""),
            r.get("Line",""), r.get("Final Call",""),
            r.get("Actual",""), r.get("Result",""),
        ])

    record_ws.clear()
    record_ws.update(range_name="A1", values=output)
    logger.info(f"  Track Record updated: {len(graded)} graded picks")
