"""
line_movement.py — Track prop line movement from open to current.

Line movement is one of the strongest signals in sports betting:
  - Steam move: line moves quickly in one direction = sharp money
  - Reverse line movement (RLM): line moves against public betting % = sharp action
  - Opening line direction: where the line started vs where it is now

Key insight: If 70% of bets are on the over but the line moves DOWN, that's
sharp money fading the public — one of the highest-edge situations in props.

Storage: SQLite table `line_snapshots` in app_data.db
Each time the pipeline runs, we record the current line for all props.
The first record for a prop key on a given date becomes the "opening line."
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

DB_PATH = Path("app_data.db")

# Minimum line movement to flag as significant (in prop units)
STEAM_THRESHOLD     = 0.5   # e.g. PTS line moved from 24.5 to 25.0 = notable
SHARP_THRESHOLD     = 1.0   # strong steam move


def init_line_movement_db():
    """Create line_snapshots table if it doesn't exist."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS line_snapshots (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_key    TEXT NOT NULL,     -- date|player|stat
                game_date       TEXT NOT NULL,
                player_name     TEXT NOT NULL,
                team            TEXT NOT NULL,
                stat            TEXT NOT NULL,
                line            REAL NOT NULL,
                over_odds       INTEGER,
                under_odds      INTEGER,
                book            TEXT DEFAULT 'consensus',
                recorded_at     TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_ls_key_date
            ON line_snapshots(snapshot_key, game_date)
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS line_movement_cache (
                snapshot_key    TEXT PRIMARY KEY,   -- date|player|stat|line
                game_date       TEXT NOT NULL,
                opening_line    REAL,
                current_line    REAL,
                movement        REAL,               -- current - opening
                movement_pct    REAL,               -- pct change
                direction       TEXT,               -- UP / DOWN / FLAT
                signal          TEXT,               -- STEAM / REVERSE / FLAT
                steam_move      INTEGER DEFAULT 0,
                sharp_move      INTEGER DEFAULT 0,
                first_seen      TEXT,
                last_updated    TEXT
            )
        """)
        conn.commit()


def record_lines(prop_cards: list[dict], game_date: str):
    """
    Snapshot current lines from tonight's prop cards into line_snapshots.
    Called each time the pipeline runs (typically every 15-30 min).
    """
    init_line_movement_db()
    now = datetime.now().isoformat()

    rows = []
    for card in prop_cards:
        key = f"{game_date}|{card.get('player_name','')}|{card.get('stat','')}"
        rows.append((
            key,
            game_date,
            card.get("player_name", ""),
            card.get("team", ""),
            card.get("stat", ""),
            card.get("line"),
            card.get("odds"),           # over odds
            card.get("best_under_odds"),
            card.get("best_over_book", "consensus"),
            now,
        ))

    with sqlite3.connect(DB_PATH) as conn:
        conn.executemany("""
            INSERT INTO line_snapshots
            (snapshot_key, game_date, player_name, team, stat, line,
             over_odds, under_odds, book, recorded_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, rows)
        conn.commit()

    logger.info(f"  Recorded {len(rows)} line snapshots for {game_date}")
    _update_movement_cache(game_date)


def _update_movement_cache(game_date: str):
    """
    Compute opening vs current line for each prop on game_date
    and upsert into line_movement_cache.
    """
    with sqlite3.connect(DB_PATH) as conn:
        # For each unique snapshot_key on this date, get min (opening) and max recorded_at (current)
        rows = conn.execute("""
            SELECT snapshot_key, player_name, team, stat,
                   MIN(recorded_at) as first_seen,
                   MAX(recorded_at) as last_updated,
                   -- Opening line = line at first snapshot
                   (SELECT line FROM line_snapshots l2
                    WHERE l2.snapshot_key = ls.snapshot_key
                    ORDER BY l2.recorded_at ASC LIMIT 1) as opening_line,
                   -- Current line = most recent
                   (SELECT line FROM line_snapshots l3
                    WHERE l3.snapshot_key = ls.snapshot_key
                    ORDER BY l3.recorded_at DESC LIMIT 1) as current_line
            FROM line_snapshots ls
            WHERE game_date = ?
            GROUP BY snapshot_key
        """, (game_date,)).fetchall()

    now = datetime.now().isoformat()
    upserts = []
    for (snap_key, player, team, stat, first_seen, last_updated, opening, current) in rows:
        if opening is None or current is None:
            continue

        movement = round(current - opening, 2)
        movement_pct = round(movement / opening * 100, 1) if opening != 0 else 0.0

        if abs(movement) < 0.05:
            direction = "FLAT"
            signal    = "FLAT"
            steam     = 0
            sharp     = 0
        elif movement > 0:
            direction = "UP"     # line moved up = books adjusting for over action
            steam     = 1 if abs(movement) >= STEAM_THRESHOLD else 0
            sharp     = 1 if abs(movement) >= SHARP_THRESHOLD else 0
            signal    = "STEAM UP" if steam else "DRIFT UP"
        else:
            direction = "DOWN"   # line moved down = books adjusting for under action
            steam     = 1 if abs(movement) >= STEAM_THRESHOLD else 0
            sharp     = 1 if abs(movement) >= SHARP_THRESHOLD else 0
            signal    = "STEAM DOWN" if steam else "DRIFT DOWN"

        cache_key = f"{game_date}|{player}|{stat}|{current}"
        upserts.append((
            cache_key, game_date, opening, current, movement, movement_pct,
            direction, signal, steam, sharp, first_seen, last_updated
        ))

    with sqlite3.connect(DB_PATH) as conn:
        conn.executemany("""
            INSERT OR REPLACE INTO line_movement_cache
            (snapshot_key, game_date, opening_line, current_line, movement,
             movement_pct, direction, signal, steam_move, sharp_move,
             first_seen, last_updated)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, upserts)
        conn.commit()


def get_movement(player_name: str, stat: str, line: float, game_date: str = None) -> dict:
    """
    Return line movement data for a specific prop.

    Returns dict with:
      opening_line, current_line, movement (units), direction (UP/DOWN/FLAT),
      signal (STEAM UP / STEAM DOWN / DRIFT / FLAT), steam_move (bool),
      sharp_move (bool), display (human-readable string for the card)
    """
    if game_date is None:
        game_date = date.today().isoformat()

    init_line_movement_db()

    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("""
            SELECT opening_line, current_line, movement, movement_pct,
                   direction, signal, steam_move, sharp_move, first_seen
            FROM line_movement_cache
            WHERE game_date = ?
              AND snapshot_key LIKE ?
            ORDER BY last_updated DESC
            LIMIT 1
        """, (game_date, f"{game_date}|{player_name}|{stat}|%")).fetchone()

    if not row:
        return {"available": False}

    opening, current, movement, movement_pct, direction, signal, steam, sharp, first_seen = row

    # Build display string
    if abs(movement) < 0.05:
        display = "— No movement"
        badge   = ""
    else:
        arrow   = "↑" if direction == "UP" else "↓"
        display = f"{arrow} {opening} → {current} ({movement:+.1f})"
        if sharp:
            badge = "🔥 SHARP MOVE"
        elif steam:
            badge = "⚡ STEAM"
        else:
            badge = f"{'📈' if direction == 'UP' else '📉'} DRIFT"
        display = f"{badge} {display}"

    return {
        "available":     True,
        "opening_line":  opening,
        "current_line":  current,
        "movement":      movement,
        "movement_pct":  movement_pct,
        "direction":     direction,
        "signal":        signal,
        "steam_move":    bool(steam),
        "sharp_move":    bool(sharp),
        "display":       display,
        "first_seen":    first_seen,
        # Betting interpretation
        "over_signal":   direction == "DOWN" and steam,   # line dropped = under bets? or sharp over
        "under_signal":  direction == "UP"  and steam,    # line rose = over bets? or sharp under
    }


def get_all_movements(game_date: str = None) -> dict[str, dict]:
    """
    Return all line movement data for a given date, keyed by 'player|stat|line'.
    Efficient bulk load for pipeline use.
    """
    if game_date is None:
        game_date = date.today().isoformat()

    init_line_movement_db()

    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("""
            SELECT snapshot_key, opening_line, current_line, movement, movement_pct,
                   direction, signal, steam_move, sharp_move
            FROM line_movement_cache
            WHERE game_date = ?
        """, (game_date,)).fetchall()

    result = {}
    for (snap_key, opening, current, movement, movement_pct,
         direction, signal, steam, sharp) in rows:
        # snap_key format: date|player|stat|line
        parts = snap_key.split("|", 3)
        if len(parts) < 4:
            continue
        lookup_key = f"{parts[1]}|{parts[2]}|{parts[3]}"

        arrow = "↑" if direction == "UP" else ("↓" if direction == "DOWN" else "→")
        if abs(movement or 0) < 0.05:
            display = "— No movement"
            badge   = ""
        else:
            badge = "🔥 SHARP" if sharp else ("⚡ STEAM" if steam else "")
            display = f"{badge} {arrow} {opening} → {current} ({movement:+.1f})".strip()

        result[lookup_key] = {
            "available":    True,
            "opening_line": opening,
            "current_line": current,
            "movement":     movement,
            "movement_pct": movement_pct,
            "direction":    direction,
            "signal":       signal,
            "steam_move":   bool(steam),
            "sharp_move":   bool(sharp),
            "display":      display,
        }

    return result


def get_steam_moves(game_date: str = None) -> list[dict]:
    """Return all steam/sharp moves for tonight. Used for the alert panel."""
    if game_date is None:
        game_date = date.today().isoformat()

    init_line_movement_db()

    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("""
            SELECT snapshot_key, opening_line, current_line, movement,
                   direction, signal, sharp_move
            FROM line_movement_cache
            WHERE game_date = ? AND steam_move = 1
            ORDER BY ABS(movement) DESC
        """, (game_date,)).fetchall()

    result = []
    for (snap_key, opening, current, movement, direction, signal, sharp) in rows:
        parts = snap_key.split("|", 3)
        if len(parts) < 4:
            continue
        result.append({
            "player_name": parts[1],
            "stat":        parts[2],
            "line":        float(parts[3]),
            "opening":     opening,
            "current":     current,
            "movement":    movement,
            "direction":   direction,
            "signal":      signal,
            "sharp":       bool(sharp),
        })
    return result
