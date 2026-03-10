"""
dfs_app.py — NBA DFS Optimizer Web App
=======================================
Run: python3 dfs_app.py
Then open: http://localhost:5001

Uses the same BDL API data as the props app (app.py).
Upload a FanDuel or DraftKings salary CSV to get projections.
"""
from __future__ import annotations

import csv
import io
import json
import logging
import threading
from datetime import date
from pathlib import Path

import pandas as pd
from flask import Flask, jsonify, request, send_from_directory, Response

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

app = Flask(__name__, template_folder="templates")

CACHE_PATH   = Path("dfs_cache.json")
SALARY_CACHE = Path("dfs_salaries.json")

_pipeline_status = {"status": "idle", "message": "Ready"}
_dfs_data        = {}


# ── Name normalization ────────────────────────────────────────────────────────

def _norm(s: str) -> str:
    import unicodedata, re
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]", "", s.lower())


# ── Team abbreviation translation ─────────────────────────────────────────────
# FD/DK use non-standard abbreviations. BDL API uses standard NBA abbrs.

FD_TO_BDL = {
    "NY":  "NYK",
    "GS":  "GSW",
    "SA":  "SAS",
    "NO":  "NOP",
    "PHO": "PHX",
}

def _fix_team(abbr: str) -> str:
    return FD_TO_BDL.get(abbr.upper(), abbr.upper())


# ── Salary CSV parsing ────────────────────────────────────────────────────────

def parse_fd_csv(content: str) -> list:
    reader  = csv.DictReader(io.StringIO(content))
    players = []
    for row in reader:
        name   = (row.get("Nickname") or "").strip()
        salary = row.get("Salary") or "0"
        pos    = row.get("Position") or ""
        team   = row.get("Team") or ""
        game   = row.get("Game") or ""
        if not name:
            continue
        try:
            salary = int(str(salary).replace(",", "").replace("$", ""))
        except ValueError:
            continue
        if salary < 3000:
            continue
        players.append({
            "player_name": name,
            "salary_fd":   salary,
            "position_fd": pos.split("/")[0].strip(),
            "team":        _fix_team(team),
            "game_key":    game,
        })
    return players


def parse_dk_csv(content: str) -> list:
    reader  = csv.DictReader(io.StringIO(content))
    players = []
    for row in reader:
        name   = (row.get("Name") or "").strip()
        salary = row.get("Salary") or "0"
        pos    = row.get("Position") or ""
        team   = row.get("TeamAbbrev") or ""
        game   = row.get("Game Info") or ""
        if not name:
            continue
        try:
            salary = int(str(salary).replace(",", "").replace("$", ""))
        except ValueError:
            continue
        if salary < 3000:
            continue
        players.append({
            "player_name": name,
            "salary_dk":   salary,
            "position_dk": pos.split("/")[0].strip(),
            "team":        _fix_team(team),
            "game_key":    game.split(" ")[0] if game else "",
        })
    return players


# ── Pipeline ──────────────────────────────────────────────────────────────────

def run_dfs_pipeline(salary_players_fd: list, salary_players_dk: list, game_date: str = None):
    global _dfs_data, _pipeline_status

    try:
        import nba_data
        import context as ctx_module
        import injuries as inj_module
        from dfs_projections import build_player_projection
        from bdl_client import get_client as _get_bdl
        from app import _compute_blowout_risk

        if not game_date:
            game_date = str(date.today())

        # Merge FD + DK salary data by player name
        all_salary = {}
        for p in salary_players_fd:
            key = _norm(p["player_name"])
            all_salary.setdefault(key, {}).update(p)
        for p in salary_players_dk:
            key = _norm(p["player_name"])
            all_salary.setdefault(key, {}).update(p)

        if not all_salary:
            _pipeline_status = {"status": "error", "message": "No salary data loaded — upload a CSV first"}
            return

        teams = list({p["team"] for p in all_salary.values() if p.get("team")})
        logger.info(f"DFS pipeline: {len(all_salary)} players, {len(teams)} teams")

        # 1. Game logs (same as props app step 2)
        # nba_data caches internally — this populates _team_df_cache
        _pipeline_status = {"status": "running", "message": f"Loading game logs for {len(teams)} teams..."}
        for team in teams:
            try:
                nba_data.get_team_game_log_df(team)
            except Exception as e:
                logger.warning(f"Could not fetch {team}: {e}")

        # Build player_logs lookup keyed by "player_name|TEAM"
        # nba_data produces GAME_DATE (uppercase) — sort on that
        player_logs = {}
        for team in teams:
            try:
                df = nba_data.get_team_game_log_df(team)
                if df.empty:
                    logger.warning(f"Empty game log for {team}")
                    continue
                for pname, plog in df.groupby("player_name"):
                    player_logs[f"{pname}|{team}"] = (
                        plog.sort_values("GAME_DATE", ascending=False).reset_index(drop=True)
                    )
                logger.info(f"  {team}: {df['player_name'].nunique()} players loaded")
            except Exception as e:
                logger.warning(f"Error processing {team}: {e}")

        logger.info(f"Total player logs: {len(player_logs)}")

        # 2. Today's schedule for opponent/location context
        _pipeline_status = {"status": "running", "message": "Loading schedule..."}
        games = nba_data.get_todays_games(game_date)
        team_to_opponent = {}
        team_to_location = {}
        for g in games:
            h = g.get("home_team_abbr", "")
            a = g.get("away_team_abbr", "")
            if h and a:
                team_to_opponent[h] = a
                team_to_opponent[a] = h
                team_to_location[h] = "Home"
                team_to_location[a] = "Away"

        # 3. Full df for context module
        full_df = nba_data.get_full_df(teams)

        # 4. Blowout risk from game odds
        blowout_map = {}
        try:
            raw_odds = _get_bdl().get_game_odds(game_date)
            odds_by_game = {}
            for row in raw_odds:
                gid = row.get("game_id")
                if gid:
                    odds_by_game.setdefault(gid, []).append(row)
            for gid, rows in odds_by_game.items():
                chosen = next((r for r in rows if r.get("vendor") == "draftkings"), rows[0])
                hs = chosen.get("spread_home_value")
                gt = chosen.get("total_value")
                for g in games:
                    if g.get("game_id") == gid:
                        for team in [g.get("home_team_abbr"), g.get("away_team_abbr")]:
                            if team:
                                blowout_map[team] = _compute_blowout_risk(
                                    float(hs) if hs is not None else None,
                                    float(gt) if gt is not None else None,
                                )
        except Exception as e:
            logger.info(f"Blowout risk skipped: {e}")

        # 5. Injury intelligence
        _pipeline_status = {"status": "running", "message": "Analyzing injuries..."}
        opp_targets = {}
        try:
            inj_intel   = inj_module.build_injury_intelligence(
                team_abbrs=teams,
                team_df_getter=nba_data.get_team_game_log_df,
            )
            opp_targets = inj_intel.get("targets", {})
        except Exception as e:
            logger.info(f"Injury intelligence skipped: {e}")

        # 6. Build projections
        _pipeline_status = {"status": "running", "message": "Building projections..."}
        projections = []

        for norm_key, sal in all_salary.items():
            pname = sal.get("player_name", "")
            team  = sal.get("team", "")
            if not pname or not team:
                continue

            # Exact match first, then fuzzy name-only match
            log = player_logs.get(f"{pname}|{team}", pd.DataFrame())
            if log.empty:
                for k, v in player_logs.items():
                    if _norm(k.split("|")[0]) == norm_key:
                        log = v
                        break

            opponent = team_to_opponent.get(team, "")
            location = team_to_location.get(team, "Home")

            ctx = {}
            try:
                ctx = ctx_module.build_player_context(
                    player_name    = pname,
                    team_abbr      = team,
                    opponent_abbr  = opponent,
                    player_log     = log,
                    full_df        = full_df,
                    game_date      = date.fromisoformat(game_date),
                    today_location = location,
                )
            except Exception:
                pass

            blowout  = blowout_map.get(team)
            opp_data = None
            for targets in opp_targets.values():
                for t in targets:
                    if _norm(t.get("player_name", "")) == norm_key:
                        opp_data = t
                        break

            proj = build_player_projection(
                player_name = pname,
                team        = team,
                log         = log,
                ctx         = ctx,
                blowout     = blowout,
                opportunity = opp_data,
                salary_dk   = sal.get("salary_dk"),
                salary_fd   = sal.get("salary_fd"),
                position_dk = sal.get("position_dk"),
                position_fd = sal.get("position_fd"),
            )

            if proj:
                proj["opponent"] = opponent
                proj["location"] = location
                proj["game_key"] = sal.get("game_key", "")
                projections.append(proj)

        projections.sort(key=lambda x: x.get("proj_dk", 0), reverse=True)
        logger.info(f"Built {len(projections)} projections")

        _dfs_data = {
            "projections": projections,
            "game_date":   game_date,
            "teams":       teams,
            "games":       games,
        }

        try:
            CACHE_PATH.write_text(json.dumps(_dfs_data, default=str))
        except Exception:
            pass

        _pipeline_status = {"status": "done", "message": f"{len(projections)} players projected"}

    except Exception as e:
        logger.exception("DFS pipeline error")
        _pipeline_status = {"status": "error", "message": str(e)}


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("templates", "dfs.html")

@app.route("/api/dfs/status")
def dfs_status():
    return jsonify(_pipeline_status)

@app.route("/api/dfs/upload_salary", methods=["POST"])
def upload_salary():
    data     = request.get_json()
    platform = data.get("platform", "fd").lower()
    content  = data.get("content", "")
    if not content:
        return jsonify({"error": "No content"}), 400
    try:
        players = parse_fd_csv(content) if platform == "fd" else parse_dk_csv(content)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    existing = {}
    if SALARY_CACHE.exists():
        try:
            existing = json.loads(SALARY_CACHE.read_text())
        except Exception:
            pass
    existing[platform] = players
    SALARY_CACHE.write_text(json.dumps(existing))

    logger.info(f"Salary loaded: {len(players)} players ({platform.upper()})")
    return jsonify({"players": len(players), "platform": platform})

@app.route("/api/dfs/salary_status")
def salary_status():
    if not SALARY_CACHE.exists():
        return jsonify({"dk": 0, "fd": 0})
    try:
        cached = json.loads(SALARY_CACHE.read_text())
        return jsonify({"dk": len(cached.get("dk", [])), "fd": len(cached.get("fd", []))})
    except Exception:
        return jsonify({"dk": 0, "fd": 0})

@app.route("/api/dfs/run", methods=["POST"])
def run_pipeline():
    global _pipeline_status
    if _pipeline_status.get("status") == "running":
        return jsonify({"error": "Already running"}), 409

    salary_fd, salary_dk = [], []
    if SALARY_CACHE.exists():
        try:
            cached    = json.loads(SALARY_CACHE.read_text())
            salary_fd = cached.get("fd", [])
            salary_dk = cached.get("dk", [])
        except Exception:
            pass

    data      = request.get_json() or {}
    game_date = data.get("date", str(date.today()))

    _pipeline_status = {"status": "running", "message": "Starting..."}
    threading.Thread(
        target=run_dfs_pipeline,
        args=(salary_fd, salary_dk, game_date),
        daemon=True,
    ).start()
    return jsonify({"started": True})

@app.route("/api/dfs/projections")
def get_projections():
    if not _dfs_data and CACHE_PATH.exists():
        try:
            return jsonify(json.loads(CACHE_PATH.read_text()))
        except Exception:
            pass
    return jsonify(_dfs_data or {"projections": [], "game_date": str(date.today())})

@app.route("/api/dfs/optimize", methods=["POST"])
def optimize():
    from dfs_optimizer import optimize_lineup, optimize_multiple_lineups, optimize_pick6

    data      = request.get_json() or {}
    platform  = data.get("platform", "dk").lower()
    n         = int(data.get("n_lineups", 1))
    locked    = data.get("locked", [])
    excluded  = data.get("excluded", [])
    stack     = data.get("game_stack")
    min_stack = int(data.get("min_stack", 2))

    projs = _dfs_data.get("projections", [])
    if not projs and CACHE_PATH.exists():
        try:
            projs = json.loads(CACHE_PATH.read_text()).get("projections", [])
        except Exception:
            pass
    if not projs:
        return jsonify({"error": "No projections — run pipeline first"}), 400

    if platform == "pick6":
        lineup = optimize_pick6(projs)
        return jsonify({"lineups": [lineup] if lineup else []})

    if n == 1:
        lineups = [optimize_lineup(projs, platform, locked, excluded, stack, min_stack)]
    else:
        lineups = optimize_multiple_lineups(projs, platform, n, locked, excluded, game_stack=stack, min_stack=min_stack)

    return jsonify({"lineups": [l for l in lineups if l]})

@app.route("/api/dfs/export_csv", methods=["POST"])
def export_csv():
    from dfs_optimizer import export_dk_csv, export_fd_csv

    data     = request.get_json() or {}
    platform = data.get("platform", "dk")
    lineups  = data.get("lineups", [])

    csv_str  = export_dk_csv(lineups) if platform == "dk" else export_fd_csv(lineups)
    filename = "dk_lineups.csv" if platform == "dk" else "fd_lineups.csv"

    return Response(
        csv_str,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


if __name__ == "__main__":
    print("\n" + "=" * 50)
    print("  NBA DFS Optimizer")
    print("  Open: http://localhost:5001")
    print("=" * 50 + "\n")

    if CACHE_PATH.exists():
        try:
            _dfs_data = json.loads(CACHE_PATH.read_text())
            logger.info(f"Loaded cached data: {len(_dfs_data.get('projections', []))} players")
        except Exception:
            pass

    app.run(host="0.0.0.0", port=5001, debug=False)
