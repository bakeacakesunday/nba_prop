"""
main.py — NBA Player Props Research CLI Tool
============================================
Data source: balldontlie.io API (auto-fetched, no CSV needed)

SETUP:
1. Make sure your API key is set in bdl_client.py
2. Run: python3 main.py --sheet "NBA Props"

That's it — tonight's schedule and props are pulled automatically.

Usage:
    python3 main.py --sheet "NBA Props"
    python3 main.py --date 2026-02-24 --sheet "NBA Props"
    python3 main.py --self-test
    python3 main.py --list-teams
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

import pandas as pd

import nba_data
import metrics
import sheets
import context as ctx_module
import tracker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── Name matching ─────────────────────────────────────────────────────────────

def _match_lines_to_players(
    lines: list[dict],
    all_players: list[dict],
) -> tuple[dict, dict, list]:
    import unicodedata, re

    def norm(s: str) -> str:
        s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
        return re.sub(r"[^a-z0-9]", "", s.lower())

    norm_to_canon = {norm(p["player_name"]): p["player_name"] for p in all_players}
    canonical_names = [p["player_name"] for p in all_players]

    matched_lines: dict[str, dict[str, list[float]]] = {}
    matched_odds:  dict[str, float | None] = {}
    notes = []

    logger.info(f"  Matching {len(lines)} line entries against {len(canonical_names)} roster players...")

    for entry in lines:
        raw_name = entry.get("player_name", "").strip()
        stat     = entry.get("stat_type", "").strip().upper()
        line_val = float(entry.get("line") or 0)
        odds_val = entry.get("odds")

        if not raw_name or not stat:
            continue

        n     = norm(raw_name)
        canon = norm_to_canon.get(n)

        if canon is None:
            for cname in canonical_names:
                if norm(cname) == n or n in norm(cname) or norm(cname) in n:
                    canon = cname
                    break

        if canon is None:
            tokens = n.split() if " " in raw_name else [n[:len(n)//2], n[len(n)//2:]]
            for cname in canonical_names:
                cn = norm(cname)
                if all(t in cn for t in tokens if len(t) > 2):
                    canon = cname
                    break

        if canon is None:
            logger.warning(f"  NO MATCH: '{raw_name}'")
            notes.append({
                "player_name": raw_name,
                "issue": "No match found in roster",
                "suggestions": ", ".join(canonical_names[:5]),
            })
            continue

        logger.info(f"  ✓ '{raw_name}' → '{canon}' | {stat} {line_val}")
        matched_lines.setdefault(canon, {}).setdefault(stat, []).append(line_val)
        matched_odds[f"{canon}|{stat}|{line_val}"] = odds_val

    logger.info(f"  Matched {len(matched_lines)} players, {sum(len(v) for v in matched_lines.values())} stat lines")
    return matched_lines, matched_odds, notes


# ── Self-test ─────────────────────────────────────────────────────────────────

def _run_self_test() -> None:
    print("\n=== SELF-TEST: Checking balldontlie API ===\n")

    from bdl_client import get_client
    client = get_client()

    print("Checking tonight's schedule...")
    games = nba_data.get_todays_games()
    if games:
        print(f"✓ Found {len(games)} games tonight:")
        for g in games:
            print(f"  {g['away_team_abbr']} @ {g['home_team_abbr']}")
    else:
        print("  No games today (check the date or try --date YYYY-MM-DD)")

    print("\nChecking team data (SAC)...")
    df = nba_data.get_team_game_log_df("SAC")
    if not df.empty:
        print(f"✓ SAC: {len(df)} player-game rows loaded")
        print(f"  Date range: {df['GAME_DATE'].min().date()} → {df['GAME_DATE'].max().date()}")
    else:
        print("  Could not fetch SAC data — check your API key in bdl_client.py")

    print("\nSelf-test complete.\n")


# ── Main pipeline ─────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="NBA Player Props Research Tool → Google Sheets (balldontlie.io)"
    )
    parser.add_argument(
        "--teams",
        default=None,
        help=(
            "Optional: comma-separated team abbreviations to analyze. "
            "If not provided, tonight's schedule is fetched automatically. "
            "Example: --teams SAS,DET,SAC"
        ),
    )
    parser.add_argument(
        "--sheet",
        default="NBA Props",
        help='Google Spreadsheet name. Default: "NBA Props".',
    )
    parser.add_argument(
        "--roster-lookback",
        type=int,
        default=5,
        dest="roster_lookback",
        help="Games to look back when building roster. Default: 5.",
    )
    parser.add_argument(
        "--date",
        default=date.today().strftime("%Y-%m-%d"),
        help="Date to analyze (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--refresh-cache",
        action="store_true",
        dest="refresh_cache",
        help="Force re-fetch all data from API.",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        dest="self_test",
        help="Run a quick sanity check and exit.",
    )
    parser.add_argument(
        "--list-teams",
        action="store_true",
        dest="list_teams",
        help="Print all NBA teams and exit.",
    )
    parser.add_argument(
        "--no-auto-props",
        action="store_true",
        dest="no_auto_props",
        help="Skip auto-fetching props (use Lines tab only).",
    )
    parser.add_argument(
        "--edge-threshold",
        type=float,
        default=0.05,
        dest="edge_threshold",
        help="Minimum edge to flag as a bet. Default: 0.05 (5%%).",
    )

    args = parser.parse_args()

    if args.self_test:
        _run_self_test()
        return

    if args.list_teams:
        teams = nba_data.get_all_teams_in_csv()
        print("\nAll NBA teams:")
        for t in teams:
            print(f"  {t}")
        return

    date_str       = args.date
    sheet_name     = args.sheet
    edge_threshold = args.edge_threshold

    print(f"\n{'='*60}")
    print(f"  NBA Props Research Tool")
    print(f"  Date: {date_str}  |  Sheet: {sheet_name}")
    print(f"{'='*60}\n")

    # ── 1. Get tonight's schedule ────────────────────────────────────────────
    logger.info("Step 1/7: Fetching tonight's schedule...")

    if args.teams:
        team_list = [t.strip().upper() for t in args.teams.split(",")]
        # Build fake game list from manually provided teams
        games = []
        for i in range(0, len(team_list) - 1, 2):
            games.append({
                "game_id":        0,
                "away_team_abbr": team_list[i],
                "home_team_abbr": team_list[i+1],
                "away_team_id":   None,
                "home_team_id":   None,
                "game_time":      "TBD",
                "status":         "scheduled",
            })
        if len(team_list) % 2 == 1:
            games.append({
                "game_id":        0,
                "away_team_abbr": team_list[-1],
                "home_team_abbr": "TBD",
                "away_team_id":   None,
                "home_team_id":   None,
                "game_time":      "TBD",
                "status":         "scheduled",
            })
        logger.info(f"  Using manually provided teams: {team_list}")
    else:
        games = nba_data.get_todays_games(date_str)
        if not games:
            print(f"\n⚠️  No games found for {date_str}.")
            print("   If this is wrong, try: --date YYYY-MM-DD")
            print("   Or manually specify teams: --teams BOS,MIA,LAL,GSW\n")
            return
        team_list = []
        for g in games:
            team_list.append(g["away_team_abbr"])
            team_list.append(g["home_team_abbr"])
        game_summaries = [g['away_team_abbr'] + "@" + g['home_team_abbr'] for g in games]
        logger.info(f"  Found {len(games)} games: {game_summaries}")

    # ── 2. Connect to Google Sheets ──────────────────────────────────────────
    logger.info("Step 2/7: Connecting to Google Sheets...")
    spreadsheet = sheets.get_or_create_spreadsheet(sheet_name)
    tabs        = sheets.ensure_tabs(spreadsheet)
    sheets.ensure_lines_template(tabs["Lines"])

    # ── 3. Fetch game logs ───────────────────────────────────────────────────
    logger.info("Step 3/7: Fetching player game logs from balldontlie.io...")
    for team in team_list:
        logger.info(f"  Fetching {team}...")
        nba_data.get_team_game_log_df(team, refresh=args.refresh_cache)

    # ── 4. Build rosters ─────────────────────────────────────────────────────
    logger.info("Step 4/7: Building rosters...")
    roster_all   = []
    player_logs  = {}

    for team in team_list:
        players = nba_data.get_active_roster_for_team(
            team, n_lookback=args.roster_lookback
        )
        roster_all.extend(players)
        for p in players:
            key = f"{p['player_name']}|{p['team']}"
            log = nba_data.get_player_game_log(p["player_name"], p["team"])
            player_logs[key] = log

    logger.info(f"  Total roster: {len(roster_all)} players across {len(team_list)} teams")

    # ── 5. Auto-fetch props ──────────────────────────────────────────────────
    logger.info("Step 5/7: Fetching props and lines...")

    auto_props = []
    raw_props_by_game: dict = {}
    player_id_lookup: dict  = {}
    if not args.no_auto_props:
        game_ids = [g["game_id"] for g in games if g.get("game_id")]
        if game_ids:
            logger.info(f"  Auto-fetching props for {len(game_ids)} games...")
            auto_props, raw_props_by_game, player_id_lookup = nba_data.get_props_for_games(game_ids)
            if auto_props:
                logger.info(f"  ✓ Fetched {len(auto_props)} props automatically")
                sheets.write_auto_props(tabs["Lines"], auto_props)
            else:
                logger.info("  No props returned (may require paid API tier)")
                logger.info("  You can still enter props manually in the Lines tab")

    # Read lines (includes any auto-fetched + manually entered)
    lines_data = sheets.read_lines(tabs["Lines"])
    logger.info(f"  Found {len(lines_data)} prop line entries.")

    matched_lines, matched_odds, match_notes = _match_lines_to_players(lines_data, roster_all)

    # ── 6. Compute metrics ───────────────────────────────────────────────────
    logger.info("Step 6/7: Computing metrics...")
    windows = {"Last5": 5, "Last10": 10, "Last20": 20}
    window_rows: dict[str, list[dict]] = {k: [] for k in windows}
    window_metrics_by_player: dict[str, dict] = {}

    for p in roster_all:
        key   = f"{p['player_name']}|{p['team']}"
        log   = player_logs.get(key, pd.DataFrame())
        pname = p["player_name"]
        team  = p["team"]
        line_map = matched_lines.get(pname, {})

        window_metrics_by_player[pname] = {}
        for window_name, n_games in windows.items():
            if log.empty:
                row = {"player_name": pname, "team": team, "games_count": 0, "minutes_avg": None}
            else:
                row = metrics.compute_metrics(log, n_games=n_games, line_map=line_map)
                row["player_name"] = pname
                row["team"] = team
            window_rows[window_name].append(row)
            window_metrics_by_player[pname][window_name] = row

    # ── Build Context ────────────────────────────────────────────────────────
    logger.info("Building context (rest, trends, matchups)...")
    full_df = nba_data.get_full_df(team_list)
    target_date_obj = date.fromisoformat(date_str)

    team_to_opponent: dict[str, str] = {}
    team_to_location: dict[str, str] = {}
    for g in games:
        h = g.get("home_team_abbr", "")
        a = g.get("away_team_abbr", "")
        if h and a:
            team_to_opponent[h] = a
            team_to_opponent[a] = h
            team_to_location[h] = "Home"
            team_to_location[a] = "Away"

    context_rows: list[dict] = []
    for p in roster_all:
        key      = f"{p['player_name']}|{p['team']}"
        log      = player_logs.get(key, pd.DataFrame())
        opponent = team_to_opponent.get(p["team"], "—")
        location = team_to_location.get(p["team"], "—")

        c = ctx_module.build_player_context(
            player_name    = p["player_name"],
            team_abbr      = p["team"],
            opponent_abbr  = opponent,
            player_log     = log,
            full_df        = full_df,
            game_date      = target_date_obj,
            today_location = location,
        )
        context_rows.append(c)

    context_by_player = {c["player_name"]: c for c in context_rows}

    # ── Build Value tab rows ─────────────────────────────────────────────────
    # ── Build Injury Intelligence ────────────────────────────────────────────
    logger.info("Building injury intelligence and opportunity modeling...")
    from injuries import build_injury_intelligence, format_opportunity_for_card
    injury_intel = build_injury_intelligence(
        team_abbrs     = team_list,
        team_df_getter = nba_data.get_team_game_log_df,
    )
    opp_targets = injury_intel.get("targets", {})
    if injury_intel.get("injuries"):
        out_names = [i["player_name"] for i in injury_intel["injuries"]]
        logger.info(f"  Out tonight: {', '.join(out_names)}")
    if injury_intel.get("opportunities"):
        logger.info(f"  ✓ {len(injury_intel['opportunities'])} opportunity signals found")

    roster_names = {p["player_name"] for p in roster_all}
    for pname, stat_map in matched_lines.items():
        if pname not in roster_names:
            team_for_player = next(
                (e.get("team","") for e in lines_data if e.get("player_name","").strip() == pname),
                ""
            )
            key = f"{pname}|{team_for_player}"
            if key not in player_logs:
                log = nba_data.get_player_game_log(pname, team_for_player)
                player_logs[key] = log
                roster_all.append({"player_name": pname, "team": team_for_player})

    value_rows: list[dict] = []
    for pname, stat_lines in matched_lines.items():
        team = next((p["team"] for p in roster_all if p["player_name"] == pname), "")
        for stat, line_vals in stat_lines.items():
            for line_val in line_vals:
                odds_key  = f"{pname}|{stat}|{line_val}"
                odds      = matched_odds.get(odds_key)
                impl_prob = metrics.american_odds_to_implied_prob(odds) if odds else None

                vrow: dict = {
                    "player_name":  pname,
                    "team":         team,
                    "stat_type":    stat,
                    "line":         line_val,
                    "odds":         odds if odds else "",
                    "implied_prob": f"{impl_prob:.1%}" if impl_prob else "N/A (add odds)",
                    "context":      context_by_player.get(pname, {}),
                    "final_call":   "",
                    "hr_signal":    "",
                    "ctx_label":    "",
                    "opportunity":  format_opportunity_for_card(pname, opp_targets),
                }

                for window_name, n_label in [("Last5","last5"),("Last10","last10"),("Last20","last20")]:
                    w = window_metrics_by_player.get(pname, {}).get(window_name, {})
                    n_lines = len(line_vals)
                    hr_key  = f"{stat}_hit_rate{line_vals.index(line_val)+1}" if n_lines > 1 else f"{stat}_hit_rate"
                    hr      = w.get(hr_key)

                    if hr is not None and impl_prob is not None:
                        edge   = metrics.compute_edge(hr, impl_prob)
                        signal = metrics.edge_signal(edge, edge_threshold)
                        vrow[f"{n_label}_hit_rate"] = f"{hr:.1%}"
                        vrow[f"{n_label}_edge"]     = f"{edge:+.1%}"
                        vrow[f"{n_label}_signal"]   = signal
                    elif hr is not None:
                        vrow[f"{n_label}_hit_rate"] = f"{hr:.1%}"
                        vrow[f"{n_label}_edge"]     = "add odds"
                        vrow[f"{n_label}_signal"]   = "—"
                    else:
                        vrow[f"{n_label}_hit_rate"] = "—"
                        vrow[f"{n_label}_edge"]     = "—"
                        vrow[f"{n_label}_signal"]   = "—"

                value_rows.append(vrow)

    # ── Compute final_call for each value row ────────────────────────────────
    # This ensures tracker.save_picks stores the model's actual signal,
    # not a blank string (which previously made the Picks History meaningless).
    logger.info("Computing final calls for value rows...")
    for vrow in value_rows:
        stat     = vrow.get("stat_type", "")
        line_val = float(vrow.get("line", 0))
        pname    = vrow.get("player_name", "")

        l5_hr_raw  = vrow.get("last5_hit_rate")
        l10_hr_raw = vrow.get("last10_hit_rate")
        l20_hr_raw = vrow.get("last20_hit_rate")

        # Convert "80.0%" strings to floats 0-1
        def _pct_to_float(v):
            if v is None or v == "—":
                return None
            try:
                s = str(v).replace("%", "").strip()
                f = float(s)
                return f / 100.0 if f > 1.0 else f
            except (ValueError, TypeError):
                return None

        l5_hr  = _pct_to_float(l5_hr_raw)
        l10_hr = _pct_to_float(l10_hr_raw)
        l20_hr = _pct_to_float(l20_hr_raw)

        # Compute weighted hit rate
        avail = [(hr, w) for hr, w in [(l5_hr, 0.45), (l10_hr, 0.35), (l20_hr, 0.20)]
                 if hr is not None]
        if avail:
            total_w = sum(w for _, w in avail)
            whr = sum(hr * w for hr, w in avail) / total_w
        else:
            whr = None

        # Simple final_call derivation based on weighted hit rate and distribution
        dist = vrow.get("distribution", {})
        hook = dist.get("hook_level", "")
        median = dist.get("median_l10")

        if whr is None:
            final_call = "⚪ Skip"
        elif "SEVERE" in hook:
            final_call = "⚪ Skip"
        elif whr >= 0.75 and median is not None and median > line_val:
            final_call = "🔥🔥 STRONG OVER"
        elif whr >= 0.65 and (median is None or median >= line_val):
            final_call = "✅ BET OVER"
        elif whr >= 0.55:
            final_call = "〰 Lean Over"
        elif whr <= 0.25 and (median is None or median < line_val):
            final_call = "🔥🔥 STRONG UNDER"
        elif whr <= 0.35:
            final_call = "✅ BET UNDER"
        elif whr <= 0.45:
            final_call = "〰 Lean Under"
        else:
            final_call = "⚪ Skip"

        vrow["final_call"] = final_call

        # Also compute hr_signal and ctx_label for fuller history
        if whr is not None:
            hr_pct = round(whr * 100)
            if hr_pct >= 75:
                vrow["hr_signal"] = f"🔥 {hr_pct}% wHR"
            elif hr_pct >= 65:
                vrow["hr_signal"] = f"✅ {hr_pct}% wHR"
            elif hr_pct <= 35:
                vrow["hr_signal"] = f"🔴 {hr_pct}% wHR"
            else:
                vrow["hr_signal"] = f"〰 {hr_pct}% wHR"
        else:
            vrow["hr_signal"] = "—"

        ctx_data = context_by_player.get(pname, {})
        ctx_parts = []
        if ctx_data.get("is_back_to_back") == "🔴 YES":
            ctx_parts.append("B2B")
        matchup = ctx_data.get(f"opp_{stat.lower()}_matchup", "")
        if "🟢" in str(matchup):
            ctx_parts.append("Soft D")
        elif "🔴" in str(matchup):
            ctx_parts.append("Tough D")
        trend = ctx_data.get(f"{stat}_trend", "")
        if "📈" in str(trend):
            ctx_parts.append("Hot")
        elif "📉" in str(trend):
            ctx_parts.append("Cold")
        vrow["ctx_label"] = " | ".join(ctx_parts) if ctx_parts else "Neutral"

    # ── 7. Write to Sheets ───────────────────────────────────────────────────
    logger.info("Step 7/7: Writing to Google Sheets...")

    sheets.write_today_slate(tabs["Today Slate"], games, date_str)
    logger.info("  ✓ Today Slate")

    sheets.write_roster(tabs["Roster"], roster_all)
    logger.info("  ✓ Roster")

    player_lines_for_headers = dict(matched_lines)
    for window_name in windows:
        sheets.write_stat_window_tab(
            tabs[window_name],
            window_rows[window_name],
            player_lines_for_headers,
        )
        logger.info(f"  ✓ {window_name}")

    sheets.write_value_tab(tabs["Value"], value_rows)
    logger.info(f"  ✓ Value ({len(value_rows)} lines analyzed)")

    # ── Distribution Analysis ────────────────────────────────────────────────
    logger.info("  Building distribution analysis and hook detection...")
    from distribution import build_distribution_profile
    for vrow in value_rows:
        pname    = vrow.get("player_name", "")
        stat     = vrow.get("stat_type", "")
        line_val = float(vrow.get("line", 0))
        key      = f"{pname}|{vrow.get('team','')}"
        log      = player_logs.get(key, pd.DataFrame())

        if not log.empty and stat in log.columns:
            dist = build_distribution_profile(log, stat, line_val)
        else:
            dist = {}

        vrow["distribution"] = dist

        # Feed hook score back into final call scoring
        hook_score = dist.get("hook_score", 0)
        if hook_score != 0:
            vrow["hook_score"]   = hook_score
            vrow["hook_level"]   = dist.get("hook_level", "—")
            vrow["hook_warning"] = dist.get("hook_warning", "—")

    sheets.write_distribution_tab(tabs["Distribution"], value_rows)
    hook_warnings = sum(1 for r in value_rows if "HOOK" in str(r.get("distribution", {}).get("hook_level", "")))
    logger.info(f"  ✓ Distribution ({hook_warnings} hook warnings flagged)")

    # ── Line Shopping ────────────────────────────────────────────────────────
    if raw_props_by_game and player_id_lookup:
        logger.info("  Building line shopping comparison...")
        from line_shopping import build_line_shopping_rows
        enriched_rows = build_line_shopping_rows(
            value_rows             = value_rows,
            raw_props_by_game      = raw_props_by_game,
            player_id_lookup       = player_id_lookup,
            window_metrics_by_player = window_metrics_by_player,
        )
        sheets.write_line_shopping_tab(tabs["Line Shopping"], enriched_rows)
        shopping_count = sum(1 for r in enriched_rows if r.get("shopping_opportunity") == "🛒 YES")
        logger.info(f"  ✓ Line Shopping ({shopping_count} shopping opportunities found)")

    sheets.write_context_tab(tabs["Context"], context_rows)
    logger.info(f"  ✓ Context ({len(context_rows)} players)")

    # ── Grade + save picks ───────────────────────────────────────────────────
    logger.info("Grading pending picks...")
    graded_count = tracker.grade_pending_picks(
        ws      = tabs["Picks History"],
        full_df = full_df,
        today   = date_str,
    )
    if graded_count:
        logger.info(f"  ✓ Graded {graded_count} picks")

    logger.info("Saving tonight's picks...")
    saved_count = tracker.save_picks(
        ws         = tabs["Picks History"],
        value_rows = value_rows,
        game_date  = date_str,
    )

    tracker.build_track_record(
        history_ws = tabs["Picks History"],
        record_ws  = tabs["Track Record"],
    )
    logger.info("  ✓ Track Record updated")

    if match_notes:
        sheets.write_notes(tabs["Notes"], match_notes)

    print(f"\n{'='*60}")
    print(f"  Done!  '{sheet_name}' updated.")
    print(f"  URL: {spreadsheet.url}")
    print(f"  Players analyzed: {len(roster_all)}")
    print(f"  Props lines: {len(value_rows)}")
    if auto_props:
        print(f"  ✓ Auto-fetched {len(auto_props)} props from sportsbooks")
    if graded_count:
        print(f"  ✓ Graded {graded_count} picks from previous nights")
    if saved_count:
        print(f"  ✓ Saved {saved_count} picks to history")
    if match_notes:
        print(f"  ⚠  {len(match_notes)} name issue(s) — see Notes tab.")
    print(f"\n  TABS:")
    print(f"  • Today Slate   — tonight's games (auto-detected)")
    print(f"  • Roster        — all active players")
    print(f"  • Last5/10/20   — stat averages and hit rates")
    print(f"  • Context       — rest days, trends, matchups")
    print(f"  • Value         — final calls sorted strongest first")
    print(f"  • Line Shopping — best lines + EV across all books")
    print(f"  • Distribution  — hook warnings, median vs mean, outcome clusters")
    print(f"  • Lines         — props (auto-filled + manual override)")
    print(f"  • Picks History — every pick logged with grade")
    print(f"  • Track Record  — model accuracy over time")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
