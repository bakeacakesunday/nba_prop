"""
sheets.py — Google Sheets create/update helpers using gspread + service account.
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import gspread
from google.oauth2.service_account import Credentials

from metrics import STAT_TYPES, build_stat_columns

logger = logging.getLogger(__name__)

_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

_REQUIRED_TABS = [
    "Today Slate", "Roster", "Lines", "Last5", "Last10", "Last20",
    "Value", "Line Shopping", "Distribution", "Context", "Picks History", "Track Record", "Notes"
]


def _get_client() -> gspread.Client:
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path:
        raise EnvironmentError(
            "GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.\n"
            "Run: export GOOGLE_APPLICATION_CREDENTIALS=\"$HOME/Desktop/nba_props/service_account.json\""
        )
    if not os.path.isfile(creds_path):
        raise FileNotFoundError(f"Service account key not found at: {creds_path}")
    creds = Credentials.from_service_account_file(creds_path, scopes=_SCOPES)
    return gspread.authorize(creds)


def get_or_create_spreadsheet(sheet_name: str) -> gspread.Spreadsheet:
    client = _get_client()
    try:
        spreadsheet = client.open(sheet_name)
        logger.info(f"Opened existing spreadsheet: '{sheet_name}'")
        return spreadsheet
    except gspread.exceptions.SpreadsheetNotFound:
        pass
    logger.info(f"Creating new spreadsheet: '{sheet_name}'")
    return client.create(sheet_name)


def ensure_tabs(spreadsheet: gspread.Spreadsheet) -> dict[str, gspread.Worksheet]:
    existing = {ws.title: ws for ws in spreadsheet.worksheets()}
    result = {}
    for tab in _REQUIRED_TABS:
        if tab in existing:
            result[tab] = existing[tab]
        else:
            logger.info(f"Creating tab: {tab}")
            result[tab] = spreadsheet.add_worksheet(title=tab, rows=500, cols=120)
    if "Sheet1" in existing and len(existing) == 1:
        try:
            spreadsheet.del_worksheet(existing["Sheet1"])
        except Exception:
            pass
    return result


def _write_sheet_data(ws: gspread.Worksheet, headers: list[str], rows: list[list]) -> None:
    ws.clear()
    if not headers:
        return
    ws.update(range_name="A1", values=[headers] + rows, value_input_option="USER_ENTERED")


# ── Tab writers ───────────────────────────────────────────────────────────────

def write_today_slate(ws: gspread.Worksheet, games: list[dict], date_str: str) -> None:
    headers = ["Date", "Away Team", "Home Team", "Game Time", "Game ID"]
    rows = [
        [
            date_str,
            g.get("away_team_abbr", ""),
            g.get("home_team_abbr", ""),
            g.get("game_time", ""),
            str(g.get("game_id", "")),
        ]
        for g in games
    ]
    _write_sheet_data(ws, headers, rows)


def write_roster(ws: gspread.Worksheet, roster_data: list[dict]) -> None:
    headers = ["Player Name", "Team", "Position"]
    rows = [[r["player_name"], r["team"], r.get("position", "")] for r in roster_data]
    _write_sheet_data(ws, headers, rows)


def ensure_lines_template(ws: gspread.Worksheet) -> None:
    """Write header row to Lines tab if it's empty."""
    if ws.get_all_values():
        return
    ws.update(
        range_name="A1",
        values=[["player_name", "team", "stat_type", "line", "odds", "vendor"]],
    )
    logger.info("Lines tab was empty — wrote header row.")


def write_auto_props(ws: gspread.Worksheet, props: list[dict]) -> None:
    """
    Write auto-fetched props to the Lines tab, replacing any previous
    auto-fetched rows but preserving any manually entered rows.

    Auto-fetched rows are identified by having a non-empty 'vendor' column.
    Manual rows have vendor = '' or are missing the vendor column.
    """
    all_vals = ws.get_all_values()
    if not all_vals:
        # Fresh tab — write headers + props
        headers = ["player_name", "team", "stat_type", "line", "odds", "vendor"]
        rows = [
            [p["player_name"], p["team"], p["stat_type"],
             str(p["line"]), str(p.get("odds","") or ""), p.get("vendor","")]
            for p in props
        ]
        ws.update(range_name="A1", values=[headers] + rows, value_input_option="USER_ENTERED")
        logger.info(f"  Wrote {len(rows)} auto-props to Lines tab")
        return

    headers = [h.strip().lower() for h in all_vals[0]]
    vendor_col = headers.index("vendor") if "vendor" in headers else None

    # Keep only manual rows (no vendor or empty vendor)
    manual_rows = []
    for row in all_vals[1:]:
        if not any(row):
            continue
        if vendor_col is None or not (len(row) > vendor_col and row[vendor_col].strip()):
            manual_rows.append(row)

    # Build new auto rows
    new_headers = ["player_name", "team", "stat_type", "line", "odds", "vendor"]
    auto_rows = [
        [p["player_name"], p["team"], p["stat_type"],
         str(p["line"]), str(p.get("odds","") or ""), p.get("vendor","")]
        for p in props
    ]

    all_new_rows = auto_rows + manual_rows
    ws.clear()
    ws.update(
        range_name="A1",
        values=[new_headers] + all_new_rows,
        value_input_option="USER_ENTERED"
    )
    logger.info(f"  Lines tab: {len(auto_rows)} auto-props + {len(manual_rows)} manual rows")


def read_lines(ws: gspread.Worksheet) -> list[dict]:
    """Read Lines tab. Returns list of {player_name, team, stat_type, line, odds}."""
    all_vals = ws.get_all_values()
    if len(all_vals) < 2:
        return []
    headers = [h.strip().lower() for h in all_vals[0]]
    result = []
    for row in all_vals[1:]:
        if not any(row):
            continue
        record = dict(zip(headers, row))
        try:
            record["line"] = float(record.get("line") or 0)
        except (ValueError, TypeError):
            continue
        odds_raw = record.get("odds", "")
        try:
            record["odds"] = float(odds_raw) if str(odds_raw).strip() else None
        except (ValueError, AttributeError):
            record["odds"] = None
        if record.get("player_name") and record.get("stat_type"):
            result.append(record)
    return result


def _build_stat_window_headers(
    player_lines: dict[str, dict[str, list[float]]]
) -> list[str]:
    base = build_stat_columns()
    stat_line_counts: dict[str, int] = {}
    for player_stat_map in player_lines.values():
        for stat, lines in player_stat_map.items():
            stat_line_counts[stat] = max(stat_line_counts.get(stat, 0), len(lines))
    extra = []
    for stat in STAT_TYPES:
        n_lines = stat_line_counts.get(stat, 0)
        if n_lines == 0:
            continue
        if n_lines == 1:
            extra += [f"{stat}_line", f"{stat}_hit_rate"]
        else:
            for i in range(1, n_lines + 1):
                extra += [f"{stat}_line{i}", f"{stat}_hit_rate{i}"]
    return base + extra


def write_stat_window_tab(
    ws: gspread.Worksheet,
    player_rows: list[dict],
    player_lines: dict[str, dict[str, list[float]]],
) -> None:
    headers = _build_stat_window_headers(player_lines)
    rows = [[pr.get(col, "") for col in headers] for pr in player_rows]
    _write_sheet_data(ws, headers, rows)


def write_value_tab(
    ws: gspread.Worksheet,
    value_rows: list[dict],
) -> None:
    headers = [
        "Player", "Team", "Stat", "Line", "Odds",
        "L5 Hit%", "L10 Hit%", "L20 Hit%",
        "Implied Prob", "Book Edge",
        "Trend", "Matchup", "Rest", "Consistency",
        "Hit Rate Signal", "Context", "FINAL CALL",
    ]

    def parse_hr(val):
        if val in (None, "", "—"):
            return None
        try:
            return float(str(val).strip("%")) / 100
        except (ValueError, TypeError):
            return None

    rows = []
    for r in value_rows:
        stat = r.get("stat_type", "PTS").upper()
        ctx  = r.get("context", {})

        l5_hr  = parse_hr(r.get("last5_hit_rate"))
        l10_hr = parse_hr(r.get("last10_hit_rate"))
        l20_hr = parse_hr(r.get("last20_hit_rate"))

        avail = [(hr, w) for hr, w in [(l5_hr, 0.3), (l10_hr, 0.5), (l20_hr, 0.2)] if hr is not None]
        if avail:
            total_w = sum(w for _, w in avail)
            whr = sum(hr * w for hr, w in avail) / total_w
        else:
            whr = None

        if whr is None:
            hr_signal, hr_score = "—", 0
        elif whr >= 0.70: hr_signal, hr_score = "🔥 Strong Over",  3
        elif whr >= 0.60: hr_signal, hr_score = "✅ Lean Over",    2
        elif whr >= 0.55: hr_signal, hr_score = "〰 Slight Over",  1
        elif whr <= 0.30: hr_signal, hr_score = "🔥 Strong Under", -3
        elif whr <= 0.40: hr_signal, hr_score = "✅ Lean Under",   -2
        elif whr <= 0.45: hr_signal, hr_score = "〰 Slight Under", -1
        else:             hr_signal, hr_score = "⚪ Neutral",       0

        odds_val     = r.get("odds")
        impl_display = "—"
        edge_display = "—"
        edge_score   = 0

        if odds_val and str(odds_val).strip():
            try:
                from metrics import american_odds_to_implied_prob, compute_edge
                ip = american_odds_to_implied_prob(float(str(odds_val).strip()))
                if ip and l10_hr is not None:
                    edge = compute_edge(l10_hr, ip)
                    impl_display = f"{ip:.1%}"
                    edge_display = f"{edge:+.1%}"
                    if edge >= 0.10:    edge_score = 2
                    elif edge >= 0.05:  edge_score = 1
                    elif edge <= -0.10: edge_score = -2
                    elif edge <= -0.05: edge_score = -1
            except (ValueError, TypeError):
                pass

        trend       = ctx.get(f"{stat}_trend", ctx.get("PTS_trend", "—")) or "—"
        matchup     = ctx.get(f"opp_{stat.lower()}_matchup", ctx.get("opp_pts_matchup", "—")) or "—"
        is_b2b      = ctx.get("is_back_to_back", "")
        days_rest   = ctx.get("days_rest")
        consistency = ctx.get(f"{stat}_consistency", ctx.get("PTS_consistency", "—")) or "—"

        trend_score   = 1 if "📈" in str(trend)    else (-1 if "📉" in str(trend)    else 0)
        matchup_score = 1 if "🟢" in str(matchup)  else (-1 if "🔴" in str(matchup)  else 0)
        cons_score    = 1 if "🎯" in str(consistency) else (-1 if "🎲" in str(consistency) else 0)

        rest_score   = 0
        rest_display = f"{days_rest}d rest" if days_rest is not None else "—"
        if str(is_b2b) == "🔴 YES":
            rest_score   = -1
            rest_display = "🔴 B2B"
        elif days_rest is not None and days_rest >= 3:
            rest_score   = 1
            rest_display = f"✅ {days_rest}d rest"

        ctx_score = trend_score + matchup_score + rest_score + cons_score
        if ctx_score >= 3:    ctx_label = "🟢🟢 Very Favorable"
        elif ctx_score == 2:  ctx_label = "🟢 Favorable"
        elif ctx_score == 1:  ctx_label = "🟡 Slight Edge"
        elif ctx_score == 0:  ctx_label = "⚪ Neutral"
        elif ctx_score == -1: ctx_label = "🟠 Concern"
        elif ctx_score == -2: ctx_label = "🔴 Unfavorable"
        else:                 ctx_label = "🔴🔴 Very Unfavorable"

        if edge_score != 0:
            final = (hr_score * 2) + (edge_score * 2) + ctx_score
        else:
            final = (hr_score * 3) + ctx_score

        if final >= 7:    final_call = "🔥🔥 STRONG OVER"
        elif final >= 4:  final_call = "✅ BET OVER"
        elif final >= 2:  final_call = "〰 Lean Over"
        elif final <= -7: final_call = "🔥🔥 STRONG UNDER"
        elif final <= -4: final_call = "✅ BET UNDER"
        elif final <= -2: final_call = "〰 Lean Under"
        else:             final_call = "⚪ Skip"

        rows.append([
            r.get("player_name", ""), r.get("team", ""), stat, r.get("line", ""),
            str(odds_val) if odds_val else "",
            f"{l5_hr:.0%}"  if l5_hr  is not None else "—",
            f"{l10_hr:.0%}" if l10_hr is not None else "—",
            f"{l20_hr:.0%}" if l20_hr is not None else "—",
            impl_display, edge_display,
            trend, matchup, rest_display, consistency,
            hr_signal, ctx_label, final_call,
        ])

        r["final_call"] = final_call
        r["hr_signal"]  = hr_signal
        r["ctx_label"]  = ctx_label
        r["last5_hit_rate"]  = f"{l5_hr:.0%}"  if l5_hr  is not None else "—"
        r["last10_hit_rate"] = f"{l10_hr:.0%}" if l10_hr is not None else "—"
        r["last20_hit_rate"] = f"{l20_hr:.0%}" if l20_hr is not None else "—"

    signal_order = {
        "🔥🔥 STRONG OVER": 0, "🔥🔥 STRONG UNDER": 1,
        "✅ BET OVER": 2,      "✅ BET UNDER": 3,
        "〰 Lean Over": 4,     "〰 Lean Under": 5,
        "⚪ Skip": 6,
    }
    rows.sort(key=lambda x: signal_order.get(x[16], 7))
    _write_sheet_data(ws, headers, rows)


def write_distribution_tab(
    ws: gspread.Worksheet,
    dist_rows: list[dict],
) -> None:
    """
    Write the Distribution tab showing outcome analysis,
    hook warnings, and line quality for each player prop.
    """
    headers = [
        "Player", "Team", "Stat", "Line",
        # Hook warning — most important column
        "Hook Warning", "Hook Level",
        # True rates
        "True Over% (L10)", "True Over% (L20)",
        # Median vs mean
        "Median (L10)", "Median (L20)", "Mean (L10)",
        "Median vs Line", "Mean/Median Gap", "Avg Misleading?",
        # Modal
        "Modal Outcome", "Modal Freq%",
        "Top 3 Outcomes",
        # Near miss
        "Near Miss% (just under line)",
        # Line quality
        "Line Quality", "Distribution Shape",
        # Final call context
        "Final Call",
    ]

    rows = []
    for r in dist_rows:
        d = r.get("distribution", {})
        if not d:
            continue

        from distribution import format_top_outcomes
        rows.append([
            r.get("player_name", ""),
            r.get("team", ""),
            r.get("stat_type", ""),
            str(r.get("line", "")),
            # Hook
            d.get("hook_warning", "—"),
            d.get("hook_level", "—"),
            # True rates
            f"{d['true_over_rate_l10']:.0%}" if d.get("true_over_rate_l10") is not None else "—",
            f"{d['true_over_rate_l20']:.0%}" if d.get("true_over_rate_l20") is not None else "—",
            # Median/mean
            str(d.get("median_l10", "—")),
            str(d.get("median_l20", "—")),
            str(d.get("mean_l10", "—")),
            str(d.get("line_vs_median", "—")),
            str(d.get("mean_median_gap", "—")),
            "⚠️ YES" if d.get("mean_misleading") else "—",
            # Modal
            str(d.get("modal_outcome", "—")),
            f"{d['modal_pct']}%" if d.get("modal_pct") is not None else "—",
            format_top_outcomes(d.get("top_outcomes", [])),
            # Near miss
            f"{d['near_miss_pct']}%" if d.get("near_miss_pct") is not None else "—",
            # Quality
            d.get("line_quality", "—"),
            d.get("dist_shape", "—"),
            # Final call
            r.get("final_call", ""),
        ])

    # Sort: severe hooks first, then by hook score ascending (worst hooks at top)
    hook_order = {
        "🚨 SEVERE HOOK":    0,
        "⚠️ HOOK WARNING":   1,
        "🟡 MILD HOOK":      2,
        "⚪ Neutral":         3,
        "🟢 SLIGHT OVER EDGE": 4,
        "🟢 UNDER FRIENDLY": 5,
        "🔥 PRIME OVER":     6,
    }
    rows.sort(key=lambda x: hook_order.get(x[5], 3))
    _write_sheet_data(ws, headers, rows)


def write_line_shopping_tab(
    ws: gspread.Worksheet,
    enriched_rows: list[dict],
) -> None:
    """
    Write the Line Shopping tab showing multi-book comparison,
    best lines, and EV for each player prop.
    """
    headers = [
        "Player", "Team", "Stat", "Consensus Line",
        # Best over
        "Best Over Line", "Best Over Book", "Best Over Odds",
        # Best under
        "Best Under Line", "Best Under Book", "Best Under Odds",
        # EV
        "Best Over EV", "Best Under EV",
        # Shopping info
        "# Books", "Line Spread", "Shopping Opportunity",
        # All books summary
        "All Books",
        # Link back to final call
        "Final Call",
    ]

    rows = []
    for r in enriched_rows:
        shop = r.get("line_shopping", {})
        if not shop:
            continue

        # Format all books as readable string
        all_books_str = " | ".join([
            f"{b['book_display']}: {b['line']} "
            f"(O:{b['over_odds'] if b['over_odds'] else '—'} "
            f"U:{b['under_odds'] if b['under_odds'] else '—'})"
            for b in shop.get("all_books", [])
        ])

        rows.append([
            r.get("player_name", ""),
            r.get("team", ""),
            r.get("stat_type", ""),
            str(shop.get("consensus_line", "")),
            # Best over
            str(shop.get("best_over_line", "—")),
            shop.get("best_over_book", "—") or "—",
            str(shop.get("best_over_odds", "—")),
            # Best under
            str(shop.get("best_under_line", "—")),
            shop.get("best_under_book", "—") or "—",
            str(shop.get("best_under_odds", "—")),
            # EV
            shop.get("best_over_ev_value", "—"),
            shop.get("best_under_ev_value", "—"),
            # Shopping
            str(shop.get("num_books", 0)),
            str(shop.get("line_spread", 0)),
            "🛒 YES" if shop.get("shopping_opportunity") else "—",
            # All books
            all_books_str,
            # Final call
            r.get("final_call", ""),
        ])

    # Sort by shopping opportunity first, then by num_books descending
    rows.sort(key=lambda x: (0 if x[14] == "🛒 YES" else 1, -int(x[12] or 0)))
    _write_sheet_data(ws, headers, rows)


def write_context_tab(ws: gspread.Worksheet, context_rows: list[dict]) -> None:
    headers = [
        "Player", "Team", "Opponent", "Tonight",
        "Days Rest", "Back-to-Back?", "Games (L7)", "Games (L14)",
        "MIN L5", "MIN L15", "Minutes Trend",
        "PTS Trend", "PTS L5", "PTS L20", "PTS Trend %",
        "REB Trend", "REB L5", "REB L20",
        "AST Trend", "AST L5", "AST L20",
        "FG3M Trend", "FG3M L5", "FG3M L20",
        "BLK Trend",  "BLK L5",  "BLK L20",
        "PTS Consistency", "REB Consistency", "AST Consistency",
        "PTS Home Avg", "PTS Away Avg",
        "REB Home Avg", "REB Away Avg",
        "AST Home Avg", "AST Away Avg",
        "Opp PTS Allowed", "Opp PTS Matchup",
        "Opp REB Allowed", "Opp AST Allowed", "Opp BLK Allowed",
        "Revenge Game?",
    ]

    col_keys = [
        "player_name", "team", "opponent", "tonight_location",
        "days_rest", "is_back_to_back", "games_last_7_days", "games_last_14_days",
        "minutes_l5_avg", "minutes_l15_avg", "minutes_trend",
        "PTS_trend", "PTS_l5_avg", "PTS_l20_avg", "PTS_trend_pct",
        "REB_trend", "REB_l5_avg", "REB_l20_avg",
        "AST_trend", "AST_l5_avg", "AST_l20_avg",
        "FG3M_trend", "FG3M_l5_avg", "FG3M_l20_avg",
        "BLK_trend",  "BLK_l5_avg",  "BLK_l20_avg",
        "PTS_consistency", "REB_consistency", "AST_consistency",
        "PTS_avg_home", "PTS_avg_away",
        "REB_avg_home", "REB_avg_away",
        "AST_avg_home", "AST_avg_away",
        "opp_pts_allowed_avg", "opp_pts_matchup",
        "opp_reb_allowed_avg", "opp_ast_allowed_avg", "opp_blk_allowed_avg",
        "revenge_game",
    ]

    rows = [[r.get(k, "") for k in col_keys] for r in context_rows]
    _write_sheet_data(ws, headers, rows)


def write_notes(ws: gspread.Worksheet, notes: list[dict]) -> None:
    headers = ["Player Name (from Lines)", "Issue", "Suggestions"]
    rows = [[n["player_name"], n["issue"], n.get("suggestions", "")] for n in notes]
    existing = ws.get_all_values()
    if existing and existing[0] == headers:
        if rows:
            ws.append_rows(rows, value_input_option="USER_ENTERED")
    else:
        _write_sheet_data(ws, headers, rows)
