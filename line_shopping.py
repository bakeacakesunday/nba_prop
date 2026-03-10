"""
line_shopping.py — Multi-book line shopping and EV calculation.

For each player prop, collects odds from every available sportsbook,
finds the best line and best odds, and calculates expected value.

Key concepts:
  - Best line for OVER: highest line value (easier to go over)
  - Best line for UNDER: lowest line value (easier to go under)
  - Best odds: highest American odds (best payout) on each side
  - EV: (hit_rate * profit_per_unit) - ((1 - hit_rate) * 1.0)
       positive EV = you have an edge over the book
"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Display names for vendors
VENDOR_DISPLAY = {
    "draftkings":  "DraftKings",
    "fanduel":     "FanDuel",
    "betmgm":      "BetMGM",
    "caesars":     "Caesars",
    "betrivers":   "BetRivers",
    "pointsbet":   "PointsBet",
    "wynnbet":     "WynnBet",
    "prizepicks":  "PrizePicks",
    "underdog":    "Underdog",
    "fanatics":    "Fanatics",
}


def american_to_decimal(odds: float) -> float:
    """Convert American odds to decimal odds."""
    if odds >= 100:
        return (odds / 100) + 1
    else:
        return (100 / abs(odds)) + 1


def american_to_implied_prob(odds: float) -> float:
    """Convert American odds to implied probability (includes vig)."""
    if odds >= 100:
        return 100 / (odds + 100)
    else:
        return abs(odds) / (abs(odds) + 100)


def calculate_ev(hit_rate: float, odds: float) -> float:
    """
    Calculate expected value per unit bet.
    Positive = you have edge, negative = book has edge.

    EV = (hit_rate * profit) - ((1 - hit_rate) * 1.0)
    where profit = decimal_odds - 1
    """
    decimal = american_to_decimal(odds)
    profit  = decimal - 1
    ev = (hit_rate * profit) - ((1 - hit_rate) * 1.0)
    return round(ev, 4)


def ev_to_display(ev: float) -> str:
    """Format EV as a readable string with signal."""
    pct = ev * 100
    if ev >= 0.10:
        return f"🔥 +{pct:.1f}%"
    elif ev >= 0.05:
        return f"✅ +{pct:.1f}%"
    elif ev >= 0:
        return f"〰 +{pct:.1f}%"
    elif ev >= -0.05:
        return f"⚪ {pct:.1f}%"
    else:
        return f"🔴 {pct:.1f}%"


def shop_lines(
    raw_props: list[dict],
    player_id: int,
    stat_type: str,
    hit_rate_l10: Optional[float] = None,
) -> Optional[dict]:
    """
    Given all raw props for a game, find the best line and odds
    for a specific player + stat combination across all books.

    Returns a dict with:
        best_over_line, best_over_odds, best_over_book
        best_under_line, best_under_odds, best_under_book
        all_books: list of {book, line, over_odds, under_odds, over_ev, under_ev}
        consensus_line: most common line across books
        line_spread: difference between highest and lowest line (shopping opportunity)
    """
    from nba_data import PROP_TYPE_MAP

    # Find matching props
    matching = [
        p for p in raw_props
        if p.get("player_id") == player_id
        and PROP_TYPE_MAP.get(p.get("prop_type", "")) == stat_type
        and p.get("market", {}).get("type") == "over_under"
    ]

    if not matching:
        return None

    # Build per-book summary
    all_books = []
    for prop in matching:
        market   = prop.get("market", {})
        vendor   = prop.get("vendor", "unknown")
        line_val = float(prop.get("line_value", 0))
        over_odds  = market.get("over_odds")
        under_odds = market.get("under_odds")

        book_entry = {
            "book":        vendor,
            "book_display": VENDOR_DISPLAY.get(vendor, vendor.title()),
            "line":        line_val,
            "over_odds":   over_odds,
            "under_odds":  under_odds,
            "over_ev":     None,
            "under_ev":    None,
            "over_ev_display":  "—",
            "under_ev_display": "—",
        }

        # Calculate EV if we have hit rates
        if hit_rate_l10 is not None and over_odds is not None:
            over_ev  = calculate_ev(hit_rate_l10, over_odds)
            under_ev = calculate_ev(1 - hit_rate_l10, under_odds) if under_odds else None
            book_entry["over_ev"]  = over_ev
            book_entry["under_ev"] = under_ev
            book_entry["over_ev_display"]  = ev_to_display(over_ev)
            book_entry["under_ev_display"] = ev_to_display(under_ev) if under_ev is not None else "—"

        all_books.append(book_entry)

    if not all_books:
        return None

    # Best over line = highest line (easier to go over)
    over_candidates = [b for b in all_books if b["over_odds"] is not None]
    best_over = None
    if over_candidates:
        # Primary: highest line. Secondary: best odds at that line
        max_line = max(b["line"] for b in over_candidates)
        at_max_line = [b for b in over_candidates if b["line"] == max_line]
        best_over = max(at_max_line, key=lambda b: b["over_odds"])

    # Best under line = lowest line (easier to go under)
    under_candidates = [b for b in all_books if b["under_odds"] is not None]
    best_under = None
    if under_candidates:
        min_line = min(b["line"] for b in under_candidates)
        at_min_line = [b for b in under_candidates if b["line"] == min_line]
        best_under = max(at_min_line, key=lambda b: b["under_odds"])

    # Best EV plays (regardless of line)
    best_over_ev  = None
    best_under_ev = None
    if hit_rate_l10 is not None:
        ev_over_candidates  = [b for b in all_books if b["over_ev"] is not None]
        ev_under_candidates = [b for b in all_books if b["under_ev"] is not None]
        if ev_over_candidates:
            best_over_ev  = max(ev_over_candidates,  key=lambda b: b["over_ev"])
        if ev_under_candidates:
            best_under_ev = max(ev_under_candidates, key=lambda b: b["under_ev"])

    # Consensus line (most common)
    lines = [b["line"] for b in all_books]
    consensus_line = max(set(lines), key=lines.count)

    # Line spread (shopping opportunity indicator)
    line_spread = round(max(lines) - min(lines), 1) if len(lines) > 1 else 0.0

    return {
        # Best line for each direction
        "best_over_line":  best_over["line"]        if best_over  else None,
        "best_over_odds":  best_over["over_odds"]   if best_over  else None,
        "best_over_book":  best_over["book_display"] if best_over  else None,
        "best_under_line": best_under["line"]        if best_under else None,
        "best_under_odds": best_under["under_odds"]  if best_under else None,
        "best_under_book": best_under["book_display"] if best_under else None,

        # Best EV plays
        "best_over_ev_book":  best_over_ev["book_display"]     if best_over_ev  else None,
        "best_over_ev_line":  best_over_ev["line"]             if best_over_ev  else None,
        "best_over_ev_value": best_over_ev["over_ev_display"]  if best_over_ev  else "—",
        "best_under_ev_book": best_under_ev["book_display"]    if best_under_ev else None,
        "best_under_ev_line": best_under_ev["line"]            if best_under_ev else None,
        "best_under_ev_value":best_under_ev["under_ev_display"] if best_under_ev else "—",

        # Summary
        "consensus_line":  consensus_line,
        "line_spread":     line_spread,
        "num_books":       len(all_books),
        "all_books":       sorted(all_books, key=lambda b: b["line"], reverse=True),

        # Shopping flag
        "shopping_opportunity": line_spread >= 0.5,
    }


def build_line_shopping_rows(
    value_rows: list[dict],
    raw_props_by_game: dict[int, list[dict]],
    player_id_lookup: dict[str, int],
    window_metrics_by_player: dict[str, dict],
) -> list[dict]:
    """
    For every value row, build a line shopping comparison across all books.
    Returns enriched rows with multi-book data added.
    """
    enriched = []

    for vrow in value_rows:
        player_name = vrow.get("player_name", "")
        stat_type   = vrow.get("stat_type", "")
        player_id   = player_id_lookup.get(player_name)

        # Get L10 hit rate for EV calc
        w = window_metrics_by_player.get(player_name, {}).get("Last10", {})
        hr_key    = f"{stat_type}_hit_rate"
        hr_l10    = w.get(hr_key)

        # Collect all raw props across all games
        all_raw = []
        for props in raw_props_by_game.values():
            all_raw.extend(props)

        shop = None
        if player_id:
            shop = shop_lines(all_raw, player_id, stat_type, hr_l10)

        enriched_row = dict(vrow)
        if shop:
            enriched_row["line_shopping"] = shop

            # Override line/odds with best over line if it's better than current
            current_line = float(vrow.get("line", 0))
            if shop["best_over_line"] and shop["best_over_line"] > current_line:
                enriched_row["best_line_available"] = shop["best_over_line"]
                enriched_row["best_line_book"]      = shop["best_over_book"]
                enriched_row["line_upgrade"]        = f"+{shop['best_over_line'] - current_line}"
            else:
                enriched_row["best_line_available"] = current_line
                enriched_row["best_line_book"]      = shop["best_over_book"]
                enriched_row["line_upgrade"]        = "—"

            enriched_row["shopping_opportunity"] = "🛒 YES" if shop["shopping_opportunity"] else "—"
            enriched_row["num_books"]            = shop["num_books"]
            enriched_row["best_over_ev"]         = shop["best_over_ev_value"]
            enriched_row["best_under_ev"]        = shop["best_under_ev_value"]
        else:
            enriched_row["line_shopping"]        = {}
            enriched_row["best_line_available"]  = vrow.get("line", "")
            enriched_row["best_line_book"]       = "—"
            enriched_row["line_upgrade"]         = "—"
            enriched_row["shopping_opportunity"] = "—"
            enriched_row["num_books"]            = 0
            enriched_row["best_over_ev"]         = "—"
            enriched_row["best_under_ev"]        = "—"

        enriched.append(enriched_row)

    return enriched
