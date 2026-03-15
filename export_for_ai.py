#!/usr/bin/env python3
"""
export_for_ai.py
────────────────
Run this after the pipeline finishes to produce a clean, AI-ready export of
tonight's props. The output file (pipeline_cache_ai.json) is what you upload
to Claude or any other AI when asking for parlay recommendations.

Usage:
    python export_for_ai.py
    python export_for_ai.py --input pipeline_cache.json --output my_export.json
    python export_for_ai.py --tier-only        # only include PRIME/STRONG/SOLID props
    python export_for_ai.py --clean-only       # only include props that pass all hard gates
    python export_for_ai.py --no-losers        # strip out props that clearly can't win

The output includes:
    _meta         — slate summary (date, game count, prop count, tier breakdown)
    _gates        — full rulebook: hard blocks, L20 thresholds, tier definitions,
                    field glossary, construction rules
    _tier_summary — tonight's PRIME/STRONG/SOLID props in one easy list
    props         — full prop cards with all flags already set
"""

import json
import argparse
from pathlib import Path
from datetime import date

# ── Gate definitions (single source of truth) ────────────────────────────────

_L20_THRESHOLDS = {
    "AST":  60,   # assists are game-script sensitive — need sustained baseline
    "REB":  60,   # rebounds require consistent role and minutes
    "RA":   60,   # rebounds + assists combined
    "PTS":  55,
    "PR":   55,   # points + rebounds
    "PRA":  55,   # points + rebounds + assists
    "PA":   55,   # points + assists
    "FG3M": 55,
    "STL":  55,
    "BLK":  50,   # most lenient — blocks are noisy, lower threshold accepted
}

_HARD_DQ_CATS = {
    "VOLATILE-FLOOR",
    "COMBINED STAT CV too high",
    "SHOT-DEPENDENT",
    "REGRESSION RISK",
    "WEAK L20",
}

_GATES_BLOCK = {
    "description": (
        "NBA props research pipeline export. Every prop in 'props' has been run "
        "through the full gate stack. Flags are already set — you do NOT need to "
        "re-evaluate the raw numbers. Trust the flags. "
        "For parlay recommendations: only use props where no_brainer_tier is "
        "PRIME, STRONG, or SOLID. Everything else is a straight-bet candidate only."
    ),

    "how_to_pick_parlays": (
        "1. Filter props where no_brainer_tier IN ('PRIME','STRONG','SOLID'). "
        "2. Verify no HARD BLOCK flags are set (see parlay_hard_blocks). "
        "3. One prop per player max — check stat_components for overlap "
        "   (e.g. PRA and RA for same player both contain REB — that is duplicate exposure). "
        "4. Max 2 legs from the same game. "
        "5. Prefer 3-5 legs. The _tier_summary section has tonight's clean pool pre-filtered."
    ),

    "parlay_hard_blocks": {
        "regression_risk": {
            "type": "HARD BLOCK",
            "meaning": (
                "Player's L5 hit rate is 45%+ higher than their L20 hit rate. "
                "They are on a hot streak built on a weak true baseline. "
                "The book has already adjusted the line to capture the streak. "
                "Do NOT parlay. Fade or skip entirely."
            ),
            "check": "prop['regression_risk'] == True",
        },
        "outlier_inflated": {
            "type": "HARD BLOCK",
            "meaning": (
                "One recent game was a statistical outlier (≥2.5x the line AND "
                "≥1.8x the rest-of-L5 median) that is artificially inflating the "
                "L5 hit rate. The book has moved the line up to capture it. "
                "See outlier_game_val for the outlier value, true_l5_hr for the "
                "real hit rate without it. Do NOT parlay."
            ),
            "check": "prop['outlier_inflated'] == True",
            "example": "Bam Adebayo scored 83 in one game, rest of L5 was 21-24. Line moved to 22.5.",
        },
        "dist_cv_too_high": {
            "type": "HARD BLOCK",
            "meaning": (
                "dist_cv (coefficient of variation) above 0.50 means the player's "
                "outputs are wildly inconsistent. Even a good hit rate on this prop "
                "is built on high variance — one bad night kills the leg. Do NOT parlay."
            ),
            "check": "prop['dist_cv'] > 0.50",
            "example": "Tim Hardaway Jr AST: cv=0.63, went 0 assists despite 80% L5 hit rate.",
        },
        "parlay_disqualified": {
            "type": "HARD BLOCK",
            "meaning": (
                "The scoring engine has explicitly disqualified this prop from parlays. "
                "See parlay_disqualify_reason for the specific cause."
            ),
            "check": "prop['parlay_disqualified'] == True",
            "reasons": list(_HARD_DQ_CATS),
        },
        "is_b2b": {
            "type": "HARD BLOCK",
            "meaning": (
                "Player is on a back-to-back (played last night). "
                "Minutes and intensity are typically reduced. Do NOT parlay."
            ),
            "check": "prop['is_b2b'].startswith('🔴')",
        },
        "gtd": {
            "type": "HARD BLOCK",
            "meaning": (
                "Player is a game-time decision tonight. "
                "Do NOT use in any parlay until officially confirmed active."
            ),
            "check": "prop['gtd'] == True",
        },
    },

    "soft_warnings": {
        "regression_soft": {
            "type": "WARNING — still parlay-eligible",
            "meaning": (
                "Player's L5 is 30-44% higher than L20 — running somewhat hot "
                "but not at hard-block levels. Parlay is still allowed but note "
                "the elevated regression risk. See regression_gap for the exact gap."
            ),
        },
        "regression_gap": {
            "type": "INFO",
            "meaning": "Raw percentage point gap between L5 and L20 hit rates. 0-29 = normal. 30-44 = soft warning. 45+ = hard block.",
        },
    },

    "l20_minimum_thresholds": {
        "description": (
            "Minimum L20 hit rate (%) required for parlay eligibility by stat type. "
            "Below these = hard block regardless of L5/L10. "
            "A 50% L20 is a coin flip over the true sample — not a parlay leg. "
            "These thresholds were calibrated after 3/12/26 results showed "
            "45-50% L20 props losing consistently despite strong L5/L10."
        ),
        "thresholds": {k: f"{v}%" for k, v in _L20_THRESHOLDS.items()},
    },

    "no_brainer_tiers": {
        "description": (
            "Quality ladder applied AFTER all hard gates pass. "
            "Only props with a tier are recommended for parlays. "
            "Tiers are assigned in a post-processing pass using _parlay_is_clean() "
            "as the single gate — no duplicate checks."
        ),
        "PRIME": {
            "summary": "Elite consistency. Best parlay legs on the board.",
            "requirements": {
                "dist_profile": "CONSISTENT or MODERATE",
                "dist_cv": "≤ 0.22",
                "dist_floor_rate": "0.0 (never hits the floor)",
                "l5_hr": "≥ 80%",
                "l10_hr": "≥ 70%",
                "l20_hr": "≥ 60%",
                "median_gap": "≥ +1.5 above line",
                "is_shot_dependent": "False (shot-independent stats only)",
                "all_hard_gates": "Must pass",
            },
        },
        "STRONG": {
            "summary": "High confidence. Solid parlay legs.",
            "requirements": {
                "dist_profile": "CONSISTENT or MODERATE",
                "dist_cv": "≤ 0.35",
                "dist_floor_rate": "≤ 0.20",
                "l5_hr": "≥ 70%",
                "l10_hr": "≥ 65%",
                "l20_hr": "≥ 55%",
                "median_gap": "≥ +0.5 above line",
                "all_hard_gates": "Must pass",
            },
        },
        "SOLID": {
            "summary": "Parlay-eligible. Includes no-distribution-data props (e.g. Landale).",
            "requirements": {
                "dist_profile": "CONSISTENT, MODERATE, or UNKNOWN",
                "l5_hr": "≥ 70%",
                "l10_hr": "≥ 65%",
                "l20_hr": "≥ 55% or unknown (no historical data available)",
                "dist_floor_rate": "≤ 0.40 if known",
                "all_hard_gates": "Must pass",
            },
        },
        "null": {
            "summary": "Not recommended for parlays. Failed a hard gate or missed tier thresholds. May still be a straight bet.",
        },
    },

    "field_glossary": {
        "l5_hr":                  "Hit rate (%) over last 5 games. Recent form signal.",
        "l10_hr":                 "Hit rate (%) over last 10 games. Medium-term baseline.",
        "l20_hr":                 "Hit rate (%) over last 20 games. True baseline — most reliable.",
        "l5_values":              "Raw game values for last 5 games, most recent first (index 0).",
        "edge_score":             "0-100 composite score. 65+ = strong edge/mispriced. 50-64 = solid. <40 = lean only.",
        "no_brainer_tier":        "PRIME / STRONG / SOLID / null. Only tier props are parlay-recommended.",
        "is_no_brainer":          "Legacy field. True = PRIME tier only.",
        "regression_risk":        "HARD BLOCK. True = L5 vs L20 gap ≥45%. Hot streak on weak baseline.",
        "regression_soft":        "WARNING only. True = L5 vs L20 gap 30-44%. Parlay still allowed.",
        "regression_gap":         "Percentage point gap between L5 and L20 hit rates.",
        "regression_note":        "Plain English explanation of regression situation.",
        "outlier_inflated":       "HARD BLOCK. True = monster game masking weak true baseline.",
        "outlier_game_val":       "The outlier game value (e.g. 83 points) that triggered the flag.",
        "true_l5_hr":             "Real L5 hit rate with the outlier game removed.",
        "outlier_note":           "Plain English explanation of outlier inflation.",
        "dist_cv":                "Coefficient of variation. Lower = more consistent. >0.50 = HARD BLOCK.",
        "dist_floor_rate":        "Rate of near-zero game outputs. High = blowout/DNP risk.",
        "dist_profile":           "CONSISTENT / MODERATE / VOLATILE / UNKNOWN. Best: CONSISTENT, MODERATE.",
        "parlay_disqualified":    "HARD BLOCK. True = engine DQ'd this prop.",
        "parlay_disqualify_reason": "Why the prop was DQ'd. Read this string.",
        "is_b2b":                 "HARD BLOCK if starts with 🔴. Back-to-back game.",
        "gtd":                    "HARD BLOCK. True = game-time decision.",
        "lock":                   "True = model's highest confidence over bet.",
        "hammer":                 "True = strong over signal, below lock threshold.",
        "median_gap":             "Median outcome minus the line. Positive = player typically clears.",
        "true_over_pct":          "% of games player cleared this exact line. Most accurate hit rate.",
        "hook_level":             "Game-script risk. SEVERE / REGRESSION / UNDER FRIENDLY = caution.",
        "stat_components":        "Primitive stats in this prop. PRA → [AST, PTS, REB]. Use to detect correlated legs.",
        "best_prop_for_player":   "True = highest-edge prop for this player tonight.",
        "is_shot_dependent":      "True = stat relies on volume shooting (PTS, FG3M). Higher variance.",
    },

    "parlay_construction_rules": [
        "RULE 1: Only use props where no_brainer_tier is PRIME, STRONG, or SOLID.",
        "RULE 2: Never use props where regression_risk=True.",
        "RULE 3: Never use props where outlier_inflated=True.",
        "RULE 4: Never use props where parlay_disqualified=True.",
        "RULE 5: Never use props where dist_cv > 0.50.",
        "RULE 6: Never use props where l20_hr is below the stat's L20 threshold.",
        "RULE 7: Never use props where is_b2b starts with 🔴.",
        "RULE 8: Never use props where gtd=True.",
        "RULE 9: One prop per player maximum. Check stat_components for overlap (e.g. RA + PRA for same player both contain REB).",
        "RULE 10: Maximum 2 legs from the same game (correlated game script risk).",
        "RULE 11: Prefer shot-independent stats (REB, AST, RA, STL) over shot-dependent (PTS, FG3M).",
        "RULE 12: Prefer 3-5 leg parlays. Joint probability collapses fast with more legs.",
        "RULE 13: regression_soft=True is a warning, not a block — flag it but prop is eligible.",
        "RULE 14: BLK and STL are excluded from parlay recommendations — too noisy and low-volume.",
    ],
}


# ── Gate check function (mirrors _parlay_is_clean in app.py) ─────────────────

def is_parlay_clean(prop: dict) -> tuple[bool, str]:
    """Returns (clean: bool, reason: str)."""
    if prop.get("regression_risk"):
        return False, "regression_risk"
    if prop.get("outlier_inflated"):
        return False, f"outlier_inflated (game val: {prop.get('outlier_game_val')})"
    cv = prop.get("dist_cv")
    if cv is not None and cv > 0.50:
        return False, f"dist_cv too high ({cv:.2f})"
    dq_cat = (prop.get("parlay_disqualify_reason") or "").split(":")[0].strip()
    if dq_cat in _HARD_DQ_CATS:
        return False, f"DQ: {dq_cat}"
    l20 = prop.get("l20_hr")
    if l20 is not None:
        stat = (prop.get("stat") or "").upper()
        thresh = _L20_THRESHOLDS.get(stat, 55)
        if l20 < thresh:
            return False, f"L20 {l20:.0f}% < {thresh}% floor for {stat}"
    if (prop.get("is_b2b") or "").startswith("🔴"):
        return False, "B2B"
    if prop.get("gtd"):
        return False, "GTD"
    return True, "clean"


# ── Tier summary builder ──────────────────────────────────────────────────────

def build_tier_summary(props: list) -> dict:
    """Build a concise pre-filtered list of tonight's tiered props."""
    tiers = {"PRIME": [], "STRONG": [], "SOLID": []}
    seen_players = set()

    # Sort by edge_score desc, then by tier priority
    tier_order = {"PRIME": 0, "STRONG": 1, "SOLID": 2}
    tiered = [p for p in props if p.get("no_brainer_tier")]
    tiered.sort(key=lambda p: (
        tier_order.get(p.get("no_brainer_tier"), 9),
        -(p.get("edge_score") or 0)
    ))

    for prop in tiered:
        tier = prop.get("no_brainer_tier")
        if tier not in tiers:
            continue
        clean, _ = is_parlay_clean(prop)
        if not clean:
            continue  # shouldn't happen if pipeline ran correctly, but safety check

        o = prop.get("odds")
        odds_str = (("+" if o > 0 else "") + str(o)) if o else "—"
        soft = " ⚠soft-hot" if prop.get("regression_soft") else ""

        tiers[tier].append({
            "player":    prop.get("player_name"),
            "stat":      prop.get("stat"),
            "line":      f"O{prop.get('line')}",
            "odds":      odds_str,
            "L5":        f"{prop.get('l5_hr', 0):.0f}%",
            "L10":       f"{prop.get('l10_hr', 0):.0f}%",
            "L20":       f"{prop.get('l20_hr', 0):.0f}%" if prop.get("l20_hr") is not None else "—",
            "edge":      round(prop.get("edge_score") or 0, 1),
            "game":      f"{prop.get('team')} vs {prop.get('opponent')}",
            "flags":     soft.strip() or "clean",
            "stat_components": prop.get("stat_components", []),
            "key":       prop.get("key"),
        })

    # Deduped one-per-player summary (best leg per player across all tiers)
    best_per_player = []
    for tier in ("PRIME", "STRONG", "SOLID"):
        for entry in tiers[tier]:
            if entry["player"] not in seen_players:
                seen_players.add(entry["player"])
                best_per_player.append({**entry, "tier": tier})

    return {
        "description": (
            "Pre-filtered parlay candidates for tonight. "
            "All props here have passed every hard gate. "
            "'all_legs' includes every tiered leg (multiple per player possible). "
            "'best_per_player' is deduplicated to one leg per player — "
            "use this as your starting point for parlay building."
        ),
        "counts": {t: len(v) for t, v in tiers.items()},
        "all_legs": {t: v for t, v in tiers.items()},
        "best_per_player": best_per_player,
    }


# ── Slate meta builder ────────────────────────────────────────────────────────

def build_meta(data: dict, props: list) -> dict:
    games = data.get("games") or data.get("game_headers") or {}
    n_games = len(games) if isinstance(games, list) else len(games)

    tier_counts = {}
    for p in props:
        t = p.get("no_brainer_tier")
        if t:
            tier_counts[t] = tier_counts.get(t, 0) + 1

    clean_count = sum(1 for p in props if is_parlay_clean(p)[0])
    blocked_reasons = {}
    for p in props:
        clean, reason = is_parlay_clean(p)
        if not clean:
            cat = reason.split(" ")[0]
            blocked_reasons[cat] = blocked_reasons.get(cat, 0) + 1

    return {
        "export_date":    date.today().isoformat(),
        "game_date":      data.get("game_date", date.today().isoformat()),
        "total_props":    len(props),
        "total_games":    n_games,
        "parlay_clean":   clean_count,
        "tier_counts":    tier_counts,
        "blocked_by":     blocked_reasons,
        "note": (
            f"{tier_counts.get('PRIME',0)} PRIME + "
            f"{tier_counts.get('STRONG',0)} STRONG + "
            f"{tier_counts.get('SOLID',0)} SOLID props available tonight."
        ),
    }


# ── Main export ───────────────────────────────────────────────────────────────

def export(
    input_path:  str  = "pipeline_cache.json",
    output_path: str  = "pipeline_cache_ai.json",
    tier_only:   bool = False,
    clean_only:  bool = False,
    no_losers:   bool = False,
) -> None:

    input_file = Path(input_path)
    if not input_file.exists():
        raise FileNotFoundError(f"Pipeline cache not found: {input_path}")

    print(f"Reading {input_path}...")
    with open(input_file) as f:
        data = json.load(f)

    props = data.get("props", [])
    print(f"  {len(props)} props loaded")

    # ── Optional filters ──────────────────────────────────────────────────────
    if tier_only:
        props = [p for p in props if p.get("no_brainer_tier")]
        print(f"  → {len(props)} props after tier_only filter")

    if clean_only:
        props = [p for p in props if is_parlay_clean(p)[0]]
        print(f"  → {len(props)} props after clean_only filter")

    if no_losers:
        # Strip out props with no hit rate data and very low edge
        props = [p for p in props
                 if (p.get("l5_hr") or 0) > 0
                 and (p.get("edge_score") or 0) > 5]
        print(f"  → {len(props)} props after no_losers filter")

    # ── Add block_reason field to every prop ──────────────────────────────────
    # Makes it trivially easy for AI to see why each prop is or isn't clean
    for prop in props:
        clean, reason = is_parlay_clean(prop)
        prop["_parlay_clean"]  = clean
        prop["_block_reason"]  = "" if clean else reason

    # ── Build output ──────────────────────────────────────────────────────────
    tier_summary = build_tier_summary(props)
    meta         = build_meta(data, props)

    output = {
        "_meta":         meta,
        "_gates":        _GATES_BLOCK,
        "_tier_summary": tier_summary,
        "props":         props,
    }

    # Remove internal pipeline keys that aren't useful for AI
    output.pop("game_headers", None)

    output_file = Path(output_path)
    with open(output_file, "w") as f:
        json.dump(output, f, default=str, indent=2)

    size_mb = output_file.stat().st_size / 1_000_000
    print(f"\n✅ Exported to {output_path}  ({size_mb:.1f} MB)")
    print(f"   {meta['total_props']} props  |  "
          f"{meta['parlay_clean']} parlay-clean  |  "
          f"PRIME:{meta['tier_counts'].get('PRIME',0)}  "
          f"STRONG:{meta['tier_counts'].get('STRONG',0)}  "
          f"SOLID:{meta['tier_counts'].get('SOLID',0)}")
    print(f"\n   Tonight's best legs (best_per_player):")
    for leg in tier_summary["best_per_player"]:
        print(f"   [{leg['tier']:<6}] {leg['player']:<26} {leg['stat']:<5} {leg['line']:<7} "
              f"{leg['odds']:<7} L5:{leg['L5']:>4} L10:{leg['L10']:>4} L20:{leg['L20']:>4}  {leg['flags']}")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export pipeline cache for AI use")
    parser.add_argument("--input",      default="pipeline_cache.json",    help="Input cache file")
    parser.add_argument("--output",     default="pipeline_cache_ai.json", help="Output AI export file")
    parser.add_argument("--tier-only",  action="store_true", help="Only include PRIME/STRONG/SOLID props")
    parser.add_argument("--clean-only", action="store_true", help="Only include props passing all hard gates")
    parser.add_argument("--no-losers",  action="store_true", help="Strip zero-data and near-zero-edge props")
    args = parser.parse_args()

    export(
        input_path  = args.input,
        output_path = args.output,
        tier_only   = args.tier_only,
        clean_only  = args.clean_only,
        no_losers   = args.no_losers,
    )
