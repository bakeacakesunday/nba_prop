"""
dfs_optimizer.py — Lineup optimizer for DraftKings and FanDuel NBA DFS.

Uses scipy.optimize.milp (mixed integer linear programming) to find the
optimal combination of players that maximizes projected fantasy points
while staying under the salary cap and satisfying position requirements.

Also handles:
- Lineup diversification (20 unique lineups via controlled randomness)
- Stacking rules (game stacks, correlation rules)
- Player locking/excluding
- CSV export in DK/FD upload format
- Pick6 / Best Ball tier optimization
"""
from __future__ import annotations

import csv
import io
import logging
import random
from copy import deepcopy
from typing import Optional

import numpy as np
from scipy.optimize import milp, LinearConstraint, Bounds

logger = logging.getLogger(__name__)


# ── Position eligibility maps ─────────────────────────────────────────────────
# Maps each player position to which roster slots they can fill

DK_SLOT_ELIGIBILITY = {
    # slot: [eligible positions]
    "PG":   ["PG"],
    "SG":   ["SG"],
    "SF":   ["SF"],
    "PF":   ["PF"],
    "C":    ["C"],
    "G":    ["PG", "SG"],
    "F":    ["SF", "PF"],
    "UTIL": ["PG", "SG", "SF", "PF", "C"],
}
DK_SLOTS    = ["PG", "SG", "SF", "PF", "C", "G", "F", "UTIL"]
DK_SALARY   = 50_000
DK_SIZE     = 8

FD_SLOT_ELIGIBILITY = {
    "PG1": ["PG"], "PG2": ["PG"],
    "SG1": ["SG"], "SG2": ["SG"],
    "SF1": ["SF"], "SF2": ["SF"],
    "PF1": ["PF"], "PF2": ["PF"],
    "C":   ["C"],
}
FD_SLOTS  = ["PG1", "PG2", "SG1", "SG2", "SF1", "SF2", "PF1", "PF2", "C"]
FD_SALARY = 60_000
FD_SIZE   = 9


def _solve_lineup(players: list[dict], platform: str,
                  locked: list[str] = None,
                  excluded: list[str] = None,
                  proj_noise: float = 0.0,
                  required_game: Optional[str] = None,
                  min_from_game: int = 0) -> Optional[list[dict]]:
    """
    Core optimizer. Returns list of selected player dicts or None if infeasible.

    players:       list of player dicts with proj_dk/proj_fd, salary_dk/fd, position
    platform:      'dk' or 'fd'
    locked:        player names that must be in lineup
    excluded:      player names that cannot be in lineup
    proj_noise:    std deviation multiplier for randomness (0 = deterministic)
    required_game: game key (e.g. 'BOS@NYK') to force stacking from
    min_from_game: minimum players from required_game
    """
    locked   = locked or []
    excluded = excluded or []

    if platform == "dk":
        slots      = DK_SLOTS
        slot_elig  = DK_SLOT_ELIGIBILITY
        salary_cap = DK_SALARY
        proj_key   = "proj_dk"
        salary_key = "salary_dk"
        size       = DK_SIZE
    else:
        slots      = FD_SLOTS
        slot_elig  = FD_SLOT_ELIGIBILITY
        salary_cap = FD_SALARY
        proj_key   = "proj_fd"
        salary_key = "salary_fd"
        size       = FD_SIZE

    # Filter to eligible players
    eligible = [p for p in players
                if p.get(salary_key) and p.get(proj_key, 0) > 0
                and p.get("player_name") not in excluded]

    if len(eligible) < size:
        return None

    n = len(eligible)
    S = len(slots)

    # Decision variables: x[i][s] = 1 if player i fills slot s
    # Flattened: index = i * S + s
    N = n * S

    # Objective: maximize sum of proj * x[i][s]
    # milp minimizes, so negate projections
    projs = np.array([p.get(proj_key, 0) for p in eligible], dtype=float)

    # Add controlled noise for lineup diversification
    if proj_noise > 0:
        noise = np.random.normal(0, proj_noise, n)
        projs = projs + noise

    c = np.zeros(N)
    for i in range(n):
        for s in range(S):
            c[i * S + s] = -projs[i]  # negative = maximize

    # Integer constraints: all variables are binary
    integrality = np.ones(N)

    # Bounds: 0 <= x <= 1
    bounds = Bounds(lb=np.zeros(N), ub=np.ones(N))

    constraints = []

    # 1. Each slot filled by exactly 1 player
    for s, slot in enumerate(slots):
        row = np.zeros(N)
        for i, p in enumerate(eligible):
            pos = p.get(f"position_{platform}", "")
            if pos in slot_elig.get(slot, []):
                row[i * S + s] = 1
        constraints.append(LinearConstraint(row, lb=1, ub=1))

    # 2. Each player used at most once (sum across slots <= 1)
    for i in range(n):
        row = np.zeros(N)
        for s in range(S):
            row[i * S + s] = 1
        constraints.append(LinearConstraint(row, lb=0, ub=1))

    # 3. Total roster size = size
    row = np.ones(N)
    constraints.append(LinearConstraint(row, lb=size, ub=size))

    # 4. Salary cap
    salaries = np.array([p.get(salary_key, 0) for p in eligible], dtype=float)
    sal_row = np.zeros(N)
    for i in range(n):
        for s in range(S):
            sal_row[i * S + s] = salaries[i]
    constraints.append(LinearConstraint(sal_row, lb=0, ub=salary_cap))

    # 5. Locked players must appear
    for locked_name in locked:
        locked_idxs = [i for i, p in enumerate(eligible)
                       if p.get("player_name") == locked_name]
        if not locked_idxs:
            continue
        row = np.zeros(N)
        for i in locked_idxs:
            for s in range(S):
                row[i * S + s] = 1
        constraints.append(LinearConstraint(row, lb=1, ub=S))

    # 6. Game stack requirement
    if required_game and min_from_game > 0:
        row = np.zeros(N)
        for i, p in enumerate(eligible):
            if p.get("game_key") == required_game:
                for s in range(S):
                    row[i * S + s] = 1
        if row.sum() > 0:
            constraints.append(LinearConstraint(row, lb=min_from_game, ub=size))

    # Solve
    try:
        result = milp(c, constraints=constraints, integrality=integrality, bounds=bounds)
        if not result.success:
            return None
    except Exception as e:
        logger.error(f"Optimizer error: {e}")
        return None

    # Extract selected players
    x = result.x
    selected = {}  # slot → player
    for i, p in enumerate(eligible):
        for s, slot in enumerate(slots):
            if x[i * S + s] > 0.5:
                selected[slot] = {**p, "slot": slot}

    if len(selected) != size:
        return None

    return list(selected.values())


def optimize_lineup(players: list[dict], platform: str,
                    locked: list[str] = None,
                    excluded: list[str] = None,
                    required_game: str = None,
                    min_from_game: int = 0) -> Optional[dict]:
    """
    Build a single optimal lineup. Returns lineup dict or None.
    """
    lineup = _solve_lineup(players, platform, locked, excluded,
                           required_game=required_game,
                           min_from_game=min_from_game)
    if not lineup:
        return None

    return _format_lineup(lineup, platform)


def optimize_multiple_lineups(players: list[dict], platform: str,
                               n_lineups: int = 20,
                               locked: list[str] = None,
                               excluded: list[str] = None,
                               max_exposure: float = 0.6,
                               game_stack: str = None,
                               min_stack: int = 2) -> list[dict]:
    """
    Generate n_lineups diverse lineups using controlled randomness.

    max_exposure: max fraction of lineups a single player can appear in
    game_stack: game key to require stacking from
    min_stack: minimum players from game_stack game
    """
    lineups   = []
    exposure  = {}  # player_name → count
    noise_std = 3.0  # fantasy points of randomness

    # Generate deterministic optimal first
    first = _solve_lineup(players, platform, locked, excluded,
                          required_game=game_stack, min_from_game=min_stack)
    if first:
        lineups.append(_format_lineup(first, platform))
        for p in first:
            exposure[p["player_name"]] = exposure.get(p["player_name"], 0) + 1

    attempts = 0
    while len(lineups) < n_lineups and attempts < n_lineups * 5:
        attempts += 1

        # Enforce exposure limits
        over_exposed = [name for name, cnt in exposure.items()
                        if cnt >= max_exposure * n_lineups]

        lineup = _solve_lineup(players, platform, locked,
                               excluded=list(set((excluded or []) + over_exposed)),
                               proj_noise=noise_std,
                               required_game=game_stack,
                               min_from_game=min_stack)

        if not lineup:
            noise_std *= 1.1  # increase noise if struggling to diversify
            continue

        formatted = _format_lineup(lineup, platform)

        # Skip duplicate lineups
        player_sets = [frozenset(l["player_names"]) for l in lineups]
        if frozenset(formatted["player_names"]) in player_sets:
            continue

        lineups.append(formatted)
        for p in lineup:
            name = p["player_name"]
            exposure[name] = exposure.get(name, 0) + 1

    logger.info(f"Generated {len(lineups)} unique {platform.upper()} lineups")
    return lineups


def optimize_pick6(players: list[dict]) -> Optional[dict]:
    """
    Pick6/Best Ball: no salary cap — just pick highest projected player
    per tier. Returns one lineup dict.
    """
    # Group by position
    by_pos = {}
    for p in players:
        pos = p.get("position_dk", "UTIL")
        by_pos.setdefault(pos, []).append(p)

    # Sort each position by proj_dk descending
    for pos in by_pos:
        by_pos[pos].sort(key=lambda x: x.get("proj_dk", 0), reverse=True)

    tiers = ["PG", "SG", "SF", "PF", "C", "FLEX"]
    selected = []
    used = set()

    for tier in tiers:
        if tier == "FLEX":
            # Best remaining player any position
            remaining = sorted(
                [p for p in players if p["player_name"] not in used],
                key=lambda x: x.get("proj_dk", 0), reverse=True
            )
            if remaining:
                pick = remaining[0]
                selected.append({**pick, "slot": "FLEX"})
                used.add(pick["player_name"])
        else:
            candidates = [p for p in by_pos.get(tier, [])
                          if p["player_name"] not in used]
            if candidates:
                pick = candidates[0]
                selected.append({**pick, "slot": tier})
                used.add(pick["player_name"])

    if not selected:
        return None

    total_proj = sum(p.get("proj_dk", 0) for p in selected)
    return {
        "platform":     "pick6",
        "players":      selected,
        "player_names": [p["player_name"] for p in selected],
        "total_proj":   round(total_proj, 2),
        "total_salary": 0,
        "salary_remaining": 0,
    }


def _format_lineup(lineup: list[dict], platform: str) -> dict:
    """Format raw optimizer output into a clean lineup dict."""
    salary_key = f"salary_{platform}"
    proj_key   = f"proj_{platform}"

    total_salary = sum(p.get(salary_key, 0) for p in lineup)
    total_proj   = sum(p.get(proj_key, 0) for p in lineup)
    cap          = 50_000 if platform == "dk" else 60_000

    return {
        "platform":        platform,
        "players":         lineup,
        "player_names":    [p["player_name"] for p in lineup],
        "total_proj":      round(total_proj, 2),
        "total_salary":    total_salary,
        "salary_remaining": cap - total_salary,
    }


def export_dk_csv(lineups: list[dict]) -> str:
    """
    Export lineups in DraftKings CSV upload format.
    Columns: PG,SG,SF,PF,C,G,F,UTIL (player names)
    """
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["PG", "SG", "SF", "PF", "C", "G", "F", "UTIL"])

    for lineup in lineups:
        if lineup.get("platform") != "dk":
            continue
        slot_map = {p["slot"]: p["player_name"] for p in lineup["players"]}
        writer.writerow([slot_map.get(slot, "") for slot in DK_SLOTS])

    return output.getvalue()


def export_fd_csv(lineups: list[dict]) -> str:
    """
    Export lineups in FanDuel CSV upload format.
    Columns: PG,PG,SG,SG,SF,SF,PF,PF,C
    """
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["PG", "PG", "SG", "SG", "SF", "SF", "PF", "PF", "C"])

    for lineup in lineups:
        if lineup.get("platform") != "fd":
            continue
        slot_map = {p["slot"]: p["player_name"] for p in lineup["players"]}
        fd_order = ["PG1", "PG2", "SG1", "SG2", "SF1", "SF2", "PF1", "PF2", "C"]
        writer.writerow([slot_map.get(slot, "") for slot in fd_order])

    return output.getvalue()
