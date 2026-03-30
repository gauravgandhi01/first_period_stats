#!/usr/bin/env python3
"""
Project the chance of 2+ combined goals in the 1st period for a matchup.

This script uses the same API-safe hybrid data path used by get_1p_stats_hybrid.py:
1) Bulk season game rows for teams and goalies
2) Exact 1P assignment when the starter is unambiguous
3) Play-by-play fallback only when needed

Inputs:
- two teams (abbr or name)
- projected starting goalie for each team

Outputs:
- projected probability of 2+ goals in 1P (Over 1.5 1P)
- fair no-vig American odds for over and under
- empirical matchup/team/goalie context for transparency
"""

import argparse
import json
import math
import os
import re
import time
import unicodedata
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
import get_1p_stats_hybrid as hybrid


DATASET_VERSION = 7
DATASET_CACHE_KEY = f"projection_dataset_v{DATASET_VERSION}_{hybrid.SEASON_ID}_{hybrid.GAME_TYPE_ID}"

# Empirical-Bayes smoothing to prevent small-sample whipsaw.
TEAM_PRIOR_GAMES = 12.0
GOALIE_PRIOR_GAMES = 8.0

# Recent-form settings.
TEAM_RECENT_WINDOW = 14
GOALIE_RECENT_WINDOW = 7
TEAM_RECENT_HALF_LIFE = 8.0
GOALIE_RECENT_HALF_LIFE = 4.0
TEAM_FORM_BASE_WEIGHT = 0.20
GOALIE_FORM_BASE_WEIGHT = 0.35
TEAM_FORM_FULL_WEIGHT_GAMES = 10.0
GOALIE_FORM_FULL_WEIGHT_GAMES = 5.0

# Model blend/constraints.
POISSON_WEIGHT = 0.75
LEAGUE_O15_ANCHOR_WEIGHT = 0.18
GOALIE_FACTOR_MIN = 0.75
GOALIE_FACTOR_MAX = 1.30
TEMPO_FACTOR_MIN = 0.85
TEMPO_FACTOR_MAX = 1.15
PROB_MIN = 0.01
PROB_MAX = 0.99
GOALIE_MIN_GAMES_FOR_QUALIFIED_RANK = 5
EMPIRICAL_WINDOWS = (5, 10, 15, 20)

# Daily Faceoff starters feed.
DAILY_FACEOFF_URL = "https://www.dailyfaceoff.com/starting-goalies"
DAILY_FACEOFF_TIMEOUT = 30
DFO_STATUS_FALLBACK = "Unconfirmed"
DFO_ET_TZ = ZoneInfo("America/New_York")
DFO_UTC_TZ = ZoneInfo("UTC")
DFO_REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/",
}
NHL_SCORE_TIMEOUT = 20
NHL_LIVE_STATE_MAP = {
    "FUT": "Scheduled",
    "PRE": "Scheduled",
    "LIVE": "Live",
    "CRIT": "Live",
    "OFF": "Final",
    "FINAL": "Final",
}

# Odds API (market dampening)
ODDS_API_BASE_URL = "https://api.the-odds-api.com/v4"
ODDS_API_DEFAULT_REGIONS = "us,us2"
ODDS_API_TIMEOUT = 30
ODDS_API_MARKET_KEY_P1_TOTALS = "totals_p1"
# Keep odds refresh less frequent than starter/status scraping by default.
ODDS_API_CACHE_MAX_AGE_SECONDS = 30 * 60
ODDS_API_EVENT_REQUEST_SLEEP_SECONDS = 0.28
ODDS_API_EVENT_MAX_RETRIES = 3
ODDS_API_MIN_HEADROOM_CREDITS = 1
ODDS_MARKET_DAMPEN_BASE_WEIGHT = 0.45
ODDS_MARKET_DAMPEN_PER_BOOK_WEIGHT = 0.07
ODDS_MARKET_DAMPEN_MAX_WEIGHT = 0.85


def normalize_text(value):
    text = value or ""
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = text.upper()
    return re.sub(r"[^A-Z0-9]+", "", text)


def clamp(value, minimum, maximum):
    return max(minimum, min(maximum, value))


def stabilized_rate(raw_rate, sample_games, prior_rate, prior_games):
    denom = sample_games + prior_games
    if denom <= 0:
        return prior_rate
    return ((raw_rate * sample_games) + (prior_rate * prior_games)) / denom


def poisson_prob_at_least_two(lmbda):
    # P(X >= 2) = 1 - P(0) - P(1) for Poisson(lambda)
    return 1.0 - math.exp(-lmbda) * (1.0 + lmbda)


def probability_to_american(probability):
    p = clamp(probability, 1e-6, 1.0 - 1e-6)
    if p >= 0.5:
        return -round((p / (1.0 - p)) * 100)
    return round(((1.0 - p) / p) * 100)


def fmt_american(odds):
    if odds > 0:
        return f"+{odds}"
    return str(odds)


def _clean_api_key(value):
    text = str(value or "").strip().strip('"').strip("'").strip()
    return text or None


def _dedupe_keep_order(values):
    out = []
    seen = set()
    for value in values:
        clean = _clean_api_key(value)
        if not clean:
            continue
        if clean in seen:
            continue
        seen.add(clean)
        out.append(clean)
    return out


def _read_odds_api_keys():
    env_key = _clean_api_key(os.getenv("THE_ODDS_API_KEY"))
    if env_key:
        return [env_key]

    script_dir = os.path.dirname(os.path.abspath(__file__))
    collected = []

    # Primary: key.json with either {"api_keys":[...]} or a single-key field.
    key_json_path = os.path.join(script_dir, "key.json")
    try:
        with open(key_json_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, list):
            collected.extend(payload)
        elif isinstance(payload, dict):
            if isinstance(payload.get("api_keys"), list):
                collected.extend(payload.get("api_keys") or [])
            for field in ("the_odds_api_key", "odds_api_key", "api_key", "key"):
                value = payload.get(field)
                if isinstance(value, str):
                    collected.append(value)
    except (OSError, ValueError, TypeError):
        pass

    # Backward-compatible fallback.
    key_txt_path = os.path.join(script_dir, "key.txt")
    try:
        with open(key_txt_path, "r", encoding="utf-8") as f:
            value = f.read()
            if value:
                collected.append(value)
    except OSError:
        pass

    return _dedupe_keep_order(collected)


def _parse_int_header(value):
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _estimate_odds_api_credits_per_event():
    region_count = max(1, len([x for x in str(ODDS_API_DEFAULT_REGIONS).split(",") if x.strip()]))
    market_count = max(1, len([x for x in str(ODDS_API_MARKET_KEY_P1_TOTALS).split(",") if x.strip()]))
    return region_count * market_count


def american_to_implied_prob(odds):
    value = float(odds)
    if value < 0:
        return (-value) / ((-value) + 100.0)
    if value > 0:
        return 100.0 / (value + 100.0)
    raise ValueError("American odds cannot be 0.")


def median(values):
    if not values:
        return None
    ordered = sorted(float(v) for v in values)
    n = len(ordered)
    mid = n // 2
    if n % 2 == 1:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def _extract_totals_market_from_bookmaker(bookmaker, target_point=1.5):
    markets = bookmaker.get("markets") or []
    best = None
    for market in markets:
        if not isinstance(market, dict):
            continue
        if market.get("key") != ODDS_API_MARKET_KEY_P1_TOTALS:
            continue

        by_point = defaultdict(dict)
        for outcome in market.get("outcomes") or []:
            if not isinstance(outcome, dict):
                continue
            name = str(outcome.get("name") or "").strip().lower()
            if name not in {"over", "under"}:
                continue
            point = outcome.get("point")
            price = outcome.get("price")
            if point is None or price is None:
                continue
            try:
                point_f = float(point)
                price_f = float(price)
            except (TypeError, ValueError):
                continue
            by_point[point_f][name] = price_f

        for point, prices in by_point.items():
            if "over" not in prices or "under" not in prices:
                continue
            candidate = {
                "point": point,
                "over_price": prices["over"],
                "under_price": prices["under"],
                "last_update": market.get("last_update"),
            }
            if best is None:
                best = candidate
                continue
            current_dist = abs(candidate["point"] - target_point)
            best_dist = abs(best["point"] - target_point)
            if current_dist < best_dist:
                best = candidate
    return best


def pair_key(team_id_a, team_id_b):
    low, high = sorted((int(team_id_a), int(team_id_b)))
    return f"{low}_{high}"


def is_fanduel_bookmaker(bookmaker_key, bookmaker_title):
    key = str(bookmaker_key or "").strip().lower()
    if key == "fanduel":
        return True
    normalized_title = re.sub(r"[^a-z0-9]+", "", str(bookmaker_title or "").lower())
    return "fanduel" in normalized_title


def assign_competition_ranks(rows, metric_key, rank_key, reverse=False):
    if reverse:
        ordered = sorted(rows, key=lambda r: (-r.get(metric_key, 0.0), -r.get("games", 0), r.get("name", "")))
    else:
        ordered = sorted(rows, key=lambda r: (r.get(metric_key, 0.0), -r.get("games", 0), r.get("name", "")))

    prev_value = None
    current_rank = 0
    for idx, row in enumerate(ordered, 1):
        value = row.get(metric_key, 0.0)
        if prev_value is None or value != prev_value:
            current_rank = idx
            prev_value = value
        row[rank_key] = current_rank


def recency_weight(index, half_life):
    if half_life <= 0:
        return 1.0
    return 0.5 ** (index / half_life)


def weighted_recent_value(samples, value_key, window, half_life):
    if not samples:
        return 0.0, 0

    ordered = sorted(samples, key=lambda s: (s.get("game_date") or "", s.get("game_id") or 0), reverse=True)
    subset = ordered[:window]
    numerator = 0.0
    denominator = 0.0
    for idx, sample in enumerate(subset):
        weight = recency_weight(idx, half_life)
        numerator += weight * float(sample.get(value_key, 0.0))
        denominator += weight
    return (numerator / denominator) if denominator > 0 else 0.0, len(subset)


def blend_with_recent(season_value, recent_value, recent_games, base_weight, full_weight_games):
    if full_weight_games <= 0:
        full_weight_games = 1.0
    weight = base_weight * clamp(recent_games / full_weight_games, 0.0, 1.0)
    blended = ((1.0 - weight) * season_value) + (weight * recent_value)
    return blended, weight


def compute_team_window_stats(samples, windows=EMPIRICAL_WINDOWS):
    ordered = sorted(samples, key=lambda s: (s.get("game_date") or "", s.get("game_id") or 0), reverse=True)
    window_stats = {}
    for window in windows:
        subset = ordered[:window]
        if not subset:
            continue
        games = len(subset)
        gf_total = sum(float(s.get("gf", 0.0)) for s in subset)
        ga_total = sum(float(s.get("ga", 0.0)) for s in subset)
        combined_total = sum(float(s.get("combined", 0.0)) for s in subset)
        ge2_count = sum(1 for s in subset if float(s.get("combined", 0.0)) >= 2.0)
        window_stats[str(window)] = {
            "games": games,
            "gf_total": gf_total,
            "ga_total": ga_total,
            "combined_total": combined_total,
            "gf_pg": gf_total / games,
            "ga_pg": ga_total / games,
            "combined_pg": combined_total / games,
            "ge2_count": ge2_count,
            "ge2_pct": ge2_count / games,
        }
    return window_stats


def compute_goalie_window_stats(samples, windows=EMPIRICAL_WINDOWS):
    ordered = sorted(samples, key=lambda s: (s.get("game_date") or "", s.get("game_id") or 0), reverse=True)
    window_stats = {}
    for window in windows:
        subset = ordered[:window]
        if not subset:
            continue
        games = len(subset)
        ga_total = sum(float(s.get("ga", 0.0)) for s in subset)
        shots_total = sum(float(s.get("shots_against", 0.0)) for s in subset)
        saves_total = sum(float(s.get("saves", 0.0)) for s in subset)
        allow1_count = sum(1 for s in subset if float(s.get("ga", 0.0)) >= 1.0)
        allow2_count = sum(1 for s in subset if float(s.get("ga", 0.0)) >= 2.0)
        window_stats[str(window)] = {
            "games": games,
            "ga_total": ga_total,
            "ga_pg": ga_total / games,
            "shots_total": shots_total,
            "saves_total": saves_total,
            "sv_pct": (saves_total / shots_total * 100.0) if shots_total > 0 else 0.0,
            "allow1_count": allow1_count,
            "allow2_count": allow2_count,
            "allow1_pct": allow1_count / games,
            "allow2_pct": allow2_count / games,
        }
    return window_stats


def build_projection_dataset(force_refresh=False, verbose=True):
    if not force_refresh:
        cached = hybrid._cache_get(DATASET_CACHE_KEY)
        if cached and cached.get("version") == DATASET_VERSION:
            if verbose:
                print("Loaded cached projection dataset.")
            return cached

    if verbose:
        print("Building projection dataset from NHL APIs...")

    team_lookup_rows = hybrid.fetch_team_lookup().get("rows", [])
    goalie_rows = hybrid.fetch_goalie_summary_game_rows().get("rows", [])
    goals_rows = hybrid.fetch_team_goals_by_period_rows().get("rows", [])
    if not goals_rows:
        raise RuntimeError("No team goals-by-period rows returned.")

    team_abbrev_to_id = {}
    team_id_to_abbrev = {}
    team_id_to_name = {}
    for row in team_lookup_rows:
        team_id = hybrid._safe_int(row.get("id"), default=None)
        tri = (row.get("triCode") or "").strip().upper()
        full_name = (row.get("fullName") or "").strip()
        if team_id is None:
            continue
        if tri:
            team_abbrev_to_id[tri] = team_id
            team_id_to_abbrev[team_id] = tri
        if full_name:
            team_id_to_name[team_id] = full_name

    goals1_map = {}
    team_scoring = defaultdict(
        lambda: {
            "games": 0,
            "goals_for": 0,
            "goals_against": 0,
            "games_2plus_combined": 0,
            "name": "",
            "abbrev": "",
        }
    )
    seen_team_games = set()
    game_two_plus_map = {}
    game_date_by_id = {}
    team_game_samples = defaultdict(list)
    game_team_goals = defaultdict(dict)

    for row in goals_rows:
        game_id = hybrid._safe_int(row.get("gameId"), default=None)
        team_id = hybrid._safe_int(row.get("teamId"), default=None)
        game_date = (row.get("gameDate") or "").strip()
        if game_id is None or team_id is None:
            continue
        if game_id not in game_date_by_id and game_date:
            game_date_by_id[game_id] = game_date

        goals1_map[(game_id, team_id)] = hybrid._safe_int(row.get("period1GoalsAgainst"))
        team_game_key = (game_id, team_id)
        if team_game_key in seen_team_games:
            continue
        seen_team_games.add(team_game_key)

        rec = team_scoring[team_id]
        goals_for = hybrid._safe_int(row.get("period1GoalsFor"))
        goals_against = hybrid._safe_int(row.get("period1GoalsAgainst"))
        combined = goals_for + goals_against

        rec["games"] += 1
        rec["goals_for"] += goals_for
        rec["goals_against"] += goals_against
        if combined >= 2:
            rec["games_2plus_combined"] += 1
        team_game_samples[team_id].append(
            {
                "game_id": game_id,
                "game_date": game_date,
                "gf": goals_for,
                "ga": goals_against,
                "combined": combined,
                "combined_2plus": 1.0 if combined >= 2 else 0.0,
            }
        )
        game_team_goals[game_id][team_id] = goals_for
        if not rec["name"]:
            rec["name"] = (row.get("teamFullName") or team_id_to_name.get(team_id) or f"Team {team_id}").strip()
        if not rec["abbrev"]:
            rec["abbrev"] = team_id_to_abbrev.get(team_id, "")
        if game_id not in game_two_plus_map:
            game_two_plus_map[game_id] = combined >= 2

    h2h_games = []
    for game_id, goals_by_team in game_team_goals.items():
        if len(goals_by_team) != 2:
            continue
        team_ids = sorted(goals_by_team.keys())
        team_a_id, team_b_id = team_ids[0], team_ids[1]
        team_a_gf = goals_by_team[team_a_id]
        team_b_gf = goals_by_team[team_b_id]
        combined = team_a_gf + team_b_gf
        h2h_games.append(
            {
                "game_id": game_id,
                "game_date": game_date_by_id.get(game_id, ""),
                "pair_key": pair_key(team_a_id, team_b_id),
                "team_a_id": team_a_id,
                "team_b_id": team_b_id,
                "team_a_gf": team_a_gf,
                "team_b_gf": team_b_gf,
                "combined_1p": combined,
            }
        )
    h2h_games.sort(key=lambda g: (g.get("game_date") or "", g.get("game_id") or 0), reverse=True)

    if not goals1_map:
        raise RuntimeError("No 1P goals-against map could be built from team data.")

    completed_game_ids = sorted({game_id for (game_id, _) in goals1_map.keys()})
    min_game_id = min(completed_game_ids)
    max_game_id = max(completed_game_ids)

    team_game_rows = hybrid.fetch_team_by_game_rows(min_game_id, max_game_id).get("rows", [])
    shots1_map = {}
    shots_total_map = {}
    completed_set = set(completed_game_ids)
    for row in team_game_rows:
        game_id = hybrid._safe_int(row.get("gameId"), default=None)
        team_id = hybrid._parse_team_id_from_team_row(row)
        if game_id is None or team_id is None or game_id not in completed_set:
            continue
        shots1_map[(game_id, team_id)] = hybrid._safe_int(row.get("shotsAgainstPeriod1"))
        shots_total_map[(game_id, team_id)] = hybrid._safe_int(row.get("shotsAgainst"))

    goalie_rows_by_team_game = defaultdict(list)
    goalie_name_map = {}
    for row in goalie_rows:
        game_id = hybrid._safe_int(row.get("gameId"), default=None)
        team_abbrev = (row.get("teamAbbrev") or "").strip().upper()
        team_id = team_abbrev_to_id.get(team_abbrev)
        goalie_id = hybrid._safe_int(row.get("playerId"), default=None)
        if game_id is None or team_id is None or goalie_id is None:
            continue
        goalie_rows_by_team_game[(game_id, team_id)].append(row)
        full_name = (row.get("goalieFullName") or "").strip()
        if full_name:
            goalie_name_map[goalie_id] = full_name

    direct_team_game_stats = {}
    unresolved_team_games = set()

    for (game_id, team_id), goals_against_p1 in goals1_map.items():
        shots_against_p1 = shots1_map.get((game_id, team_id))
        team_total_shots_against = shots_total_map.get((game_id, team_id))
        goalie_rows_for_team_game = goalie_rows_by_team_game.get((game_id, team_id), [])

        if shots_against_p1 is None or team_total_shots_against is None:
            unresolved_team_games.add((game_id, team_id))
            continue
        if not goalie_rows_for_team_game:
            unresolved_team_games.add((game_id, team_id))
            continue

        rows_with_shots = [r for r in goalie_rows_for_team_game if hybrid._safe_int(r.get("shotsAgainst")) > 0]
        chosen = None
        if len(rows_with_shots) == 1 and hybrid._safe_int(rows_with_shots[0].get("shotsAgainst")) == team_total_shots_against:
            chosen = rows_with_shots[0]
        elif (
            len(goalie_rows_for_team_game) == 1
            and hybrid._safe_int(goalie_rows_for_team_game[0].get("shotsAgainst")) == team_total_shots_against
        ):
            chosen = goalie_rows_for_team_game[0]

        if chosen is None:
            unresolved_team_games.add((game_id, team_id))
            continue

        goalie_id = hybrid._safe_int(chosen.get("playerId"), default=None)
        if goalie_id is None:
            unresolved_team_games.add((game_id, team_id))
            continue

        saves_p1 = max(shots_against_p1 - goals_against_p1, 0)
        direct_team_game_stats[(game_id, team_id)] = {
            "goalie_id": goalie_id,
            "shots_against": shots_against_p1,
            "goals_against": goals_against_p1,
            "saves": saves_p1,
        }

    goalie_totals = defaultdict(
        lambda: {"games": 0, "goals_against": 0, "shots_against": 0, "saves": 0}
    )
    goalie_by_team = defaultdict(
        lambda: {"games": 0, "goals_against": 0, "shots_against": 0, "saves": 0}
    )
    goalie_game_samples = defaultdict(list)

    def add_goalie_sample(team_id, goalie_id, game_id, shots_against, goals_against, saves):
        if shots_against <= 0:
            return
        for bucket in (goalie_totals[goalie_id], goalie_by_team[(team_id, goalie_id)]):
            bucket["games"] += 1
            bucket["goals_against"] += goals_against
            bucket["shots_against"] += shots_against
            bucket["saves"] += saves
        goalie_game_samples[(team_id, goalie_id)].append(
            {
                "game_id": game_id,
                "game_date": game_date_by_id.get(game_id, ""),
                "ga": goals_against,
                "shots_against": shots_against,
                "saves": saves,
            }
        )

    for (game_id, team_id), stats in direct_team_game_stats.items():
        add_goalie_sample(
            team_id=team_id,
            goalie_id=stats["goalie_id"],
            game_id=game_id,
            shots_against=stats["shots_against"],
            goals_against=stats["goals_against"],
            saves=stats["saves"],
        )

    unresolved_game_ids = sorted({game_id for (game_id, _) in unresolved_team_games})
    failed_fallback_games = 0
    if unresolved_game_ids:
        unresolved_by_game = defaultdict(set)
        for game_id, team_id in unresolved_team_games:
            unresolved_by_game[game_id].add(team_id)

        if verbose:
            print(f"Resolving {len(unresolved_game_ids)} fallback games via play-by-play...")

        with ThreadPoolExecutor(max_workers=hybrid.MAX_WORKERS) as executor:
            futures = {executor.submit(hybrid.get_play_by_play, gid): gid for gid in unresolved_game_ids}
            completed = 0
            for future in as_completed(futures):
                game_id, pbp_data = future.result()
                completed += 1

                if pbp_data is None:
                    failed_fallback_games += 1
                else:
                    pbp_stats_by_team = hybrid.process_game_for_goalies_by_team_from_pbp(pbp_data)
                    for team_id in unresolved_by_game.get(game_id, set()):
                        for goalie_id, stats in pbp_stats_by_team.get(team_id, {}).items():
                            add_goalie_sample(
                                team_id=team_id,
                                goalie_id=goalie_id,
                                game_id=game_id,
                                shots_against=stats["shots_against"],
                                goals_against=stats["goals_against"],
                                saves=stats["saves"],
                            )

                if verbose and (completed % 50 == 0 or completed == len(unresolved_game_ids)):
                    print(f"  Fallback progress: {completed}/{len(unresolved_game_ids)}")

    for goalie_id in list(goalie_totals.keys()):
        if goalie_id not in goalie_name_map:
            goalie_name_map[goalie_id] = hybrid.get_goalie_name(goalie_id)

    team_rows = []
    total_team_games = 0
    total_team_goals_for = 0
    total_team_goals_against = 0
    for team_id, rec in team_scoring.items():
        games = rec["games"]
        goals_for = rec["goals_for"]
        goals_against = rec["goals_against"]
        team_samples = team_game_samples.get(team_id, [])
        team_window_stats = compute_team_window_stats(team_samples)
        recent_gf_pg, recent_games = weighted_recent_value(
            team_samples,
            value_key="gf",
            window=TEAM_RECENT_WINDOW,
            half_life=TEAM_RECENT_HALF_LIFE,
        )
        recent_ga_pg, _ = weighted_recent_value(
            team_samples,
            value_key="ga",
            window=TEAM_RECENT_WINDOW,
            half_life=TEAM_RECENT_HALF_LIFE,
        )
        recent_2plus_pct, _ = weighted_recent_value(
            team_samples,
            value_key="combined_2plus",
            window=TEAM_RECENT_WINDOW,
            half_life=TEAM_RECENT_HALF_LIFE,
        )
        total_team_games += games
        total_team_goals_for += goals_for
        total_team_goals_against += goals_against
        team_rows.append(
            {
                "team_id": team_id,
                "abbrev": rec["abbrev"] or team_id_to_abbrev.get(team_id, ""),
                "name": rec["name"] or team_id_to_name.get(team_id, f"Team {team_id}"),
                "games": games,
                "goals_for": goals_for,
                "goals_against": goals_against,
                "gf_pg": (goals_for / games) if games > 0 else 0.0,
                "ga_pg": (goals_against / games) if games > 0 else 0.0,
                "combined_pg": ((goals_for + goals_against) / games) if games > 0 else 0.0,
                "games_2plus_combined": rec["games_2plus_combined"],
                "games_2plus_pct": (rec["games_2plus_combined"] / games) if games > 0 else 0.0,
                "recent_games": recent_games,
                "recent_gf_pg": recent_gf_pg,
                "recent_ga_pg": recent_ga_pg,
                "recent_2plus_pct": recent_2plus_pct,
                "window_stats": team_window_stats,
            }
        )
    team_rows.sort(key=lambda t: t["abbrev"] or t["name"])

    goalie_rows_out = []
    for (team_id, goalie_id), stats in goalie_by_team.items():
        games = stats["games"]
        shots = stats["shots_against"]
        goals_against = stats["goals_against"]
        saves = stats["saves"]
        goalie_samples = goalie_game_samples.get((team_id, goalie_id), [])
        goalie_window_stats = compute_goalie_window_stats(goalie_samples)
        recent_ga_pg, recent_games = weighted_recent_value(
            goalie_samples,
            value_key="ga",
            window=GOALIE_RECENT_WINDOW,
            half_life=GOALIE_RECENT_HALF_LIFE,
        )
        goalie_rows_out.append(
            {
                "team_id": team_id,
                "team_abbrev": team_id_to_abbrev.get(team_id, ""),
                "goalie_id": goalie_id,
                "name": goalie_name_map.get(goalie_id, f"Goalie {goalie_id}"),
                "games": games,
                "goals_against": goals_against,
                "shots_against": shots,
                "saves": saves,
                "ga_pg": (goals_against / games) if games > 0 else 0.0,
                "save_pct": (saves / shots * 100) if shots > 0 else 0.0,
                "recent_games": recent_games,
                "recent_ga_pg": recent_ga_pg,
                "window_stats": goalie_window_stats,
            }
        )

    goalie_rows_out.sort(key=lambda g: (g["team_abbrev"], -g["games"], g["name"]))
    ranked_goalies_all = [g for g in goalie_rows_out if g["games"] > 0]
    ranked_goalies_qualified = [g for g in ranked_goalies_all if g["games"] >= GOALIE_MIN_GAMES_FOR_QUALIFIED_RANK]

    assign_competition_ranks(ranked_goalies_all, "ga_pg", "rank_ga_pg_all", reverse=False)
    assign_competition_ranks(ranked_goalies_all, "save_pct", "rank_save_pct_all", reverse=True)
    assign_competition_ranks(ranked_goalies_qualified, "ga_pg", "rank_ga_pg_qualified", reverse=False)
    assign_competition_ranks(ranked_goalies_qualified, "save_pct", "rank_save_pct_qualified", reverse=True)

    total_goalies_all = len(ranked_goalies_all)
    total_goalies_qualified = len(ranked_goalies_qualified)
    for goalie in goalie_rows_out:
        goalie["rank_total_goalies_all"] = total_goalies_all
        goalie["rank_total_goalies_qualified"] = total_goalies_qualified
        goalie["rank_min_games_qualified"] = GOALIE_MIN_GAMES_FOR_QUALIFIED_RANK

    league_games_total = len(game_two_plus_map)
    league_games_2plus = sum(1 for flag in game_two_plus_map.values() if flag)
    league_team_goal_rate = (total_team_goals_for / total_team_games) if total_team_games > 0 else 0.0
    league_combined_rate = (
        ((total_team_goals_for + total_team_goals_against) / total_team_games) if total_team_games > 0 else 0.0
    )
    total_goalie_games = sum(g["games"] for g in goalie_totals.values())
    total_goalie_ga = sum(g["goals_against"] for g in goalie_totals.values())
    league_goalie_ga_rate = (total_goalie_ga / total_goalie_games) if total_goalie_games > 0 else league_team_goal_rate

    dataset = {
        "version": DATASET_VERSION,
        "season_id": hybrid.SEASON_ID,
        "game_type_id": hybrid.GAME_TYPE_ID,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "league": {
            "team_goal_rate": league_team_goal_rate,
            "combined_rate": league_combined_rate,
            "game_2plus_pct": (league_games_2plus / league_games_total) if league_games_total > 0 else 0.0,
            "goalie_ga_rate": league_goalie_ga_rate,
        },
        "teams": team_rows,
        "goalies": goalie_rows_out,
        "h2h_games": h2h_games,
        "meta": {
            "games_total": league_games_total,
            "games_2plus": league_games_2plus,
            "direct_team_games": len(direct_team_game_stats),
            "fallback_team_games": len(unresolved_team_games),
            "fallback_games": len(unresolved_game_ids),
            "failed_fallback_games": failed_fallback_games,
            "goalies_all": total_goalies_all,
            "goalies_qualified": total_goalies_qualified,
            "goalie_rank_min_games": GOALIE_MIN_GAMES_FOR_QUALIFIED_RANK,
        },
    }

    hybrid._cache_set(DATASET_CACHE_KEY, dataset)
    if verbose:
        print("Projection dataset cached.")
    return dataset


def index_dataset(dataset):
    teams = dataset.get("teams", [])
    goalies = dataset.get("goalies", [])

    teams_by_id = {}
    for team in teams:
        teams_by_id[team["team_id"]] = team

    goalies_by_team = defaultdict(list)
    for goalie in goalies:
        goalies_by_team[goalie["team_id"]].append(goalie)
    for team_id in goalies_by_team:
        goalies_by_team[team_id].sort(key=lambda g: (-g["games"], g["ga_pg"], g["name"]))

    return teams_by_id, goalies_by_team


def resolve_team(query, teams_by_id):
    q = normalize_text(query)
    if not q:
        raise ValueError("Team input cannot be empty.")

    exact = []
    partial = []
    for team in teams_by_id.values():
        ab = normalize_text(team["abbrev"])
        nm = normalize_text(team["name"])
        if q == ab or q == nm:
            exact.append(team)
        elif q in ab or q in nm:
            partial.append(team)

    candidates = exact if exact else partial
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        options = ", ".join(f"{t['abbrev']} ({t['name']})" for t in sorted(candidates, key=lambda x: x["abbrev"]))
        raise ValueError(f"Ambiguous team '{query}'. Matches: {options}")
    raise ValueError(f"Team '{query}' was not found in season dataset.")


def resolve_goalie(query, team_goalies):
    q = normalize_text(query)
    if not q:
        raise ValueError("Goalie input cannot be empty.")

    if query.isdigit():
        goalie_id = int(query)
        by_id = [g for g in team_goalies if g["goalie_id"] == goalie_id]
        if len(by_id) == 1:
            return by_id[0]

    exact = [g for g in team_goalies if normalize_text(g["name"]) == q]
    if len(exact) == 1:
        return exact[0]
    if len(exact) > 1:
        raise ValueError(f"Multiple exact goalie matches for '{query}'.")

    partial = [g for g in team_goalies if q in normalize_text(g["name"])]
    if len(partial) == 1:
        return partial[0]
    if len(partial) > 1:
        names = ", ".join(f"{g['name']} (GP {g['games']})" for g in partial)
        raise ValueError(f"Ambiguous goalie '{query}'. Matches: {names}")
    raise ValueError(f"Goalie '{query}' not found for selected team.")


def goalie_rank_text(goalie):
    ga_all = goalie.get("rank_ga_pg_all")
    sv_all = goalie.get("rank_save_pct_all")
    total_all = goalie.get("rank_total_goalies_all")
    ga_q = goalie.get("rank_ga_pg_qualified")
    sv_q = goalie.get("rank_save_pct_qualified")
    total_q = goalie.get("rank_total_goalies_qualified")
    min_q = goalie.get("rank_min_games_qualified", GOALIE_MIN_GAMES_FOR_QUALIFIED_RANK)

    if ga_q and sv_q and total_q:
        return (
            f"GA rank #{ga_q}/{total_q}, SV rank #{sv_q}/{total_q} "
            f"(qualified, {min_q}+ GP)"
        )
    if ga_all and sv_all and total_all:
        return f"GA rank #{ga_all}/{total_all}, SV rank #{sv_all}/{total_all} (all goalies)"
    return "Rank unavailable"


def get_window_stats_row(window_stats, window):
    return (window_stats or {}).get(str(window))


def format_team_o15(team):
    parts = []
    for window in EMPIRICAL_WINDOWS:
        row = get_window_stats_row(team.get("window_stats"), window)
        if not row:
            continue
        parts.append(f"L{window}: {int(row['ge2_count'])}/{int(row['games'])} 2+ ({row['ge2_pct'] * 100:.1f}%)")
    return " | ".join(parts) if parts else "No games"


def format_team_gf_ga(team):
    parts = []
    for window in EMPIRICAL_WINDOWS:
        row = get_window_stats_row(team.get("window_stats"), window)
        if not row:
            continue
        parts.append(f"L{window}: GF {row['gf_pg']:.2f}, GA {row['ga_pg']:.2f} ({int(row['games'])}g)")
    return " | ".join(parts) if parts else "No games"


def format_goalie_ga(goalie):
    parts = []
    for window in EMPIRICAL_WINDOWS:
        row = get_window_stats_row(goalie.get("window_stats"), window)
        if not row:
            continue
        parts.append(f"L{window}: {int(row['ga_total'])} GA ({row['ga_pg']:.2f}/GP, {int(row['games'])}g)")
    return " | ".join(parts) if parts else "No games"


def format_goalie_allow2(goalie):
    parts = []
    for window in EMPIRICAL_WINDOWS:
        row = get_window_stats_row(goalie.get("window_stats"), window)
        if not row:
            continue
        parts.append(f"L{window}: {int(row['allow2_count'])}/{int(row['games'])} allow 2+ ({row['allow2_pct'] * 100:.1f}%)")
    return " | ".join(parts) if parts else "No games"


def head_to_head_games(dataset, team_away, team_home):
    key = pair_key(team_away["team_id"], team_home["team_id"])
    games = [g for g in dataset.get("h2h_games", []) if g.get("pair_key") == key]
    games.sort(key=lambda g: (g.get("game_date") or "", g.get("game_id") or 0), reverse=True)
    return games


def format_head_to_head_recent(games, team_away, team_home, max_games=5):
    lines = []
    for game in games[:max_games]:
        away_gf = game["team_a_gf"] if game["team_a_id"] == team_away["team_id"] else game["team_b_gf"]
        home_gf = game["team_a_gf"] if game["team_a_id"] == team_home["team_id"] else game["team_b_gf"]
        game_label = game.get("game_date") or f"game {game['game_id']}"
        lines.append(
            f"{game_label}: {team_away['abbrev']} {away_gf}-{home_gf} {team_home['abbrev']} (1P total {game['combined_1p']})"
        )
    return " | ".join(lines) if lines else "No meetings in dataset"


def dfo_build_url(date_str=None):
    if date_str:
        return f"{DAILY_FACEOFF_URL}/{date_str}"
    return DAILY_FACEOFF_URL


def dfo_normalize_status(raw_status):
    text = (raw_status or "").strip()
    if not text:
        return DFO_STATUS_FALLBACK

    lowered = text.lower()
    if lowered == "confirmed":
        return "Confirmed"
    if lowered == "likely":
        return "Likely"
    if lowered in {"projected", "expected", "probable"}:
        return "Projected"
    if lowered in {"unconfirmed", "not confirmed"}:
        return "Unconfirmed"
    return text


def fetch_daily_faceoff_starting_goalies(date_str=None, timeout=DAILY_FACEOFF_TIMEOUT):
    source_url = dfo_build_url(date_str)
    response = requests.get(source_url, headers=DFO_REQUEST_HEADERS, timeout=timeout)
    if response.status_code != 200:
        raise RuntimeError(f"Daily Faceoff request failed with HTTP {response.status_code}")

    html = response.text
    if "Attention Required! | Cloudflare" in html or "Sorry, you have been blocked" in html:
        raise RuntimeError("Daily Faceoff blocked this request (Cloudflare).")

    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html, re.DOTALL)
    if not match:
        raise RuntimeError("Daily Faceoff page payload (__NEXT_DATA__) not found.")

    try:
        next_data = json.loads(match.group(1))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Failed to decode Daily Faceoff page payload: {exc}") from exc

    page_props = ((next_data.get("props") or {}).get("pageProps") or {})
    rows = page_props.get("data")
    if not isinstance(rows, list):
        raise RuntimeError("Unexpected Daily Faceoff page shape: pageProps.data is not a list.")

    pulled_at_utc = datetime.now(DFO_UTC_TZ).isoformat(timespec="seconds")
    target_date = page_props.get("date") or date_str or datetime.now(DFO_ET_TZ).strftime("%Y-%m-%d")
    games = []
    for row in rows:
        if not isinstance(row, dict):
            continue

        game_time_utc = None
        game_time_et = None
        date_gmt = row.get("dateGmt")
        if date_gmt:
            try:
                parsed_utc = datetime.fromisoformat(date_gmt.replace("Z", "+00:00")).astimezone(DFO_UTC_TZ)
                game_time_utc = parsed_utc.isoformat(timespec="seconds")
                game_time_et = parsed_utc.astimezone(DFO_ET_TZ).strftime("%Y-%m-%d %I:%M %p ET")
            except ValueError:
                game_time_utc = None
                game_time_et = None

        games.append(
            {
                "source_url": source_url,
                "game_date": row.get("date") or target_date,
                "game_time_utc": game_time_utc,
                "game_time_et": game_time_et,
                "away_team_name": row.get("awayTeamName"),
                "home_team_name": row.get("homeTeamName"),
                "away_goalie_name": row.get("awayGoalieName") or "TBD",
                "home_goalie_name": row.get("homeGoalieName") or "TBD",
                "away_status": dfo_normalize_status(row.get("awayNewsStrengthName")),
                "home_status": dfo_normalize_status(row.get("homeNewsStrengthName")),
                "away_status_updated_at_utc": row.get("awayNewsCreatedAt"),
                "home_status_updated_at_utc": row.get("homeNewsCreatedAt"),
            }
        )

    games.sort(
        key=lambda g: (
            g.get("game_time_utc") or "",
            g.get("away_team_name") or "",
            g.get("home_team_name") or "",
        )
    )

    return {
        "source_url": source_url,
        "pulled_at_utc": pulled_at_utc,
        "target_date": target_date,
        "games": games,
    }


def nhl_score_build_url(date_str=None):
    target = date_str or datetime.now(DFO_ET_TZ).strftime("%Y-%m-%d")
    return f"{hybrid.WEB_BASE_URL}/score/{target}"


def fetch_nhl_daily_game_status(date_str=None, timeout=NHL_SCORE_TIMEOUT):
    score_url = nhl_score_build_url(date_str)
    response = requests.get(score_url, headers=DFO_REQUEST_HEADERS, timeout=timeout)
    if response.status_code != 200:
        raise RuntimeError(f"NHL score request failed with HTTP {response.status_code}")

    payload = response.json()
    games = payload.get("games")
    if not isinstance(games, list):
        raise RuntimeError("Unexpected NHL score payload shape: 'games' not found.")

    by_team_pair = {}
    for game in games:
        if not isinstance(game, dict):
            continue
        away_team = game.get("awayTeam") or {}
        home_team = game.get("homeTeam") or {}
        away_id = hybrid._safe_int(away_team.get("id"))
        home_id = hybrid._safe_int(home_team.get("id"))
        if away_id <= 0 or home_id <= 0:
            continue

        state_code = str(game.get("gameState") or "").upper()
        state_label = NHL_LIVE_STATE_MAP.get(state_code, state_code or "Scheduled")
        period_descriptor = game.get("periodDescriptor") or {}
        period = hybrid._safe_int(period_descriptor.get("number") or game.get("period"))
        clock_payload = game.get("clock") or {}
        clock_time = clock_payload.get("timeRemaining")
        in_intermission = bool(clock_payload.get("inIntermission"))

        first_period_goals = 0
        for goal in game.get("goals") or []:
            if not isinstance(goal, dict):
                continue
            goal_period_descriptor = goal.get("periodDescriptor") or {}
            goal_period_type = str(goal_period_descriptor.get("periodType") or "REG").upper()
            goal_period = hybrid._safe_int(goal.get("period") or goal_period_descriptor.get("number"))
            if goal_period == 1 and goal_period_type == "REG":
                first_period_goals += 1

        first_period_complete = False
        if state_code in {"OFF", "FINAL"}:
            first_period_complete = True
        elif state_code in {"LIVE", "CRIT"}:
            if period > 1:
                first_period_complete = True
            elif period == 1 and in_intermission:
                first_period_complete = True

        first_period_total_result = None
        if first_period_complete:
            first_period_total_result = "OVER" if first_period_goals >= 2 else "UNDER"

        by_team_pair[tuple(sorted((away_id, home_id)))] = {
            "nhl_game_id": hybrid._safe_int(game.get("id")),
            "state_code": state_code,
            "state_label": state_label,
            "period": period,
            "clock": clock_time,
            "in_intermission": in_intermission,
            "away_score": hybrid._safe_int(away_team.get("score")),
            "home_score": hybrid._safe_int(home_team.get("score")),
            "first_period_goals": first_period_goals,
            "first_period_complete": first_period_complete,
            "first_period_total_result": first_period_total_result,
            "source_url": score_url,
        }

    return {
        "source_url": score_url,
        "target_date": date_str or datetime.now(DFO_ET_TZ).strftime("%Y-%m-%d"),
        "games_count": len(games),
        "by_team_pair": by_team_pair,
    }


def fetch_daily_market_totals_p1(
    dataset,
    date_str=None,
    target_team_pairs=None,
    verbose=True,
    max_age_seconds=None,
    force_refresh=False,
):
    api_keys = _read_odds_api_keys()
    if not api_keys:
        return {
            "source_url": None,
            "target_date": date_str or datetime.now(DFO_ET_TZ).strftime("%Y-%m-%d"),
            "by_team_pair": {},
            "warnings": ["THE_ODDS_API_KEY/key.json not found; market dampening disabled."],
        }

    target_date = date_str or datetime.now(DFO_ET_TZ).strftime("%Y-%m-%d")
    cache_key = f"oddsapi_totals_p1_{target_date}"
    cache_max_age = ODDS_API_CACHE_MAX_AGE_SECONDS if max_age_seconds is None else max(0, int(max_age_seconds))
    if not force_refresh:
        cached = hybrid._cache_get(cache_key, max_age_seconds=cache_max_age)
        if cached is not None and isinstance(cached, dict):
            return cached

    teams_by_id, _ = index_dataset(dataset)
    warnings = []
    by_team_pair = {}
    events_url = f"{ODDS_API_BASE_URL}/sports/icehockey_nhl/events"
    events = None
    events_failure_response = None
    key_remaining_credits = {}
    for candidate_key in api_keys:
        candidate_response = requests.get(
            events_url,
            params={"api_key": candidate_key},
            timeout=ODDS_API_TIMEOUT,
        )
        if candidate_response.status_code == 200:
            if events is None:
                candidate_events = candidate_response.json()
                if not isinstance(candidate_events, list):
                    return {
                        "source_url": events_url,
                        "target_date": target_date,
                        "by_team_pair": {},
                        "warnings": ["Odds API events response shape unexpected."],
                    }
                events = candidate_events
            key_remaining_credits[candidate_key] = _parse_int_header(
                candidate_response.headers.get("x-requests-remaining")
            )
            continue
        events_failure_response = candidate_response
        if candidate_response.status_code in {401, 403, 429}:
            continue
        break

    if events is None:
        status_code = events_failure_response.status_code if events_failure_response is not None else "no_response"
        text_preview = (events_failure_response.text[:220] if events_failure_response is not None else "")
        return {
            "source_url": events_url,
            "target_date": target_date,
            "by_team_pair": {},
            "warnings": [f"Odds API events request failed ({status_code}): {text_preview}"],
        }

    target_pairs_set = set(str(p) for p in (target_team_pairs or []))
    event_candidates = []
    now_utc = datetime.now(DFO_UTC_TZ)
    skipped_started_events = 0
    skipped_unparseable_events = 0

    for event in events:
        if not isinstance(event, dict):
            continue

        commence_time = str(event.get("commence_time") or "")
        if commence_time:
            # Keep only same-date ET events to match daily slate and only fetch
            # pregame odds (skip events that have already started).
            try:
                event_dt_utc = datetime.fromisoformat(commence_time.replace("Z", "+00:00")).astimezone(DFO_UTC_TZ)
                event_dt_et = event_dt_utc.astimezone(DFO_ET_TZ)
                if event_dt_et.strftime("%Y-%m-%d") != target_date:
                    continue
                if event_dt_utc <= now_utc:
                    skipped_started_events += 1
                    continue
            except ValueError:
                skipped_unparseable_events += 1
                continue
        else:
            skipped_unparseable_events += 1
            continue

        away_name = event.get("away_team") or ""
        home_name = event.get("home_team") or ""
        try:
            team_away = resolve_team_for_daily_feed(away_name, teams_by_id)
            team_home = resolve_team_for_daily_feed(home_name, teams_by_id)
        except ValueError:
            continue

        pair = pair_key(team_away["team_id"], team_home["team_id"])
        if target_pairs_set and pair not in target_pairs_set:
            continue

        event_id = event.get("id")
        if not event_id:
            continue

        event_candidates.append(
            {
                "event_id": event_id,
                "pair": pair,
                "away_name": away_name,
                "home_name": home_name,
            }
        )

    credits_per_event = _estimate_odds_api_credits_per_event()
    estimated_required_credits = (len(event_candidates) * credits_per_event) + ODDS_API_MIN_HEADROOM_CREDITS
    key_credits_ordered = [(k, key_remaining_credits.get(k)) for k in api_keys if k in key_remaining_credits]

    if verbose and skipped_started_events > 0:
        print(f"Odds API pregame filter: skipped {skipped_started_events} started events.")
    if verbose and skipped_unparseable_events > 0:
        print(f"Odds API pregame filter: skipped {skipped_unparseable_events} events with invalid commence_time.")

    active_api_key = None
    for candidate_key, remaining in key_credits_ordered:
        if remaining is None or remaining >= estimated_required_credits:
            active_api_key = candidate_key
            break
    if active_api_key is None and key_credits_ordered:
        active_api_key = max(key_credits_ordered, key=lambda item: -1 if item[1] is None else item[1])[0]
    if not active_api_key:
        return {
            "source_url": events_url,
            "target_date": target_date,
            "by_team_pair": {},
            "warnings": ["Odds API key check failed: no usable key found."],
        }

    active_remaining = key_remaining_credits.get(active_api_key)
    if active_remaining is not None and active_remaining < estimated_required_credits:
        warnings.append(
            "Odds API quota precheck: selected key may be short "
            f"(remaining={active_remaining}, estimated_needed={estimated_required_credits}, "
            f"events={len(event_candidates)}, credits_per_event={credits_per_event})."
        )
    if verbose:
        remaining_text = "unknown" if active_remaining is None else str(active_remaining)
        print(
            "Odds API quota precheck: "
            f"remaining={remaining_text}, estimated_needed={estimated_required_credits}, "
            f"events={len(event_candidates)}, credits_per_event={credits_per_event}"
        )

    for candidate in event_candidates:
        event_id = candidate["event_id"]
        pair = candidate["pair"]
        away_name = candidate["away_name"]
        home_name = candidate["home_name"]

        event_odds_url = f"{ODDS_API_BASE_URL}/sports/icehockey_nhl/events/{event_id}/odds"
        event_odds_response = None
        fallback_keys = sorted(
            [k for k in api_keys if k != active_api_key],
            key=lambda key: -1 if key_remaining_credits.get(key) is None else key_remaining_credits.get(key),
            reverse=True,
        )
        key_candidates = [active_api_key] + fallback_keys
        max_attempts = max(1, min(len(key_candidates), ODDS_API_EVENT_MAX_RETRIES))
        for attempt, candidate_key in enumerate(key_candidates[:max_attempts]):
            if ODDS_API_EVENT_REQUEST_SLEEP_SECONDS > 0:
                time.sleep(ODDS_API_EVENT_REQUEST_SLEEP_SECONDS)
            event_odds_response = requests.get(
                event_odds_url,
                params={
                    "api_key": candidate_key,
                    "regions": ODDS_API_DEFAULT_REGIONS,
                    "markets": ODDS_API_MARKET_KEY_P1_TOTALS,
                    "oddsFormat": "american",
                    "dateFormat": "iso",
                },
                timeout=ODDS_API_TIMEOUT,
            )
            remaining_after_call = _parse_int_header(event_odds_response.headers.get("x-requests-remaining"))
            if remaining_after_call is not None:
                key_remaining_credits[candidate_key] = remaining_after_call
            if event_odds_response.status_code == 200:
                active_api_key = candidate_key
                break
            if event_odds_response.status_code in {401, 403, 429}:
                if event_odds_response.status_code == 429:
                    time.sleep(0.8 * (attempt + 1))
                continue
            break
        if event_odds_response is None:
            continue
        if event_odds_response.status_code != 200:
            if event_odds_response.status_code == 429:
                warnings.append("Odds API frequency limit hit; market dampening may be partial.")
                continue
            warnings.append(
                f"{away_name} at {home_name}: odds request failed ({event_odds_response.status_code})"
            )
            continue

        payload = event_odds_response.json()
        bookmaker_rows = payload.get("bookmakers") or []
        entries = []
        for bookmaker in bookmaker_rows:
            if not isinstance(bookmaker, dict):
                continue
            extracted = _extract_totals_market_from_bookmaker(bookmaker, target_point=1.5)
            if not extracted:
                continue
            try:
                over_imp = american_to_implied_prob(extracted["over_price"])
                under_imp = american_to_implied_prob(extracted["under_price"])
                over_novig = over_imp / max(over_imp + under_imp, 1e-9)
            except (ValueError, ZeroDivisionError):
                continue
            entries.append(
                {
                    "bookmaker_key": bookmaker.get("key"),
                    "bookmaker_title": bookmaker.get("title"),
                    "point": extracted["point"],
                    "over_price": extracted["over_price"],
                    "under_price": extracted["under_price"],
                    "over_no_vig_prob": over_novig,
                    "last_update": extracted.get("last_update"),
                }
            )

        if not entries:
            continue

        over_probs = [row["over_no_vig_prob"] for row in entries]
        points = [row["point"] for row in entries]
        fanduel = next(
            (
                row
                for row in entries
                if is_fanduel_bookmaker(row.get("bookmaker_key"), row.get("bookmaker_title"))
            ),
            None,
        )
        by_team_pair[pair] = {
            "event_id": event_id,
            "source_url": event_odds_url,
            "book_count": len(entries),
            "consensus_point": median(points),
            "consensus_over_no_vig_prob": median(over_probs),
            "fanduel": {
                "bookmaker_key": fanduel.get("bookmaker_key"),
                "bookmaker_title": fanduel.get("bookmaker_title"),
                "point": fanduel.get("point"),
                "over_price": fanduel.get("over_price"),
                "under_price": fanduel.get("under_price"),
                "over_no_vig_prob": fanduel.get("over_no_vig_prob"),
                "last_update": fanduel.get("last_update"),
            }
            if fanduel
            else None,
            "bookmakers": entries,
        }

    result = {
        "source_url": events_url,
        "target_date": target_date,
        "by_team_pair": by_team_pair,
        "warnings": warnings,
        "cache_max_age_seconds": cache_max_age,
        "fetched_at": datetime.now().isoformat(timespec="seconds"),
    }
    hybrid._cache_set(cache_key, result)
    if verbose:
        print(
            f"Odds API totals_p1 market rows: {len(by_team_pair)} mapped games "
            f"(target date {target_date}, warnings {len(warnings)})"
        )
    return result


def resolve_team_for_daily_feed(team_name, teams_by_id):
    normalized = normalize_text(team_name)
    alias_by_normalized = {
        "STLOUISBLUES": "St. Louis Blues",
        "UTAHHOCKEYCLUB": "Utah Mammoth",
    }
    lookup_name = alias_by_normalized.get(normalized, team_name)
    return resolve_team(lookup_name, teams_by_id)


def resolve_goalie_for_daily_feed(goalie_name, team_goalies):
    if not team_goalies:
        raise ValueError("No team goalie rows available for feed resolution.")

    cleaned_name = (goalie_name or "").strip()
    if cleaned_name and normalize_text(cleaned_name) not in {"", "TBD", "TOBEDETERMINED"}:
        try:
            return resolve_goalie(cleaned_name, team_goalies), "matched_feed_name"
        except ValueError:
            pass

    return team_goalies[0], "fallback_top_sample"


def build_daily_projection_slate(
    dataset,
    date_str=None,
    verbose=True,
    odds_cache_max_age_seconds=None,
    force_refresh_odds=False,
):
    teams_by_id, goalies_by_team = index_dataset(dataset)
    league = dataset["league"]

    feed = fetch_daily_faceoff_starting_goalies(date_str=date_str)
    warnings = []
    projected_games = []
    live_status_by_pair = {}
    market_by_pair = {}
    target_pairs = set()
    for game in feed.get("games", []):
        away_name = game.get("away_team_name") or ""
        home_name = game.get("home_team_name") or ""
        try:
            team_away = resolve_team_for_daily_feed(away_name, teams_by_id)
            team_home = resolve_team_for_daily_feed(home_name, teams_by_id)
            target_pairs.add(pair_key(team_away["team_id"], team_home["team_id"]))
        except ValueError:
            continue
    try:
        live_payload = fetch_nhl_daily_game_status(date_str=feed.get("target_date"))
        live_status_by_pair = live_payload.get("by_team_pair", {})
    except Exception as exc:
        warnings.append(f"NHL live status unavailable: {exc}")
    try:
        market_payload = fetch_daily_market_totals_p1(
            dataset,
            date_str=feed.get("target_date"),
            target_team_pairs=target_pairs,
            verbose=verbose,
            max_age_seconds=odds_cache_max_age_seconds,
            force_refresh=force_refresh_odds,
        )
        market_by_pair = market_payload.get("by_team_pair", {})
        warnings.extend(market_payload.get("warnings", []))
    except Exception as exc:
        warnings.append(f"Odds API market pull unavailable: {exc}")

    for game in feed["games"]:
        away_name = game.get("away_team_name") or "Unknown Away"
        home_name = game.get("home_team_name") or "Unknown Home"

        row = {
            "source_url": game.get("source_url"),
            "game_date": game.get("game_date"),
            "game_time_utc": game.get("game_time_utc"),
            "game_time_et": game.get("game_time_et"),
            "away": {
                "team_name_feed": away_name,
                "goalie_name_feed": game.get("away_goalie_name"),
                "status": game.get("away_status", DFO_STATUS_FALLBACK),
                "status_updated_at_utc": game.get("away_status_updated_at_utc"),
            },
            "home": {
                "team_name_feed": home_name,
                "goalie_name_feed": game.get("home_goalie_name"),
                "status": game.get("home_status", DFO_STATUS_FALLBACK),
                "status_updated_at_utc": game.get("home_status_updated_at_utc"),
            },
            "projection": None,
            "trends": {},
            "notes": [],
            "error": None,
            "game_status": None,
        }

        try:
            team_away = resolve_team_for_daily_feed(away_name, teams_by_id)
            team_home = resolve_team_for_daily_feed(home_name, teams_by_id)
            if team_away["team_id"] == team_home["team_id"]:
                raise ValueError("Resolved away/home teams to the same franchise.")
        except ValueError as exc:
            msg = f"{away_name} at {home_name}: team resolution failed ({exc})"
            warnings.append(msg)
            row["error"] = msg
            projected_games.append(row)
            continue

        live_status = live_status_by_pair.get(tuple(sorted((team_away["team_id"], team_home["team_id"]))))
        if live_status:
            row["game_status"] = live_status
        market_key = pair_key(team_away["team_id"], team_home["team_id"])
        market_totals = market_by_pair.get(market_key)
        if market_totals:
            fanduel_market = market_totals.get("fanduel") or None
            if fanduel_market is None:
                for bookmaker_row in market_totals.get("bookmakers") or []:
                    if is_fanduel_bookmaker(bookmaker_row.get("bookmaker_key"), bookmaker_row.get("bookmaker_title")):
                        fanduel_market = bookmaker_row
                        break
            row["market_totals_p1"] = {
                "book_count": market_totals.get("book_count", 0),
                "consensus_point": market_totals.get("consensus_point"),
                "consensus_over_no_vig_prob": market_totals.get("consensus_over_no_vig_prob"),
                "fanduel": {
                    "bookmaker_key": fanduel_market.get("bookmaker_key"),
                    "bookmaker_title": fanduel_market.get("bookmaker_title"),
                    "point": fanduel_market.get("point"),
                    "over_price": fanduel_market.get("over_price"),
                    "under_price": fanduel_market.get("under_price"),
                    "over_no_vig_prob": fanduel_market.get("over_no_vig_prob"),
                    "last_update": fanduel_market.get("last_update"),
                }
                if fanduel_market
                else None,
            }

        away_goalies = goalies_by_team.get(team_away["team_id"], [])
        home_goalies = goalies_by_team.get(team_home["team_id"], [])
        if not away_goalies or not home_goalies:
            msg = f"{team_away['abbrev']} at {team_home['abbrev']}: missing goalie season rows in dataset"
            warnings.append(msg)
            row["error"] = msg
            projected_games.append(row)
            continue

        away_goalie, away_resolution = resolve_goalie_for_daily_feed(game.get("away_goalie_name"), away_goalies)
        home_goalie, home_resolution = resolve_goalie_for_daily_feed(game.get("home_goalie_name"), home_goalies)
        if away_resolution != "matched_feed_name":
            row["notes"].append(f"Away starter fallback used: {away_goalie['name']}")
        if home_resolution != "matched_feed_name":
            row["notes"].append(f"Home starter fallback used: {home_goalie['name']}")

        result = project_matchup(team_away, team_home, away_goalie, home_goalie, league)
        model_prob_over = result["probability_over_1p_1_5"]
        market_prob_over = (market_totals or {}).get("consensus_over_no_vig_prob") if market_totals else None
        book_count = int((market_totals or {}).get("book_count") or 0)
        dampening_weight = 0.0
        final_prob_over = model_prob_over
        if market_prob_over is not None:
            dampening_weight = clamp(
                ODDS_MARKET_DAMPEN_BASE_WEIGHT + (ODDS_MARKET_DAMPEN_PER_BOOK_WEIGHT * book_count),
                ODDS_MARKET_DAMPEN_BASE_WEIGHT,
                ODDS_MARKET_DAMPEN_MAX_WEIGHT,
            )
            final_prob_over = clamp(
                ((1.0 - dampening_weight) * model_prob_over) + (dampening_weight * float(market_prob_over)),
                PROB_MIN,
                PROB_MAX,
            )

        h2h_games = head_to_head_games(dataset, team_away, team_home)
        h2h_count = len(h2h_games)
        h2h_ge2_count = sum(1 for g in h2h_games if g.get("combined_1p", 0) >= 2)
        h2h_avg_combined = (
            sum(float(g.get("combined_1p", 0.0)) for g in h2h_games) / h2h_count if h2h_count > 0 else 0.0
        )

        row["away"].update(
            {
                "team_id": team_away["team_id"],
                "team_abbrev": team_away["abbrev"],
                "team_name": team_away["name"],
                "goalie_id": away_goalie["goalie_id"],
                "goalie_name": away_goalie["name"],
                "goalie_games": away_goalie["games"],
                "goalie_ga_pg": away_goalie["ga_pg"],
                "goalie_save_pct": away_goalie["save_pct"],
                "team_o15_pct": team_away["games_2plus_pct"],
                "team_combined_pg": team_away["combined_pg"],
            }
        )
        row["home"].update(
            {
                "team_id": team_home["team_id"],
                "team_abbrev": team_home["abbrev"],
                "team_name": team_home["name"],
                "goalie_id": home_goalie["goalie_id"],
                "goalie_name": home_goalie["name"],
                "goalie_games": home_goalie["games"],
                "goalie_ga_pg": home_goalie["ga_pg"],
                "goalie_save_pct": home_goalie["save_pct"],
                "team_o15_pct": team_home["games_2plus_pct"],
                "team_combined_pg": team_home["combined_pg"],
            }
        )
        row["projection"] = {
            "prob_over_1p_1_5": final_prob_over,
            "prob_under_1p_1_5": 1.0 - final_prob_over,
            "over_american_odds": probability_to_american(final_prob_over),
            "under_american_odds": probability_to_american(1.0 - final_prob_over),
            "lambda_total": result["lambda_total"],
            "model_prob_over_1p_1_5_raw": model_prob_over,
            "market_prob_over_1p_1_5_consensus": market_prob_over,
            "market_book_count": book_count,
            "market_dampening_weight": dampening_weight,
        }
        row["trends"] = {
            "away_team_l5_l10_l15_o15": format_team_o15(team_away),
            "home_team_l5_l10_l15_o15": format_team_o15(team_home),
            "away_goalie_l5_l10_l15_allow2": format_goalie_allow2(away_goalie),
            "home_goalie_l5_l10_l15_allow2": format_goalie_allow2(home_goalie),
            "h2h_games": h2h_count,
            "h2h_o15_hits": h2h_ge2_count,
            "h2h_o15_pct": (h2h_ge2_count / h2h_count) if h2h_count > 0 else None,
            "h2h_avg_combined_1p": h2h_avg_combined if h2h_count > 0 else None,
            "h2h_recent_scores": format_head_to_head_recent(h2h_games, team_away, team_home, max_games=5),
        }
        projected_games.append(row)

    status_counts = defaultdict(int)
    for game in projected_games:
        status_counts[(game["away"].get("status") or DFO_STATUS_FALLBACK)] += 1
        status_counts[(game["home"].get("status") or DFO_STATUS_FALLBACK)] += 1

    live_games = sum(1 for g in projected_games if (g.get("game_status") or {}).get("state_code") in {"LIVE", "CRIT"})
    first_period_graded = sum(1 for g in projected_games if (g.get("game_status") or {}).get("first_period_complete"))
    first_period_over = sum(
        1
        for g in projected_games
        if (g.get("game_status") or {}).get("first_period_total_result") == "OVER"
    )
    first_period_under = sum(
        1
        for g in projected_games
        if (g.get("game_status") or {}).get("first_period_total_result") == "UNDER"
    )
    market_games_with_data = sum(
        1
        for g in projected_games
        if ((g.get("projection") or {}).get("market_prob_over_1p_1_5_consensus")) is not None
    )

    payload = {
        "source_url": feed["source_url"],
        "pulled_at_utc": feed["pulled_at_utc"],
        "target_date": feed["target_date"],
        "games": projected_games,
        "warnings": warnings,
        "meta": {
            "total_games": len(projected_games),
            "projectable_games": sum(1 for g in projected_games if g.get("projection")),
            "failed_games": sum(1 for g in projected_games if not g.get("projection")),
            "status_counts": dict(sorted(status_counts.items(), key=lambda kv: kv[0])),
            "live_games": live_games,
            "first_period_graded_games": first_period_graded,
            "first_period_over_games": first_period_over,
            "first_period_under_games": first_period_under,
            "market_games_with_data": market_games_with_data,
        },
    }

    if verbose:
        print(
            f"Daily slate: {payload['meta']['projectable_games']}/{payload['meta']['total_games']} games projected "
            f"from {feed['source_url']} ({feed['target_date']})"
        )
        print(
            f"Live games: {payload['meta']['live_games']} | "
            f"1P graded: {payload['meta']['first_period_graded_games']} "
            f"(OVER {payload['meta']['first_period_over_games']} / UNDER {payload['meta']['first_period_under_games']})"
        )
        if warnings:
            print(f"Daily slate warnings: {len(warnings)}")
    return payload


def print_daily_projection_report(slate_payload):
    print("\n" + "=" * 92)
    print("NHL DAILY 1P O1.5 REPORT")
    print("=" * 92)
    print(f"Date: {slate_payload.get('target_date')} | Source: {slate_payload.get('source_url')}")
    print(f"Pulled: {slate_payload.get('pulled_at_utc')} UTC")
    print(
        f"Projected games: {slate_payload['meta'].get('projectable_games', 0)}/"
        f"{slate_payload['meta'].get('total_games', 0)}"
    )

    status_counts = slate_payload.get("meta", {}).get("status_counts", {})
    if status_counts:
        status_text = ", ".join(f"{k}: {v}" for k, v in status_counts.items())
        print(f"Starter statuses: {status_text}")

    print("-" * 92)
    for game in slate_payload.get("games", []):
        away = game["away"]
        home = game["home"]
        game_time = game.get("game_time_et") or game.get("game_time_utc") or "Time TBD"
        print(
            f"{away.get('team_abbrev', away.get('team_name_feed', 'AWY'))} at "
            f"{home.get('team_abbrev', home.get('team_name_feed', 'HME'))} ({game_time})"
        )
        print(
            f"  Away starter: {away.get('goalie_name', away.get('goalie_name_feed', 'TBD'))} "
            f"[{away.get('status', DFO_STATUS_FALLBACK)}]"
        )
        print(
            f"  Home starter: {home.get('goalie_name', home.get('goalie_name_feed', 'TBD'))} "
            f"[{home.get('status', DFO_STATUS_FALLBACK)}]"
        )
        game_status = game.get("game_status") or {}
        if game_status:
            state_text = game_status.get("state_label") or game_status.get("state_code") or "Scheduled"
            if (game_status.get("state_code") or "").upper() in {"LIVE", "CRIT"}:
                period = game_status.get("period")
                clock = game_status.get("clock")
                if period:
                    state_text = f"{state_text} P{period}"
                if clock:
                    state_text = f"{state_text} {clock}"
            game_line = f"  Game state: {state_text}"
            if game_status.get("first_period_complete"):
                game_line += (
                    f" | 1P {game_status.get('first_period_total_result')} "
                    f"({game_status.get('first_period_goals')} goals)"
                )
            print(game_line)
        if game.get("projection"):
            projection = game["projection"]
            print(
                f"  O1.5: {projection['prob_over_1p_1_5'] * 100:.1f}% | "
                f"Fair O {fmt_american(projection['over_american_odds'])} / "
                f"U {fmt_american(projection['under_american_odds'])}"
            )
            h2h_games = game.get("trends", {}).get("h2h_games", 0)
            h2h_pct = game.get("trends", {}).get("h2h_o15_pct")
            if h2h_games > 0 and h2h_pct is not None:
                print(f"  H2H O1.5: {h2h_pct * 100:.1f}% ({game['trends']['h2h_o15_hits']}/{h2h_games})")
        else:
            print(f"  Projection unavailable: {game.get('error', 'unknown error')}")
        if game.get("notes"):
            print(f"  Notes: {' | '.join(game['notes'])}")

    if slate_payload.get("warnings"):
        print("\nWarnings:")
        for warning in slate_payload["warnings"]:
            print(f"  - {warning}")


def choose_goalie_interactive(team, team_goalies):
    if not team_goalies:
        raise ValueError(f"No goalie data found for {team['abbrev']} this season.")

    print()
    print(f"{team['abbrev']} starter options (1P season stats):")
    for idx, goalie in enumerate(team_goalies, 1):
        rank_summary = goalie_rank_text(goalie)
        print(
            f"  {idx:>2}. {goalie['name']:<24} "
            f"GP {goalie['games']:>2} | 1P GA/GP {goalie['ga_pg']:.2f} | SV% {goalie['save_pct']:.1f}% | {rank_summary}"
        )

    while True:
        choice = input(f"Choose {team['abbrev']} starter by number, name, or id: ").strip()
        if not choice:
            continue
        if choice.isdigit():
            idx = int(choice)
            if 1 <= idx <= len(team_goalies):
                return team_goalies[idx - 1]
        try:
            return resolve_goalie(choice, team_goalies)
        except ValueError as exc:
            print(f"  {exc}")


def project_matchup(team_away, team_home, away_goalie, home_goalie, league):
    league_goal_rate = league["team_goal_rate"]
    league_combined_rate = league["combined_rate"] if league["combined_rate"] > 0 else (league_goal_rate * 2.0)
    league_goalie_ga_rate = league["goalie_ga_rate"] if league["goalie_ga_rate"] > 0 else league_goal_rate

    away_off_season = stabilized_rate(team_away["gf_pg"], team_away["games"], league_goal_rate, TEAM_PRIOR_GAMES)
    home_off_season = stabilized_rate(team_home["gf_pg"], team_home["games"], league_goal_rate, TEAM_PRIOR_GAMES)
    away_def_season = stabilized_rate(team_away["ga_pg"], team_away["games"], league_goal_rate, TEAM_PRIOR_GAMES)
    home_def_season = stabilized_rate(team_home["ga_pg"], team_home["games"], league_goal_rate, TEAM_PRIOR_GAMES)

    away_off, away_off_recent_weight = blend_with_recent(
        away_off_season,
        team_away.get("recent_gf_pg", away_off_season),
        team_away.get("recent_games", 0),
        TEAM_FORM_BASE_WEIGHT,
        TEAM_FORM_FULL_WEIGHT_GAMES,
    )
    home_off, home_off_recent_weight = blend_with_recent(
        home_off_season,
        team_home.get("recent_gf_pg", home_off_season),
        team_home.get("recent_games", 0),
        TEAM_FORM_BASE_WEIGHT,
        TEAM_FORM_FULL_WEIGHT_GAMES,
    )
    away_def, away_def_recent_weight = blend_with_recent(
        away_def_season,
        team_away.get("recent_ga_pg", away_def_season),
        team_away.get("recent_games", 0),
        TEAM_FORM_BASE_WEIGHT,
        TEAM_FORM_FULL_WEIGHT_GAMES,
    )
    home_def, home_def_recent_weight = blend_with_recent(
        home_def_season,
        team_home.get("recent_ga_pg", home_def_season),
        team_home.get("recent_games", 0),
        TEAM_FORM_BASE_WEIGHT,
        TEAM_FORM_FULL_WEIGHT_GAMES,
    )

    away_goalie_ga_pg_season = stabilized_rate(
        away_goalie["ga_pg"], away_goalie["games"], league_goalie_ga_rate, GOALIE_PRIOR_GAMES
    )
    home_goalie_ga_pg_season = stabilized_rate(
        home_goalie["ga_pg"], home_goalie["games"], league_goalie_ga_rate, GOALIE_PRIOR_GAMES
    )
    away_goalie_ga_pg, away_goalie_recent_weight = blend_with_recent(
        away_goalie_ga_pg_season,
        away_goalie.get("recent_ga_pg", away_goalie_ga_pg_season),
        away_goalie.get("recent_games", 0),
        GOALIE_FORM_BASE_WEIGHT,
        GOALIE_FORM_FULL_WEIGHT_GAMES,
    )
    home_goalie_ga_pg, home_goalie_recent_weight = blend_with_recent(
        home_goalie_ga_pg_season,
        home_goalie.get("recent_ga_pg", home_goalie_ga_pg_season),
        home_goalie.get("recent_games", 0),
        GOALIE_FORM_BASE_WEIGHT,
        GOALIE_FORM_FULL_WEIGHT_GAMES,
    )

    away_goalie_factor = clamp(away_goalie_ga_pg / max(away_def, 1e-6), GOALIE_FACTOR_MIN, GOALIE_FACTOR_MAX)
    home_goalie_factor = clamp(home_goalie_ga_pg / max(home_def, 1e-6), GOALIE_FACTOR_MIN, GOALIE_FACTOR_MAX)

    lambda_away_base = 0.5 * (away_off + home_def)
    lambda_home_base = 0.5 * (home_off + away_def)
    lambda_away_goalie_adj = lambda_away_base * home_goalie_factor
    lambda_home_goalie_adj = lambda_home_base * away_goalie_factor

    away_tempo = team_away["combined_pg"] / max(league_combined_rate, 1e-6)
    home_tempo = team_home["combined_pg"] / max(league_combined_rate, 1e-6)
    tempo_factor = clamp(math.sqrt(max(away_tempo, 1e-6) * max(home_tempo, 1e-6)), TEMPO_FACTOR_MIN, TEMPO_FACTOR_MAX)

    lambda_total = (lambda_away_goalie_adj + lambda_home_goalie_adj) * tempo_factor
    poisson_prob = poisson_prob_at_least_two(lambda_total)
    away_empirical_rate, away_empirical_weight = blend_with_recent(
        team_away["games_2plus_pct"],
        team_away.get("recent_2plus_pct", team_away["games_2plus_pct"]),
        team_away.get("recent_games", 0),
        TEAM_FORM_BASE_WEIGHT,
        TEAM_FORM_FULL_WEIGHT_GAMES,
    )
    home_empirical_rate, home_empirical_weight = blend_with_recent(
        team_home["games_2plus_pct"],
        team_home.get("recent_2plus_pct", team_home["games_2plus_pct"]),
        team_home.get("recent_games", 0),
        TEAM_FORM_BASE_WEIGHT,
        TEAM_FORM_FULL_WEIGHT_GAMES,
    )
    empirical_prob = 0.5 * (away_empirical_rate + home_empirical_rate)
    base_prob = clamp((POISSON_WEIGHT * poisson_prob) + ((1.0 - POISSON_WEIGHT) * empirical_prob), PROB_MIN, PROB_MAX)
    league_o15_prob = float(league.get("game_2plus_pct", 0.0))
    if league_o15_prob <= 0:
        league_o15_prob = poisson_prob_at_least_two(league_combined_rate)
    final_prob = clamp(
        ((1.0 - LEAGUE_O15_ANCHOR_WEIGHT) * base_prob) + (LEAGUE_O15_ANCHOR_WEIGHT * league_o15_prob),
        PROB_MIN,
        PROB_MAX,
    )

    over_odds = probability_to_american(final_prob)
    under_odds = probability_to_american(1.0 - final_prob)

    return {
        "probability_over_1p_1_5": final_prob,
        "probability_under_1p_1_5": 1.0 - final_prob,
        "over_american_odds": over_odds,
        "under_american_odds": under_odds,
        "lambda_total": lambda_total,
        "components": {
            "away_off": away_off,
            "home_off": home_off,
            "away_def": away_def,
            "home_def": home_def,
            "away_off_season": away_off_season,
            "home_off_season": home_off_season,
            "away_def_season": away_def_season,
            "home_def_season": home_def_season,
            "away_off_recent": team_away.get("recent_gf_pg", away_off_season),
            "home_off_recent": team_home.get("recent_gf_pg", home_off_season),
            "away_def_recent": team_away.get("recent_ga_pg", away_def_season),
            "home_def_recent": team_home.get("recent_ga_pg", home_def_season),
            "away_off_recent_weight": away_off_recent_weight,
            "home_off_recent_weight": home_off_recent_weight,
            "away_def_recent_weight": away_def_recent_weight,
            "home_def_recent_weight": home_def_recent_weight,
            "away_recent_games": team_away.get("recent_games", 0),
            "home_recent_games": team_home.get("recent_games", 0),
            "away_goalie_ga_pg_smoothed": away_goalie_ga_pg,
            "home_goalie_ga_pg_smoothed": home_goalie_ga_pg,
            "away_goalie_ga_pg_season": away_goalie_ga_pg_season,
            "home_goalie_ga_pg_season": home_goalie_ga_pg_season,
            "away_goalie_ga_pg_recent": away_goalie.get("recent_ga_pg", away_goalie_ga_pg_season),
            "home_goalie_ga_pg_recent": home_goalie.get("recent_ga_pg", home_goalie_ga_pg_season),
            "away_goalie_recent_weight": away_goalie_recent_weight,
            "home_goalie_recent_weight": home_goalie_recent_weight,
            "away_goalie_recent_games": away_goalie.get("recent_games", 0),
            "home_goalie_recent_games": home_goalie.get("recent_games", 0),
            "away_goalie_factor": away_goalie_factor,
            "home_goalie_factor": home_goalie_factor,
            "lambda_away_pre_tempo": lambda_away_goalie_adj,
            "lambda_home_pre_tempo": lambda_home_goalie_adj,
            "tempo_factor": tempo_factor,
            "poisson_prob": poisson_prob,
            "empirical_prob": empirical_prob,
            "base_prob_pre_league_anchor": base_prob,
            "league_o15_prob": league_o15_prob,
            "league_anchor_weight": LEAGUE_O15_ANCHOR_WEIGHT,
            "away_empirical_rate": away_empirical_rate,
            "home_empirical_rate": home_empirical_rate,
            "away_empirical_weight": away_empirical_weight,
            "home_empirical_weight": home_empirical_weight,
        },
    }


def parse_args():
    parser = argparse.ArgumentParser(
        description="Project 1P 2+ combined goals probability and fair odds for a selected NHL matchup."
    )
    parser.add_argument("--away", help="Away team (abbr or team name)")
    parser.add_argument("--home", help="Home team (abbr or team name)")
    parser.add_argument("--away-goalie", help="Projected away starter (name or id)")
    parser.add_argument("--home-goalie", help="Projected home starter (name or id)")
    parser.add_argument("--force-refresh", action="store_true", help="Rebuild projection dataset from APIs and cache")
    parser.add_argument("--non-interactive", action="store_true", help="Do not prompt; require all matchup args")
    parser.add_argument("--list-teams", action="store_true", help="List available teams and exit")
    parser.add_argument(
        "--daily-slate",
        action="store_true",
        help="Build and print today's Daily Faceoff-driven game-by-game O1.5 report",
    )
    parser.add_argument(
        "--daily-date",
        help="Daily Faceoff date override (YYYY-MM-DD), used with --daily-slate",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    dataset = build_projection_dataset(force_refresh=args.force_refresh, verbose=True)
    teams_by_id, goalies_by_team = index_dataset(dataset)
    league = dataset["league"]

    if args.daily_slate:
        try:
            slate = build_daily_projection_slate(dataset, date_str=args.daily_date, verbose=True)
        except Exception as exc:
            raise SystemExit(f"Failed to build daily slate: {exc}") from exc
        print_daily_projection_report(slate)
        return

    if args.list_teams:
        print("\nTeams in dataset:")
        for team in sorted(teams_by_id.values(), key=lambda t: t["abbrev"]):
            print(f"  {team['abbrev']:<4} {team['name']}")
        return

    if args.non_interactive and (not args.away or not args.home or not args.away_goalie or not args.home_goalie):
        raise SystemExit("--non-interactive requires --away --home --away-goalie --home-goalie")

    while True:
        away_input = args.away if args.away else input("Away team (abbr or name): ").strip()
        home_input = args.home if args.home else input("Home team (abbr or name): ").strip()
        try:
            team_away = resolve_team(away_input, teams_by_id)
            team_home = resolve_team(home_input, teams_by_id)
            if team_away["team_id"] == team_home["team_id"]:
                raise ValueError("Away and home teams must be different.")
            break
        except ValueError as exc:
            if args.non_interactive or (args.away and args.home):
                raise SystemExit(str(exc))
            print(exc)
            args.away = None
            args.home = None

    away_goalies = goalies_by_team.get(team_away["team_id"], [])
    home_goalies = goalies_by_team.get(team_home["team_id"], [])
    if not away_goalies:
        raise SystemExit(f"No goalie data found for {team_away['abbrev']}.")
    if not home_goalies:
        raise SystemExit(f"No goalie data found for {team_home['abbrev']}.")

    if args.away_goalie:
        try:
            away_goalie = resolve_goalie(args.away_goalie, away_goalies)
        except ValueError as exc:
            raise SystemExit(str(exc))
    else:
        away_goalie = choose_goalie_interactive(team_away, away_goalies)

    if args.home_goalie:
        try:
            home_goalie = resolve_goalie(args.home_goalie, home_goalies)
        except ValueError as exc:
            raise SystemExit(str(exc))
    else:
        home_goalie = choose_goalie_interactive(team_home, home_goalies)

    result = project_matchup(team_away, team_home, away_goalie, home_goalie, league)
    final_prob = result["probability_over_1p_1_5"]
    h2h_games = head_to_head_games(dataset, team_away, team_home)
    h2h_count = len(h2h_games)
    h2h_ge2_count = sum(1 for g in h2h_games if g.get("combined_1p", 0) >= 2)
    h2h_avg_combined = (
        sum(float(g.get("combined_1p", 0.0)) for g in h2h_games) / h2h_count if h2h_count > 0 else 0.0
    )

    print("\n" + "=" * 76)
    print("1P 2+ GOALS PROJECTION")
    print("=" * 76)
    print(f"Season: {dataset['season_id']} | Generated: {dataset['generated_at']}")
    print(f"Matchup: {team_away['abbrev']} ({team_away['name']}) at {team_home['abbrev']} ({team_home['name']})")
    print(f"Starters: {team_away['abbrev']} - {away_goalie['name']} | {team_home['abbrev']} - {home_goalie['name']}")
    print(f"  {team_away['abbrev']} starter ranks: {goalie_rank_text(away_goalie)}")
    print(f"  {team_home['abbrev']} starter ranks: {goalie_rank_text(home_goalie)}")
    print(
        f"Goalie 1P sample sizes: {team_away['abbrev']} starter GP={away_goalie['games']}, "
        f"{team_home['abbrev']} starter GP={home_goalie['games']}"
    )

    print("\nProjection:")
    print(f"  Chance of 2+ goals in 1P: {final_prob * 100:.1f}%")
    print(f"  Fair American odds (Over 1.5 1P): {fmt_american(result['over_american_odds'])}")
    print(f"  Fair American odds (Under 1.5 1P): {fmt_american(result['under_american_odds'])}")

    print("\nEmpirical matchup snapshot:")
    if h2h_count > 0:
        print(
            f"  H2H this season: {h2h_count} game(s) | "
            f"1P avg combined goals {h2h_avg_combined:.2f} | "
            f"O1.5 hit rate {h2h_ge2_count}/{h2h_count} ({(h2h_ge2_count / h2h_count) * 100:.1f}%)"
        )
        print(f"  Recent H2H 1P scores: {format_head_to_head_recent(h2h_games, team_away, team_home, max_games=5)}")
    else:
        print("  H2H this season: no meetings found.")

    print(f"  {team_away['abbrev']} team 1P GF/GA form: {format_team_gf_ga(team_away)}")
    print(f"  {team_home['abbrev']} team 1P GF/GA form: {format_team_gf_ga(team_home)}")
    print(f"  {team_away['abbrev']} O1.5 (2+) in last 5/10/15: {format_team_o15(team_away)}")
    print(f"  {team_home['abbrev']} O1.5 (2+) in last 5/10/15: {format_team_o15(team_home)}")

    print(
        f"  {team_away['abbrev']} {away_goalie['name']} 1P GA last 5/10/15: "
        f"{format_goalie_ga(away_goalie)}"
    )
    print(
        f"  {team_home['abbrev']} {home_goalie['name']} 1P GA last 5/10/15: "
        f"{format_goalie_ga(home_goalie)}"
    )
    print(
        f"  {team_away['abbrev']} {away_goalie['name']} games allowing 2+ in 1P: "
        f"{format_goalie_allow2(away_goalie)}"
    )
    print(
        f"  {team_home['abbrev']} {home_goalie['name']} games allowing 2+ in 1P: "
        f"{format_goalie_allow2(home_goalie)}"
    )

    if away_goalie["games"] < 5 or home_goalie["games"] < 5:
        print("\nNote: one or both selected goalies have <5 1P games; uncertainty is higher.")

    print("\nMeta:")
    print(
        f"  Data path: direct={dataset['meta']['direct_team_games']} team-games, "
        f"fallback={dataset['meta']['fallback_team_games']} team-games across {dataset['meta']['fallback_games']} games"
    )
    if dataset["meta"]["failed_fallback_games"] > 0:
        print(f"  Warning: {dataset['meta']['failed_fallback_games']} fallback game downloads failed.")


if __name__ == "__main__":
    main()
