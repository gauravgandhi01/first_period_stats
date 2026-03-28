import json
import os
import random
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter

# API base URLs
WEB_BASE_URL = "https://api-web.nhle.com/v1"
STATS_BASE_URL = "https://api.nhle.com/stats/rest/en"
RECORDS_BASE_URL = "https://records.nhl.com/site"

# Fixed season context (requested hardcoded)
SEASON_ID = 20252026
GAME_TYPE_ID = 2  # Regular season

# Networking + rate limiting
REQUEST_TIMEOUT = 20
RETRY_ATTEMPTS = 4
RETRY_DELAY = 1.0
MIN_REQUEST_INTERVAL = 0.10
MAX_WORKERS = 4
STATS_PAGE_SIZE = 100
_RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})

# Cache freshness controls
SEASON_STATS_CACHE_MAX_AGE_SECONDS = 6 * 60 * 60
LOOKUP_CACHE_MAX_AGE_SECONDS = 30 * 24 * 60 * 60

# Constants for PBP parsing
_SHOT_EVENTS = frozenset({"shot-on-goal", "goal"})

# Cache directory
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

# Thread/session state
_thread_local = threading.local()
_request_lock = threading.Lock()
_last_request_monotonic = 0.0


def _get_session():
    """Create a pooled session per thread."""
    if not hasattr(_thread_local, "session"):
        session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=MAX_WORKERS,
            pool_maxsize=MAX_WORKERS,
            max_retries=0,
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update({"User-Agent": "nhl-first-period-hybrid/1.0"})
        _thread_local.session = session
    return _thread_local.session


def _throttle_request():
    """Global pacing across workers to reduce bursty traffic."""
    global _last_request_monotonic

    if MIN_REQUEST_INTERVAL <= 0:
        return

    with _request_lock:
        now = time.monotonic()
        elapsed = now - _last_request_monotonic
        if elapsed < MIN_REQUEST_INTERVAL:
            time.sleep(MIN_REQUEST_INTERVAL - elapsed)
            now = time.monotonic()
        _last_request_monotonic = now


def _retry_sleep(attempt):
    """Exponential backoff plus small jitter."""
    base = RETRY_DELAY * (2 ** attempt)
    jitter = random.uniform(0, RETRY_DELAY * 0.25)
    time.sleep(base + jitter)


def _request_json(url, params=None):
    """Request JSON with retries for transient errors."""
    session = _get_session()
    last_status_code = None

    for attempt in range(RETRY_ATTEMPTS):
        try:
            _throttle_request()
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            last_status_code = response.status_code
        except requests.exceptions.RequestException:
            response = None

        if response is None:
            if attempt < RETRY_ATTEMPTS - 1:
                _retry_sleep(attempt)
                continue
            return None, None

        if response.status_code == 200:
            try:
                return response.json(), 200
            except ValueError:
                if attempt < RETRY_ATTEMPTS - 1:
                    _retry_sleep(attempt)
                    continue
                return None, 200

        if response.status_code in _RETRYABLE_STATUS_CODES and attempt < RETRY_ATTEMPTS - 1:
            _retry_sleep(attempt)
            continue

        return None, response.status_code

    return None, last_status_code


def _cache_get(key, max_age_seconds=None):
    """Read JSON cache entry."""
    path = os.path.join(CACHE_DIR, f"{key}.json")
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                payload = json.load(f)

            if max_age_seconds is not None:
                fetched_at = payload.get("fetched_at") if isinstance(payload, dict) else None
                if not fetched_at:
                    return None
                try:
                    fetched_dt = datetime.fromisoformat(fetched_at)
                except (TypeError, ValueError):
                    return None
                age_seconds = (datetime.now() - fetched_dt).total_seconds()
                if age_seconds > max_age_seconds:
                    return None

            return payload
        except (OSError, json.JSONDecodeError):
            try:
                os.remove(path)
            except OSError:
                pass
    return None


def _cache_set(key, data):
    """Write JSON cache entry atomically."""
    path = os.path.join(CACHE_DIR, f"{key}.json")
    tmp_path = f"{path}.{threading.get_ident()}.tmp"
    try:
        with open(tmp_path, "w") as f:
            json.dump(data, f)
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass


def _safe_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_team_id_from_team_row(row):
    team_id = row.get("teamId")
    if team_id is not None:
        return _safe_int(team_id, default=None)
    row_id = row.get("id")
    if isinstance(row_id, dict):
        return _safe_int(row_id.get("db:TEAMID"), default=None)
    return None


def fetch_stats_paginated(url, base_params, cache_key):
    """Fetch all rows from stats/rest endpoint that uses start/limit pagination."""
    cached = _cache_get(cache_key, max_age_seconds=SEASON_STATS_CACHE_MAX_AGE_SECONDS)
    if cached is not None:
        return cached

    all_rows = []
    total = None
    page = 0

    while True:
        params = dict(base_params)
        params["start"] = page * STATS_PAGE_SIZE
        params["limit"] = STATS_PAGE_SIZE

        data, status_code = _request_json(url, params=params)
        if data is None:
            status_text = status_code if status_code is not None else "request failed"
            raise RuntimeError(f"Failed to fetch paged stats data: {url} ({status_text})")

        if total is None:
            total = _safe_int(data.get("total"), default=0)

        rows = data.get("data", [])
        if not rows:
            break

        all_rows.extend(rows)
        if len(all_rows) >= total:
            break

        page += 1

    payload = {
        "rows": all_rows,
        "total": total,
        "fetched_at": datetime.now().isoformat(timespec="seconds"),
    }
    _cache_set(cache_key, payload)
    return payload


def fetch_team_lookup():
    """Fetch team metadata for triCode -> teamId mapping."""
    cache_key = "stats_team_lookup"
    cached = _cache_get(cache_key, max_age_seconds=LOOKUP_CACHE_MAX_AGE_SECONDS)
    if cached is not None:
        return cached

    url = f"{STATS_BASE_URL}/team"
    params = {"start": 0, "limit": 200}
    data, status_code = _request_json(url, params=params)
    if data is None:
        status_text = status_code if status_code is not None else "request failed"
        raise RuntimeError(f"Failed to fetch team lookup: {status_text}")

    payload = {
        "rows": data.get("data", []),
        "fetched_at": datetime.now().isoformat(timespec="seconds"),
    }
    _cache_set(cache_key, payload)
    return payload


def fetch_goalie_summary_game_rows():
    """Fetch full goalie summary game rows for the season/game type."""
    url = f"{STATS_BASE_URL}/goalie/summary"
    params = {
        "isGame": "true",
        "cayenneExp": f"seasonId={SEASON_ID} and gameTypeId={GAME_TYPE_ID}",
        "sort": json.dumps(
            [
                {"property": "gameId", "direction": "ASC"},
                {"property": "teamAbbrev", "direction": "ASC"},
                {"property": "playerId", "direction": "ASC"},
            ]
        ),
    }
    cache_key = f"stats_goalie_summary_game_{SEASON_ID}_{GAME_TYPE_ID}_v2"
    return fetch_stats_paginated(url, params, cache_key)


def fetch_team_goals_by_period_rows():
    """Fetch team goals-by-period game rows for the season/game type."""
    url = f"{STATS_BASE_URL}/team/goalsbyperiod"
    params = {
        "isGame": "true",
        "cayenneExp": f"seasonId={SEASON_ID} and gameTypeId={GAME_TYPE_ID}",
        "sort": json.dumps(
            [
                {"property": "gameId", "direction": "ASC"},
                {"property": "teamId", "direction": "ASC"},
            ]
        ),
    }
    cache_key = f"stats_team_goalsbyperiod_game_{SEASON_ID}_{GAME_TYPE_ID}_v2"
    return fetch_stats_paginated(url, params, cache_key)


def fetch_team_by_game_rows(min_game_id, max_game_id):
    """Fetch team-by-game rows from records endpoint for gameId range."""
    cache_key = f"records_team_by_game_stats_{min_game_id}_{max_game_id}"
    cached = _cache_get(cache_key, max_age_seconds=SEASON_STATS_CACHE_MAX_AGE_SECONDS)
    if cached is not None:
        return cached

    url = f"{RECORDS_BASE_URL}/api/team-by-game-stats"
    params = {
        "limit": 6000,
        "cayenneExp": f"gameId>={min_game_id} and gameId<={max_game_id}",
    }
    data, status_code = _request_json(url, params=params)
    if data is None:
        status_text = status_code if status_code is not None else "request failed"
        raise RuntimeError(f"Failed to fetch team-by-game-stats: {status_text}")

    payload = {
        "rows": data.get("data", []),
        "fetched_at": datetime.now().isoformat(timespec="seconds"),
    }
    _cache_set(cache_key, payload)
    return payload


def get_play_by_play(game_id):
    """Fetch play-by-play with disk caching."""
    cache_key = f"pbp_{game_id}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return game_id, cached

    url = f"{WEB_BASE_URL}/gamecenter/{game_id}/play-by-play"
    data, _ = _request_json(url)
    if data is not None:
        _cache_set(cache_key, data)
        return game_id, data
    return game_id, None


def process_game_for_goalies_by_team_from_pbp(pbp_data):
    """
    Exact first-period extraction from play-by-play, grouped by defending team.

    Returns:
        dict[team_id][goalie_id] = {shots_against, goals_against, saves}
    """
    result = defaultdict(lambda: defaultdict(lambda: {"saves": 0, "goals_against": 0, "shots_against": 0}))
    if not pbp_data:
        return result

    home_team_id = _safe_int((pbp_data.get("homeTeam") or {}).get("id"), default=None)
    away_team_id = _safe_int((pbp_data.get("awayTeam") or {}).get("id"), default=None)

    for play in pbp_data.get("plays", []):
        period_num = play.get("periodDescriptor", {}).get("number")
        if period_num is not None and period_num > 1:
            break
        if period_num != 1:
            continue

        event_type = play.get("typeDescKey", "")
        if event_type not in _SHOT_EVENTS:
            continue

        details = play.get("details", {})
        shooter_team_id = _safe_int(details.get("eventOwnerTeamId"), default=None)
        goalie_id = _safe_int(details.get("goalieInNetId"), default=None)
        if shooter_team_id is None or goalie_id is None:
            continue

        if home_team_id is None or away_team_id is None:
            continue

        if shooter_team_id == home_team_id:
            defending_team_id = away_team_id
        elif shooter_team_id == away_team_id:
            defending_team_id = home_team_id
        else:
            continue

        goalie_stats = result[defending_team_id][goalie_id]
        goalie_stats["shots_against"] += 1
        if event_type == "goal":
            goalie_stats["goals_against"] += 1
        else:
            goalie_stats["saves"] += 1

    return result


def get_goalie_name(goalie_id):
    """Name lookup for goalies not present in summary rows."""
    cache_key = f"player_{goalie_id}"
    cached = _cache_get(cache_key)
    if cached is not None:
        first_name = cached.get("firstName", {}).get("default", "")
        last_name = cached.get("lastName", {}).get("default", "")
        return f"{first_name} {last_name}".strip() or f"Goalie {goalie_id}"

    url = f"{WEB_BASE_URL}/player/{goalie_id}/landing"
    data, _ = _request_json(url)
    if data is not None:
        _cache_set(cache_key, data)
        first_name = data.get("firstName", {}).get("default", "")
        last_name = data.get("lastName", {}).get("default", "")
        return f"{first_name} {last_name}".strip() or f"Goalie {goalie_id}"
    return f"Goalie {goalie_id}"


def print_and_log(message, log_file=None):
    print(message)
    if log_file:
        log_file.write(message + "\n")


def main():
    start_time = time.time()
    output_filename = f"nhl_first_period_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    print("=" * 70)
    print("NHL 2025-2026 Season - First Period Goalie Statistics (HYBRID)")
    print("=" * 70)
    print(f"Output will be saved to: {output_filename}")
    print(
        f"Settings: season={SEASON_ID}, gameType={GAME_TYPE_ID}, "
        f"{MAX_WORKERS} workers, {MIN_REQUEST_INTERVAL:.2f}s min spacing"
    )
    print()

    # 1) Bulk fetch datasets
    print("Fetching bulk datasets...")
    team_lookup_payload = fetch_team_lookup()
    goalie_payload = fetch_goalie_summary_game_rows()
    goals_payload = fetch_team_goals_by_period_rows()

    team_lookup_rows = team_lookup_payload.get("rows", [])
    goalie_rows = goalie_payload.get("rows", [])
    goals_rows = goals_payload.get("rows", [])

    print(f"  Team lookup rows: {len(team_lookup_rows)}")
    print(f"  Goalie summary rows: {len(goalie_rows)}")
    print(f"  Team goals-by-period rows: {len(goals_rows)}")

    if not goalie_rows or not goals_rows:
        print("No data available from bulk endpoints")
        return

    # triCode (teamAbbrev) -> teamId
    team_abbrev_to_id = {}
    team_id_to_abbrev = {}
    team_id_to_name = {}
    for row in team_lookup_rows:
        team_id = _safe_int(row.get("id"), default=None)
        tri = (row.get("triCode") or "").strip().upper()
        full_name = (row.get("fullName") or "").strip()
        if team_id is not None and tri:
            team_abbrev_to_id[tri] = team_id
            team_id_to_abbrev[team_id] = tri
        if team_id is not None and full_name:
            team_id_to_name[team_id] = full_name

    # goals1_map[(game_id, team_id)] = period1GoalsAgainst
    goals1_map = {}
    for row in goals_rows:
        game_id = _safe_int(row.get("gameId"), default=None)
        team_id = _safe_int(row.get("teamId"), default=None)
        if game_id is None or team_id is None:
            continue
        goals1_map[(game_id, team_id)] = _safe_int(row.get("period1GoalsAgainst"))

    if not goals1_map:
        print("No period-1 goals-against data found")
        return

    # Team first-period scoring aggregate from goals-by-period rows.
    team_scoring_by_team = defaultdict(
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
    league_game_2plus_map = {}  # game_id -> True/False for 2+ combined 1P goals
    for row in goals_rows:
        game_id = _safe_int(row.get("gameId"), default=None)
        team_id = _safe_int(row.get("teamId"), default=None)
        if game_id is None or team_id is None:
            continue

        team_game_key = (game_id, team_id)
        if team_game_key in seen_team_games:
            continue
        seen_team_games.add(team_game_key)

        rec = team_scoring_by_team[team_id]
        goals_for = _safe_int(row.get("period1GoalsFor"))
        goals_against = _safe_int(row.get("period1GoalsAgainst"))
        combined_goals = goals_for + goals_against

        rec["games"] += 1
        rec["goals_for"] += goals_for
        rec["goals_against"] += goals_against
        if combined_goals >= 2:
            rec["games_2plus_combined"] += 1
        if not rec["name"]:
            rec["name"] = (row.get("teamFullName") or team_id_to_name.get(team_id) or f"Team {team_id}").strip()
        if not rec["abbrev"]:
            rec["abbrev"] = team_id_to_abbrev.get(team_id, "")
        if game_id not in league_game_2plus_map:
            league_game_2plus_map[game_id] = combined_goals >= 2

    team_scoring_results = []
    for team_id, stats in team_scoring_by_team.items():
        games = stats["games"]
        goals_for = stats["goals_for"]
        goals_against = stats["goals_against"]
        combined_goals_total = goals_for + goals_against
        games_2plus_combined = stats["games_2plus_combined"]
        team_scoring_results.append(
            {
                "abbrev": stats["abbrev"],
                "name": stats["name"] or team_id_to_name.get(team_id, f"Team {team_id}"),
                "games": games,
                "goals_for": goals_for,
                "goals_against": goals_against,
                "combined_goals_total": combined_goals_total,
                "combined_goals_per_game": (combined_goals_total / games) if games > 0 else 0.0,
                "games_2plus_combined": games_2plus_combined,
                "games_2plus_combined_pct": (games_2plus_combined / games * 100) if games > 0 else 0.0,
                "goals_for_per_game": (goals_for / games) if games > 0 else 0.0,
            }
        )

    team_scoring_results.sort(
        key=lambda x: (-x["combined_goals_per_game"], -x["combined_goals_total"], -x["goals_for"], x["name"])
    )

    league_games_total = len(league_game_2plus_map)
    league_games_2plus = sum(1 for v in league_game_2plus_map.values() if v)
    league_games_2plus_pct = (league_games_2plus / league_games_total * 100) if league_games_total > 0 else 0.0

    completed_game_ids = sorted({game_id for (game_id, _) in goals1_map.keys()})
    min_game_id = min(completed_game_ids)
    max_game_id = max(completed_game_ids)
    print(f"  Completed games found: {len(completed_game_ids)} ({min_game_id} -> {max_game_id})")

    # team-by-game rows (shotsAgainstPeriod1 and shotsAgainst total)
    team_game_payload = fetch_team_by_game_rows(min_game_id, max_game_id)
    team_game_rows = team_game_payload.get("rows", [])
    print(f"  Team-by-game rows fetched: {len(team_game_rows)}")

    shots1_map = {}
    shots_total_map = {}
    completed_set = set(completed_game_ids)
    for row in team_game_rows:
        game_id = _safe_int(row.get("gameId"), default=None)
        team_id = _parse_team_id_from_team_row(row)
        if game_id is None or team_id is None or game_id not in completed_set:
            continue
        shots1_map[(game_id, team_id)] = _safe_int(row.get("shotsAgainstPeriod1"))
        shots_total_map[(game_id, team_id)] = _safe_int(row.get("shotsAgainst"))

    # goalie rows by team-game
    goalie_rows_by_team_game = defaultdict(list)
    goalie_name_map = {}
    for row in goalie_rows:
        game_id = _safe_int(row.get("gameId"), default=None)
        team_abbrev = (row.get("teamAbbrev") or "").strip().upper()
        team_id = team_abbrev_to_id.get(team_abbrev)
        goalie_id = _safe_int(row.get("playerId"), default=None)
        if game_id is None or team_id is None or goalie_id is None:
            continue
        goalie_rows_by_team_game[(game_id, team_id)].append(row)

        full_name = (row.get("goalieFullName") or "").strip()
        if full_name:
            goalie_name_map[goalie_id] = full_name

    # 2) Determine direct vs fallback team-games
    direct_team_game_stats = {}  # (game_id, team_id) -> {goalie_id, stats}
    unresolved_team_games = set()
    reason_counts = defaultdict(int)

    for (game_id, team_id), goals_against_p1 in goals1_map.items():
        shots_against_p1 = shots1_map.get((game_id, team_id))
        if shots_against_p1 is None:
            unresolved_team_games.add((game_id, team_id))
            reason_counts["missing_shots_against_period1"] += 1
            continue

        team_total_shots_against = shots_total_map.get((game_id, team_id))
        if team_total_shots_against is None:
            unresolved_team_games.add((game_id, team_id))
            reason_counts["missing_team_total_shots"] += 1
            continue

        goalie_rows_for_team_game = goalie_rows_by_team_game.get((game_id, team_id), [])
        if not goalie_rows_for_team_game:
            unresolved_team_games.add((game_id, team_id))
            reason_counts["missing_goalie_rows"] += 1
            continue

        rows_with_shots = [r for r in goalie_rows_for_team_game if _safe_int(r.get("shotsAgainst")) > 0]
        chosen_goalie_row = None

        if len(rows_with_shots) == 1 and _safe_int(rows_with_shots[0].get("shotsAgainst")) == team_total_shots_against:
            chosen_goalie_row = rows_with_shots[0]
        elif len(goalie_rows_for_team_game) == 1 and _safe_int(goalie_rows_for_team_game[0].get("shotsAgainst")) == team_total_shots_against:
            chosen_goalie_row = goalie_rows_for_team_game[0]

        if chosen_goalie_row is None:
            unresolved_team_games.add((game_id, team_id))
            if len(rows_with_shots) > 1:
                reason_counts["multi_goalies_with_shots"] += 1
            else:
                reason_counts["shots_mismatch"] += 1
            continue

        goalie_id = _safe_int(chosen_goalie_row.get("playerId"), default=None)
        if goalie_id is None:
            unresolved_team_games.add((game_id, team_id))
            reason_counts["bad_goalie_id"] += 1
            continue

        saves_p1 = max(shots_against_p1 - goals_against_p1, 0)
        direct_team_game_stats[(game_id, team_id)] = {
            "goalie_id": goalie_id,
            "shots_against": shots_against_p1,
            "goals_against": goals_against_p1,
            "saves": saves_p1,
        }

    unresolved_game_ids = sorted({game_id for (game_id, _) in unresolved_team_games})

    print()
    print(f"Direct team-games (bulk exact path): {len(direct_team_game_stats)}")
    print(f"Fallback team-games (play-by-play needed): {len(unresolved_team_games)}")
    print(f"Fallback games (unique): {len(unresolved_game_ids)}")
    if reason_counts:
        print("Fallback reasons:")
        for key, value in sorted(reason_counts.items(), key=lambda x: (-x[1], x[0])):
            print(f"  {key}: {value}")
    if team_scoring_results:
        top_team = team_scoring_results[0]
        print(
            f"Top 1P combo team so far: {top_team['abbrev'] or top_team['name']} "
            f"({top_team['combined_goals_per_game']:.2f} combined goals/game)"
        )
    print(
        f"League 2+ combined 1P goals: {league_games_2plus}/{league_games_total} "
        f"games ({league_games_2plus_pct:.1f}%)"
    )

    # 3) Aggregate direct stats
    all_goalie_stats = defaultdict(
        lambda: {"games": 0, "total_saves": 0, "total_goals_against": 0, "total_shots_against": 0}
    )

    for stats in direct_team_game_stats.values():
        if stats["shots_against"] <= 0:
            continue
        goalie_id = stats["goalie_id"]
        all_goalie_stats[goalie_id]["games"] += 1
        all_goalie_stats[goalie_id]["total_saves"] += stats["saves"]
        all_goalie_stats[goalie_id]["total_goals_against"] += stats["goals_against"]
        all_goalie_stats[goalie_id]["total_shots_against"] += stats["shots_against"]

    # 4) Fallback PBP processing for unresolved team-games only
    failed_fallback_games = 0
    if unresolved_game_ids:
        unresolved_by_game = defaultdict(set)
        for game_id, team_id in unresolved_team_games:
            unresolved_by_game[game_id].add(team_id)

        print()
        print(f"Processing {len(unresolved_game_ids)} fallback games with play-by-play...")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(get_play_by_play, gid): gid for gid in unresolved_game_ids}
            completed = 0

            for future in as_completed(futures):
                game_id, pbp_data = future.result()
                completed += 1

                if pbp_data is None:
                    failed_fallback_games += 1
                else:
                    pbp_stats_by_team = process_game_for_goalies_by_team_from_pbp(pbp_data)
                    unresolved_teams = unresolved_by_game.get(game_id, set())

                    for team_id in unresolved_teams:
                        team_goalie_stats = pbp_stats_by_team.get(team_id, {})
                        for goalie_id, stats in team_goalie_stats.items():
                            if stats["shots_against"] <= 0:
                                continue
                            all_goalie_stats[goalie_id]["games"] += 1
                            all_goalie_stats[goalie_id]["total_saves"] += stats["saves"]
                            all_goalie_stats[goalie_id]["total_goals_against"] += stats["goals_against"]
                            all_goalie_stats[goalie_id]["total_shots_against"] += stats["shots_against"]

                if completed % 25 == 0 or completed == len(unresolved_game_ids):
                    print(
                        f"  Fallback processed {completed}/{len(unresolved_game_ids)} "
                        f"(failed: {failed_fallback_games})"
                    )

    if failed_fallback_games > 0:
        print(f"\nWarning: {failed_fallback_games} fallback games could not be downloaded.")

    # 5) Ensure names for all goalies
    all_goalie_ids = sorted(all_goalie_stats.keys())
    missing_name_ids = [gid for gid in all_goalie_ids if gid not in goalie_name_map]
    if missing_name_ids:
        print(f"\nFetching {len(missing_name_ids)} missing goalie names...")
        for idx, gid in enumerate(missing_name_ids, 1):
            goalie_name_map[gid] = get_goalie_name(gid)
            if idx % 20 == 0 or idx == len(missing_name_ids):
                print(f"  Fetched {idx}/{len(missing_name_ids)} names")

    # 6) Build results
    goalie_results = []
    for goalie_id, stats in all_goalie_stats.items():
        if stats["games"] <= 0:
            continue
        shots_against = stats["total_shots_against"]
        save_pct = (stats["total_saves"] / shots_against * 100) if shots_against > 0 else 0
        gaa = (stats["total_goals_against"] / stats["games"]) * 3 if stats["games"] > 0 else 0
        goalie_results.append(
            {
                "name": goalie_name_map.get(goalie_id, f"Goalie {goalie_id}"),
                "games": stats["games"],
                "goals_against": stats["total_goals_against"],
                "saves": stats["total_saves"],
                "shots_against": stats["total_shots_against"],
                "save_pct": save_pct,
                "gaa": gaa,
            }
        )

    goalie_results.sort(key=lambda x: x["save_pct"])
    min_games = 5
    qualified_goalies = [g for g in goalie_results if g["games"] >= min_games]
    all_goalies = goalie_results

    # 7) Write report (same table format as existing scripts)
    print("\nGenerating report...")
    with open(output_filename, "w") as f:
        header = "=" * 70
        print_and_log(header, f)
        print_and_log("WORST FIRST PERIOD GOALIES - 2025-2026 Season", f)
        print_and_log(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", f)
        print_and_log(header, f)
        print_and_log("", f)
        print_and_log(
            f"Data path: bulk exact for {len(direct_team_game_stats)} team-games, "
            f"play-by-play fallback for {len(unresolved_team_games)} team-games "
            f"across {len(unresolved_game_ids)} games",
            f,
        )
        print_and_log("", f)

        print_and_log("TEAMS - HIGHEST 1P COMBINED GOALS ENVIRONMENT:", f)
        print_and_log("", f)
        team_column_header = (
            f"{'Rank':<6}{'Team':<6}{'Team Name':<24} {'GP':>4} {'1P GF':>6} "
            f"{'1P GA':>6} {'Comb/G':>7} {'2+ CG':>6} {'2+%':>6}"
        )
        print_and_log(team_column_header, f)
        print_and_log("-" * 86, f)

        for idx, team in enumerate(team_scoring_results[:20], 1):
            line = (
                f"{idx:<6}"
                f"{team['abbrev']:<6}"
                f"{team['name']:<24} "
                f"{team['games']:>4} "
                f"{team['goals_for']:>6} "
                f"{team['goals_against']:>6} "
                f"{team['combined_goals_per_game']:>7.2f} "
                f"{team['games_2plus_combined']:>6} "
                f"{team['games_2plus_combined_pct']:>5.1f}%"
            )
            print_and_log(line, f)

        print_and_log("", f)
        print_and_log(
            f"League-wide: {league_games_2plus}/{league_games_total} games with 2+ combined 1P goals "
            f"({league_games_2plus_pct:.1f}%)",
            f,
        )
        print_and_log("", f)
        print_and_log("=" * 70, f)
        print_and_log(f"ALL TEAMS - FIRST PERIOD SCORING ({len(team_scoring_results)} total):", f)
        print_and_log("=" * 70, f)
        print_and_log("", f)
        print_and_log(team_column_header, f)
        print_and_log("-" * 86, f)

        for idx, team in enumerate(team_scoring_results, 1):
            line = (
                f"{idx:<6}"
                f"{team['abbrev']:<6}"
                f"{team['name']:<24} "
                f"{team['games']:>4} "
                f"{team['goals_for']:>6} "
                f"{team['goals_against']:>6} "
                f"{team['combined_goals_per_game']:>7.2f} "
                f"{team['games_2plus_combined']:>6} "
                f"{team['games_2plus_combined_pct']:>5.1f}%"
            )
            print_and_log(line, f)

        print_and_log("", f)
        print_and_log("=" * 70, f)
        print_and_log("", f)

        print_and_log(f"WORST PERFORMERS (Minimum {min_games} games in 1st period):", f)
        print_and_log("", f)

        column_header = (
            f"{'Rank':<6}{'Goalie':<25} {'GP':>4} {'GA':>4} {'Saves':>6} "
            f"{'SA':>5} {'SV%':>7} {'GAA':>6}"
        )
        print_and_log(column_header, f)
        print_and_log("-" * 75, f)

        for idx, goalie in enumerate(qualified_goalies[:20], 1):
            line = (
                f"{idx:<6}"
                f"{goalie['name']:<25} "
                f"{goalie['games']:>4} "
                f"{goalie['goals_against']:>4} "
                f"{goalie['saves']:>6} "
                f"{goalie['shots_against']:>5} "
                f"{goalie['save_pct']:>6.2f}% "
                f"{goalie['gaa']:>6.2f}"
            )
            print_and_log(line, f)

        print_and_log("", f)
        print_and_log("=" * 70, f)
        print_and_log(f"ALL GOALIES (Complete List - {len(all_goalies)} total):", f)
        print_and_log("=" * 70, f)
        print_and_log("", f)
        print_and_log(column_header, f)
        print_and_log("-" * 75, f)

        for idx, goalie in enumerate(all_goalies, 1):
            line = (
                f"{idx:<6}"
                f"{goalie['name']:<25} "
                f"{goalie['games']:>4} "
                f"{goalie['goals_against']:>4} "
                f"{goalie['saves']:>6} "
                f"{goalie['shots_against']:>5} "
                f"{goalie['save_pct']:>6.2f}% "
                f"{goalie['gaa']:>6.2f}"
            )
            print_and_log(line, f)

        print_and_log("", f)
        print_and_log(f"Total goalies: {len(all_goalies)}", f)
        print_and_log(f"Qualified goalies ({min_games}+ GP): {len(qualified_goalies)}", f)
        print_and_log("", f)
        print_and_log("Legend:", f)
        print_and_log("  GP  = Games Played (in 1st period)", f)
        print_and_log("  GA  = Goals Against (1st period only)", f)
        print_and_log("  SA  = Shots Against (1st period only)", f)
        print_and_log("  1P GF = Team first-period goals scored", f)
        print_and_log("  1P GA = Team first-period goals allowed", f)
        print_and_log("  Comb/G = (1P GF + 1P GA) / GP", f)
        print_and_log("  2+ CG% = % of games with 2+ combined 1P goals", f)
        print_and_log("  SV% = Save Percentage (1st period only) - LOWER IS WORSE", f)
        print_and_log(
            "  GAA = Goals Against Average (1st period, normalized to 60 min) - HIGHER IS WORSE", f
        )
        print_and_log("", f)
        print_and_log("NOTE: Sorted by WORST save percentage first", f)

    elapsed = time.time() - start_time
    print()
    print(f"Results saved to: {output_filename}")
    print(f"Completed in {elapsed:.1f} seconds")


if __name__ == "__main__":
    main()
