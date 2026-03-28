#!/usr/bin/env python3
"""
Cron-friendly updater for Daily Faceoff starter-driven 1P O1.5 slate outputs.

What this updates:
1) A latest JSON "sheet" with daily game projections and goalie statuses
2) A latest CSV "sheet" for spreadsheet-friendly usage
3) Optionally the interactive dashboard HTML

Example manual run:
  python3 update_starting_goalie_sheet.py

Example crontab (every 5 minutes):
  */5 * * * * /usr/bin/python3 /Users/ggandhi001/nhl_tools/firstperiodstats/update_starting_goalie_sheet.py >> /Users/ggandhi001/nhl_tools/firstperiodstats/live/cron_update.log 2>&1
"""

import argparse
import csv
import json
import os
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import build_1p_projection_dashboard as dashboard
import project_1p_two_plus as proj

try:
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None


ET_TZ = ZoneInfo("America/New_York")
UTC_TZ = ZoneInfo("UTC")


def now_et_date():
    return datetime.now(ET_TZ).strftime("%Y-%m-%d")


def flatten_game_row(game):
    away = game.get("away") or {}
    home = game.get("home") or {}
    projection = game.get("projection") or {}
    trends = game.get("trends") or {}

    notes = " | ".join(game.get("notes") or [])
    h2h_pct = trends.get("h2h_o15_pct")
    h2h_pct_text = f"{h2h_pct * 100:.1f}%" if h2h_pct is not None else ""
    game_status = game.get("game_status") or {}
    return {
        "game_date": game.get("game_date", ""),
        "game_time_et": game.get("game_time_et", ""),
        "game_time_utc": game.get("game_time_utc", ""),
        "away_team": away.get("team_abbrev") or away.get("team_name") or away.get("team_name_feed") or "",
        "home_team": home.get("team_abbrev") or home.get("team_name") or home.get("team_name_feed") or "",
        "away_goalie": away.get("goalie_name") or away.get("goalie_name_feed") or "",
        "away_status": away.get("status", ""),
        "away_status_updated_at_utc": away.get("status_updated_at_utc", ""),
        "home_goalie": home.get("goalie_name") or home.get("goalie_name_feed") or "",
        "home_status": home.get("status", ""),
        "home_status_updated_at_utc": home.get("status_updated_at_utc", ""),
        "prob_over_1p_1_5": projection.get("prob_over_1p_1_5", ""),
        "over_american_odds": projection.get("over_american_odds", ""),
        "under_american_odds": projection.get("under_american_odds", ""),
        "h2h_o15_pct": h2h_pct_text,
        "h2h_games": trends.get("h2h_games", ""),
        "h2h_hits": trends.get("h2h_o15_hits", ""),
        "game_state": game_status.get("state_label", ""),
        "game_state_code": game_status.get("state_code", ""),
        "period": game_status.get("period", ""),
        "clock": game_status.get("clock", ""),
        "first_period_goals": game_status.get("first_period_goals", ""),
        "first_period_complete": game_status.get("first_period_complete", ""),
        "first_period_result": game_status.get("first_period_total_result", ""),
        "away_score_live": game_status.get("away_score", ""),
        "home_score_live": game_status.get("home_score", ""),
        "error": game.get("error", ""),
        "notes": notes,
    }


def write_json(path, payload):
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_csv(path, rows):
    headers = [
        "game_date",
        "game_time_et",
        "game_time_utc",
        "away_team",
        "home_team",
        "away_goalie",
        "away_status",
        "away_status_updated_at_utc",
        "home_goalie",
        "home_status",
        "home_status_updated_at_utc",
        "prob_over_1p_1_5",
        "over_american_odds",
        "under_american_odds",
        "h2h_o15_pct",
        "h2h_games",
        "h2h_hits",
        "game_state",
        "game_state_code",
        "period",
        "clock",
        "first_period_goals",
        "first_period_complete",
        "first_period_result",
        "away_score_live",
        "home_score_live",
        "error",
        "notes",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


@contextmanager
def lock_file(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "w", encoding="utf-8") as f:
        if fcntl is not None:
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError("Another updater process is already running.") from exc
        try:
            f.write(str(os.getpid()))
            f.flush()
            yield
        finally:
            if fcntl is not None:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)


def parse_args():
    parser = argparse.ArgumentParser(description="Update starter-driven daily sheet outputs.")
    parser.add_argument("--date", help="Target date (YYYY-MM-DD). Defaults to current ET date.")
    parser.add_argument(
        "--output-dir",
        default=str(Path(__file__).resolve().parent / "live"),
        help="Directory to write latest sheet outputs.",
    )
    parser.add_argument(
        "--dashboard-path",
        default=str(Path(__file__).resolve().parent / "index.html"),
        help="Path to dashboard HTML output.",
    )
    parser.add_argument("--skip-dashboard", action="store_true", help="Do not rebuild dashboard.")
    parser.add_argument(
        "--force-refresh-dataset",
        action="store_true",
        help="Force rebuild the base projection dataset from APIs (normally cached).",
    )
    parser.add_argument("--quiet", action="store_true", help="Reduce stdout logging.")
    return parser.parse_args()


def main():
    args = parse_args()
    target_date = args.date or now_et_date()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    latest_json_path = output_dir / "daily_slate_latest.json"
    latest_csv_path = output_dir / "daily_slate_latest.csv"
    latest_meta_path = output_dir / "daily_slate_latest.meta.json"
    lock_path = output_dir / ".update_starting_goalie_sheet.lock"

    if not args.quiet:
        print(f"[{datetime.now(UTC_TZ).isoformat(timespec='seconds')}] update_starting_goalie_sheet start")
        print(f"Target date: {target_date}")
        print(f"Output dir: {output_dir}")

    try:
        with lock_file(lock_path):
            dataset = proj.build_projection_dataset(
                force_refresh=args.force_refresh_dataset,
                verbose=not args.quiet,
            )
            slate_payload = proj.build_daily_projection_slate(
                dataset,
                date_str=target_date,
                verbose=not args.quiet,
            )

            rows = [flatten_game_row(game) for game in slate_payload.get("games", [])]

            write_json(latest_json_path, slate_payload)
            write_csv(latest_csv_path, rows)
            meta = {
                "updated_at_utc": datetime.now(UTC_TZ).isoformat(timespec="seconds"),
                "target_date": target_date,
                "sheet_json": str(latest_json_path),
                "sheet_csv": str(latest_csv_path),
                "games_total": slate_payload.get("meta", {}).get("total_games", 0),
                "games_projectable": slate_payload.get("meta", {}).get("projectable_games", 0),
                "games_failed": slate_payload.get("meta", {}).get("failed_games", 0),
                "games_live": slate_payload.get("meta", {}).get("live_games", 0),
                "first_period_graded_games": slate_payload.get("meta", {}).get("first_period_graded_games", 0),
                "first_period_over_games": slate_payload.get("meta", {}).get("first_period_over_games", 0),
                "first_period_under_games": slate_payload.get("meta", {}).get("first_period_under_games", 0),
                "status_counts": slate_payload.get("meta", {}).get("status_counts", {}),
            }
            write_json(latest_meta_path, meta)

            if not args.skip_dashboard:
                dashboard.build_dashboard_html(
                    force_refresh=False,
                    output_path=args.dashboard_path,
                    slate_date=target_date,
                )

    except RuntimeError as exc:
        if "already running" in str(exc):
            if not args.quiet:
                print(str(exc))
            return 0
        raise

    if not args.quiet:
        print(f"Wrote: {latest_json_path}")
        print(f"Wrote: {latest_csv_path}")
        print(f"Wrote: {latest_meta_path}")
        if not args.skip_dashboard:
            print(f"Updated dashboard: {args.dashboard_path}")
        print(f"[{datetime.now(UTC_TZ).isoformat(timespec='seconds')}] update_starting_goalie_sheet complete")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
