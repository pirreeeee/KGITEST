from __future__ import annotations

import argparse
import sqlite3
from datetime import date, timedelta
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
DATABASE_PATH = ROOT / "data" / "life_pulse.db"


def connect() -> sqlite3.Connection:
    if not DATABASE_PATH.exists():
        raise FileNotFoundError(
            f"Database not found at {DATABASE_PATH}. Run the server once first."
        )
    conn = sqlite3.connect(DATABASE_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_streak(conn: sqlite3.Connection, agent_id: str) -> sqlite3.Row:
    row = conn.execute(
        """
        SELECT agent_id,
               current_streak_days,
               longest_historical_streak,
               active_shields_count,
               last_study_date
        FROM AgentStreaks
        WHERE agent_id = ?
        """,
        (agent_id,),
    ).fetchone()
    if not row:
        raise ValueError(f"Unknown agent_id: {agent_id}")
    return row


def format_streak(row: sqlite3.Row) -> str:
    return (
        f"agent_id={row['agent_id']}, "
        f"current_streak_days={row['current_streak_days']}, "
        f"longest_historical_streak={row['longest_historical_streak']}, "
        f"active_shields_count={row['active_shields_count']}, "
        f"last_study_date={row['last_study_date']}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Prepare a daily streak demo scenario for Run daily streak check."
    )
    parser.add_argument("--agent-id", default="A003")
    parser.add_argument("--current-streak", type=int, default=3)
    parser.add_argument("--longest-streak", type=int, default=8)
    parser.add_argument("--shields", type=int, default=1)
    parser.add_argument(
        "--days-since-last-study",
        type=int,
        default=2,
        help="Use 2 to simulate a missed day before today's daily streak check.",
    )
    args = parser.parse_args()

    if args.current_streak < 0 or args.longest_streak < 0 or args.shields < 0:
        print("Streak values must be non-negative.", file=sys.stderr)
        return 1
    if args.days_since_last_study < 0:
        print("days-since-last-study must be non-negative.", file=sys.stderr)
        return 1

    today = date.today()
    last_study_date = today - timedelta(days=args.days_since_last_study)

    with connect() as conn:
        before = fetch_streak(conn, args.agent_id)
        deleted = conn.execute(
            """
            DELETE FROM StreakEvaluations
            WHERE agent_id = ? AND evaluation_date = ?
            """,
            (args.agent_id, today.isoformat()),
        ).rowcount
        updated = conn.execute(
            """
            UPDATE AgentStreaks
            SET current_streak_days = ?,
                longest_historical_streak = ?,
                active_shields_count = ?,
                last_study_date = ?
            WHERE agent_id = ?
            """,
            (
                args.current_streak,
                max(args.longest_streak, args.current_streak),
                args.shields,
                last_study_date.isoformat(),
                args.agent_id,
            ),
        ).rowcount
        if updated != 1:
            raise ValueError(f"Unknown agent_id: {args.agent_id}")
        conn.commit()
        after = fetch_streak(conn, args.agent_id)

    if args.days_since_last_study <= 1:
        expected = "no streak break will be detected"
    elif args.shields > 0:
        expected = "shield should be consumed and streak should be preserved"
    else:
        expected = "streak should reset because no shield is available"

    print("Prepared daily streak demo.")
    print(f"Database: {DATABASE_PATH}")
    print(f"Today: {today.isoformat()}")
    print(f"Before: {format_streak(before)}")
    print(f"After:  {format_streak(after)}")
    print(f"Cleared today's StreakEvaluations rows: {deleted}")
    print("Next step: refresh the browser, then click 'Run daily streak check'.")
    print(f"Expected result: {expected}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
