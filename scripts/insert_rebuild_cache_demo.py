from __future__ import annotations

import argparse
import json
import sqlite3
import uuid
from datetime import date, datetime, time, timedelta
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


def week_window(target_date: date) -> tuple[datetime, datetime]:
    week_start = target_date - timedelta(days=target_date.weekday())
    week_end = week_start + timedelta(days=7)
    return (
        datetime.combine(week_start, time.min),
        datetime.combine(week_end, time.min),
    )


def epoch_week_number(target_date: date) -> str:
    iso_year, iso_week, _weekday = target_date.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def fetch_agent_metrics(
    conn: sqlite3.Connection, agent_id: str, target_date: date
) -> dict[str, int | str]:
    start_at, end_at = week_window(target_date)
    epoch = epoch_week_number(target_date)
    cache_row = conn.execute(
        """
        SELECT weekly_points_total
        FROM LeaderboardStandings
        WHERE agent_id = ? AND epoch_week_number = ?
        """,
        (agent_id, epoch),
    ).fetchone()
    ledger_row = conn.execute(
        """
        SELECT COALESCE(SUM(points_awarded), 0) AS weekly_points_total
        FROM PointLedger
        WHERE agent_id = ?
          AND occurred_at >= ?
          AND occurred_at < ?
        """,
        (
            agent_id,
            start_at.isoformat(timespec="seconds"),
            end_at.isoformat(timespec="seconds"),
        ),
    ).fetchone()
    lifetime_row = conn.execute(
        """
        SELECT COALESCE(SUM(points_awarded), 0) AS lifetime_points
        FROM PointLedger
        WHERE agent_id = ?
        """,
        (agent_id,),
    ).fetchone()
    if cache_row is None:
        raise ValueError(
            f"No LeaderboardStandings row found for {agent_id} in epoch {epoch}. "
            "Open the app once or run Rebuild cache first."
        )
    return {
        "epoch": epoch,
        "cache_weekly_points": int(cache_row["weekly_points_total"]),
        "ledger_weekly_points": int(ledger_row["weekly_points_total"]),
        "lifetime_points": int(lifetime_row["lifetime_points"]),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Insert a PointLedger event to demo Rebuild cache."
    )
    parser.add_argument("--agent-id", default="A003")
    parser.add_argument("--points", type=int, default=50)
    parser.add_argument("--event-type", default="manual_demo_bonus")
    args = parser.parse_args()

    if args.points == 0:
        print("points must be non-zero.", file=sys.stderr)
        return 1

    occurred_at = datetime.now().replace(microsecond=0)
    metadata = {
        "source": "demo_helper",
        "note": "Use Rebuild cache to sync LeaderboardStandings from PointLedger.",
    }

    with connect() as conn:
        before = fetch_agent_metrics(conn, args.agent_id, occurred_at.date())
        transaction_id = uuid.uuid4().hex
        inserted = conn.execute(
            """
            INSERT INTO PointLedger(
                transaction_id,
                agent_id,
                event_type,
                points_awarded,
                occurred_at,
                metadata_json
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                transaction_id,
                args.agent_id,
                args.event_type,
                args.points,
                occurred_at.isoformat(timespec="seconds"),
                json.dumps(metadata, sort_keys=True),
            ),
        ).rowcount
        if inserted != 1:
            raise RuntimeError("PointLedger insert failed.")
        conn.commit()
        after = fetch_agent_metrics(conn, args.agent_id, occurred_at.date())

    print("Inserted rebuild-cache demo event.")
    print(f"Database: {DATABASE_PATH}")
    print(f"Agent: {args.agent_id}")
    print(f"Epoch: {before['epoch']}")
    print(
        "Before: "
        f"cache_weekly_points={before['cache_weekly_points']}, "
        f"ledger_weekly_points={before['ledger_weekly_points']}, "
        f"lifetime_points={before['lifetime_points']}"
    )
    print(
        "After insert: "
        f"cache_weekly_points={after['cache_weekly_points']}, "
        f"ledger_weekly_points={after['ledger_weekly_points']}, "
        f"lifetime_points={after['lifetime_points']}"
    )
    print(f"Inserted transaction_id: {transaction_id}")
    print("Next step: do not refresh the browser. Click 'Rebuild cache'.")
    print(
        "Expected result: leaderboard cache should catch up to the higher "
        "PointLedger total."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
