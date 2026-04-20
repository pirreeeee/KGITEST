from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
DATABASE_PATH = ROOT / "data" / "life_pulse.db"
SHIELD_EVERY_N_DAYS = 3
MAX_ACTIVE_SHIELDS = 2


def connect(db_path: str | Path = DATABASE_PATH) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS Branches (
            branch_id TEXT PRIMARY KEY,
            branch_name TEXT NOT NULL,
            city TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS Agents (
            agent_id TEXT PRIMARY KEY,
            agent_name TEXT NOT NULL,
            branch_id TEXT NOT NULL,
            role_title TEXT NOT NULL,
            FOREIGN KEY (branch_id) REFERENCES Branches(branch_id)
        );

        CREATE TABLE IF NOT EXISTS PointLedger (
            transaction_id TEXT PRIMARY KEY,
            agent_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            points_awarded INTEGER NOT NULL,
            occurred_at TEXT NOT NULL,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            FOREIGN KEY (agent_id) REFERENCES Agents(agent_id)
        );

        CREATE INDEX IF NOT EXISTS idx_point_ledger_agent_time
            ON PointLedger(agent_id, occurred_at);

        CREATE INDEX IF NOT EXISTS idx_point_ledger_time
            ON PointLedger(occurred_at);

        CREATE TABLE IF NOT EXISTS LeaderboardStandings (
            standing_id TEXT PRIMARY KEY,
            agent_id TEXT NOT NULL,
            branch_id TEXT NOT NULL,
            epoch_week_number TEXT NOT NULL,
            weekly_points_total INTEGER NOT NULL,
            last_recalculated_at TEXT NOT NULL,
            UNIQUE(agent_id, epoch_week_number),
            FOREIGN KEY (agent_id) REFERENCES Agents(agent_id),
            FOREIGN KEY (branch_id) REFERENCES Branches(branch_id)
        );

        CREATE INDEX IF NOT EXISTS idx_leaderboard_epoch_points
            ON LeaderboardStandings(epoch_week_number, weekly_points_total DESC);

        CREATE TABLE IF NOT EXISTS AgentStreaks (
            streak_id TEXT PRIMARY KEY,
            agent_id TEXT NOT NULL UNIQUE,
            current_streak_days INTEGER NOT NULL DEFAULT 0,
            longest_historical_streak INTEGER NOT NULL DEFAULT 0,
            active_shields_count INTEGER NOT NULL DEFAULT 0,
            last_study_date TEXT,
            FOREIGN KEY (agent_id) REFERENCES Agents(agent_id)
        );

        CREATE TABLE IF NOT EXISTS StudySessions (
            session_id TEXT PRIMARY KEY,
            agent_id TEXT NOT NULL,
            module_id TEXT NOT NULL,
            completed_at TEXT NOT NULL,
            sprint_minutes INTEGER NOT NULL,
            quiz_score INTEGER NOT NULL,
            bio_rhythm_respected INTEGER NOT NULL,
            FOREIGN KEY (agent_id) REFERENCES Agents(agent_id)
        );

        CREATE INDEX IF NOT EXISTS idx_study_sessions_agent_time
            ON StudySessions(agent_id, completed_at);

        CREATE TABLE IF NOT EXISTS StreakEvaluations (
            evaluation_id TEXT PRIMARY KEY,
            agent_id TEXT NOT NULL,
            evaluation_date TEXT NOT NULL,
            status TEXT NOT NULL,
            UNIQUE(agent_id, evaluation_date),
            FOREIGN KEY (agent_id) REFERENCES Agents(agent_id)
        );
        """
    )
    conn.commit()


def new_id() -> str:
    return uuid.uuid4().hex


def epoch_week_number(target_date: date) -> str:
    iso_year, iso_week, _weekday = target_date.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def week_start_for(target_date: date) -> date:
    return target_date - timedelta(days=target_date.weekday())


def parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value)


def days_until_next_shield(current_streak_days: int) -> int:
    if current_streak_days <= 0:
        return SHIELD_EVERY_N_DAYS
    remainder = current_streak_days % SHIELD_EVERY_N_DAYS
    return SHIELD_EVERY_N_DAYS if remainder == 0 else SHIELD_EVERY_N_DAYS - remainder


def calculate_point_events(
    quiz_score: int, bio_rhythm_respected: bool
) -> list[tuple[str, int]]:
    events = [("module_completed", 10)]
    if int(quiz_score) == 100:
        events.append(("quiz_perfect", 5))
    if bio_rhythm_respected:
        events.append(("bio_rhythm_bonus", 2))
    return events


def ensure_agent_exists(conn: sqlite3.Connection, agent_id: str) -> None:
    row = conn.execute(
        "SELECT 1 FROM Agents WHERE agent_id = ?",
        (agent_id,),
    ).fetchone()
    if not row:
        raise ValueError(f"unknown agent_id: {agent_id}")


def add_ledger_event(
    conn: sqlite3.Connection,
    agent_id: str,
    event_type: str,
    points_awarded: int,
    occurred_at: datetime,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    transaction_id = new_id()
    entry = {
        "transaction_id": transaction_id,
        "agent_id": agent_id,
        "event_type": event_type,
        "points_awarded": int(points_awarded),
        "occurred_at": occurred_at.isoformat(timespec="seconds"),
        "metadata_json": json.dumps(metadata or {}, sort_keys=True),
    }
    conn.execute(
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
            entry["transaction_id"],
            entry["agent_id"],
            entry["event_type"],
            entry["points_awarded"],
            entry["occurred_at"],
            entry["metadata_json"],
        ),
    )
    return entry


def seed_demo_data(
    conn: sqlite3.Connection, force: bool = False, today: date | None = None
) -> None:
    existing = conn.execute("SELECT COUNT(*) AS count FROM Agents").fetchone()["count"]
    if existing and not force:
        return

    today = today or date.today()
    week_start = week_start_for(today)
    branches = [
        ("B-NG", "Nangang District Branch", "Taipei"),
        ("B-XY", "Xinyi Financial Center", "Taipei"),
        ("B-DA", "Daan Advisory Hub", "Taipei"),
        ("B-NH", "Neihu Digital Branch", "Taipei"),
    ]
    agents = [
        ("A001", "Ivy Chen", "B-NG", "Senior Agent"),
        ("A002", "Marcus Lin", "B-NG", "Agent"),
        ("A003", "Rina Wang", "B-NG", "Agent"),
        ("A004", "Derek Hsu", "B-NG", "Agent"),
        ("A005", "Joanne Tsai", "B-XY", "Agent"),
        ("A006", "Leo Chang", "B-XY", "Agent"),
        ("A007", "Nora Huang", "B-XY", "Agent"),
        ("A008", "Kevin Yu", "B-DA", "Agent"),
        ("A009", "Mia Lo", "B-DA", "Agent"),
        ("A010", "Oscar Wu", "B-DA", "Agent"),
        ("A011", "Sandy Kao", "B-NH", "Agent"),
        ("A012", "Ben Liu", "B-NH", "Agent"),
    ]
    weekly_targets = {
        "A001": 102,
        "A002": 94,
        "A003": 87,
        "A004": 76,
        "A005": 68,
        "A006": 61,
        "A007": 55,
        "A008": 49,
        "A009": 42,
        "A010": 36,
        "A011": 31,
        "A012": 24,
    }
    streaks = {
        "A001": (5, 14, 1, today),
        "A002": (4, 9, 0, today),
        "A003": (3, 8, 1, today),
        "A004": (2, 7, 0, today),
        "A005": (6, 11, 2, today),
        "A006": (1, 5, 0, today - timedelta(days=1)),
        "A007": (0, 6, 0, today - timedelta(days=4)),
        "A008": (3, 10, 1, today),
        "A009": (2, 5, 0, today - timedelta(days=1)),
        "A010": (7, 12, 2, today),
        "A011": (1, 3, 0, today),
        "A012": (4, 4, 1, today),
    }

    with conn:
        for table in [
            "StreakEvaluations",
            "StudySessions",
            "LeaderboardStandings",
            "AgentStreaks",
            "PointLedger",
            "Agents",
            "Branches",
        ]:
            conn.execute(f"DELETE FROM {table}")

        conn.executemany(
            "INSERT INTO Branches(branch_id, branch_name, city) VALUES (?, ?, ?)",
            branches,
        )
        conn.executemany(
            """
            INSERT INTO Agents(agent_id, agent_name, branch_id, role_title)
            VALUES (?, ?, ?, ?)
            """,
            agents,
        )
        for agent_id, values in streaks.items():
            current, longest, shields, last_study = values
            conn.execute(
                """
                INSERT INTO AgentStreaks(
                    streak_id,
                    agent_id,
                    current_streak_days,
                    longest_historical_streak,
                    active_shields_count,
                    last_study_date
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    new_id(),
                    agent_id,
                    current,
                    longest,
                    shields,
                    last_study.isoformat(),
                ),
            )

        for index, (agent_id, points) in enumerate(weekly_targets.items()):
            timestamp = datetime.combine(
                week_start + timedelta(days=index % max(1, today.weekday() + 1)),
                time(hour=9 + (index % 8), minute=15),
            )
            for event_type, event_points in decompose_points(points):
                add_ledger_event(
                    conn,
                    agent_id,
                    event_type,
                    event_points,
                    timestamp,
                    {
                        "source": "demo_seed",
                        "module_id": f"seed-{agent_id.lower()}",
                    },
                )
                timestamp += timedelta(minutes=9)

        for agent_id, values in streaks.items():
            current, _longest, _shields, last_study = values
            days_to_seed = min(current, 5)
            for offset in range(days_to_seed):
                completed_date = last_study - timedelta(days=offset)
                conn.execute(
                    """
                    INSERT INTO StudySessions(
                        session_id,
                        agent_id,
                        module_id,
                        completed_at,
                        sprint_minutes,
                        quiz_score,
                        bio_rhythm_respected
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        new_id(),
                        agent_id,
                        "daily-compliance-recall",
                        datetime.combine(completed_date, time(hour=8, minute=30)).isoformat(
                            timespec="seconds"
                        ),
                        7,
                        100 if offset % 2 == 0 else 85,
                        1,
                    ),
                )

        refresh_leaderboard_standings(conn, today)


def decompose_points(points: int) -> list[tuple[str, int]]:
    remaining = points
    events: list[tuple[str, int]] = []
    while remaining >= 10 and remaining not in {11, 13}:
        events.append(("module_completed", 10))
        remaining -= 10
    while remaining >= 5 and remaining not in {6, 8}:
        events.append(("quiz_perfect", 5))
        remaining -= 5
    while remaining > 0:
        if remaining == 1 and events:
            event_type, event_points = events.pop()
            remaining += event_points
            if event_points == 10:
                events.extend([("quiz_perfect", 5), ("bio_rhythm_bonus", 2)])
                remaining -= 7
            else:
                events.extend([("bio_rhythm_bonus", 2), ("bio_rhythm_bonus", 2)])
                remaining -= 4
        else:
            events.append(("bio_rhythm_bonus", 2))
            remaining -= 2
    return events


def get_agents(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT a.agent_id, a.agent_name, a.role_title, b.branch_name, b.city
        FROM Agents a
        JOIN Branches b ON b.branch_id = a.branch_id
        ORDER BY a.agent_name
        """
    ).fetchall()
    return [dict(row) for row in rows]


def get_dashboard(
    conn: sqlite3.Connection, agent_id: str, target_date: date | None = None
) -> dict[str, Any]:
    target_date = target_date or date.today()
    ensure_agent_exists(conn, agent_id)
    refresh_leaderboard_standings(conn, target_date)
    epoch = epoch_week_number(target_date)
    agent = conn.execute(
        """
        SELECT a.agent_id, a.agent_name, a.role_title, b.branch_id, b.branch_name, b.city
        FROM Agents a
        JOIN Branches b ON b.branch_id = a.branch_id
        WHERE a.agent_id = ?
        """,
        (agent_id,),
    ).fetchone()

    return {
        "agent": dict(agent),
        "epoch_week_number": epoch,
        "streak": get_streak(conn, agent_id),
        "relative_leaderboard": get_relative_leaderboard(conn, agent_id, epoch),
        "branch_arena": get_branch_arena(conn, epoch),
        "recent_ledger": get_recent_ledger(conn, agent_id),
        "lifetime_points": get_lifetime_points(conn, agent_id),
    }


def record_study_completion(
    conn: sqlite3.Connection,
    agent_id: str,
    module_id: str,
    quiz_score: int,
    bio_rhythm_respected: bool,
    completed_at: datetime | None = None,
    sprint_minutes: int = 7,
) -> dict[str, Any]:
    ensure_agent_exists(conn, agent_id)
    if not 0 <= int(quiz_score) <= 100:
        raise ValueError("quiz_score must be between 0 and 100")
    if sprint_minutes < 7:
        raise ValueError("a sprint must be at least 7 minutes")

    completed_at = completed_at or datetime.now().replace(microsecond=0)
    events = calculate_point_events(quiz_score, bio_rhythm_respected)

    with conn:
        conn.execute(
            """
            INSERT INTO StudySessions(
                session_id,
                agent_id,
                module_id,
                completed_at,
                sprint_minutes,
                quiz_score,
                bio_rhythm_respected
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                new_id(),
                agent_id,
                module_id,
                completed_at.isoformat(timespec="seconds"),
                sprint_minutes,
                int(quiz_score),
                1 if bio_rhythm_respected else 0,
            ),
        )
        ledger_entries = [
            add_ledger_event(
                conn,
                agent_id,
                event_type,
                points,
                completed_at,
                {
                    "module_id": module_id,
                    "quiz_score": quiz_score,
                    "bio_rhythm_respected": bio_rhythm_respected,
                },
            )
            for event_type, points in events
        ]
        streak = apply_streak_on_completion(conn, agent_id, completed_at.date())
        refresh_leaderboard_standings(conn, completed_at.date())

    return {
        "awarded_total": sum(entry["points_awarded"] for entry in ledger_entries),
        "ledger_entries": ledger_entries,
        "streak": streak,
    }


def run_daily_streak_evaluation(
    conn: sqlite3.Connection,
    agent_id: str,
    evaluation_date: date | None = None,
) -> dict[str, Any]:
    ensure_agent_exists(conn, agent_id)
    evaluation_date = evaluation_date or date.today()
    row = ensure_streak_row(conn, agent_id)
    already_evaluated = conn.execute(
        """
        SELECT status
        FROM StreakEvaluations
        WHERE agent_id = ? AND evaluation_date = ?
        """,
        (agent_id, evaluation_date.isoformat()),
    ).fetchone()
    if already_evaluated:
        result = get_streak(conn, agent_id)
        result["status"] = already_evaluated["status"]
        result["already_evaluated"] = True
        return result

    last_study_date = parse_date(row["last_study_date"])
    current = row["current_streak_days"]
    longest = row["longest_historical_streak"]
    shields = row["active_shields_count"]
    status = "ok"

    if last_study_date is None:
        current = 0
        status = "no_study_history"
    elif (evaluation_date - last_study_date).days > 1:
        if shields > 0:
            shields -= 1
            status = "shield_consumed"
        else:
            current = 0
            status = "streak_reset"

    with conn:
        conn.execute(
            """
            UPDATE AgentStreaks
            SET current_streak_days = ?,
                longest_historical_streak = ?,
                active_shields_count = ?
            WHERE agent_id = ?
            """,
            (current, longest, shields, agent_id),
        )
        conn.execute(
            """
            INSERT INTO StreakEvaluations(
                evaluation_id,
                agent_id,
                evaluation_date,
                status
            )
            VALUES (?, ?, ?, ?)
            """,
            (new_id(), agent_id, evaluation_date.isoformat(), status),
        )

    result = get_streak(conn, agent_id)
    result["status"] = status
    result["already_evaluated"] = False
    return result


def apply_streak_on_completion(
    conn: sqlite3.Connection, agent_id: str, study_date: date
) -> dict[str, Any]:
    row = ensure_streak_row(conn, agent_id)
    last_study_date = parse_date(row["last_study_date"])
    current = row["current_streak_days"]
    longest = row["longest_historical_streak"]
    shields = row["active_shields_count"]
    shield_consumed = 0
    shield_granted = False
    status = "same_day"

    if last_study_date is None:
        current = 1
        status = "started"
    elif study_date <= last_study_date:
        return {
            **get_streak(conn, agent_id),
            "status": status,
            "shield_consumed": shield_consumed,
            "shield_granted": shield_granted,
        }
    else:
        gap_days = (study_date - last_study_date).days
        missed_days = max(0, gap_days - 1)
        if missed_days == 0:
            current += 1
            status = "incremented"
        elif shields >= missed_days:
            shields -= missed_days
            shield_consumed = missed_days
            current += 1
            status = "shield_consumed"
        else:
            shields = 0
            current = 1
            status = "reset_then_started"

    longest = max(longest, current)
    if current > 0 and current % SHIELD_EVERY_N_DAYS == 0 and shields < MAX_ACTIVE_SHIELDS:
        shields += 1
        shield_granted = True

    conn.execute(
        """
        UPDATE AgentStreaks
        SET current_streak_days = ?,
            longest_historical_streak = ?,
            active_shields_count = ?,
            last_study_date = ?
        WHERE agent_id = ?
        """,
        (current, longest, shields, study_date.isoformat(), agent_id),
    )
    return {
        **get_streak(conn, agent_id),
        "status": status,
        "shield_consumed": shield_consumed,
        "shield_granted": shield_granted,
    }


def refresh_leaderboard_standings(
    conn: sqlite3.Connection, target_date: date | None = None
) -> None:
    target_date = target_date or date.today()
    epoch = epoch_week_number(target_date)
    start_date = week_start_for(target_date)
    end_date = start_date + timedelta(days=7)
    start_at = datetime.combine(start_date, time.min).isoformat(timespec="seconds")
    end_at = datetime.combine(end_date, time.min).isoformat(timespec="seconds")
    recalculated_at = datetime.now().replace(microsecond=0).isoformat(timespec="seconds")
    rows = conn.execute(
        """
        SELECT
            a.agent_id,
            a.branch_id,
            COALESCE(SUM(pl.points_awarded), 0) AS weekly_points_total
        FROM Agents a
        LEFT JOIN PointLedger pl
            ON pl.agent_id = a.agent_id
            AND pl.occurred_at >= ?
            AND pl.occurred_at < ?
        GROUP BY a.agent_id, a.branch_id
        """,
        (start_at, end_at),
    ).fetchall()

    conn.execute(
        "DELETE FROM LeaderboardStandings WHERE epoch_week_number = ?",
        (epoch,),
    )
    conn.executemany(
        """
        INSERT INTO LeaderboardStandings(
            standing_id,
            agent_id,
            branch_id,
            epoch_week_number,
            weekly_points_total,
            last_recalculated_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (
                new_id(),
                row["agent_id"],
                row["branch_id"],
                epoch,
                row["weekly_points_total"],
                recalculated_at,
            )
            for row in rows
        ],
    )


def get_relative_leaderboard(
    conn: sqlite3.Connection, agent_id: str, epoch_week_number_value: str
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            s.agent_id,
            a.agent_name,
            b.branch_name,
            s.weekly_points_total
        FROM LeaderboardStandings s
        JOIN Agents a ON a.agent_id = s.agent_id
        JOIN Branches b ON b.branch_id = s.branch_id
        WHERE s.epoch_week_number = ?
        ORDER BY s.weekly_points_total DESC, a.agent_name ASC
        """,
        (epoch_week_number_value,),
    ).fetchall()
    ranked = [
        {
            **dict(row),
            "rank": index + 1,
            "is_current_agent": row["agent_id"] == agent_id,
        }
        for index, row in enumerate(rows)
    ]
    current_index = next(
        (index for index, row in enumerate(ranked) if row["agent_id"] == agent_id), 0
    )
    start = max(0, current_index - 2)
    end = min(len(ranked), current_index + 3)
    if end - start < 5:
        start = max(0, end - 5)
        end = min(len(ranked), start + 5)
    return ranked[start:end]


def get_branch_arena(
    conn: sqlite3.Connection, epoch_week_number_value: str
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            b.branch_id,
            b.branch_name,
            b.city,
            COUNT(s.agent_id) AS agent_count,
            COALESCE(SUM(s.weekly_points_total), 0) AS branch_points_total,
            ROUND(COALESCE(AVG(s.weekly_points_total), 0), 1) AS average_agent_points
        FROM Branches b
        LEFT JOIN LeaderboardStandings s
            ON s.branch_id = b.branch_id
            AND s.epoch_week_number = ?
        GROUP BY b.branch_id, b.branch_name, b.city
        ORDER BY branch_points_total DESC, average_agent_points DESC, b.branch_name ASC
        """,
        (epoch_week_number_value,),
    ).fetchall()
    return [
        {
            **dict(row),
            "rank": index + 1,
        }
        for index, row in enumerate(rows)
    ]


def get_recent_ledger(
    conn: sqlite3.Connection, agent_id: str, limit: int = 8
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT transaction_id, event_type, points_awarded, occurred_at, metadata_json
        FROM PointLedger
        WHERE agent_id = ?
        ORDER BY occurred_at DESC, transaction_id DESC
        LIMIT ?
        """,
        (agent_id, limit),
    ).fetchall()
    return [
        {
            **dict(row),
            "metadata": json.loads(row["metadata_json"] or "{}"),
        }
        for row in rows
    ]


def get_lifetime_points(conn: sqlite3.Connection, agent_id: str) -> int:
    row = conn.execute(
        """
        SELECT COALESCE(SUM(points_awarded), 0) AS total
        FROM PointLedger
        WHERE agent_id = ?
        """,
        (agent_id,),
    ).fetchone()
    return int(row["total"])


def get_streak(conn: sqlite3.Connection, agent_id: str) -> dict[str, Any]:
    row = ensure_streak_row(conn, agent_id)
    return {
        "streak_id": row["streak_id"],
        "agent_id": row["agent_id"],
        "current_streak_days": row["current_streak_days"],
        "longest_historical_streak": row["longest_historical_streak"],
        "active_shields_count": row["active_shields_count"],
        "last_study_date": row["last_study_date"],
        "next_shield_in_days": days_until_next_shield(row["current_streak_days"]),
    }


def ensure_streak_row(conn: sqlite3.Connection, agent_id: str) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM AgentStreaks WHERE agent_id = ?",
        (agent_id,),
    ).fetchone()
    if row:
        return row
    conn.execute(
        """
        INSERT INTO AgentStreaks(
            streak_id,
            agent_id,
            current_streak_days,
            longest_historical_streak,
            active_shields_count,
            last_study_date
        )
        VALUES (?, ?, 0, 0, 0, NULL)
        """,
        (new_id(), agent_id),
    )
    return conn.execute(
        "SELECT * FROM AgentStreaks WHERE agent_id = ?",
        (agent_id,),
    ).fetchone()
