from __future__ import annotations

import unittest
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

import app
import core


class CoreRulesTest(unittest.TestCase):
    def setUp(self) -> None:
        temp_root = Path(__file__).resolve().parents[1] / ".tmp"
        temp_root.mkdir(exist_ok=True)
        self.db_path = temp_root / f"test-{uuid.uuid4().hex}.db"
        self.conn = core.connect(self.db_path)
        core.init_db(self.conn)
        core.seed_demo_data(self.conn, force=True, today=date(2026, 4, 17))

    def tearDown(self) -> None:
        self.conn.close()
        for suffix in ["", "-journal", "-wal", "-shm"]:
            path = Path(f"{self.db_path}{suffix}")
            if path.exists():
                path.unlink()

    def test_completion_posts_formula_events_to_immutable_ledger(self) -> None:
        before_count = self.conn.execute(
            "SELECT COUNT(*) AS count FROM PointLedger WHERE agent_id = 'A003'"
        ).fetchone()["count"]
        result = core.record_study_completion(
            self.conn,
            agent_id="A003",
            module_id="regulatory-recall",
            quiz_score=100,
            bio_rhythm_respected=True,
            completed_at=datetime(2026, 4, 17, 9, 30),
        )
        after_count = self.conn.execute(
            "SELECT COUNT(*) AS count FROM PointLedger WHERE agent_id = 'A003'"
        ).fetchone()["count"]
        after_rows = self.conn.execute(
            """
            SELECT event_type, points_awarded
            FROM PointLedger
            WHERE agent_id = 'A003'
            ORDER BY occurred_at DESC
            LIMIT 3
            """
        ).fetchall()

        self.assertEqual(result["awarded_total"], 17)
        self.assertEqual(before_count + 3, after_count)
        self.assertEqual(
            {row["event_type"] for row in after_rows},
            {"module_completed", "quiz_perfect", "bio_rhythm_bonus"},
        )

    def test_one_missed_day_consumes_shield_and_keeps_streak(self) -> None:
        self.conn.execute(
            """
            UPDATE AgentStreaks
            SET current_streak_days = 3,
                longest_historical_streak = 3,
                active_shields_count = 1,
                last_study_date = ?
            WHERE agent_id = 'A003'
            """,
            ((date(2026, 4, 17) - timedelta(days=2)).isoformat(),),
        )
        result = core.record_study_completion(
            self.conn,
            agent_id="A003",
            module_id="regulatory-recall",
            quiz_score=85,
            bio_rhythm_respected=False,
            completed_at=datetime(2026, 4, 17, 8, 0),
        )

        self.assertEqual(result["streak"]["status"], "shield_consumed")
        self.assertEqual(result["streak"]["current_streak_days"], 4)
        self.assertEqual(result["streak"]["active_shields_count"], 0)

    def test_missed_day_without_shield_resets_streak(self) -> None:
        self.conn.execute(
            """
            UPDATE AgentStreaks
            SET current_streak_days = 5,
                longest_historical_streak = 5,
                active_shields_count = 0,
                last_study_date = ?
            WHERE agent_id = 'A003'
            """,
            ((date(2026, 4, 17) - timedelta(days=2)).isoformat(),),
        )
        result = core.record_study_completion(
            self.conn,
            agent_id="A003",
            module_id="regulatory-recall",
            quiz_score=85,
            bio_rhythm_respected=False,
            completed_at=datetime(2026, 4, 17, 8, 0),
        )

        self.assertEqual(result["streak"]["status"], "reset_then_started")
        self.assertEqual(result["streak"]["current_streak_days"], 1)
        self.assertEqual(result["streak"]["active_shields_count"], 0)

    def test_daily_streak_evaluation_is_idempotent(self) -> None:
        self.conn.execute(
            """
            UPDATE AgentStreaks
            SET current_streak_days = 3,
                longest_historical_streak = 6,
                active_shields_count = 1,
                last_study_date = ?
            WHERE agent_id = 'A003'
            """,
            ((date(2026, 4, 17) - timedelta(days=2)).isoformat(),),
        )

        first = core.run_daily_streak_evaluation(
            self.conn, "A003", evaluation_date=date(2026, 4, 17)
        )
        second = core.run_daily_streak_evaluation(
            self.conn, "A003", evaluation_date=date(2026, 4, 17)
        )

        self.assertEqual(first["status"], "shield_consumed")
        self.assertEqual(first["active_shields_count"], 0)
        self.assertTrue(second["already_evaluated"])
        self.assertEqual(second["active_shields_count"], 0)

    def test_relative_leaderboard_contains_selected_agent(self) -> None:
        dashboard = core.get_dashboard(
            self.conn, "A003", target_date=date(2026, 4, 17)
        )
        rows = dashboard["relative_leaderboard"]

        self.assertLessEqual(len(rows), 5)
        self.assertTrue(any(row["agent_id"] == "A003" for row in rows))
        self.assertEqual([row["rank"] for row in rows], [1, 2, 3, 4, 5])

    def test_api_bool_parser_handles_string_false(self) -> None:
        self.assertFalse(app.parse_bool("false"))
        self.assertFalse(app.parse_bool("0"))
        self.assertTrue(app.parse_bool("true"))
        self.assertTrue(app.parse_bool(True))


if __name__ == "__main__":
    unittest.main()
