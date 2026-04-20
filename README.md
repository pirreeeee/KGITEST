# L.I.F.E. Pulse Streak Shield Leaderboard

Mini project for Project 7, "The Social Arena & Streak Shield Leaderboard".

## What It Builds

- Relative leaderboard: the selected agent is placed in context with nearby peers.
- Streak visualizer: consecutive study days, shield count, and lifetime points.
- Branch arena: weekly branch totals for Taipei offices.
- Point ledger: immutable transaction rows used to rebuild weekly standings.
- Daily streak engine: shields are consumed before a streak is reset.

## Run

```powershell
python app.py
```

Open:

```text
http://127.0.0.1:8000
```

On Windows with `uv` available:

```powershell
.\run_server.cmd
```

Reset the demo data:

```powershell
python app.py --reset-demo-data
```

## Test

```powershell
python -m unittest discover -s tests
```

On Windows with `uv` available:

```powershell
.\run_tests.cmd
```

## Point Rules

The backend posts one ledger transaction per point event.

- `module_completed`: +10
- `quiz_perfect`: +5 when quiz score is 100
- `bio_rhythm_bonus`: +2 when the sprint respects the Bio-Rhythm rule

Weekly leaderboard standings are rebuilt from `PointLedger`, not manually overwritten.
Lifetime points and streak state remain on the agent profile across weekly epochs.

## Streak Rules

- Completing at least one 7-minute sprint on a new calendar day increments the streak.
- Every 3-day streak milestone grants one shield, capped at 2 active shields.
- If a day is missed and a shield is available, one shield is consumed and the streak survives.
- If a day is missed without a shield, the current streak resets to 0 in the daily job or restarts at 1 on the next completed sprint.

## SQLite Tables

- `PointLedger`: immutable point transaction log.
- `LeaderboardStandings`: weekly cached view rebuilt from `PointLedger`.
- `AgentStreaks`: current streak state, longest streak, shield count, and last study date.
- `StudySessions`: sprint completion records.
- `StreakEvaluations`: idempotency log for the daily streak job.
- `Agents` and `Branches`: demo roster and office data.

## Implementation Notes

This project uses only the Python standard library and SQLite, so it can run without package installation. The HTTP server exposes JSON APIs for sprint completion, weekly cache rebuilds, dashboard data, and daily streak checks. The frontend is plain HTML, CSS, and JavaScript.
