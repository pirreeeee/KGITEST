from __future__ import annotations

import argparse
import json
import mimetypes
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import core


ROOT = Path(__file__).resolve().parent
STATIC_ROOT = ROOT / "static"
INDEX_PATH = ROOT / "index.html"


class LifePulseHandler(BaseHTTPRequestHandler):
    server_version = "LifePulse/1.0"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        try:
            if parsed.path == "/":
                self.send_file(INDEX_PATH, "text/html; charset=utf-8")
                return
            if parsed.path.startswith("/static/"):
                self.serve_static(parsed.path.removeprefix("/static/"))
                return
            if parsed.path == "/api/agents":
                with db_connection() as conn:
                    self.send_json({"agents": core.get_agents(conn)})
                return
            if parsed.path == "/api/dashboard":
                query = parse_qs(parsed.query)
                agent_id = query.get("agent_id", ["A003"])[0]
                with db_connection() as conn:
                    self.send_json(core.get_dashboard(conn, agent_id))
                return
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
        except ValueError as exc:
            self.send_json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # pragma: no cover - defensive HTTP boundary
            self.send_json({"error": str(exc)}, HTTPStatus.INTERNAL_SERVER_ERROR)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        try:
            if parsed.path == "/api/complete-study":
                payload = self.read_json()
                agent_id = payload.get("agent_id", "A003")
                completed_at = parse_optional_datetime(payload.get("completed_at"))
                with db_connection() as conn:
                    result = core.record_study_completion(
                        conn,
                        agent_id=agent_id,
                        module_id=payload.get("module_id", "regulatory-recall"),
                        quiz_score=int(payload.get("quiz_score", 100)),
                        bio_rhythm_respected=parse_bool(
                            payload.get("bio_rhythm_respected", True)
                        ),
                        completed_at=completed_at,
                        sprint_minutes=int(payload.get("sprint_minutes", 7)),
                    )
                    result["dashboard"] = core.get_dashboard(conn, agent_id)
                    self.send_json(result, HTTPStatus.CREATED)
                return
            if parsed.path == "/api/run-daily-streak-check":
                payload = self.read_json()
                agent_id = payload.get("agent_id", "A003")
                with db_connection() as conn:
                    result = core.run_daily_streak_evaluation(conn, agent_id)
                    result["dashboard"] = core.get_dashboard(conn, agent_id)
                    self.send_json(result)
                return
            if parsed.path == "/api/rebuild-weekly-cache":
                payload = self.read_json()
                agent_id = payload.get("agent_id", "A003")
                with db_connection() as conn:
                    core.refresh_leaderboard_standings(conn)
                    self.send_json(core.get_dashboard(conn, agent_id))
                return
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
        except ValueError as exc:
            self.send_json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # pragma: no cover - defensive HTTP boundary
            self.send_json({"error": str(exc)}, HTTPStatus.INTERNAL_SERVER_ERROR)

    def read_json(self) -> dict:
        content_length = int(self.headers.get("Content-Length", "0"))
        if content_length == 0:
            return {}
        body = self.rfile.read(content_length).decode("utf-8")
        return json.loads(body)

    def serve_static(self, relative_path: str) -> None:
        path = (STATIC_ROOT / relative_path).resolve()
        if not path.is_file() or STATIC_ROOT.resolve() not in path.parents:
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return
        mime_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        if mime_type.startswith("text/") or mime_type == "application/javascript":
            mime_type = f"{mime_type}; charset=utf-8"
        self.send_file(path, mime_type)

    def send_file(self, path: Path, content_type: str) -> None:
        data = path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def send_json(self, payload: dict, status: HTTPStatus = HTTPStatus.OK) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, format: str, *args) -> None:
        print(f"{self.address_string()} - {format % args}")


def db_connection():
    conn = core.connect()
    core.init_db(conn)
    core.seed_demo_data(conn)
    return conn


def parse_optional_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


def parse_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the L.I.F.E. Pulse demo app.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=8000, type=int)
    parser.add_argument("--reset-demo-data", action="store_true")
    args = parser.parse_args()

    with core.connect() as conn:
        core.init_db(conn)
        core.seed_demo_data(conn, force=args.reset_demo_data)

    server = ThreadingHTTPServer((args.host, args.port), LifePulseHandler)
    print(f"L.I.F.E. Pulse running at http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
