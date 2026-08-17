"""Run the sideband observatory server.

    uv run python -m pulse_system.interaction.api --file out.jsonl

Serves the built viewer too when web/dist exists, so the whole observatory
is one URL; otherwise run `npm run dev` in web/ and point it at this port.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import uvicorn

from pulse_system.interaction.api.app import create_app

_REPO_ROOT = Path(__file__).resolve().parents[4]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--file", required=True,
                    help="Pulse metrics JSONL to watch (need not exist yet)")
    ap.add_argument("--db", default=None,
                    help="run's SQLite file, read-only — enables /engrams "
                         "session inspection")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--static", default=str(_REPO_ROOT / "web" / "dist"),
                    help="built viewer to serve at / (skipped if absent)")
    args = ap.parse_args()

    app = create_app(args.file, db_path=args.db, static_dir=args.static)
    print(f"watching {args.file}" + (f" + db {args.db}" if args.db else ""))
    print(f"  SSE:    http://{args.host}:{args.port}/events")
    print(f"  status: http://{args.host}:{args.port}/status")
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
