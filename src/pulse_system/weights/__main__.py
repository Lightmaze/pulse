"""Offline CLI for exporting, resetting and rolling back field weights."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pulse_system.substrate.storage.store import Storage


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m pulse_system.weights",
        description=(
            "Operate on PC-01 field weights while preserving factory weights. "
            "Stop the runtime before mutation commands."
        ),
    )
    parser.add_argument("--db", required=True, help="SQLite runtime database")
    commands = parser.add_subparsers(dest="command", required=True)

    export = commands.add_parser("export", help="export only field weights")
    export.add_argument("--output", required=True)

    restore = commands.add_parser("import", help="replace field weights")
    restore.add_argument("--input", required=True)

    checkpoint = commands.add_parser(
        "checkpoint", help="create an in-database rollback point"
    )
    checkpoint.add_argument("--label")

    rollback = commands.add_parser(
        "rollback", help="restore one in-database rollback point"
    )
    rollback.add_argument("checkpoint_id")

    reset = commands.add_parser(
        "reset", help="clear field weights and return to factory weights"
    )
    reset.add_argument(
        "--yes",
        action="store_true",
        help="confirm the destructive field-layer reset",
    )

    commands.add_parser("list", help="list rollback points")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    storage = Storage(args.db)
    try:
        if args.command == "export":
            payload = storage.export_field_weights()
            target = Path(args.output)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            result = {
                "output": str(target),
                "connections": len(payload["connections"]),
                "components": len(payload["components"]),
            }
        elif args.command == "import":
            payload = json.loads(
                Path(args.input).read_text(encoding="utf-8")
            )
            result = storage.import_field_weights(payload)
        elif args.command == "checkpoint":
            result = {
                "checkpoint_id": storage.checkpoint_field_weights(args.label)
            }
        elif args.command == "rollback":
            result = storage.rollback_field_weights(args.checkpoint_id)
        elif args.command == "reset":
            if not args.yes:
                raise SystemExit(
                    "reset refused: pass --yes after stopping the runtime"
                )
            result = storage.reset_field_weights()
        else:
            result = {"checkpoints": storage.list_weight_checkpoints()}
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    finally:
        storage.close()


if __name__ == "__main__":
    raise SystemExit(main())
