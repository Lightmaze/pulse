"""One command, one organism: `pulse`.

Starts the runtime, attaches the API to it, serves the workbench, and keeps
ticking until you stop it. Before this existed the three streams all had code
and none of them had a process — the engine could tick, the claustrum could
modulate and the delegator could route, but nothing held them together for
longer than a test.

`pulse` defaults to the real Pi Harness and fails at startup when Pi is not
available. Offline/demo behavior is an explicit choice and is always reported
as mock; production never falls back to it.

    pulse                      # real Pi Harness
    pulse --mock               # explicit legacy/test Harness
    pulse --pi-provider deepseek --pi-model deepseek-v4-flash
    pulse --db run.db --port 8100
    pulse --with-claustrum --with-router     # Tuning and Delegation streams on
"""

from __future__ import annotations

import argparse
import contextlib
import logging
import sys
from pathlib import Path

if sys.platform == "win32":  # pragma: no cover - platform branch
    # The banner names the three streams in Chinese and the default Windows
    # console encoding is cp1252, so without this the very first command a
    # Windows user runs dies in a UnicodeEncodeError from the print statement
    # -- after the runtime has already booted and created an engram. demo.py
    # has carried this same two lines since June.
    with contextlib.suppress(AttributeError, OSError):
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")

_logger = logging.getLogger("pulse_system.serve")

_BANNER = """
  pulse — {n} engram(s), {streams}
  workbench : {web}
  api       : http://{host}:{port}
  profile   : {profile}
  database  : {db}
  harness   : {harness}
  substrate : {substrate} (embeddings / legacy delegation)
"""


def _parse(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="pulse",
        description="Run the pulse runtime and its workbench.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8000)
    p.add_argument(
        "--profile",
        choices=("safe", "workspace", "lab"),
        default="safe",
        help="HTTP capability ceiling (default: %(default)s, read-only)",
    )
    p.add_argument(
        "--origin",
        action="append",
        default=None,
        metavar="ORIGIN",
        help="exact allowed Workbench origin; repeatable, no paths or wildcards",
    )
    p.add_argument(
        "--allow-network-bind",
        action="store_true",
        help="permit a non-loopback --host only when explicit --origin values are also set",
    )
    p.add_argument(
        "--db",
        default=".pulse/run.db",
        help="SQLite file. The same path resumes the same front engram; "
             "':memory:' is allowed but forfeits that (default: %(default)s)",
    )
    p.add_argument("--metrics", default=None,
                   help="event stream JSONL (default: alongside --db)")
    p.add_argument(
        "--provider",
        default="deepseek",
        help="embedding/legacy-delegation provider; also the Pi provider fallback",
    )
    p.add_argument("--model", default=None)
    p.add_argument(
        "--mock",
        action="store_true",
        help="explicit offline legacy/test Harness (production default is Pi)",
    )
    p.add_argument(
        "--no-mock",
        action="store_true",
        help="deprecated no-op; real Pi is already the default",
    )
    p.add_argument("--pi-executable", default="pi",
                   help="Pi executable name or path (default: %(default)s)")
    p.add_argument("--pi-provider", default=None,
                   help="Pi provider override (default: --provider)")
    p.add_argument("--pi-model", default=None,
                   help="Pi model override (default: --model/provider default)")
    p.add_argument(
        "--enable-codex-read-only-sandbox",
        action="store_true",
        help="enable the read-only command adapter; also requires the "
             "explicit executable, live-gate artifact, effective config, and "
             "at least one --harness-command",
    )
    p.add_argument(
        "--codex-sandbox-executable",
        default=None,
        metavar="PATH",
        help="proper standalone Codex CLI used only for the no-model sandbox surface",
    )
    p.add_argument(
        "--codex-sandbox-live-gate",
        default=None,
        metavar="PATH",
        help="short-lived live-gate artifact generated outside the workspace",
    )
    p.add_argument(
        "--codex-sandbox-config",
        default=None,
        metavar="PATH",
        help="effective Codex config bound by the live-gate artifact",
    )
    p.add_argument(
        "--harness-command",
        action="append",
        default=[],
        metavar="EXECUTABLE",
        help="allow one command executable through the approval boundary; repeatable",
    )
    p.add_argument(
        "--enable-harness-pipe-sessions",
        action="store_true",
        help="enable durable non-interactive background PIPE sessions; requires "
             "the verified read-only sandbox and a separate lifecycle gate",
    )
    p.add_argument(
        "--harness-pipe-lifecycle-gate",
        default=None,
        metavar="PATH",
        help="short-lived owner-death containment artifact generated outside the workspace",
    )
    p.add_argument(
        "--harness-pipe-capacity",
        type=int,
        default=8,
        metavar="COUNT",
        help="maximum live PIPE sessions (default: %(default)s)",
    )
    p.add_argument("--turn-timeout", type=float, default=600.0,
                   metavar="SECONDS",
                   help="maximum wall time for one Harness turn (default: %(default)s)")
    p.add_argument(
        "--pulse-workers",
        type=int,
        default=4,
        metavar="COUNT",
        help="simultaneous Engram Harness turns (default: %(default)s)",
    )
    p.add_argument(
        "--pi-resident-sessions",
        type=int,
        default=8,
        metavar="COUNT",
        help="maximum live Pi subprocesses; bindings remain durable "
             "(default: %(default)s)",
    )
    p.add_argument("--with-claustrum", action="store_true",
                   help="attach Tuning / the claustrum rhythm stream (off by "
                        "default to preserve unmodulated behavior)")
    p.add_argument("--with-router", action="store_true",
                   help="attach the tunnel stream's learned router")
    p.add_argument("--tick", type=float, default=0.1, metavar="SECONDS")
    p.add_argument(
        "--lease-ttl",
        type=float,
        default=30.0,
        metavar="SECONDS",
        help="durable Runtime owner lease TTL (default: %(default)s)",
    )
    p.add_argument(
        "--lease-renew",
        type=float,
        default=5.0,
        metavar="SECONDS",
        help="Runtime owner lease heartbeat interval (default: %(default)s)",
    )
    p.add_argument("--no-web", action="store_true",
                   help="API only; do not serve the workbench bundle")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


def _build_api_security(args: argparse.Namespace):
    """Validate the HTTP and tool ceiling before Runtime touches the database."""

    from ..interaction.api.security import (
        ApiSecurityConfigurationError,
        CapabilityProfile,
        LocalApiSecurity,
    )

    security = LocalApiSecurity(
        args.profile,
        allowed_origins=args.origin,
        host=args.host,
        allow_network_bind=args.allow_network_bind,
    )
    codex_options = {
        "--enable-codex-read-only-sandbox": args.enable_codex_read_only_sandbox,
        "--codex-sandbox-executable": args.codex_sandbox_executable is not None,
        "--codex-sandbox-live-gate": args.codex_sandbox_live_gate is not None,
        "--codex-sandbox-config": args.codex_sandbox_config is not None,
        "--harness-command": bool(args.harness_command),
    }
    pipe_options = {
        "--enable-harness-pipe-sessions": args.enable_harness_pipe_sessions,
        "--harness-pipe-lifecycle-gate": args.harness_pipe_lifecycle_gate is not None,
    }
    if security.profile is CapabilityProfile.SAFE:
        requested = [
            option
            for option, enabled in {**codex_options, **pipe_options}.items()
            if enabled
        ]
        if requested:
            raise ApiSecurityConfigurationError(
                "the safe profile cannot configure command or PIPE capabilities: "
                + ", ".join(requested)
            )
    elif security.profile is CapabilityProfile.WORKSPACE:
        requested = [option for option, enabled in pipe_options.items() if enabled]
        if requested:
            raise ApiSecurityConfigurationError(
                "durable PIPE sessions require --profile lab: " + ", ".join(requested)
            )
    return security


def _web_dir() -> Path | None:
    """The built workbench, if someone has built it.

    Absent is a normal state, not a failure: the API is useful on its own and
    building the bundle needs node. Say which it is rather than serving a 404
    that reads like a broken install.
    """
    d = Path(__file__).resolve().parents[3] / "web" / "dist"
    return d if (d / "index.html").exists() else None


def main(argv: list[str] | None = None) -> int:
    args = _parse(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    try:
        import uvicorn
    except ImportError:
        print(
            "uvicorn is not installed, so there is no server to run.\n"
            "  uv sync --extra observatory",
            file=sys.stderr,
        )
        return 2

    from ..interaction.api.app import create_app
    from ..interaction.api.security import ApiSecurityConfigurationError
    from ..agent.harness import HarnessError
    from .runtime import RuntimeAssembly, RuntimeServiceConfig, ServiceError

    if args.no_mock:
        _logger.warning(
            "--no-mock is deprecated and has no effect; Pi is already the default"
        )

    try:
        api_security = _build_api_security(args)
    except ApiSecurityConfigurationError as exc:
        print(
            f"Pulse API security configuration failed: {exc}\n"
            "  remedy: keep --host on loopback, choose an explicit Profile, "
            "and opt into origins/capabilities deliberately",
            file=sys.stderr,
        )
        return 2

    db = Path(args.db)
    if db.name != ":memory:":
        db.parent.mkdir(parents=True, exist_ok=True)
    metrics = Path(args.metrics) if args.metrics else db.with_suffix(".jsonl")
    metrics.parent.mkdir(parents=True, exist_ok=True)

    try:
        outcome = RuntimeAssembly.open(RuntimeServiceConfig(
            db_path=str(db),
            metrics_path=str(metrics),
            mock=args.mock,
            provider=args.provider,
            model=args.model,
            pi_executable=args.pi_executable,
            pi_provider=args.pi_provider,
            pi_model=args.pi_model,
            codex_sandbox_enabled=args.enable_codex_read_only_sandbox,
            codex_sandbox_executable=args.codex_sandbox_executable,
            codex_sandbox_permission_profile=":read-only",
            codex_sandbox_live_gate=args.codex_sandbox_live_gate,
            codex_sandbox_config=args.codex_sandbox_config,
            harness_command_allowlist=tuple(args.harness_command),
            harness_pipe_sessions_enabled=args.enable_harness_pipe_sessions,
            harness_pipe_lifecycle_gate=args.harness_pipe_lifecycle_gate,
            harness_pipe_session_capacity=args.harness_pipe_capacity,
            harness_turn_timeout_sec=args.turn_timeout,
            pulse_worker_capacity=args.pulse_workers,
            pi_resident_session_limit=args.pi_resident_sessions,
            tick_interval=args.tick,
            runtime_lease_ttl_sec=args.lease_ttl,
            runtime_lease_renew_interval_sec=args.lease_renew,
            with_claustrum=args.with_claustrum,
            with_router=args.with_router,
        ))
        service = outcome.raise_for_error()
    except HarnessError as exc:
        print(
            f"Pulse Harness startup failed: {exc.code}\n"
            f"  {exc.detail}\n"
            f"  remedy: {exc.remedy}",
            file=sys.stderr,
        )
        return 2
    except ServiceError as exc:
        print(
            f"Pulse Runtime startup failed: {exc.error}\n"
            f"  {exc.detail}\n"
            f"  remedy: {exc.remedy}",
            file=sys.stderr,
        )
        return 2

    web = None if args.no_web else _web_dir()
    app = create_app(
        metrics,
        db_path=db,
        static_dir=web,
        runtime=service,
        replay_bytes=service.config.metrics_replay_bytes,
        api_security=api_security,
    )

    # Own the runtime's lifetime from the server's, so Ctrl-C stops the tick
    # loop rather than orphaning it.
    @contextlib.asynccontextmanager
    async def lifespan(_app):
        await service.start()
        try:
            yield
        finally:
            await service.stop()
            service.close()

    app.router.lifespan_context = lifespan

    streams = ["脉冲"]
    if args.with_claustrum:
        streams.append("调律")
    if args.with_router:
        streams.append("委派")

    print(_BANNER.format(
        n=len(service.engrams.list_active()) if hasattr(
            service.engrams, "list_active") else "?",
        streams=" + ".join(streams),
        web=str(web) if web else "not built — run `npm install && npm run build` in web/",
        host=args.host, port=args.port,
        profile=(
            f"{api_security.profile.value} (token required for writes)"
            if api_security.token_required
            else "safe (HTTP mutations disabled)"
        ),
        db=db,
        harness=(
            "mock (explicit legacy/test)"
            if args.mock
            else f"Pi via {args.pi_executable}"
        ),
        substrate=f"{args.provider} (mock)" if args.mock else args.provider,
    ))
    continuity = "resumed" if service.resumed else "new"
    print(
        f"  PulseWorld {service.world_id} from {db}\n"
        f"  {continuity} compatibility continuity Engram "
        f"{service.continuity_engram_id}\n"
    )
    if api_security.token_required:
        print(
            "  Workbench startup token (this process only):\n"
            f"  {api_security.access_token}\n"
            "  Send it only as Authorization: Bearer <token>; the browser keeps it "
            "in this tab's sessionStorage.\n"
        )

    uvicorn.run(app, host=args.host, port=args.port,
                log_level="debug" if args.verbose else "warning")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
