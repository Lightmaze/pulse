"""Interactive CLI: the pulse engine running in the background while you
talk to the front agent — the first real demonstration of the three-flow
architecture (engine / front agent / conversation) on one event loop.

    uv run python -m pulse_system --mock          # no API key needed
    uv run python -m pulse_system                 # real DeepSeek (DEEPSEEK_API_KEY)
    uv run python -m pulse_system --db pulse.db   # persistent network

Commands inside the REPL: /help /status /engrams /inject /quit
Anything else is sent to the front agent.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path

from pulse_system.agent.delegate import Delegator, DelegatorConfig
from pulse_system.agent.front import FrontAgent, FrontAgentConfig
from pulse_system.agent.tools import ToolRegistry, make_tavily_search, real_web_fetch
from pulse_system.core.claustrum import ClaustrumModulator
from pulse_system.core.connection import ConnectionConfig, ConnectionNetwork
from pulse_system.core.delegation import DelegationRouter
from pulse_system.core.dendrite import DendriteConfig, DendriteProcessor
from pulse_system.core.engram import EngramManager
from pulse_system.core.pulse import PulseEngine, PulseEngineConfig
from pulse_system.core.runtime import (
    RuntimeConfig,
    RuntimeManager,
    RuntimePublicationGate,
)
from pulse_system.core.sensory import InteroceptionChannel, SensoryCortex
from pulse_system.core.sleep import SleepEngine
from pulse_system.core.types import EngramStatus, Message, MessageRole
from pulse_system.education.library import Library
from pulse_system.interaction.metrics import MetricsRecorder
from pulse_system.substrate.llm import LLMAdapter, LLMCallError
from pulse_system.substrate.storage import Storage

_FRONT_SEED = (
    "You are the front-stage consciousness of a pulse-based continuous "
    "intelligence system. Other engrams in the network keep thinking in the "
    "background and their thoughts reach you as injected context. Respond "
    "to the human plainly and concretely."
)


@dataclass
class System:
    store: Storage
    llm: LLMAdapter
    mgr: EngramManager
    engine: PulseEngine
    runtime: RuntimeManager
    front: FrontAgent
    metrics: MetricsRecorder
    library: Library
    delegator: Delegator
    sleep: SleepEngine
    publication_gate: RuntimePublicationGate
    router: DelegationRouter | None = None
    claustrum: ClaustrumModulator | None = None


def build_system(args: argparse.Namespace) -> System:
    # The observatory tails files, so it needs file-backed metrics (flushed
    # per event) and a file-backed db for session reading. Chosen defaults
    # land under <workspace>/.pulse/, next to the library. Derived before
    # Storage() so the db path override actually takes effect.
    observatory_on = getattr(args, "observatory", None) is not None
    if observatory_on:
        pulse_dir = (Path(args.workspace) if args.workspace else Path.cwd()) / ".pulse"
        if args.metrics_file is None:
            args.metrics_file = str(pulse_dir / "metrics.jsonl")
        if args.db == ":memory:":
            args.db = str(pulse_dir / "run.db")
            Path(args.db).parent.mkdir(parents=True, exist_ok=True)
            Path(args.db).unlink(missing_ok=True)  # fresh network per launch

    publication_gate = RuntimePublicationGate(f"cli:{os.getpid()}", 1)
    publication_permit = publication_gate.publication_permit

    llm = LLMAdapter(
        provider=args.provider,
        model=args.model,
        max_tokens=args.max_tokens,
        mock=args.mock,
    )
    store = Storage(args.db)
    store.bind_runtime_publication_permit(publication_permit)
    conn_net = ConnectionNetwork(store, ConnectionConfig())

    workspace = Path(args.workspace) if args.workspace else Path.cwd()
    library = Library(
        workspace / ".pulse" / "library",
        publication_authority=publication_permit,
    )
    metrics = MetricsRecorder(
        args.metrics_file,
        flush_every=1 if observatory_on else 200,
        publication_permit=publication_permit,
    )

    mgr = EngramManager(store, llm, conn_net, library=library)
    dendrite = DendriteProcessor(mgr, DendriteConfig())
    runtime = RuntimeManager(RuntimeConfig(
        hourly_token_budget=args.budget,
        daily_token_budget=args.budget * 12,
        cache_read_discount=llm.cache_read_discount,
    ))

    # Learning components are opt-in flags so runs stay comparable to the
    # `baseline` tag; succession listeners keep their slot state coherent.
    router = None
    if args.with_router:
        router = DelegationRouter(store, metrics=metrics)
        mgr.add_succession_listener(router.reassign_engram)
        mgr.add_archive_listener(router.mask_engram)
    claustrum = None
    if args.with_claustrum:
        claustrum = ClaustrumModulator(store, metrics=metrics)
        mgr.add_succession_listener(claustrum.reassign_engram)
        mgr.add_archive_listener(claustrum.mask_engram)

    sensory = None
    if args.with_interoception:
        sensory = SensoryCortex(dendrite)
        mgr.add_succession_listener(sensory.reassign_engram)

    engine = PulseEngine(
        storage=store, engram_manager=mgr, connection_network=conn_net,
        dendrite=dendrite, runtime=runtime, metrics=metrics,
        claustrum=claustrum, sensory=sensory,
        config=PulseEngineConfig(
            tick_interval=args.tick_interval,
            # The network view needs the resting graph; ~10s cadence at the
            # default tick_interval. Off otherwise (baseline byte-parity).
            topology_interval_ticks=20 if observatory_on else None,
        ),
    )

    if sensory is not None:
        intero_engram = mgr.create(initial_messages=[
            Message(role=MessageRole.USER, content=(
                "你是这个系统的内感受。你会持续收到系统自身的状态数值,"
                "请把它们转化为第一人称的自我体验——感到专注、闲散、"
                "疲惫或被什么消耗着。"
            )),
        ])
        sensory.bind(
            intero_engram.id,
            InteroceptionChannel(
                lambda: {**runtime.snapshot(), **metrics.summary()},
                interval_seconds=120.0,
            ),
            wait_time=5.0,
        )

    # .pulse holds the library and db; file_write (and any write-path tool)
    # must not reach it, or a poisoned engram could persist a skill for other
    # engrams to read back. Reads stay allowed (discover_skills/file_read).
    tools = ToolRegistry(
        mock=args.mock,
        workspace_root=workspace,
        protected_roots=[workspace / ".pulse"],
        publication_permit=publication_permit,
    )
    if not args.mock:
        tools.register(
            "web_fetch", "Read the content of a web page", real_web_fetch
        )
        tavily_key = os.environ.get("TAVILY_API_KEY")
        if tavily_key:
            tools.register(
                "web_search", "Search the web for information",
                make_tavily_search(tavily_key),
            )

    front_engram = mgr.create(initial_messages=[
        Message(role=MessageRole.USER, content=_FRONT_SEED),
    ])
    store.update_engram_metadata(front_engram.id, self_excitability=0.5)

    delegator = Delegator(
        store, mgr, tools, library=library, metrics=metrics, router=router,
        config=DelegatorConfig(max_think_iterations=5),
    )
    sleep = SleepEngine(
        store, mgr, conn_net, library,
        delegator=delegator, router=router, metrics=metrics,
    )
    tools.register(
        "delegate",
        "Delegate heavy execution to another engram (@<id> targets an existing one)",
        delegator.as_tool(front_engram.id),
    )
    tools.register(
        "discover_skills",
        "List skills available to this engram",
        library.discover_tool(front_engram.id),
    )

    front = FrontAgent(
        front_engram.id, mgr, tools, FrontAgentConfig(max_think_iterations=5)
    )

    return System(store=store, llm=llm, mgr=mgr, engine=engine,
                  runtime=runtime, front=front, metrics=metrics,
                  library=library, delegator=delegator, sleep=sleep,
                  publication_gate=publication_gate,
                  router=router, claustrum=claustrum)


def _print_status(system: System) -> None:
    engrams = system.store.list_engrams(status=EngramStatus.ACTIVE)
    total_conns = sum(
        len(system.store.get_outgoing(e.id)) for e in engrams
    )
    snap = system.runtime.snapshot()
    llm_stats = system.llm.get_stats()
    print(f"  engrams (active)      : {len(engrams)}")
    print(f"  connections           : {total_conns}")
    print(f"  ticks                 : {system.engine.tick_count}")
    print(f"  pulses                : {snap['total_pulses']}")
    print(f"  billable tokens today : {snap['billable_tokens_today']}"
          f" (remaining {snap['daily_budget_remaining']})")
    print(f"  cache hit rate        : {llm_stats.cache_hit_rate:.1%}"
          f" ({llm_stats.cached_input_tokens} cached tokens)")
    m = system.metrics.summary()
    hb = m.get("heartbeat")
    if hb:
        print(f"  heartbeat (n/N)       : {hb['active']}/{hb['total']}"
              f" = {hb['ratio']:.0%}")
    counts = m.get("event_counts", {})
    if counts:
        top = ", ".join(f"{k}:{v}" for k, v in sorted(counts.items()))
        print(f"  metric events         : {top}")


def _print_engrams(system: System) -> None:
    engrams = system.store.list_engrams(status=EngramStatus.ACTIVE)
    engrams.sort(key=lambda e: e.metadata.recent_activity, reverse=True)
    for e in engrams:
        marker = " (front)" if e.id == system.front.engram_id else ""
        print(f"  [{e.id}] activity={e.metadata.recent_activity:.2f} "
              f"pulses={e.total_pulses} ctx≈{e.metadata.token_count}tok{marker}")


_HELP = """  /status                    network, budget and dynamics overview
  /engrams                   list active engrams by activity
  /inject <id> <txt>         inject an external event into an engram
  /delegations               list delegation records
  /delegate <task>           delegate by hand (tunnel stream; routed when --with-router)
  /outcome <id> <verdict>    record a delegation outcome (adopted/revised/discarded)
  /sleep [cycles]            run a sleep night (NREM consolidation + REM integration)
  /quit                      stop the engine and exit
  anything else              talk to the front agent (say 委派: <task> to make it delegate)"""


def _print_delegations(system: System) -> None:
    records = system.store.list_delegations()
    if not records:
        print("  (no delegations yet)")
        return
    for r in records:
        outcome = r["outcome"] or "-"
        summary = (r["result_summary"] or "")[:60]
        print(f"  [{r['id']}] {r['caller_id']} -> {r['target_id']} "
              f"({r['mode']}, outcome={outcome})")
        print(f"      task: {r['task'][:70]}")
        if summary:
            print(f"      result: {summary}")


async def repl(system: System) -> None:
    stop = asyncio.Event()
    engine_task = asyncio.create_task(system.engine.run(stop))
    print("pulse-system — engine running in background. /help for commands.")

    try:
        while True:
            try:
                line = (await asyncio.to_thread(input, "you> ")).strip()
            except (EOFError, KeyboardInterrupt):
                break
            if not line:
                continue
            if line in ("/quit", "/exit"):
                break
            if line == "/help":
                print(_HELP)
                continue
            if line == "/status":
                _print_status(system)
                continue
            if line == "/engrams":
                _print_engrams(system)
                continue
            if line.startswith("/inject"):
                parts = line.split(maxsplit=2)
                if len(parts) < 3:
                    print("  usage: /inject <engram_id> <text>")
                    continue
                system.engine.inject_external_event(parts[1], parts[2])
                print(f"  injected into {parts[1]}")
                continue
            if line == "/delegations":
                _print_delegations(system)
                continue
            if line.startswith("/delegate"):
                parts = line.split(maxsplit=1)
                if len(parts) < 2:
                    print("  usage: /delegate <task> — tunnel stream by hand: "
                          "routed via the delegation MLP when --with-router, else a "
                          "fresh mainline engram")
                    continue
                try:
                    results = await asyncio.to_thread(
                        system.delegator.delegate_routed,
                        system.front.engram_id, parts[1],
                    )
                    for r in results:
                        print(f"  [{r.record_id}] → {r.target_id} ({r.mode})")
                        print(f"    {r.content[:200]}")
                    if len(results) > 1:
                        print("  (canary pair — judge with /outcome <id> "
                              "adopted|revised|discarded)")
                except (RuntimeError, ValueError) as e:
                    print(f"  {e}")
                continue
            if line.startswith("/outcome"):
                parts = line.split(maxsplit=2)
                if len(parts) < 3:
                    print("  usage: /outcome <delegation_id> adopted|revised|discarded")
                    continue
                try:
                    ok = system.delegator.record_outcome(parts[1], parts[2])
                    print("  recorded" if ok else f"  no delegation {parts[1]}")
                except ValueError as e:
                    print(f"  {e}")
                continue
            if line.startswith("/sleep"):
                parts = line.split()
                cycles = int(parts[1]) if len(parts) > 1 else None
                print("  sleeping (engine keeps ticking at low volume)...")
                report = await asyncio.to_thread(
                    system.sleep.run_night, cycles
                )
                print(f"  night done: {len(report.consolidated)} consolidated, "
                      f"{len(report.deep_reads)} deep reads, "
                      f"{len(report.hubs_spawned)} hubs, "
                      f"{report.router_updates} router updates, "
                      f"{report.errors} errors")
                continue
            if line.startswith("/"):
                print(f"  unknown command: {line.split()[0]} — try /help")
                continue

            try:
                reply = await asyncio.to_thread(
                    system.front.receive_user_message, line
                )
                print(f"front> {reply}")
            except LLMCallError as e:
                print(f"  [LLM error: {e}]")
    finally:
        stop.set()
        await engine_task
        system.metrics.flush()
        system.publication_gate.revoke(reason="cli_shutdown")
        system.publication_gate.wait_for_publication_drain()
        system.store.close()
        print("engine stopped.")


def main(argv: list[str] | None = None) -> int:
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8")
        # piped stdin arrives as UTF-8 bytes but defaults to the console
        # codepage on Windows — CJK input turns into surrogates without this
        sys.stdin.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(
        prog="pulse-system", description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--mock", action="store_true",
                        help="mock LLM and tools, no API key needed")
    parser.add_argument("--provider", default="deepseek",
                        choices=["deepseek", "openai"])
    parser.add_argument("--model", default=None)
    parser.add_argument("--db", default=":memory:",
                        help="sqlite path for a persistent network (default in-memory)")
    parser.add_argument("--workspace", default=None,
                        help="file-tool sandbox root (default cwd); the "
                             "library lives at <workspace>/.pulse/library")
    parser.add_argument("--metrics-file", default=None,
                        help="append dynamics metrics as JSONL to this path")
    parser.add_argument("--with-router", action="store_true",
                        help="attach the delegation MLP (learning component)")
    parser.add_argument("--with-claustrum", action="store_true",
                        help="attach the claustrum modulator (learning component)")
    parser.add_argument("--with-interoception", action="store_true",
                        help="create an interoception engram fed by runtime state")
    parser.add_argument("--observatory", type=int, nargs="?", const=8000,
                        default=None, metavar="PORT",
                        help="serve the workspace GUI + sideband API on PORT "
                             "(default 8000) while the REPL runs")
    parser.add_argument("--budget", type=int, default=100_000,
                        help="hourly billable-token budget")
    parser.add_argument("--max-tokens", type=int, default=1024)
    parser.add_argument("--tick-interval", type=float, default=0.5)
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="-v engine info, -vv debug")
    args = parser.parse_args(argv)

    level = (logging.WARNING, logging.INFO, logging.DEBUG)[min(args.verbose, 2)]
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    system = build_system(args)

    if args.observatory is not None:
        try:
            _start_observatory(args)
        except ImportError:
            print("observatory needs fastapi+uvicorn: "
                  "uv sync  (or: pip install 'pulse-system[observatory]')")
            return 1

    asyncio.run(repl(system))
    return 0


def _start_observatory(args: argparse.Namespace) -> None:
    """Sideband workspace server in a daemon thread.

    Same stance as running it as a separate process: it reads the metrics
    JSONL and the sqlite file, never the engine objects — sharing a process
    here is packaging, not coupling.
    """
    import threading

    import uvicorn

    from pulse_system.interaction.api import create_app

    web_dist = Path(__file__).resolve().parents[2] / "web" / "dist"
    app = create_app(
        args.metrics_file,
        db_path=args.db,
        static_dir=web_dist if web_dist.is_dir() else None,
    )
    server = uvicorn.Server(uvicorn.Config(
        app, host="127.0.0.1", port=args.observatory, log_level="warning",
    ))
    threading.Thread(target=server.run, daemon=True).start()
    url = f"http://127.0.0.1:{args.observatory}"
    if web_dist.is_dir():
        print(f"observatory: {url}/?live=  (workspace GUI + live stream)")
    else:
        print(f"observatory API: {url}  (GUI not built — run: cd web && "
              f"npm install && npm run build)")


if __name__ == "__main__":
    sys.exit(main())
