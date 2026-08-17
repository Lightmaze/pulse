"""Pulse Continuous Intelligence System — End-to-End Demo.

Simulates a "micro-life" of the system: import conversations, build
connections, run the pulse engine, think with the front agent, converse
through a clone session.

Run:  uv run python demo.py
"""

import os
import sys
import tempfile
import textwrap
from datetime import datetime, timedelta, timezone

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from pulse_system.agent.clone import CloneManager
from pulse_system.agent.front import FrontAgent, FrontAgentConfig
from pulse_system.agent.tools import ToolRegistry
from pulse_system.core.connection import ConnectionConfig, ConnectionNetwork
from pulse_system.core.dendrite import DendriteConfig, DendriteProcessor
from pulse_system.core.engram import EngramManager
from pulse_system.core.pulse import PulseEngine, PulseEngineConfig
from pulse_system.core.runtime import RuntimeConfig, RuntimeManager
from pulse_system.core.types import EngramStatus, Message, MessageRole
from pulse_system.education.ingest import ConversationImporter
from pulse_system.education.index import IndexManager
from pulse_system.education.project import ProjectManager
from pulse_system.substrate.llm import LLMAdapter
from pulse_system.substrate.storage import Storage


def banner(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}\n")


def section(title: str) -> None:
    print(f"\n--- {title} ---\n")


def main():
    banner("Pulse Continuous Intelligence System — Demo")

    # ─── 1. Initialize ──────────────────────────────────────────
    section("1. System Initialization")

    store = Storage(":memory:")
    llm = LLMAdapter(mock=True)
    conn_config = ConnectionConfig(
        stdp_strength=0.1,
        embedding_threshold=0.2,
    )
    conn_net = ConnectionNetwork(store, conn_config)
    mgr = EngramManager(store, llm, conn_net)
    dendrite = DendriteProcessor(mgr, DendriteConfig(
        silence_threshold=0.0,
        default_max_wait=0.0,
    ))
    runtime = RuntimeManager(RuntimeConfig(
        budget_per_tick=10,
        hourly_token_budget=1_000_000,
        daily_token_budget=10_000_000,
    ))
    engine = PulseEngine(
        storage=store, engram_manager=mgr, connection_network=conn_net,
        dendrite=dendrite, runtime=runtime,
        config=PulseEngineConfig(
            propagation_threshold=0.2,
            budget_per_tick=10,
            spontaneous_check_interval=1000.0,
            decay_interval=1000.0,
        ),
    )
    tools = ToolRegistry(mock=True)
    pm = ProjectManager(store)
    idx_mgr = IndexManager(mgr, store)
    importer = ConversationImporter(mgr, conn_net, llm)
    clone_mgr = CloneManager(store, llm)

    print("All managers initialized (mock LLM mode).")

    # ─── 2. Import Phase ────────────────────────────────────────
    section("2. Import Phase")

    project = pm.create("pulse-computing", "Pulse Computing Research Project")
    print(f"Project created: {project.name} (id={project.id})")

    conversations = {
        "pulse_neural.md": textwrap.dedent("""\
            ## User
            What are pulse neural networks?

            ## Assistant
            Pulse neural networks are biologically inspired computing models where information is encoded in the timing and frequency of discrete pulses (spikes), rather than continuous activation values. Each neuron fires when its membrane potential crosses a threshold, producing a spike that propagates to connected neurons. The timing relationships between spikes carry information — this is the basis of spike-timing-dependent plasticity (STDP).

            ## User
            How does STDP work in these networks?

            ## Assistant
            STDP is a learning rule where the strength of a synaptic connection changes based on the relative timing of pre-synaptic and post-synaptic spikes. If neuron A fires just before neuron B, the connection A→B is strengthened (Long-Term Potentiation). If A fires after B, the connection is weakened (Long-Term Depression). The magnitude of change decreases exponentially with the time difference. This creates a natural mechanism for learning causal relationships.
        """),
        "llm_context.md": textwrap.dedent("""\
            ## User
            What is context engineering for LLMs?

            ## Assistant
            Context engineering is the practice of carefully managing what information enters an LLM's context window. Since LLMs have finite context windows and their outputs are heavily influenced by the content and ordering of their input, engineering this context is crucial. Key techniques include: prefix caching (keeping stable prefixes for efficiency), append-only session management (never modifying earlier messages), and natural-language instruction injection rather than structured templates.

            ## User
            Why is prefix caching important?

            ## Assistant
            LLM APIs can cache the computational results of processing a message prefix. If your session history is append-only — you never modify earlier messages, only add new ones — then each subsequent API call can reuse the cached prefix computation. This dramatically reduces latency and cost. Breaking the prefix (by editing an earlier message) forces a full recomputation.
        """),
        "distributed_arch.md": textwrap.dedent("""\
            ## User
            How do distributed systems handle state synchronization?

            ## Assistant
            Distributed systems use several approaches to synchronize state: consensus protocols (like Raft or Paxos) for strong consistency, eventual consistency models with conflict resolution (CRDTs, vector clocks), and event sourcing where state changes are captured as an immutable log of events. The choice depends on the consistency vs. availability trade-off described by the CAP theorem.

            ## User
            What is event sourcing?

            ## Assistant
            Event sourcing stores all changes to application state as a sequence of events. Instead of storing the current state, you store the history of state transitions. The current state can be reconstructed by replaying events. This approach provides a complete audit trail, enables temporal queries, and naturally supports event-driven architectures. It maps well to append-only data models.
        """),
    }

    tmpdir = tempfile.mkdtemp()
    for name, content in conversations.items():
        with open(os.path.join(tmpdir, name), "w", encoding="utf-8") as f:
            f.write(content)

    engrams = importer.import_directory(tmpdir, "markdown", project_id=project.id)
    print(f"Imported {len(engrams)} engrams:")
    for e in engrams:
        session = mgr.get_session(e.id)
        first_msg = session[0].content[:60] if session else "(empty)"
        print(f"  [{e.id}] {len(session)} messages — \"{first_msg}...\"")

    conn_count = importer.post_import_initialize([e.id for e in engrams])
    print(f"\nInitial connections from embeddings: {conn_count}")

    if conn_count == 0:
        # Say why, rather than letting a zero stand there looking like a bug.
        # Mock embeddings are hashes of the text: they carry no semantics, so
        # the pairwise cosines here run about -0.004 to +0.12, all under the
        # 0.2 threshold. Lowering the threshold would manufacture edges out of
        # hash noise — a network that looks connected and means nothing. The
        # honest demo builds its topology the way the system actually claims
        # to: from firing order. See section 4.
        print("  (mock embeddings are hashes, not meaning — every pair falls")
        print("   under the 0.2 threshold. Real embeddings connect here;")
        print("   in mock mode the network is built by STDP instead.)")

    boosted = pm.boost_intra_connections(project.id, 1.5)
    print(f"Intra-cluster boost applied to {boosted} connections")

    # ─── 3. Index Creation ──────────────────────────────────────
    section("3. Index Creation")

    index_id = idx_mgr.create_index(
        project_id=project.id,
        title="脉冲计算系统设计",
        structure=(
            "1. 理论基础 — 脉冲神经网络、STDP、生物启发计算\n"
            "2. 上下文工程 — LLM prefix cache、append-only session\n"
            "3. 分布式架构 — 事件溯源、最终一致性\n"
            "4. 系统实现 — engram 生命周期、脉冲引擎、前台意识"
        ),
        commitment="掌握从理论到实现的完整路径，构建真正能持续思考的智能系统。",
    )

    index_session = mgr.get_session(index_id)
    print(f"Index engram created: {index_id}")
    print(f"Initial content:\n{index_session[0].content[:200]}...")

    # ─── 4. Pulse Engine ────────────────────────────────────────
    section("4. Pulse Engine — 10 Ticks")

    target = engrams[0]
    engine.inject_external_event(
        target.id,
        "用户问：脉冲计算跟传统 agent 有什么区别？"
    )
    print(f"External event injected into engram {target.id}")

    for tick in range(10):
        results = engine.tick()
        if results:
            for eid, output in results:
                tag = "*" if eid == target.id else " "
                print(f"  tick {tick + 1}: [{tag}] {eid} pulsed → \"{output[:50]}...\"")

                targets = conn_net.get_propagation_targets(eid, 0.2)
                if targets:
                    target_ids = [c.to_id for c in targets]
                    print(f"           propagated to: {target_ids}")

    # ─── 4b. STDP ───────────────────────────────────────────────
    section("4b. STDP — the topology comes from firing order")

    # The distinctive claim of this system is that connections are not
    # configured, they are a residue of when things fired. Mock embeddings
    # cannot show that (section 2), so show it directly: fire the three
    # imported engrams in a known order with known gaps and read the edges
    # back out.
    a, b, c = (e.id for e in engrams)
    pairs = ((a, b, "A→B", 1), (b, c, "B→C", 4), (a, c, "A→C", 5))

    def edge(src, dst):
        hit = [x for x in conn_net.get_propagation_targets(src, 0.0)
               if x.to_id == dst]
        return hit[0].weight if hit else None

    def fire(n):
        """One pass of A → B → C, with B 1s after A and C 5s after A."""
        t = datetime.now(timezone.utc) + timedelta(seconds=n * 60)
        conn_net.stdp_update([
            (a, t), (b, t + timedelta(seconds=1)), (c, t + timedelta(seconds=5)),
        ])

    print("Fire A → B → C repeatedly. B lands 1s after A; C lands 5s after A.")
    print("Nothing below is configured — the numbers are a residue of timing.\n")
    print(f"  {'pass':<6}{'A→B (Δ1s)':>12}{'B→C (Δ4s)':>12}{'A→C (Δ5s)':>12}")

    for n in range(1, 5):
        fire(n)
        row = "".join(f"{edge(s, d):>12.4f}" for s, d, _, _ in pairs)
        print(f"  {n:<6}{row}")

    # Pass 1 gives every new edge the same weight: a first co-firing records
    # *that* two engrams fired together, not how tightly. The timing term only
    # enters when an existing edge is reinforced — which is why the columns are
    # equal on line 1 and fan out afterwards.
    print("\n  Pass 1 is flat: a first co-firing records that two engrams fired")
    print("  together, not how tightly. Timing enters on reinforcement — so the")
    print("  tightest pair (A→B) pulls ahead of the loosest (A→C) from pass 2 on.")

    # Anti-causal firing: the edge that disagrees with the new order pays.
    before = edge(a, b)
    t1 = datetime.now(timezone.utc) + timedelta(seconds=600)
    conn_net.stdp_update([(b, t1), (a, t1 + timedelta(seconds=1))])
    print(f"\nNow fire it backwards, B → A:")
    print(f"  A→B  {before:.4f} → {edge(a, b):.4f}   (LTD — the anti-causal edge pays)")

    # A topology means propagation has somewhere to go. The threshold is 0.2:
    # whichever pathway the firing order actually reinforced is the one that
    # became a route.
    thr = 0.2
    routes = conn_net.get_propagation_targets(a, thr)
    print(f"\nAt propagation threshold {thr}, A routes to {len(routes)} engram(s): "
          f"{[t.to_id[:8] for t in routes] or '—'}")
    for s, d, label, _ in pairs:
        w = edge(s, d)
        # Only A's own outgoing edges are routes *from A*; B→C is above the
        # threshold too, but it is a route out of B.
        mark = "   ← a route from A" if (w >= thr and s == a) else ""
        print(f"  {label} {w:.4f}  {'≥' if w >= thr else '<'} {thr}{mark}")

    # ─── 5. Front Agent ────────────────────────────────────────
    section("5. Front Agent — Think")

    front_engram = mgr.create(initial_messages=[
        Message(role=MessageRole.USER, content="你是脉冲计算系统的前台意识。你可以访问系统中所有 engram 的知识。"),
    ])
    store.update_engram_metadata(front_engram.id, self_excitability=0.5)
    front = FrontAgent(front_engram.id, mgr, tools, FrontAgentConfig(max_think_iterations=3))

    response = front.receive_user_message("帮我总结脉冲计算的核心公理")
    print(f"Front agent response:\n  {response}")

    status = front.get_status()
    print(f"\nStatus: {status}")

    # ─── 6. Clone Session ───────────────────────────────────────
    section("6. Clone Session — Conversation with Engram Steering")

    clone_session = clone_mgr.create_session(topic="脉冲计算讨论")
    print(f"Clone session created: {clone_session.id} (topic: {clone_session.topic})")

    user_messages = [
        "脉冲计算的核心思想是什么？",
        "STDP learning 在系统中扮演什么角色？",
        "这跟传统的 event sourcing 有什么关系？",
    ]

    for msg in user_messages:
        print(f"\n  [User] {msg}")
        response = clone_session.process_message(msg)
        print(f"  [Clone] {response[:100]}...")

        active = clone_session.get_active_engrams()
        if active:
            print(f"  [Active engrams] {active}")

    history = clone_session.get_history()
    engram_contributions = [m for m in history if m.role == "engram"]
    print(f"\nTotal engram contributions in conversation: {len(engram_contributions)}")

    summary = clone_session.finalize()
    print(f"Session summary: {summary['message_count']} messages, "
          f"{summary['engrams_activated']} engram activations")

    # ─── 7. System Status ───────────────────────────────────────
    section("7. System Status")

    all_engrams = store.list_engrams(status=EngramStatus.ACTIVE)
    print(f"Total active engrams: {len(all_engrams)}")
    print()

    sorted_engrams = sorted(all_engrams, key=lambda e: e.metadata.recent_activity, reverse=True)
    print("Activity ranking:")
    for i, e in enumerate(sorted_engrams[:10], 1):
        label = f"project={e.project_id}" if e.project_id else "no project"
        print(f"  {i}. [{e.id}] activity={e.metadata.recent_activity:.2f} "
              f"pulses={e.total_pulses} tokens={e.metadata.token_count} ({label})")

    total_connections = 0
    total_weight = 0.0
    for e in all_engrams:
        outgoing = store.get_outgoing(e.id)
        total_connections += len(outgoing)
        total_weight += sum(c.weight for c in outgoing)

    avg_weight = total_weight / total_connections if total_connections > 0 else 0
    print(f"\nConnection network:")
    print(f"  Total edges: {total_connections}")
    print(f"  Average weight: {avg_weight:.4f}")

    recently_active = sum(
        1 for e in all_engrams if e.last_pulse_at is not None
    )
    total = len(all_engrams)
    heartbeat = recently_active / total if total > 0 else 0
    print(f"\nHeartbeat: {recently_active}/{total} = {heartbeat:.2%}")

    runtime_stats = runtime.get_stats()
    print(f"\nRuntime stats:")
    print(f"  Total pulses: {runtime_stats.total_pulses}")
    print(f"  Tokens this hour: {runtime_stats.tokens_this_hour}")
    print(f"  Tokens today: {runtime_stats.tokens_today}")

    llm_stats = llm.get_stats()
    print(f"\nLLM stats:")
    print(f"  Total calls: {llm_stats.total_calls}")
    print(f"  Cache hit rate: {llm_stats.cache_hit_rate:.1%}")

    store.close()

    banner("Demo Complete")
    print("Intelligence is not a response to the world,")
    print("but a life lived within it.\n")


if __name__ == "__main__":
    main()
