#!/usr/bin/env bash
# Deterministic acceptance for the product-only open-source release.
# Every public claim must be backed by a command that exits 0. The default
# pipeline strips provider credentials and rejects research/development history
# from the exported tree and its reachable Git graph.
#
# Usage:
#   bash scripts/release_check.sh          # everything
#   bash scripts/release_check.sh nokey    # one check by id
#   bash scripts/release_check.sh nokey demo  # several
#
# Exit 0 only if every requested check passed.

set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
ROOT="$PWD"

PASS=0; FAIL=0; SKIP=0
FAILED_IDS=""

ok()   { printf '  \033[32mPASS\033[0m  %s\n' "$1"; PASS=$((PASS+1)); }
no()   { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; FAIL=$((FAIL+1)); FAILED_IDS="$FAILED_IDS $CURRENT"; }
skip() { printf '  \033[33mSKIP\033[0m  %s\n' "$1"; SKIP=$((SKIP+1)); }
head_() { CURRENT="$1"; printf '\n\033[1m[%s] %s\033[0m\n' "$1" "$2"; }

# Run the suite with every provider key stripped so acceptance never depends on
# credentials from the release operator's environment.
nokey() { env -u DEEPSEEK_API_KEY -u OPENAI_API_KEY -u ANTHROPIC_API_KEY -u TAVILY_API_KEY "$@"; }

# ── secrets ──────────────────────────────────────────────────────
check_secrets() {
  head_ secrets "No credential may enter public history"
  if git log -p --all 2>/dev/null | grep -aqE '(^|[^[:alnum:]_])sk-[a-zA-Z0-9_-]{20,}'; then
    no "a key-shaped string exists in git history"
  else ok "no key-shaped string in any commit"; fi

  if git log --all --diff-filter=A --name-only --format="" 2>/dev/null \
     | grep -qiE '(^|/)\.env$|(^|/)\.env\.(local|dev|development|prod|production|staging|test)$|\.pem$|(^|/)credentials?'; then
    no "a credential file was committed at some point"
  else ok "no credential file ever added"; fi

  if grep -qE '^\.env$' .gitignore 2>/dev/null; then
    ok ".env is gitignored"
  else no ".gitignore lacks a .env pattern"; fi
}

# ── nokey ────────────────────────────────────────────────────────
check_nokey() {
  head_ nokey "The suite must be green for someone who has no API key"
  local out pytest_root
  pytest_root="$(mktemp -d "$ROOT/.pytest-release.XXXXXX")" \
    || { no "could not create a repository-local pytest temp root"; return; }
  out="$(nokey uv run --no-sync pytest -q -p no:cacheprovider \
    --basetemp "$pytest_root/run" 2>&1 | tail -1)"
  rm -rf -- "$pytest_root"
  if printf '%s' "$out" | grep -q 'failed\|error'; then
    no "pytest without a key: $out"
  else ok "pytest without a key: $out"; fi
}

# ── demo ─────────────────────────────────────────────────────────
check_demo() {
  head_ demo "README's first command must work with no key"
  if nokey timeout 300 uv run --no-sync python demo.py >/dev/null 2>&1; then
    ok "demo.py exits 0 with no key"
  else no "demo.py fails with no key"; fi

  # A demo of a connection-learning system that shows no connections is worse
  # than no demo. Assert the STDP table actually differentiates.
  local out
  out="$(nokey timeout 300 uv run --no-sync python demo.py 2>/dev/null | grep -A6 'pass  ')"
  if printf '%s' "$out" | grep -qE '^  4 '; then
    ok "the STDP table reaches pass 4"
  else no "demo prints no STDP weight table"; fi
}

# ── license ──────────────────────────────────────────────────────
check_license() {
  head_ license "Without a licence, publishing grants nobody anything"
  [ -f LICENSE ] && ok "LICENSE present" || no "LICENSE missing"
  [ -f NOTICE ]  && ok "NOTICE present"  || no "NOTICE missing"
  grep -q 'Apache License' LICENSE 2>/dev/null \
    && ok "LICENSE is the Apache-2.0 text" || no "LICENSE is not Apache-2.0"
  grep -q '^license = "Apache-2.0"' pyproject.toml \
    && ok "pyproject declares the licence" || no "pyproject has no licence field"
}

# ── readme ───────────────────────────────────────────────────────
check_readme() {
  head_ readme "The entry document must not assert what is not so"

  # Hardcoded counts rot. This one rotted twice.
  if grep -qE '[0-9]{3} tests (passing|\()' README.md; then
    no "README hardcodes a test count again"
  else ok "no hardcoded test count"; fi

  # Paths that exist on exactly one machine.
  if grep -q '\.\./Docs' README.md; then
    no "README points at ../Docs, which no reader has"
  else ok "no ../Docs reference"; fi

  # Every path drawn in the module map must exist...
  local missing=0 p
  for p in $(grep -oE '(src/pulse_system|core|agent|education|substrate|interaction)/[a-z_]+/[a-z_]+\.py' README.md | sort -u); do
    [ -f "$p" ] || [ -f "src/pulse_system/$p" ] || { echo "      missing: $p"; missing=1; }
  done
  [ "$missing" = 0 ] && ok "every file in the module map exists" \
                     || no "the module map draws files that do not exist"

  local required missing=""
  for required in README.zh-CN.md ESSENCE.md ARCHITECTURE.md PUBLIC_RELEASE.md \
                  docs/CAPABILITIES.md docs/evidence/README.md; do
    grep -q "$required" README.md || missing="$missing $required"
  done
  [ -z "$missing" ] && ok "README links the current product surface" \
                     || no "README omits release files:$missing"

}

# ── build ────────────────────────────────────────────────────────
check_build() {
  head_ build "The published artifact must contain what we think it does"
  rm -rf dist
  if uv build >/dev/null 2>&1; then ok "uv build succeeds"
  else no "uv build fails"; return; fi

  local sdist leaked
  sdist="$(ls dist/*.tar.gz 2>/dev/null | head -1)"
  if [ -z "$sdist" ]; then no "no sdist produced"; return; fi
  leaked="$(tar tzf "$sdist" | grep -E '/(docs|web|\.fgrun)/' | head -3)"
  if [ -n "$leaked" ]; then
    no "sdist ships directories the release excludes:"; printf '      %s\n' $leaked
  else ok "sdist carries no docs/, web/ or run data"; fi
}

# ── providers ────────────────────────────────────────────────────
check_providers() {
  head_ providers "Every declared provider must be usable, and say so when it is not"
  nokey uv run --no-sync python - <<'PY'
import sys
from pulse_system.substrate.llm.adapter import _PROVIDER_PROFILES as PROVIDER_PROFILES, LLMAdapter, LLMCallError

required = {"base_url", "model", "api_key_env"}
bad = []
for name, p in PROVIDER_PROFILES.items():
    miss = required - set(p)
    if miss:
        bad.append(f"{name} lacks {sorted(miss)}")
        continue
    try:
        a = LLMAdapter(mock=True, provider=name)
        a.complete([{"role": "user", "content": "x"}])
    except Exception as e:
        bad.append(f"{name} unusable in mock: {type(e).__name__}: {e}")

    env = p["api_key_env"]
    if not env:
        # A local provider (ollama and friends) needs no key, so refusing
        # would be wrong. It must build instead.
        try:
            LLMAdapter(provider=name)
        except Exception as e:
            bad.append(f"{name} needs no key but will not build: {type(e).__name__}: {e}")
        continue

    # A provider that does need a key must refuse by naming ITS OWN variable --
    # the OpenAI client's default error names OPENAI_API_KEY for everyone,
    # which sends a newcomer to set the wrong thing.
    try:
        LLMAdapter(provider=name, api_key=None)
        bad.append(f"{name} built a client with no key instead of refusing")
    except LLMCallError as e:
        if env not in str(e):
            bad.append(f"{name} refuses without naming {env}")
    except Exception as e:
        bad.append(f"{name} raised {type(e).__name__} instead of LLMCallError")

if len(PROVIDER_PROFILES) < 6:
    bad.append(f"only {len(PROVIDER_PROFILES)} providers; a stranger runs what they already have a key for")

print("\n".join(f"      {b}" for b in bad))
sys.exit(1 if bad else 0)
PY
  # shellcheck disable=SC2181
  [ $? = 0 ] && ok "all providers construct, run in mock, and refuse by name" \
             || no "provider table has gaps (listed above)"
}

# ── ci ───────────────────────────────────────────────────────────
check_ci() {
  head_ ci "CI must run the same no-credential acceptance path"
  local wf=.github/workflows/ci.yml
  [ -f "$wf" ] || { no "no $wf"; return; }
  # Keep parser availability failures distinct from invalid YAML.
  local yamlerr
  yamlerr="$(uv run --no-sync python -c "import sys,yaml;yaml.safe_load(open(sys.argv[1],encoding='utf-8'))" "$wf" 2>&1)"
  if [ -z "$yamlerr" ]; then
    ok "workflow is valid YAML"
  elif printf '%s' "$yamlerr" | grep -q "No module named 'yaml'"; then
    no "cannot check the workflow: pyyaml is not installed (run uv sync)"
  else
    no "workflow is not valid YAML: $(printf '%s' "$yamlerr" | tail -1)"
  fi
  grep -q 'pytest' "$wf" && ok "workflow runs pytest" || no "workflow never runs pytest"
  local key missing_blank=""
  for key in DEEPSEEK_API_KEY OPENAI_API_KEY ANTHROPIC_API_KEY; do
    grep -qE "^  ${key}: \"\"$" "$wf" || missing_blank="$missing_blank $key"
  done
  if [ -n "$missing_blank" ]; then
    no "workflow does not explicitly blank provider keys:$missing_blank"
  elif grep -qiE '\$\{\{[[:space:]]*(secrets|vars)\.[^}]*(API_KEY|TOKEN)' "$wf"; then
    no "workflow imports a provider credential"
  else ok "workflow explicitly runs with blank provider keys"; fi
}

# ── contrib ──────────────────────────────────────────────────────
check_contrib() {
  head_ contrib "A stranger who wants to help must be told how"
  [ -f CONTRIBUTING.md ] && ok "CONTRIBUTING.md present" || no "CONTRIBUTING.md missing"
  [ -f SECURITY.md ]     && ok "SECURITY.md present"     || no "SECURITY.md missing"
  [ -f ESSENCE.md ]      && ok "ESSENCE.md present"      || no "ESSENCE.md missing"
  [ -f ARCHITECTURE.md ] && ok "ARCHITECTURE.md present" || no "ARCHITECTURE.md missing"
  [ -f PUBLIC_RELEASE.md ] && ok "PUBLIC_RELEASE.md present" || no "PUBLIC_RELEASE.md missing"
}

# ── weights ─────────────────────────────────────────────────────
check_weights() {
  head_ weights "Factory and field weights must remain independently operable"
  if uv run --no-sync pytest -q tests/test_weight_layers.py >/dev/null 2>&1; then
    ok "weight-layer export/reset/rollback tests pass"
  else no "weight-layer tests fail"; fi
}

# ── publictree ───────────────────────────────────────────────────
check_publictree() {
  head_ publictree "The first public release must have one clean root commit"
  local OUT="${PUBLIC_TREE:-$ROOT}"
  [ -d "$OUT/.git" ] || { no "public tree is not a Git repository: $OUT"; return; }
  local refs commits parents unexpected_paths unexpected_email origin tag_commit
  refs="$(git -C "$OUT" for-each-ref --format='%(refname)' refs/heads refs/tags | tr '\n' ' ')"
  commits="$(git -C "$OUT" rev-list --count --all)"
  parents="$(git -C "$OUT" cat-file -p HEAD | grep -c '^parent ' || true)"
  unexpected_paths="$(git -C "$OUT" ls-files | while IFS= read -r path; do
    case "$path" in
      .env.example|.gitattributes|.gitignore|.node-version|.nvmrc|.python-version|\
      ARCHITECTURE.md|ARCHITECTURE.zh-CN.md|CHANGELOG.md|CHANGELOG.zh-CN.md|\
      CONTRIBUTING.md|CONTRIBUTING.zh-CN.md|ESSENCE.md|ESSENCE.zh-CN.md|\
      LICENSE|NOTICE|PUBLIC_RELEASE.md|PUBLIC_RELEASE.zh-CN.md|PUBLIC_SOURCE.json|\
      README.md|README.zh-CN.md|SECURITY.md|SECURITY.zh-CN.md|demo.py|pyproject.toml|uv.lock|\
      .github/workflows/ci.yml|\
      docs/CAPABILITIES.md|docs/CAPABILITIES.zh-CN.md|\
      docs/architecture/TERMS.md|docs/architecture/TERMS.zh-CN.md|\
      docs/evidence/README.md|docs/evidence/README.zh-CN.md|docs/evidence/capability-evidence.v1.json|\
      scripts/check_language_boundary.py|scripts/check_ui_language_boundary.cjs|scripts/release_check.sh|\
      src/pulse_system/__init__.py|src/pulse_system/__main__.py|src/pulse_system/cli.py|\
      src/pulse_system/version.py|src/pulse_system/weights/*|\
      src/pulse_system/agent/*|src/pulse_system/core/*|src/pulse_system/education/*|\
      src/pulse_system/habitat/*|src/pulse_system/interaction/*|src/pulse_system/service/*|\
      src/pulse_system/substrate/*|tests/*|web/*) ;;
      *) printf '%s\n' "$path" ;;
    esac
  done)"
  unexpected_email="$(git -C "$OUT" log --all --format='%ae' | grep -v '^maintainers@users\.noreply\.github\.com$' || true)"
  origin="$(git -C "$OUT" remote get-url origin 2>/dev/null || true)"
  tag_commit="$(git -C "$OUT" rev-parse 'v0.2.0-alpha.1^{commit}' 2>/dev/null || true)"
  [ "$refs" = "refs/heads/main refs/tags/v0.2.0-alpha.1 " ] \
    && ok "only main and the first public release tag exist" || no "unexpected refs: $refs"
  [ "$commits" = 1 ] && ok "exactly one public root commit exists" \
                       || no "$commits commits (expected one clean root commit)"
  [ "$parents" = 0 ] && ok "public HEAD has no parent" || no "public HEAD has $parents parent commits"
  [ "$tag_commit" = "$(git -C "$OUT" rev-parse HEAD)" ] \
    && ok "release tag resolves to public HEAD" || no "release tag does not resolve to public HEAD"
  [ -z "$unexpected_paths" ] && ok "every tracked path is on the public allowlist" \
                              || no "unexpected tracked paths: $unexpected_paths"
  [ "$origin" = "https://github.com/Lightmaze/pulse.git" ] \
    && ok "origin is the intended public repository" \
    || no "unexpected public origin: $origin"
  [ -z "$unexpected_email" ] && ok "public history uses only the project noreply identity" \
                               || no "public history contains a private author email"
  if [ -f "$OUT/PUBLIC_SOURCE.json" ]; then
    if python - "$OUT/PUBLIC_SOURCE.json" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1])
d = json.loads(p.read_text(encoding="utf-8"))
assert d["schema_version"] == 2
assert d["release"] == "0.2.0-alpha.1"
assert d["public_history_mode"] == "clean_root"
assert d["source_history_included"] is False
assert "history_preserved_from" not in d
assert isinstance(d["snapshot_file_count"], int) and d["snapshot_file_count"] > 0
assert len(d["snapshot_sha256"]) == 64
assert "source_commit" not in d
assert "source_tree" not in d
PY
    then
      ok "public source provenance is machine-readable"
    else
      no "PUBLIC_SOURCE.json is invalid"
    fi
  else
    no "PUBLIC_SOURCE.json is missing"
  fi
}

# ── claims ───────────────────────────────────────────────────────
check_claims() {
  head_ claims "Published claims stay within the public evidence boundary"
  if uv run --no-sync python - <<'PY'
import json
from pathlib import Path

data = json.loads(Path("docs/evidence/capability-evidence.v1.json").read_text(encoding="utf-8"))
allowed = {"NOT_CLAIMED", "NOT_APPLICABLE"}
for capability in data.get("capabilities", []):
    name = capability.get("capability", "<unnamed>")
    for level in ("e2_live", "e4_longitudinal"):
        status = capability.get(level)
        if status not in allowed:
            raise SystemExit(f"{name}: {level} must remain outside this release ({status!r})")
PY
  then ok "E2 and E4 remain explicitly unclaimed"
  else no "the evidence catalog exceeds the public claim boundary"
  fi

  # Strong agency language is allowed only when the README also states the limit.
  if grep -qiE 'autonom|conscious|self-aware|sentien' README.md README.zh-CN.md; then
    grep -qiE 'does not claim|not give you evidence|open question|not a result|不声称|没有证据' README.md README.zh-CN.md \
      && ok "agency language is accompanied by a clear disclaimer" \
      || no "README uses agency language without a clear disclaimer"
  else ok "no autonomy language"; fi
}

# ── docs ─────────────────────────────────────────────────────────
check_docs() {
  head_ docs "Only current product documentation may enter the public selection"
  local actual expected
  actual="$(uv run --no-sync python - <<'PY'
import subprocess
from pathlib import Path

root = Path.cwd()
tracked = subprocess.check_output(["git", "ls-files", "-z", "docs/"]).split(b"\0")
for raw in tracked:
    if not raw:
        continue
    path = raw.decode("utf-8")
    if path.startswith("docs/"):
        print(path)
PY
)"
  actual="$(printf '%s\n' "$actual" | tr -d '\r' | LC_ALL=C sort)"
  expected="$(printf '%s\n' \
    docs/CAPABILITIES.md \
    docs/CAPABILITIES.zh-CN.md \
    docs/architecture/TERMS.md \
    docs/architecture/TERMS.zh-CN.md \
    docs/evidence/README.md \
    docs/evidence/README.zh-CN.md \
    docs/evidence/capability-evidence.v1.json | LC_ALL=C sort)"
  [ "$actual" = "$expected" ] && ok "public docs are the paired current product documents" \
                                 || no "public docs selection contains unexpected files"
}

# ── language ─────────────────────────────────────────────────────
check_language() {
  head_ language "English and Chinese public surfaces must remain independent"
  if uv run --no-sync python scripts/check_language_boundary.py . \
    --pair README.md=README.zh-CN.md \
    --pair ARCHITECTURE.md=ARCHITECTURE.zh-CN.md \
    --pair ESSENCE.md=ESSENCE.zh-CN.md \
    --pair SECURITY.md=SECURITY.zh-CN.md \
    --pair PUBLIC_RELEASE.md=PUBLIC_RELEASE.zh-CN.md \
    --pair CONTRIBUTING.md=CONTRIBUTING.zh-CN.md \
    --pair CHANGELOG.md=CHANGELOG.zh-CN.md \
    --pair docs/CAPABILITIES.md=docs/CAPABILITIES.zh-CN.md \
    --pair docs/architecture/TERMS.md=docs/architecture/TERMS.zh-CN.md \
    --pair docs/evidence/README.md=docs/evidence/README.zh-CN.md \
    --pair web/README.md=web/README.zh-CN.md \
    --english-resource web/src/locales/en.ts \
    --chinese-resource web/src/locales/zh-CN.ts \
    --chinese-resource web/src/locales/zh-ui.ts \
    --english-resource web/src/workbench/locales/en.ts \
    --chinese-resource web/src/workbench/locales/zh-CN.ts \
    --shared-ui web/src/i18n.tsx \
    --shared-ui web/src/workbench/model.ts \
    --shared-ui web/src/components/Sidebar.tsx \
    --shared-ui web/src/pages/SettingsPage.tsx \
    --shared-ui web/src/App.tsx \
    --allow-term Engram --allow-term Center --allow-term Purpose \
    --allow-term Worker --allow-term Goal --allow-term 'Full Access' \
    --allow-term Pulse --allow-term PulseWorld --allow-term TaskFront \
    --allow-term Harness --allow-term Pi --allow-term Profile \
    --allow-term JSONL --allow-term SQLite --allow-term HTTP --allow-term API \
    --allow-term GET --allow-term MetricsRecorder \
    --allow-term Node.js --allow-term npm --allow-term Python --allow-term uv \
    --allow-term pulse-eval --allow-term Home --allow-term End \
    --allow-term Enter --allow-term Shift+Enter --allow-term v0.1 \
    --allow-term ID --allow-term Life --allow-term PIPE_SESSION \
    --allow-term PTY --allow-term UNCERTAIN; then
    ok "document and locale resource boundaries pass"
  else
    no "language structure check fails"
  fi
  if node scripts/check_ui_language_boundary.cjs .; then
    ok "shared UI contains no localized Chinese copy"
  else
    no "shared UI contains localized Chinese copy"
  fi
}

ALL="secrets nokey demo license readme build providers ci contrib weights publictree claims docs language"
WANT="${*:-$ALL}"

printf '\033[1mRelease acceptance — %s\033[0m\n' "$ROOT"
for c in $WANT; do
  if command -v "check_$c" >/dev/null 2>&1 || declare -F "check_$c" >/dev/null; then
    "check_$c"
  else
    printf '\n  unknown check: %s (have: %s)\n' "$c" "$ALL"; FAIL=$((FAIL+1))
  fi
done

printf '\n\033[1m%d passed, %d failed, %d skipped\033[0m\n' "$PASS" "$FAIL" "$SKIP"
[ "$FAIL" = 0 ] || printf 'failing checks:%s\n' "$(echo "$FAILED_IDS" | tr ' ' '\n' | sort -u | tr '\n' ' ')"
exit $([ "$FAIL" = 0 ] && echo 0 || echo 1)
