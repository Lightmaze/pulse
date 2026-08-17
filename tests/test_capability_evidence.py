from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
EVIDENCE_PATH = ROOT / "docs" / "evidence" / "capability-evidence.v1.json"
STATUSES = {
    "VERIFIED",
    "FAILED",
    "BLOCKED_BY_ENVIRONMENT",
    "NOT_CLAIMED",
    "NOT_APPLICABLE",
}
LEVELS = (
    "e0_exists",
    "e1_contract",
    "e2_live",
    "e3_release",
    "e4_longitudinal",
)


def _document() -> dict:
    return json.loads(EVIDENCE_PATH.read_text(encoding="utf-8"))


def test_capability_evidence_has_exact_schema_and_unique_ids() -> None:
    document = _document()

    assert document["schema_version"] == "capability-evidence.v1"
    assert set(document["statuses"]) == STATUSES
    capabilities = document["capabilities"]
    ids = [item["capability"] for item in capabilities]
    assert ids == sorted(ids) or len(ids) == len(set(ids))
    assert len(ids) == len(set(ids))

    exact_keys = {"capability", *LEVELS, "claim", "evidence"}
    for item in capabilities:
        assert set(item) == exact_keys
        assert item["claim"] in {"internal", "experimental", "alpha"}
        assert all(item[level] in STATUSES for level in LEVELS)


def test_capability_evidence_links_are_relative_existing_files() -> None:
    for item in _document()["capabilities"]:
        assert item["evidence"], item["capability"]
        for raw in item["evidence"]:
            path = Path(raw)
            assert not path.is_absolute(), raw
            assert ".." not in path.parts, raw
            assert (ROOT / path).is_file(), raw


def test_blocked_or_unclaimed_live_evidence_does_not_claim_release() -> None:
    for item in _document()["capabilities"]:
        if item["e2_live"] in {"FAILED", "BLOCKED_BY_ENVIRONMENT"}:
            assert item["e3_release"] != "VERIFIED", item["capability"]
