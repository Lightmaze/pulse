"""The write side of the v0.1 runtime contract.

One PulseWorld exposes TaskFronts and life ActivityCenters, while three
different information streams reach its Engrams:

- **content** — `POST /engrams/{id}/inject`. The only path by which anything
  a human says enters an engram's session. It goes into the dendrite queue on
  the same road a propagated pulse takes.
- **Tuning** — `GET/POST /tuning`. Rhythm only: when engrams fire, how
  long they wait, what propagates, how hard inhibition gates. Never content.
  `commanded` and `observed` are returned side by side because a pulse is
  asynchronous and a panel showing one number would assert an effect that has
  not landed yet (§2.2).
- **tunnel** — `POST /delegate`, `GET /delegations`. Who does the work. Routing
  is about *whom*, not about *what to say*.

The line between them is the free-context rule and it is not stylistic: sideband data never
enters an engram's LLM context. There is no code path in this module that
composes a string for a model. Tuning writes numbers into config objects;
injection passes the human's text through byte for byte.

Runtime-stream writes answer 202, not 200. What comes back is not a result —
the result happens later, in the pulse — it is an acknowledgement that the
work is queued. Display-identity metadata is the exception: PATCH commits
immediately and answers 200 with the stored identity.

Mounting (this module never touches `app.py`)::

    app.state.runtime = service
    app.include_router(routes_write.router)

    # or, binding the service explicitly:
    app.include_router(create_write_router(service))

Failures follow §6: `{"error", "detail", "remedy"}` at the top level of the
body, never nested under FastAPI's `detail`. That is why the request bodies are
parsed by hand instead of by a pydantic model — a 422 from the framework would
name the field but not the way forward.
"""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from pulse_system.service.runtime import TUNING_KNOBS, RuntimeService, ServiceError

__all__ = ["create_write_router", "router"]


def _refuse(exc: ServiceError) -> JSONResponse:
    return JSONResponse(status_code=exc.status, content=exc.payload())


def _resolve(explicit: RuntimeService | None, request: Request) -> RuntimeService:
    if explicit is not None:
        return explicit
    service = getattr(request.app.state, "runtime", None)
    if service is None:
        raise ServiceError(
            "no_runtime",
            "this app has no RuntimeService attached, so nothing can be "
            "written to",
            "set app.state.runtime = RuntimeService(...) before including "
            "this router, or mount create_write_router(service)",
            status=503,
        )
    return service


async def _body(request: Request) -> dict[str, Any]:
    raw = await request.body()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except ValueError as exc:
        raise ServiceError(
            "malformed_body",
            f"the request body is not valid JSON ({exc})",
            "send a JSON object with Content-Type: application/json",
            status=400,
        ) from None
    if not isinstance(parsed, dict):
        raise ServiceError(
            "malformed_body",
            f"the request body is a {type(parsed).__name__}, not an object",
            "send a JSON object, e.g. {\"content\": \"...\"}",
            status=400,
        )
    return parsed


def _require_str(body: dict, key: str, *, example: str) -> str:
    value = body.get(key)
    if value is None:
        raise ServiceError(
            f"missing_{key}",
            f"the request has no {key!r} field",
            f"send {example}",
            status=400,
        )
    if not isinstance(value, str):
        raise ServiceError(
            f"invalid_{key}",
            f"{key!r} is a {type(value).__name__}, not a string",
            f"send {example}",
            status=400,
        )
    return value


def _optional_str(body: dict, key: str, *, example: str) -> str | None:
    value = body.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ServiceError(
            f"invalid_{key}",
            f"{key!r} is a {type(value).__name__}, not a string or null",
            f"send {example}",
            status=400,
        )
    return value


def _present_str(body: dict, key: str, *, example: str) -> str | None:
    """Return an optional field, but reject explicit null when it is present."""
    if key not in body:
        return None
    value = body[key]
    if not isinstance(value, str):
        raise ServiceError(
            f"invalid_{key}",
            f"{key!r} is a {type(value).__name__}, not a string",
            f"send {example}, or omit {key!r}",
            status=400,
        )
    return value


def _optional_task_subject(body: dict) -> str | None:
    value = body.get("subject_engram_id")
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ServiceError(
            "invalid_task_subject",
            "subject_engram_id must be a non-empty string or null",
            "send an active Engram id, null, or omit subject_engram_id",
            status=400,
        )
    return value


def _present_number(
    body: dict,
    key: str,
    *,
    example: str,
) -> int | float | None:
    if key not in body:
        return None
    value = body[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ServiceError(
            f"invalid_{key}",
            f"{key!r} is a {type(value).__name__}, not a number",
            f"send {example}, or omit {key!r}",
            status=400,
        )
    return value


def _require_positive_int(body: dict, key: str, *, example: str) -> int:
    value = body.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ServiceError(
            f"invalid_{key}",
            f"{key!r} must be a positive integer",
            f"send {example}",
            status=400,
        )
    return value


def _reject_unknown(body: dict, allowed: set[str], noun: str) -> None:
    unknown = sorted(set(body) - allowed)
    if unknown:
        raise ServiceError(
            f"unknown_{noun}_field",
            f"{unknown} is not part of the {noun.replace('_', ' ')} contract",
            f"send only {sorted(allowed)}",
            status=400,
        )


def _require_patch(body: dict, allowed: set[str], noun: str) -> None:
    _reject_unknown(body, allowed, noun)
    if not body:
        raise ServiceError(
            f"empty_{noun}_patch",
            f"the request names no {noun.replace('_', ' ')} field to change",
            f"send at least one of {sorted(allowed)}",
            status=400,
        )


def _purpose_history_limit(raw: str | None) -> int:
    if raw is None:
        return 20
    try:
        return int(raw)
    except (TypeError, ValueError):
        raise ServiceError(
            "living_portfolio_history_limit_invalid",
            "purpose_history_limit must be an integer in [1, 100]",
            "send purpose_history_limit=1..100, or omit it to use 20",
            status=400,
        ) from None


def _role_accountability_limit(raw: str | None) -> int:
    if raw is None:
        return 32
    try:
        value = int(raw)
    except (TypeError, ValueError):
        raise ServiceError(
            "role_accountability_limit_invalid",
            "limit must be an integer in [1, 64]",
            "send limit=1..64, or omit it to use 32",
            status=400,
        ) from None
    if not 1 <= value <= 64:
        raise ServiceError(
            "role_accountability_limit_invalid",
            "limit must be an integer in [1, 64]",
            "send limit=1..64, or omit it to use 32",
            status=400,
        )
    return value


def _purpose_amendment_limit(raw: str | None) -> int:
    if raw is None:
        return 20
    try:
        value = int(raw)
    except (TypeError, ValueError):
        raise ServiceError(
            "purpose_amendment_limit_invalid",
            "limit must be an integer in [1, 100]",
            "send limit=1..100, or omit it to use 20",
            status=400,
        ) from None
    if not 1 <= value <= 100:
        raise ServiceError(
            "purpose_amendment_limit_invalid",
            "limit must be an integer in [1, 100]",
            "send limit=1..100, or omit it to use 20",
            status=400,
        )
    return value


def create_write_router(
    service: RuntimeService | None = None,
    *,
    prefix: str = "",
) -> APIRouter:
    """Build the §2 write router.

    `service` binds one runtime at import time. Left None, the runtime is read
    from `request.app.state.runtime` on each call, so an app can be assembled
    before its runtime exists.
    """
    router = APIRouter(prefix=prefix, tags=["write"])

    # ── PulseWorld, TaskFronts and life ActivityCenters ─────────

    @router.get("/runtime/shutdown")
    async def runtime_shutdown(request: Request) -> JSONResponse:
        """In-memory shutdown evidence, readable after durable Storage closes."""

        runtime = _resolve(service, request)
        return JSONResponse(
            status_code=200,
            content=runtime.shutdown_snapshot(),
        )

    @router.get("/world")
    async def world(request: Request) -> JSONResponse:
        """The one shared world plus its user-facing fronts and life centers."""
        try:
            runtime = _resolve(service, request)
            payload = runtime.snapshot()
            payload["shutdown"] = runtime.shutdown_snapshot()
            payload["task_fronts"] = runtime.list_task_fronts()
            payload["activity_centers"] = runtime.list_activity_centers()
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(status_code=200, content=payload)

    @router.get("/scheduling")
    async def scheduling(request: Request) -> JSONResponse:
        """Owner lease, Center admission, and reservation facts only."""
        try:
            payload = _resolve(service, request).scheduling_snapshot()
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(status_code=200, content=payload)

    @router.get("/task-fronts")
    async def task_fronts(
        request: Request,
        status: str | None = None,
    ) -> JSONResponse:
        try:
            runtime = _resolve(service, request)
            rows = runtime.list_task_fronts(status=status)
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(status_code=200, content={"task_fronts": rows})

    @router.get("/task-fronts/{front_id}")
    async def task_front(front_id: str, request: Request) -> JSONResponse:
        try:
            runtime = _resolve(service, request)
            payload = runtime.get_task_front(front_id)
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(status_code=200, content=payload)

    @router.post("/task-fronts", status_code=201)
    async def create_task_front(request: Request) -> JSONResponse:
        try:
            runtime = _resolve(service, request)
            body = await _body(request)
            _reject_unknown(
                body,
                {"content", "title", "project_id", "subject_engram_id"},
                "task_front",
            )
            content = _require_str(
                body,
                "content",
                example='{"content": "<first natural-language message>"}',
            )
            title = _optional_str(
                body,
                "title",
                example='"title": "<task title>" or null',
            )
            project_id = _optional_str(
                body,
                "project_id",
                example='"project_id": "<existing project id>" or null',
            )
            subject_engram_id = _optional_task_subject(body)
            payload = runtime.create_task_front(
                content,
                title=title,
                project_id=project_id,
                subject_engram_id=subject_engram_id,
            )
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(status_code=201, content=payload)

    @router.post("/task-fronts/{front_id}/messages", status_code=202)
    async def send_task_front_message(
        front_id: str,
        request: Request,
    ) -> JSONResponse:
        try:
            runtime = _resolve(service, request)
            body = await _body(request)
            _reject_unknown(body, {"content"}, "task_front_message")
            content = _require_str(
                body,
                "content",
                example='{"content": "<natural-language message>"}',
            )
            event_id = runtime.send_task_front_message(front_id, content)
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(status_code=202, content={"event_id": event_id})

    @router.patch("/task-fronts/{front_id}")
    async def update_task_front(front_id: str, request: Request) -> JSONResponse:
        try:
            runtime = _resolve(service, request)
            body = await _body(request)
            _require_patch(body, {"title", "status"}, "task_front")
            title = _present_str(
                body,
                "title",
                example='"title": "<task title>"',
            )
            status = _present_str(
                body,
                "status",
                example='"status": "open|closed|archived"',
            )
            updated = runtime.update_task_front(
                front_id,
                title=title,
                status=status,
            )
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(status_code=200, content={"task_front": updated})

    # A subject-bound task begins as an offer.  Only the subject's Pi tool can
    # decide it; this HTTP surface intentionally exposes no accept/refuse
    # endpoint.

    @router.get("/task-offers")
    async def task_offers(
        request: Request,
        subject_engram_id: str | None = None,
        status: str | None = None,
    ) -> JSONResponse:
        try:
            runtime = _resolve(service, request)
            rows = runtime.list_task_offers(
                subject_engram_id=subject_engram_id,
                status=status,
            )
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(status_code=200, content={"task_offers": rows})

    @router.get("/task-offers/{offer_id}")
    async def task_offer(offer_id: str, request: Request) -> JSONResponse:
        try:
            payload = _resolve(service, request).get_task_offer(offer_id)
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(status_code=200, content=payload)

    @router.post("/task-offers", status_code=201)
    async def create_task_offer(request: Request) -> JSONResponse:
        try:
            runtime = _resolve(service, request)
            body = await _body(request)
            _reject_unknown(
                body,
                {"subject_engram_id", "content", "title", "project_id"},
                "task_offer",
            )
            subject_engram_id = _require_str(
                body,
                "subject_engram_id",
                example='{"subject_engram_id": "<active Engram id>"}',
            )
            content = _require_str(
                body,
                "content",
                example='{"content": "<proposed task terms>"}',
            )
            title = _optional_str(
                body,
                "title",
                example='"title": "<task title>" or null',
            )
            project_id = _optional_str(
                body,
                "project_id",
                example='"project_id": "<existing project id>" or null',
            )
            payload = runtime.create_task_offer(
                subject_engram_id,
                content,
                title=title,
                project_id=project_id,
            )
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(status_code=201, content=payload)

    @router.post("/task-offers/{offer_id}/revisions", status_code=201)
    async def revise_task_offer(
        offer_id: str,
        request: Request,
    ) -> JSONResponse:
        try:
            runtime = _resolve(service, request)
            body = await _body(request)
            _reject_unknown(
                body,
                {"expected_revision", "content", "title", "project_id"},
                "task_offer_revision",
            )
            expected_revision = _require_positive_int(
                body,
                "expected_revision",
                example='{"expected_revision": 1}',
            )
            content = _require_str(
                body,
                "content",
                example='{"content": "<revised task terms>"}',
            )
            title = _optional_str(
                body,
                "title",
                example='"title": "<revised task title>" or null',
            )
            project_id = _optional_str(
                body,
                "project_id",
                example='"project_id": "<existing project id>" or null',
            )
            payload = runtime.revise_task_offer(
                offer_id,
                expected_revision=expected_revision,
                content=content,
                title=title,
                project_id=project_id,
            )
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(status_code=201, content=payload)

    @router.post("/task-offers/{offer_id}/remind")
    async def remind_task_offer(
        offer_id: str,
        request: Request,
    ) -> JSONResponse:
        try:
            runtime = _resolve(service, request)
            body = await _body(request)
            _reject_unknown(body, {"expected_revision"}, "task_offer_reminder")
            expected_revision = _require_positive_int(
                body,
                "expected_revision",
                example='{"expected_revision": 1}',
            )
            payload = runtime.remind_task_offer(
                offer_id,
                expected_revision=expected_revision,
            )
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(status_code=200, content=payload)

    @router.post("/task-offers/{offer_id}/withdraw")
    async def withdraw_task_offer(
        offer_id: str,
        request: Request,
    ) -> JSONResponse:
        try:
            runtime = _resolve(service, request)
            body = await _body(request)
            _reject_unknown(body, {"expected_revision"}, "task_offer_withdrawal")
            expected_revision = _require_positive_int(
                body,
                "expected_revision",
                example='{"expected_revision": 1}',
            )
            payload = runtime.withdraw_task_offer(
                offer_id,
                expected_revision=expected_revision,
            )
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(status_code=200, content=payload)

    # Accepted work remains a subject-owned relationship.  HTTP may observe
    # it and propose changed terms, but has no pause/resume/exit mutation.

    @router.get("/task-relationships")
    async def task_relationships(
        request: Request,
        subject_engram_id: str | None = None,
        status: str | None = None,
    ) -> JSONResponse:
        try:
            rows = _resolve(service, request).list_task_relationships(
                subject_engram_id=subject_engram_id,
                status=status,
            )
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(status_code=200, content={"task_relationships": rows})

    @router.get("/task-relationships/{relationship_id}")
    async def task_relationship(
        relationship_id: str,
        request: Request,
    ) -> JSONResponse:
        try:
            payload = _resolve(service, request).get_task_relationship(
                relationship_id
            )
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(status_code=200, content=payload)

    @router.post("/task-relationships/{relationship_id}/terms")
    async def propose_task_relationship_terms(
        relationship_id: str,
        request: Request,
    ) -> JSONResponse:
        try:
            runtime = _resolve(service, request)
            body = await _body(request)
            _reject_unknown(
                body,
                {"expected_revision", "content"},
                "task_relationship_terms",
            )
            expected_revision = _require_positive_int(
                body,
                "expected_revision",
                example='{"expected_revision": 1}',
            )
            content = _require_str(
                body,
                "content",
                example='{"content": "<changed task terms>"}',
            )
            payload = runtime.propose_task_relationship_terms(
                relationship_id,
                expected_revision=expected_revision,
                content=content,
            )
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(status_code=202, content=payload)

    @router.get("/activity-centers")
    async def activity_centers(
        request: Request,
        kind: str | None = None,
        status: str | None = None,
    ) -> JSONResponse:
        try:
            runtime = _resolve(service, request)
            rows = runtime.list_activity_centers(kind=kind, status=status)
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(
            status_code=200,
            content={"activity_centers": rows},
        )

    @router.get("/activity-centers/{center_id}")
    async def activity_center(center_id: str, request: Request) -> JSONResponse:
        try:
            runtime = _resolve(service, request)
            payload = runtime.get_activity_center(center_id)
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(status_code=200, content=payload)

    @router.get("/engrams/{engram_id}/living-portfolio")
    async def living_portfolio(
        engram_id: str,
        request: Request,
        purpose_history_limit: str | None = None,
    ) -> JSONResponse:
        try:
            runtime = _resolve(service, request)
            history_limit = _purpose_history_limit(purpose_history_limit)
            payload = runtime.get_living_portfolio(
                engram_id,
                purpose_history_limit=history_limit,
            )
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(status_code=200, content=payload)

    @router.get("/engrams/{engram_id}/purpose-amendments")
    async def purpose_amendments(
        engram_id: str,
        request: Request,
        limit: str | None = None,
    ) -> JSONResponse:
        try:
            unknown = sorted(set(request.query_params) - {"limit"})
            duplicate_limit = len(request.query_params.getlist("limit")) > 1
            if unknown or duplicate_limit:
                raise ServiceError(
                    "purpose_amendment_query_invalid",
                    (
                        f"unknown purpose-amendment query fields: {unknown}"
                        if unknown
                        else "limit may be supplied at most once"
                    ),
                    "send only the optional limit=1..100 query",
                    status=400,
                )
            runtime = _resolve(service, request)
            payload = runtime.get_purpose_amendment_attempts(
                engram_id,
                limit=_purpose_amendment_limit(limit),
            )
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(status_code=200, content=payload)

    @router.get("/engrams/{engram_id}/role-accountability")
    async def role_accountability(
        engram_id: str,
        request: Request,
        limit: str | None = None,
    ) -> JSONResponse:
        try:
            unknown = sorted(set(request.query_params) - {"limit"})
            duplicate_limit = len(request.query_params.getlist("limit")) > 1
            if unknown or duplicate_limit:
                raise ServiceError(
                    "role_accountability_query_invalid",
                    (
                        f"unknown role-accountability query fields: {unknown}"
                        if unknown
                        else "limit may be supplied at most once"
                    ),
                    "send only the optional limit=1..64 query",
                    status=400,
                )
            runtime = _resolve(service, request)
            payload = runtime.get_role_accountability(
                engram_id,
                limit=_role_accountability_limit(limit),
            )
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(status_code=200, content=payload)

    @router.post("/activity-centers/{center_id}/messages", status_code=202)
    async def send_activity_center_message(
        center_id: str,
        request: Request,
    ) -> JSONResponse:
        try:
            runtime = _resolve(service, request)
            body = await _body(request)
            _reject_unknown(body, {"content"}, "activity_center_message")
            content = _require_str(
                body,
                "content",
                example='{"content": "<natural-language stimulus>"}',
            )
            event_id = runtime.send_activity_center_message(center_id, content)
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(status_code=202, content={"event_id": event_id})

    @router.post("/activity-centers", status_code=201)
    async def create_activity_center(request: Request) -> JSONResponse:
        try:
            runtime = _resolve(service, request)
            body = await _body(request)
            _reject_unknown(
                body,
                {
                    "kind",
                    "title",
                    "description",
                    "origin",
                    "autonomy",
                    "project_id",
                    "stimulus",
                },
                "activity_center",
            )
            kind = _require_str(
                body,
                "kind",
                example='{"kind": "hobby", "title": "<name>"}',
            )
            title = _require_str(
                body,
                "title",
                example='{"kind": "hobby", "title": "<name>"}',
            )
            description = _present_str(
                body,
                "description",
                example='"description": "<what this means>"',
            )
            origin = _present_str(
                body,
                "origin",
                example='"origin": "user|self|shared|system"',
            )
            project_id = _optional_str(
                body,
                "project_id",
                example='"project_id": "<existing project id>" or null',
            )
            stimulus = _optional_str(
                body,
                "stimulus",
                example='"stimulus": "<natural-language stimulus>" or null',
            )
            autonomy = _present_number(
                body,
                "autonomy",
                example='"autonomy": 0.0..1.0',
            )
            payload = runtime.create_activity_center(
                kind,
                title,
                description=description if description is not None else "",
                origin=origin if origin is not None else "user",
                autonomy=autonomy if autonomy is not None else 1.0,
                project_id=project_id,
                stimulus=stimulus,
            )
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(status_code=201, content=payload)

    @router.patch("/activity-centers/{center_id}")
    async def update_activity_center(
        center_id: str,
        request: Request,
    ) -> JSONResponse:
        try:
            runtime = _resolve(service, request)
            body = await _body(request)
            _require_patch(
                body,
                {"title", "description", "status", "autonomy"},
                "activity_center",
            )
            title = _present_str(
                body,
                "title",
                example='"title": "<life-center title>"',
            )
            description = _present_str(
                body,
                "description",
                example='"description": "<what this means>"',
            )
            status = _present_str(
                body,
                "status",
                example='"status": "active|dormant|paused|completed|archived"',
            )
            autonomy = _present_number(
                body,
                "autonomy",
                example='"autonomy": 0.0..1.0',
            )
            updated = runtime.update_activity_center(
                center_id,
                title=title,
                description=description,
                status=status,
                autonomy=autonomy,
            )
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(
            status_code=200,
            content={"activity_center": updated},
        )

    @router.post("/activity-centers/{center_id}/members", status_code=201)
    async def add_center_member(
        center_id: str,
        request: Request,
    ) -> JSONResponse:
        try:
            runtime = _resolve(service, request)
            body = await _body(request)
            _reject_unknown(body, {"engram_id", "relation"}, "center_membership")
            engram_id = _require_str(
                body,
                "engram_id",
                example='{"engram_id": "<existing engram id>"}',
            )
            relation = _present_str(
                body,
                "relation",
                example='"relation": "participant|shared"',
            ) or "participant"
            membership = runtime.add_center_membership(
                center_id,
                engram_id,
                relation,
            )
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(
            status_code=201,
            content={"membership": membership},
        )

    # ── §2.1 content ─────────────────────────────────────────────

    @router.post("/engrams/{engram_id}/inject", status_code=202)
    async def inject(engram_id: str, request: Request) -> JSONResponse:
        """{"content": str, "source": "user"} → 202 {"event_id": str}

        202, not 200: the pulse is asynchronous. What returns is "queued", and
        the engram's answer arrives later through the event stream.
        """
        try:
            runtime = _resolve(service, request)
            body = await _body(request)
            content = _require_str(
                body, "content",
                example='{"content": "<what you want to say>", '
                        '"source": "user"}',
            )
            source = _optional_str(
                body, "source", example='"source": "user"',
            ) or "user"
            if source != "user":
                raise ServiceError(
                    "source_spoof_forbidden",
                    "the public input route cannot claim an internal or control source",
                    'send "source": "user"; adapters use typed internal provenance',
                    status=403,
                )
            event_id = runtime.inject(engram_id, content, source=source)
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(status_code=202, content={"event_id": event_id})

    # ── Display identity ─────────────────────────────────────────

    @router.patch("/engrams/{engram_id}/identity")
    async def update_identity(engram_id: str, request: Request) -> JSONResponse:
        """User-owned name/nickname; the machine signature never changes."""
        try:
            runtime = _resolve(service, request)
            body = await _body(request)
            identity = runtime.update_identity(engram_id, body)
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(status_code=200, content=identity)

    # ── §2.2 Tuning (claustrum rhythm stream) ────────────────────

    @router.get("/tuning")
    async def read_tuning(request: Request) -> JSONResponse:
        """→ {commanded, observed, applied_at_tick}

        The three are separate on purpose. Between the knob moving and the tick
        landing, `commanded` and `observed` disagree — and that disagreement is
        the honest state of the system, not a rendering glitch to be smoothed
        over.
        """
        try:
            runtime = _resolve(service, request)
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(status_code=200, content=runtime.tuning().as_dict())

    @router.post("/tuning", status_code=202)
    async def command_tuning(request: Request) -> JSONResponse:
        """{activity, wait, propagation_threshold, gate} → 202 {commanded,
        will_apply_from_tick}

        A field sent as `null` hands that knob back to the claustrum; a field
        left out is untouched. Takeover and autonomy are per item.
        """
        try:
            runtime = _resolve(service, request)
            body = await _body(request)
            values = {k: body[k] for k in body if k in TUNING_KNOBS}
            unknown = sorted(set(body) - set(TUNING_KNOBS))
            if unknown:
                raise ServiceError(
                    "unknown_tuning_key",
                    f"{unknown} is not a rhythm parameter",
                    "tuning acts on rhythm only — use "
                    f"{list(TUNING_KNOBS)}; to influence content, POST "
                    "/engrams/{id}/inject instead",
                    status=400,
                )
            if not body:
                raise ServiceError(
                    "empty_tuning",
                    "the request names no knob to move",
                    'send at least one of '
                    f'{list(TUNING_KNOBS)}, e.g. {{"activity": 0.05}}; '
                    "use GET /tuning to read the current state",
                    status=400,
                )
            commanded, will_apply = runtime.command_tuning(values)
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(
            status_code=202,
            content={
                "commanded": commanded,
                "will_apply_from_tick": will_apply,
            },
        )

    # ── §2.3 tunnel ──────────────────────────────────────────────

    @router.post("/delegate", status_code=202)
    async def delegate(request: Request) -> JSONResponse:
        """{"task": str, "to": id|null, "caller_id": id, "center_id": id}
        → 202 {"delegation_id": str}

        `to: null` lets the delegation router choose among active members of the same
        Center. When exactly one TaskFront is open, caller/center may be omitted.
        """
        try:
            runtime = _resolve(service, request)
            body = await _body(request)
            task = _require_str(
                body, "task",
                example='{"task": "<what to do>", "to": null, '
                        '"backend": null}',
            )
            to = _optional_str(
                body, "to", example='"to": "<engram_id>" or "to": null',
            )
            backend = _optional_str(
                body, "backend",
                example='"backend": "pi" or null',
            )
            caller_id = _optional_str(
                body,
                "caller_id",
                example='"caller_id": "<TaskFront focal Engram id>"',
            )
            center_id = _optional_str(
                body,
                "center_id",
                example='"center_id": "<TaskFront ActivityCenter id>"',
            )
            body_key = _optional_str(
                body,
                "idempotency_key",
                example='"idempotency_key": "<stable request id>"',
            )
            delegation_id = runtime.delegate(
                task,
                to=to,
                backend=backend,
                caller_id=caller_id,
                center_id=center_id,
                idempotency_key=(
                    request.headers.get("Idempotency-Key") or body_key
                ),
            )
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(
            status_code=202, content={"delegation_id": delegation_id}
        )

    @router.get("/delegations")
    async def delegations(request: Request, limit: int = 50) -> JSONResponse:
        """→ {"delegations": [...]} — records with routing decision and result."""
        try:
            runtime = _resolve(service, request)
            if limit < 1 or limit > 500:
                raise ServiceError(
                    "invalid_limit",
                    f"limit={limit} is outside [1, 500]",
                    "request between 1 and 500 records, e.g. "
                    "GET /delegations?limit=50",
                    status=400,
                )
            records = runtime.delegations(limit=limit)
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(
            status_code=200, content={"delegations": records}
        )

    @router.post("/delegations/{record_id}/outcome")
    async def delegation_outcome(
        record_id: str,
        request: Request,
    ) -> JSONResponse:
        """Record adopted/revised/discarded and update only tunnel learning."""

        try:
            runtime = _resolve(service, request)
            body = await _body(request)
            outcome = _require_str(
                body,
                "outcome",
                example='{"outcome": "adopted"}',
            )
            update = runtime.record_delegation_outcome(record_id, outcome)
        except ServiceError as exc:
            return _refuse(exc)
        return JSONResponse(status_code=200, content=update)

    return router


#: Late-bound router for `app.state.runtime`-style mounting.
router = create_write_router()
