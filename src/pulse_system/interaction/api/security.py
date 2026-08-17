"""Process-local capability boundary for the Workbench HTTP API.

Loopback is a network binding choice, not authentication.  This module keeps
the two concerns separate: exact CORS origins constrain browser reads, while a
per-start bearer token and an explicit capability profile constrain mutations.
The token is deliberately absent from representations, logs, responses and
persistent configuration.
"""

from __future__ import annotations

import ipaddress
import logging
import re
import secrets
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Callable, Final, Iterable
from urllib.parse import urlsplit

from starlette.datastructures import Headers
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from pulse_system.version import PUBLIC_VERSION

_logger = logging.getLogger("pulse_system.api.security")

DEFAULT_ALLOWED_ORIGINS: Final[tuple[str, ...]] = (
    "http://127.0.0.1:5173",
    "http://localhost:5173",
)
MUTATING_METHODS: Final[frozenset[str]] = frozenset(
    {"POST", "PUT", "PATCH", "DELETE"}
)
RUNTIME_PROFILE_SCHEMA: Final[str] = "pulse-runtime-profile.v1"
PUBLIC_PRODUCT_VERSION: Final[str] = PUBLIC_VERSION
_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_-]{32,256}\Z")


class CapabilityProfile(StrEnum):
    """HTTP capability ceiling selected by the operator at process start."""

    SAFE = "safe"
    WORKSPACE = "workspace"
    LAB = "lab"

    @property
    def write_enabled(self) -> bool:
        return self is not CapabilityProfile.SAFE


class ApiSecurityConfigurationError(ValueError):
    """Raised before Runtime or database startup when the boundary is unsafe."""


def validate_origin(value: str) -> str:
    """Return one exact HTTP(S) origin or fail closed.

    An origin is only ``scheme://host[:port]``.  Paths, credentials, queries,
    fragments and wildcard values are intentionally rejected rather than
    normalized into a broader policy than the operator wrote.
    """

    if not isinstance(value, str) or value == "" or value != value.strip():
        raise ApiSecurityConfigurationError(
            "origins must be non-empty exact values without surrounding whitespace"
        )
    if value.casefold() == "null" or "*" in value:
        raise ApiSecurityConfigurationError("wildcard and null origins are forbidden")
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ApiSecurityConfigurationError(
            "origins must use http:// or https:// and include a host"
        )
    if parsed.username is not None or parsed.password is not None:
        raise ApiSecurityConfigurationError("origins must not contain user information")
    if parsed.path or parsed.query or parsed.fragment:
        raise ApiSecurityConfigurationError(
            "origins must not contain a path, query, fragment, or trailing slash"
        )
    if parsed.hostname is None:
        raise ApiSecurityConfigurationError("origins must include a valid host")
    try:
        parsed.port
    except ValueError as exc:
        raise ApiSecurityConfigurationError("origin port is invalid") from exc
    return value


def validate_origins(values: Iterable[str]) -> tuple[str, ...]:
    origins: list[str] = []
    seen: set[str] = set()
    for value in values:
        origin = validate_origin(value)
        if origin not in seen:
            origins.append(origin)
            seen.add(origin)
    if not origins:
        raise ApiSecurityConfigurationError("at least one exact origin is required")
    return tuple(origins)


def is_loopback_host(host: str) -> bool:
    if not isinstance(host, str) or host == "" or host != host.strip():
        return False
    candidate = host[1:-1] if host.startswith("[") and host.endswith("]") else host
    if candidate.casefold() == "localhost":
        return True
    try:
        return ipaddress.ip_address(candidate).is_loopback
    except ValueError:
        return False


def validate_network_bind(
    host: str,
    *,
    allow_network_bind: bool,
    explicit_origins: bool,
) -> bool:
    """Validate the double opt-in and return whether the host is loopback."""

    if not isinstance(host, str) or host == "" or host != host.strip():
        raise ApiSecurityConfigurationError("host must be a non-empty value")
    loopback = is_loopback_host(host)
    if not loopback and not (allow_network_bind and explicit_origins):
        raise ApiSecurityConfigurationError(
            "non-loopback binding requires --allow-network-bind and at least one "
            "explicit --origin"
        )
    return loopback


@dataclass(frozen=True, slots=True, init=False)
class LocalApiSecurity:
    """Validated, process-local API policy.

    ``allowed_origins=None`` means the fixed development origins.  Supplying
    any tuple is an explicit origin choice, which is mandatory together with
    ``allow_network_bind`` for a non-loopback host.
    """

    profile: CapabilityProfile
    access_token: str = field(repr=False)
    allowed_origins: tuple[str, ...]
    host: str
    allow_network_bind: bool
    loopback_only: bool

    def __init__(
        self,
        profile: CapabilityProfile | str = CapabilityProfile.SAFE,
        *,
        access_token: str | None = None,
        allowed_origins: Iterable[str] | None = None,
        host: str = "127.0.0.1",
        allow_network_bind: bool = False,
    ) -> None:
        try:
            parsed_profile = CapabilityProfile(profile)
        except (TypeError, ValueError) as exc:
            raise ApiSecurityConfigurationError(
                "profile must be safe, workspace, or lab"
            ) from exc
        if type(allow_network_bind) is not bool:
            raise ApiSecurityConfigurationError("allow_network_bind must be a bool")
        explicit_origins = allowed_origins is not None
        origins = validate_origins(
            DEFAULT_ALLOWED_ORIGINS if allowed_origins is None else allowed_origins
        )
        loopback = validate_network_bind(
            host,
            allow_network_bind=allow_network_bind,
            explicit_origins=explicit_origins,
        )
        token = secrets.token_urlsafe(32) if access_token is None else access_token
        if not isinstance(token, str) or _TOKEN_PATTERN.fullmatch(token) is None:
            raise ApiSecurityConfigurationError(
                "access_token must be a 32–256 character URL-safe secret"
            )

        object.__setattr__(self, "profile", parsed_profile)
        object.__setattr__(self, "access_token", token)
        object.__setattr__(self, "allowed_origins", origins)
        object.__setattr__(self, "host", host)
        object.__setattr__(self, "allow_network_bind", allow_network_bind)
        object.__setattr__(self, "loopback_only", loopback)

    @property
    def write_enabled(self) -> bool:
        return self.profile.write_enabled

    @property
    def token_required(self) -> bool:
        return self.profile.write_enabled

    def accepts_authorization(self, value: str | None) -> bool:
        if value is None:
            return False
        scheme, separator, credential = value.partition(" ")
        if separator == "" or scheme.casefold() != "bearer" or " " in credential:
            return False
        return secrets.compare_digest(credential, self.access_token)

    def public_projection(self) -> dict[str, object]:
        return {
            "schema_version": RUNTIME_PROFILE_SCHEMA,
            "product_version": PUBLIC_PRODUCT_VERSION,
            "profile": self.profile.value,
            "write_enabled": self.write_enabled,
            "token_required": self.token_required,
            "loopback_only": self.loopback_only,
        }


def _fault(status: int, error: str, detail: str, remedy: str) -> JSONResponse:
    headers = {"Cache-Control": "no-store"}
    if status == 401:
        headers["WWW-Authenticate"] = "Bearer"
    return JSONResponse(
        status_code=status,
        content={"error": error, "detail": detail, "remedy": remedy},
        headers=headers,
    )


class ApiSecurityMiddleware:
    """Pure ASGI middleware so SSE streaming semantics remain untouched."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        security: LocalApiSecurity,
        route_template: Callable[[Scope], str] | None = None,
    ) -> None:
        self.app = app
        self.security = security
        self.route_template = route_template or (lambda _scope: "unmatched")

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        method = str(scope.get("method", "GET")).upper()
        if method not in MUTATING_METHODS:
            await self.app(scope, receive, send)
            return

        template = self.route_template(scope)

        if not self.security.write_enabled:
            _logger.warning(
                "api_mutation_rejected profile=%s method=%s route_template=%s error=%s",
                self.security.profile.value,
                method,
                template,
                "profile_write_denied",
            )
            response = _fault(
                403,
                "profile_write_denied",
                "the safe profile does not permit HTTP state changes",
                "restart with --profile workspace or --profile lab when writes are intended",
            )
            await response(scope, receive, send)
            return

        authorization = Headers(scope=scope).get("authorization")
        if not self.security.accepts_authorization(authorization):
            _logger.warning(
                "api_mutation_rejected profile=%s method=%s route_template=%s error=%s",
                self.security.profile.value,
                method,
                template,
                "api_token_invalid",
            )
            response = _fault(
                401,
                "api_token_invalid",
                "a valid bearer token from this process start is required",
                "enter the startup token in this tab's Workbench security control",
            )
            await response(scope, receive, send)
            return

        await self.app(scope, receive, send)
