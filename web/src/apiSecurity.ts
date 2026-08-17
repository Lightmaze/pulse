export type CapabilityProfile = "safe" | "workspace" | "lab";

export interface RuntimeProfile {
  schema_version: "pulse-runtime-profile.v1";
  product_version: string;
  profile: CapabilityProfile;
  write_enabled: boolean;
  token_required: boolean;
  loopback_only: boolean;
}

export const API_TOKEN_SESSION_KEY = "pulse.api.startup-token:v1";
const MUTATING_METHODS = new Set(["POST", "PUT", "PATCH", "DELETE"]);
const TOKEN_PATTERN = /^[A-Za-z0-9_-]{32,256}$/;

function record(value: unknown): Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function session(): Storage | null {
  if (typeof window === "undefined") return null;
  try {
    return window.sessionStorage;
  } catch {
    return null;
  }
}

export function isApiTokenShape(value: string): boolean {
  return TOKEN_PATTERN.test(value);
}

export function readApiToken(): string | null {
  try {
    const value = session()?.getItem(API_TOKEN_SESSION_KEY) ?? null;
    return value !== null && isApiTokenShape(value) ? value : null;
  } catch {
    return null;
  }
}

export function setApiToken(value: string): void {
  if (!isApiTokenShape(value)) {
    throw new Error("The startup token is not a valid URL-safe token.");
  }
  const storage = session();
  if (storage === null) {
    throw new Error("Session storage is unavailable in this browser context.");
  }
  storage.setItem(API_TOKEN_SESSION_KEY, value);
}

export function clearApiToken(): void {
  try {
    session()?.removeItem(API_TOKEN_SESSION_KEY);
  } catch {
    // A hardened browser may revoke storage while the page is open.
  }
}

function requestMethod(init: RequestInit): string {
  return (init.method ?? "GET").toUpperCase();
}

export function withApiAuthorization(init: RequestInit = {}): RequestInit {
  if (!MUTATING_METHODS.has(requestMethod(init))) return init;
  const headers = new Headers(init.headers);
  const token = readApiToken();
  if (token === null) headers.delete("Authorization");
  else headers.set("Authorization", `Bearer ${token}`);
  return { ...init, headers };
}

export function apiFetch(
  input: RequestInfo | URL,
  init: RequestInit = {},
): Promise<Response> {
  return fetch(input, withApiAuthorization(init));
}

export function parseRuntimeProfile(value: unknown): RuntimeProfile | null {
  const row = record(value);
  const profile = row.profile;
  if (
    row.schema_version !== "pulse-runtime-profile.v1" ||
    typeof row.product_version !== "string" ||
    row.product_version === "" ||
    (profile !== "safe" && profile !== "workspace" && profile !== "lab") ||
    typeof row.write_enabled !== "boolean" ||
    typeof row.token_required !== "boolean" ||
    typeof row.loopback_only !== "boolean"
  ) {
    return null;
  }
  const writesExpected = profile !== "safe";
  if (row.write_enabled !== writesExpected || row.token_required !== writesExpected) {
    return null;
  }
  return {
    schema_version: "pulse-runtime-profile.v1",
    product_version: row.product_version,
    profile,
    write_enabled: row.write_enabled,
    token_required: row.token_required,
    loopback_only: row.loopback_only,
  };
}

function joinUrl(base: string, path: string): string {
  return `${base.replace(/\/+$/, "")}${path}`;
}

export async function fetchRuntimeProfile(
  base: string,
  signal?: AbortSignal,
): Promise<RuntimeProfile> {
  const response = await apiFetch(joinUrl(base, "/runtime-profile"), { signal });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  const profile = parseRuntimeProfile(await response.json());
  if (profile === null) throw new Error("The runtime returned an invalid Profile projection.");
  return profile;
}
