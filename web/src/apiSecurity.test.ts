// @ts-nocheck
import assert from "node:assert/strict";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { createServer } from "vite";

class MemoryStorage {
  values = new Map();
  get length() { return this.values.size; }
  clear() { this.values.clear(); }
  getItem(key) { return this.values.get(key) ?? null; }
  key(index) { return [...this.values.keys()][index] ?? null; }
  removeItem(key) { this.values.delete(key); }
  setItem(key, value) { this.values.set(key, String(value)); }
}

const sessionStorage = new MemoryStorage();
globalThis.window = { sessionStorage };

const vite = await createServer({
  root: fileURLToPath(new URL("../", import.meta.url)),
  logLevel: "silent",
  server: { middlewareMode: true },
  appType: "custom",
});
const security = await vite.ssrLoadModule("/src/apiSecurity.ts");
await vite.close();

const TOKEN = `test_token_${"a".repeat(40)}`;

test("runtime Profile parser freezes the safe/write relationship", () => {
  const base = {
    schema_version: "pulse-runtime-profile.v1",
    product_version: "0.2.0-alpha.1",
    profile: "safe",
    write_enabled: false,
    token_required: false,
    loopback_only: true,
  };
  assert.deepEqual(security.parseRuntimeProfile(base), base);
  assert.equal(
    security.parseRuntimeProfile({ ...base, write_enabled: true }),
    null,
  );
  assert.equal(
    security.parseRuntimeProfile({
      ...base,
      profile: "workspace",
      write_enabled: true,
      token_required: true,
    })?.profile,
    "workspace",
  );
});

test("GET remains unauthenticated and mutation receives the session token", () => {
  security.clearApiToken();
  const read = security.withApiAuthorization({ signal: undefined });
  assert.equal(new Headers(read.headers).get("authorization"), null);

  security.setApiToken(TOKEN);
  const write = security.withApiAuthorization({
    method: "POST",
    headers: { "content-type": "application/json", authorization: "Bearer stale" },
  });
  assert.equal(new Headers(write.headers).get("authorization"), `Bearer ${TOKEN}`);
  assert.equal(new Headers(write.headers).get("content-type"), "application/json");
});

test("token is session-only, shape checked, and clearable", () => {
  assert.equal(sessionStorage.getItem(security.API_TOKEN_SESSION_KEY), TOKEN);
  assert.equal(security.readApiToken(), TOKEN);
  assert.throws(() => security.setApiToken("short secret"));
  security.clearApiToken();
  assert.equal(security.readApiToken(), null);
});
