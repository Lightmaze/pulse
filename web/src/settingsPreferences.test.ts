// @ts-nocheck
import assert from "node:assert/strict";
import test from "node:test";
import {
  DEFAULT_SETTINGS_PREFERENCES,
  SETTINGS_STORAGE_KEYS,
  readSettingsPreferences,
} from "./settingsPreferencesModel.ts";

test("settings preferences accept only supported persisted values", () => {
  const values = new Map<string, string>([
    [SETTINGS_STORAGE_KEYS.accent, "violet"],
    [SETTINGS_STORAGE_KEYS.motion, "reduce"],
  ]);
  assert.deepEqual(
    readSettingsPreferences({ getItem: (key) => values.get(key) ?? null }),
    { accent: "violet", motion: "reduce" },
  );

  values.set(SETTINGS_STORAGE_KEYS.accent, "orange");
  values.set(SETTINGS_STORAGE_KEYS.motion, "always");
  assert.deepEqual(
    readSettingsPreferences({ getItem: (key) => values.get(key) ?? null }),
    DEFAULT_SETTINGS_PREFERENCES,
  );
});

test("settings preferences fail safely when storage is blocked", () => {
  assert.deepEqual(readSettingsPreferences(null), DEFAULT_SETTINGS_PREFERENCES);
  assert.deepEqual(
    readSettingsPreferences({
      getItem: () => {
        throw new Error("storage blocked");
      },
    }),
    DEFAULT_SETTINGS_PREFERENCES,
  );
});
