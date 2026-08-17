export type AccentPreference = "pulse" | "blue" | "violet" | "green";
export type MotionPreference = "system" | "reduce";

export interface SettingsPreferences {
  accent: AccentPreference;
  motion: MotionPreference;
}

export const SETTINGS_STORAGE_KEYS = {
  accent: "pulse.settings.accent:v1",
  motion: "pulse.settings.motion:v1",
} as const;

export const DEFAULT_SETTINGS_PREFERENCES: SettingsPreferences = {
  accent: "pulse",
  motion: "system",
};

function isAccent(value: string | null): value is AccentPreference {
  return value === "pulse" || value === "blue" || value === "violet" || value === "green";
}

function isMotion(value: string | null): value is MotionPreference {
  return value === "system" || value === "reduce";
}

export function readSettingsPreferences(
  storage: Pick<Storage, "getItem"> | null,
): SettingsPreferences {
  if (storage === null) return DEFAULT_SETTINGS_PREFERENCES;
  try {
    const accent = storage.getItem(SETTINGS_STORAGE_KEYS.accent);
    const motion = storage.getItem(SETTINGS_STORAGE_KEYS.motion);
    return {
      accent: isAccent(accent) ? accent : DEFAULT_SETTINGS_PREFERENCES.accent,
      motion: isMotion(motion) ? motion : DEFAULT_SETTINGS_PREFERENCES.motion,
    };
  } catch {
    return DEFAULT_SETTINGS_PREFERENCES;
  }
}
