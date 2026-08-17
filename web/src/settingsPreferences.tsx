import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import {
  DEFAULT_SETTINGS_PREFERENCES,
  SETTINGS_STORAGE_KEYS,
  readSettingsPreferences,
  type AccentPreference,
  type MotionPreference,
  type SettingsPreferences,
} from "./settingsPreferencesModel";

function initialPreferences(): SettingsPreferences {
  try {
    return readSettingsPreferences(window.localStorage);
  } catch {
    return DEFAULT_SETTINGS_PREFERENCES;
  }
}

interface SettingsPreferencesValue extends SettingsPreferences {
  setAccent: (accent: AccentPreference) => void;
  setMotion: (motion: MotionPreference) => void;
}

const SettingsPreferencesContext = createContext<SettingsPreferencesValue | null>(null);

function persistPreference(key: string, value: string): void {
  try {
    window.localStorage.setItem(key, value);
  } catch {
    // The current view still updates when browser storage is unavailable.
  }
}

export function SettingsPreferencesProvider({ children }: { children: ReactNode }) {
  const [preferences, setPreferences] = useState<SettingsPreferences>(initialPreferences);

  useEffect(() => {
    document.documentElement.dataset.accent = preferences.accent;
    document.documentElement.dataset.motion = preferences.motion;
  }, [preferences]);

  const setAccent = useCallback((accent: AccentPreference) => {
    setPreferences((current) => ({ ...current, accent }));
    persistPreference(SETTINGS_STORAGE_KEYS.accent, accent);
  }, []);

  const setMotion = useCallback((motion: MotionPreference) => {
    setPreferences((current) => ({ ...current, motion }));
    persistPreference(SETTINGS_STORAGE_KEYS.motion, motion);
  }, []);

  const value = useMemo<SettingsPreferencesValue>(
    () => ({ ...preferences, setAccent, setMotion }),
    [preferences, setAccent, setMotion],
  );

  return (
    <SettingsPreferencesContext.Provider value={value}>
      {children}
    </SettingsPreferencesContext.Provider>
  );
}

export function useSettingsPreferences(): SettingsPreferencesValue {
  const value = useContext(SettingsPreferencesContext);
  if (value === null) {
    throw new Error("useSettingsPreferences must be used inside SettingsPreferencesProvider");
  }
  return value;
}
