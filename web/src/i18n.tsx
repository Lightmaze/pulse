import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { enMessages, type MessageKey } from "./locales/en.ts";
import { zhCNMessages } from "./locales/zh-CN.ts";

export type Locale = "en" | "zh-CN";

const LOCALE_KEY = "pulse.locale:v1";

const messages: Record<Locale, Record<MessageKey, string>> = {
  en: enMessages,
  "zh-CN": zhCNMessages,
};

function isLocale(value: string | null): value is Locale {
  return value === "en" || value === "zh-CN";
}

function initialLocale(): Locale {
  const fromUrl = new URLSearchParams(window.location.search).get("lang");
  if (isLocale(fromUrl)) return fromUrl;
  try {
    const saved = window.localStorage.getItem(LOCALE_KEY);
    if (isLocale(saved)) return saved;
  } catch {
    // Storage can be unavailable in hardened browser contexts.
  }
  const browserLanguage = navigator.languages?.[0] ?? navigator.language;
  if (browserLanguage.toLowerCase().startsWith("zh")) return "zh-CN";
  return "en";
}

export function translate(
  locale: Locale,
  key: MessageKey,
  values: Record<string, string | number> = {},
): string {
  return messages[locale][key].replace(/\{(\w+)\}/g, (_, name: string) =>
    String(values[name] ?? `{${name}}`),
  );
}

const RUNTIME_FAULT_KEYS: Record<string, MessageKey> = {
  "The runtime returned an incomplete causal window.": "runtime.incompleteCausalWindow",
  "The runtime returned events outside the selected causal scope.": "runtime.causalScopeMismatch",
  "The causal stream crossed the selected scope boundary.": "runtime.causalBoundary",
  "Causal stream disconnected.": "runtime.causalDisconnected",
};

export function localizedRuntimeFault(locale: Locale, detail: string): string {
  const key = RUNTIME_FAULT_KEYS[detail];
  if (key !== undefined) return translate(locale, key);
  if (/Unexpected token|not valid JSON|JSON parse|invalid JSON/i.test(detail)) {
    return translate(locale, "runtime.unreadableResponse");
  }
  return detail;
}

export function currentLocale(): Locale {
  const lang = document.documentElement.lang;
  return isLocale(lang) ? lang : initialLocale();
}

interface I18nValue {
  locale: Locale;
  setLocale: (locale: Locale) => void;
  t: (key: MessageKey, values?: Record<string, string | number>) => string;
}

const I18nContext = createContext<I18nValue | null>(null);

export function LocaleProvider({ children }: { children: ReactNode }) {
  const [locale, setLocaleState] = useState<Locale>(initialLocale);

  useEffect(() => {
    document.documentElement.lang = locale;
  }, [locale]);

  const setLocale = useCallback((next: Locale) => {
    setLocaleState(next);
    try {
      window.localStorage.setItem(LOCALE_KEY, next);
    } catch {
      // The active view still switches even if persistence is unavailable.
    }
  }, []);

  const value = useMemo<I18nValue>(
    () => ({
      locale,
      setLocale,
      t: (key, values) => translate(locale, key, values),
    }),
    [locale, setLocale],
  );

  return <I18nContext.Provider value={value}>{children}</I18nContext.Provider>;
}

export function useI18n(): I18nValue {
  const value = useContext(I18nContext);
  if (value === null) throw new Error("useI18n must be used inside LocaleProvider");
  return value;
}
