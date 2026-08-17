import { useEffect, useState } from "react";
import { useI18n } from "../i18n";
import {
  clearApiToken,
  fetchRuntimeProfile,
  readApiToken,
  setApiToken,
  type RuntimeProfile,
} from "../apiSecurity";
import { zhText } from "../locales/zh-ui.ts";

export function SecurityProfileBadge({ base }: { base: string }) {
  const { locale } = useI18n();
  const zh = locale === "zh-CN";
  const [profile, setProfile] = useState<RuntimeProfile | null>(null);
  const [failed, setFailed] = useState(false);
  const [draft, setDraft] = useState("");
  const [hasToken, setHasToken] = useState(() => readApiToken() !== null);
  const [tokenError, setTokenError] = useState<string | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    setFailed(false);
    void fetchRuntimeProfile(base, controller.signal)
      .then((next) => {
        setProfile(next);
        if (next.profile === "safe") {
          clearApiToken();
          setHasToken(false);
        } else {
          setHasToken(readApiToken() !== null);
        }
      })
      .catch((cause: unknown) => {
        if (cause instanceof Error && cause.name === "AbortError") return;
        setFailed(true);
      });
    return () => controller.abort();
  }, [base]);

  const save = () => {
    try {
      setApiToken(draft);
      setDraft("");
      setHasToken(true);
      setTokenError(null);
    } catch {
      setTokenError(zh ? zhText("components.SecurityProfileBadge.line47") : "Invalid token or session storage unavailable");
    }
  };

  const clear = () => {
    clearApiToken();
    setDraft("");
    setHasToken(false);
    setTokenError(null);
  };

  if (failed) {
    return (
      <div className="pw-security-profile is-unknown" role="status">
        {zh ? zhText("components.SecurityProfileBadge.line61") : "Profile: unknown"}
      </div>
    );
  }
  if (profile === null) {
    return (
      <div className="pw-security-profile is-loading" role="status">
        {zh ? zhText("components.SecurityProfileBadge.line68") : "Reading Profile…"}
      </div>
    );
  }

  const label = profile.profile.toUpperCase();
  if (!profile.token_required) {
    return (
      <div className="pw-security-profile is-safe" role="status">
        <strong>{label}</strong>
        <span>{zh ? zhText("components.SecurityProfileBadge.line78") : "HTTP read-only"}</span>
      </div>
    );
  }

  return (
    <details className={`pw-security-profile is-${profile.profile}`} open={!hasToken}>
      <summary>
        <strong>{label}</strong>
        <span>{hasToken ? (zh ? zhText("components.SecurityProfileBadge.line87") : "token set for this tab") : (zh ? zhText("components.SecurityProfileBadge.line87.2") : "startup token required")}</span>
      </summary>
      <div className="pw-security-token-form">
        <label>
          <span>{zh ? zhText("components.SecurityProfileBadge.line91") : "Startup token"}</span>
          <input
            type="password"
            value={draft}
            autoComplete="off"
            spellCheck={false}
            onChange={(event) => setDraft(event.target.value)}
          />
        </label>
        <div>
          <button type="button" onClick={save} disabled={draft === ""}>
            {zh ? zhText("components.SecurityProfileBadge.line102") : "Save for this tab"}
          </button>
          {hasToken && (
            <button type="button" className="is-quiet" onClick={clear}>
              {zh ? zhText("components.SecurityProfileBadge.line106") : "Clear"}
            </button>
          )}
        </div>
        {tokenError !== null && <p role="alert">{tokenError}</p>}
      </div>
    </details>
  );
}
