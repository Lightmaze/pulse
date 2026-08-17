import { useEffect, useState } from "react";
import { useI18n } from "../i18n";
import { apiBase, useViewer } from "../store";

interface Provider {
  provider: string;
  base_url: string;
  model: string;
  api_key_env: string;
  key_configured: boolean;
  cache_read_discount: number;
}

/**
 * 模型仓库 — per-Engram substrate binding's adapter registry, read from the server's static
 * provider table. Configuration state is a boolean; key values never
 * cross the wire.
 */
export function ModelsPage() {
  const { t } = useI18n();
  const mode = useViewer((s) => s.mode);
  const liveUrl = useViewer((s) => s.liveUrl);
  const [providers, setProviders] = useState<Provider[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch(`${apiBase({ mode, liveUrl })}/substrates`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json() as Promise<{ providers: Provider[] }>;
      })
      .then((body) => setProviders(body.providers))
      .catch((e: unknown) =>
        setError(
          e instanceof TypeError
            ? t("models.apiOffline")
            : e instanceof Error
              ? e.message
              : String(e),
        ),
      );
  }, [mode, liveUrl, t]);

  return (
    <div className="models-page">
      <div className="page-title">
        {t("models.title")}
        <span className="page-sub">
          {t("models.subtitle")}
        </span>
      </div>
      {error !== null && <div className="inspector-note error">{error}</div>}
      <div className="model-cards">
        {providers?.map((p) => (
          <div key={p.provider} className="model-card">
            <div className="model-card-head">
              <span className="model-provider">{p.provider}</span>
              <span className={p.key_configured ? "badge ok" : "badge off"}>
                {p.key_configured ? t("models.configured") : t("models.unconfigured")}
              </span>
            </div>
            <div className="model-name">{p.model}</div>
            <div className="model-meta">{p.base_url}</div>
            <div className="model-meta">
              key: <code>{p.api_key_env}</code> · {t("models.cacheDiscount")} ×
              {p.cache_read_discount}
            </div>
          </div>
        ))}
      </div>
      {providers !== null && (
        <div className="inspector-note">
          {t("models.note")}
        </div>
      )}
    </div>
  );
}
