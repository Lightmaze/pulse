import { useI18n, type Locale } from "../i18n";
import { useViewer, type Page } from "../store";

/**
 * Workspace nav (frontend-design.md §9). The item set mirrors the converging
 * agent-workspace shape (codex-style), with one deliberate ontological merge:
 * Pulse has no separate "agent repo": an engram is a session (the independent-Engram rule), so
 * one sessions view covers both. Evaluation remains a placeholder.
 */
export function Sidebar() {
  const { locale, setLocale, t } = useI18n();
  const page = useViewer((s) => s.page);
  const liveStatus = useViewer((s) => s.liveStatus);
  const runName = useViewer((s) => s.run?.name ?? null);

  const items: { page: Page; label: string; hint: string }[] = [
    { page: "sessions", label: t("nav.sessions"), hint: t("nav.sessionsHint") },
    { page: "trace", label: t("nav.trace"), hint: t("nav.traceHint") },
    { page: "models", label: t("nav.models"), hint: t("nav.modelsHint") },
    { page: "cost", label: t("nav.cost"), hint: t("nav.costHint") },
  ];
  const statusLabel: Record<string, string> = {
    off: t("status.engineOff"),
    connecting: t("status.connecting"),
    open: t("status.engineOnline"),
    error: t("status.reconnecting"),
  };

  return (
    <nav className="sidebar">
      <div className="sidebar-brand">
        Pulse<span>{t("brand.subtitle")}</span>
      </div>
      <div className="sidebar-items">
        {items.map((item) => (
          <button
            key={item.page}
            className={page === item.page ? "active" : ""}
            title={item.hint}
            onClick={() => useViewer.getState().setPage(item.page)}
          >
            {item.label}
          </button>
        ))}
        <button className="disabled" disabled title={t("nav.evalHint")}>
          {t("nav.eval")}
        </button>
      </div>
      <div className="sidebar-foot">
        <label className="locale-picker">
          <span>{t("locale.label")}</span>
          <select
            aria-label={t("locale.label")}
            value={locale}
            onChange={(event) => setLocale(event.target.value as Locale)}
          >
            <option value="en">{t("locale.english")}</option>
            <option value="zh-CN">{t("locale.chinese")}</option>
          </select>
        </label>
        <div className={`sidebar-live ${liveStatus}`}>
          {statusLabel[liveStatus] ?? liveStatus}
        </div>
        {runName !== null && (
          <div className="sidebar-run" title={runName}>
            {runName}
          </div>
        )}
      </div>
    </nav>
  );
}
