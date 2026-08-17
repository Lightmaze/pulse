import { useEffect, useState } from "react";
import { useI18n } from "../i18n";
import {
  faultText,
  patchIdentity,
  type ActiveEngram,
  type Fetched,
} from "../pulse";
import { Meter, RailBand, RailFault, RailIdle, RailWaiting, relTime } from "./RailParts";

const FLASH_MS = 2500;

function IdentityEditor({
  engram,
  base,
  onSaved,
  onClose,
}: {
  engram: ActiveEngram;
  base: string | null;
  onSaved: () => void;
  onClose: () => void;
}) {
  const { t } = useI18n();
  const [name, setName] = useState(engram.name ?? "");
  const [nickname, setNickname] = useState(engram.nickname ?? "");
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState<string | null>(null);

  useEffect(() => {
    setName(engram.name ?? "");
    setNickname(engram.nickname ?? "");
    setMessage(null);
  }, [engram.engram_id, engram.name, engram.nickname]);

  const changed =
    name.trim() !== (engram.name ?? "") || nickname.trim() !== (engram.nickname ?? "");

  return (
    <div className="identity-editor" onClick={(event) => event.stopPropagation()}>
      <label>
        <span>{t("identity.name")}</span>
        <input value={name} maxLength={80} onChange={(event) => setName(event.target.value)} />
      </label>
      <label>
        <span>{t("identity.nickname")}</span>
        <input
          value={nickname}
          maxLength={80}
          onChange={(event) => setNickname(event.target.value)}
        />
      </label>
      <div className="identity-signature" title={engram.engram_id}>
        {t("identity.signature")} · {engram.engram_id}
      </div>
      <div className="identity-actions">
        <button
          className="primary"
          disabled={base === null || saving || !changed || name.trim() === ""}
          onClick={() => {
            if (base === null) return;
            const updates: { name?: string; nickname?: string | null } = {};
            if (name.trim() !== (engram.name ?? "")) updates.name = name.trim();
            if (nickname.trim() !== (engram.nickname ?? "")) {
              updates.nickname = nickname.trim() === "" ? null : nickname.trim();
            }
            setSaving(true);
            setMessage(null);
            patchIdentity(base, engram.engram_id, updates)
              .then(() => {
                setMessage(t("identity.saved"));
                onSaved();
              })
              .catch((error: unknown) => setMessage(faultText(error)))
              .finally(() => setSaving(false));
          }}
        >
          {saving ? t("identity.saving") : t("identity.save")}
        </button>
        <button onClick={onClose}>{t("identity.cancel")}</button>
      </div>
      {message !== null && <div className="identity-message">{message}</div>}
    </div>
  );
}

/**
 * 当前活跃 — who is in the room.
 *
 * The most volatile of the three bands, so it sits where the eye lands. Rows
 * are the roster from GET /pulse/active; the flash comes from the event stream,
 * so a firing shows at pulse latency rather than at poll latency.
 */
export function RailActive({
  active,
  hostUp,
  lastFireMs,
  nowMs,
  selected,
  onSelect,
  base,
  onIdentitySaved,
}: {
  active: Fetched<ActiveEngram[]>;
  hostUp: boolean;
  lastFireMs: Map<string, number>;
  nowMs: number;
  selected: string | null;
  onSelect: (id: string) => void;
  base: string | null;
  onIdentitySaved: () => void;
}) {
  const { locale, t } = useI18n();
  const rows = active.data;
  const firingCount = rows?.filter((e) => e.firing).length ?? null;

  const body = (() => {
    if (active.state === "failed") {
      return (
        <RailFault
          kind="offline"
          title={hostUp ? t("active.readFailed") : t("rail.offline")}
          detail={active.detail}
          remedy={active.remedy}
          onRetry={active.reload}
        />
      );
    }
    if (active.state === "absent") {
      return (
        <RailFault
          kind="absent"
          title={t("active.routeAbsent")}
          detail={active.detail}
          remedy={active.remedy}
          onRetry={active.reload}
        />
      );
    }
    if (rows === null) return <RailWaiting>{t("active.loading")}</RailWaiting>;
    if (rows.length === 0) {
      return <RailIdle>{t("active.none")}</RailIdle>;
    }

    const sorted = [...rows].sort((a, b) => {
      if (a.firing !== b.firing) return a.firing ? -1 : 1;
      return (b.last_fired_at ?? "").localeCompare(a.last_fired_at ?? "");
    });

    return (
      <>
        {firingCount === 0 && <RailIdle>{t("active.quiet")}</RailIdle>}
        {sorted.map((e) => {
          const since = lastFireMs.get(e.engram_id);
          const flash = since !== undefined && nowMs - since < FLASH_MS;
          return (
            <div
              key={e.engram_id}
              className={
                "rail-engram" +
                (e.firing ? " firing" : "") +
                (flash ? " flash" : "") +
                (selected === e.engram_id ? " selected" : "")
              }
              title={`${e.engram_id}${e.name === null ? "" : ` · ${e.name}`}`}
              onClick={() => onSelect(e.engram_id)}
            >
              <div className="rail-engram-top">
                <span className={`fire-pip${e.firing ? " on" : ""}`} />
                <span className="rail-engram-title">{e.name ?? e.engram_id}</span>
                {e.nickname !== null && (
                  <span className="rail-engram-nickname">{e.nickname}</span>
                )}
                <span className="rail-engram-when">
                  {e.firing ? t("active.firing") : relTime(e.last_fired_at, nowMs, locale)}
                </span>
              </div>
              <div className="rail-engram-meters">
                <Meter value={e.inhibition} label={t("active.inhibition")} tone="inhibition" />
                <Meter value={e.gate} label="gate" tone="gate" />
              </div>
              {selected === e.engram_id && (
                <IdentityEditor
                  engram={e}
                  base={base}
                  onSaved={onIdentitySaved}
                  onClose={() => onSelect(e.engram_id)}
                />
              )}
            </div>
          );
        })}
      </>
    );
  })();

  return (
    <RailBand
      title={t("active.title")}
      subtitle={t("active.subtitle")}
      note={
        active.state === "ok" && rows !== null ? (
          <span className={firingCount === 0 ? "" : "hot"}>
            {t("active.count", { firing: firingCount ?? 0, total: rows.length })}
          </span>
        ) : (
          <span className="unknown">—</span>
        )
      }
    >
      {body}
    </RailBand>
  );
}
