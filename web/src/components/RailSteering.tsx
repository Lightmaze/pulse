import { useI18n } from "../i18n";
import { RailBand, RailIdle } from "./RailParts";

/**
 * Steering is the companion-computing intervention stream, not the claustrum.
 * The clone-session feed is not wired into the v0.1 service yet, so the rail
 * names the stream and reports absence instead of fabricating advisories.
 */
export function RailSteering() {
  const { t } = useI18n();
  return (
    <RailBand title={t("steering.title")} subtitle={t("steering.subtitle")}>
      <RailIdle>{t("steering.empty")}</RailIdle>
      <div className="rail-note">{t("steering.note")}</div>
    </RailBand>
  );
}
