import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type KeyboardEvent,
} from "react";
import { useI18n, type Locale } from "../i18n";
import { faultText } from "../pulse";
import {
  fetchActivityCenterDetail,
  fetchLivingPortfolio,
  fetchPurposeAmendments,
  fetchTaskOffers,
  remindTaskOffer,
  reviseTaskOffer,
  sendActivityCenterMessage,
  updateActivityCenter,
  withdrawTaskOffer,
  type ActivityCenterDetail,
  type ActivityCenterSummary,
  type ActivityCenterUpdate,
  type CenterMessageView,
  type LivingPortfolio,
  type LivingPortfolioItem,
  type LivingPortfolioRelation,
  type LivingPortfolioState,
  type LivingConcernView,
  type LivingOrientationState,
  type LivingOrientationView,
  type PurposeRevisionView,
  type PurposeAmendmentAttempt,
  type PurposeAmendmentsProjection,
  type TaskOfferStatus,
  type TaskOfferSummary,
} from "../world";
import { MessageContent } from "./ConversationPane";
import { HexMark, Icon } from "./Icons";
import { shortSignature, statusLabel, wcopy } from "./model";
import { zhText } from "../locales/zh-ui.ts";

function formatMoment(timestamp: string | null, locale: Locale): string {
  if (timestamp === null) return "—";
  const parsed = Date.parse(timestamp);
  if (!Number.isFinite(parsed)) return timestamp;
  return new Intl.DateTimeFormat(locale, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(parsed);
}

function concernDisposition(disposition: string, locale: Locale): string {
  if (locale === "en") {
    return {
      quiet: "quiet",
      revisit: "revisit",
      resolved: "resolved",
    }[disposition] ?? disposition;
  }
  return {
    quiet: zhText("workbench.LivingCenterPane.line63"),
    revisit: zhText("workbench.LivingCenterPane.line64"),
    resolved: zhText("workbench.LivingCenterPane.line65"),
  }[disposition] ?? disposition;
}

function ConcernCard({ concern }: { concern: LivingConcernView }) {
  const { locale } = useI18n();
  const revisitTime = concern.revisit_at === null
    ? null
    : formatMoment(concern.revisit_at, locale);
  const overdue = concern.disposition === "revisit" &&
    concern.revisit_at !== null &&
    Date.parse(concern.revisit_at) <= Date.now();
  return (
    <article className={`pw-concern-card disposition-${concern.disposition}${overdue ? " is-due" : ""}`}>
      <header>
        <span>{concernDisposition(concern.disposition, locale)}</span>
        <small>rev {concern.revision}</small>
      </header>
      <p>{concern.content}</p>
      <footer>
        {revisitTime !== null && (
          <span>
            <Icon name="clock" size={12} />
            {overdue
              ? locale === "zh-CN" ? (zhText("workbench.LivingCenterPane.line89.head") + String(revisitTime) + "") : `due now · ${revisitTime}`
              : locale === "zh-CN" ? (zhText("workbench.LivingCenterPane.line90.head") + String(revisitTime) + "") : `revisit · ${revisitTime}`}
          </span>
        )}
        {concern.last_reentry_event_id !== null && (
          <span>
            <Icon name="route" size={12} />
            {locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line96") : "re-entry recorded"}
          </span>
        )}
        <code>{shortSignature(concern.causal_id)}</code>
      </footer>
    </article>
  );
}

function orientationStateLabel(
  state: LivingOrientationState,
  locale: Locale,
): string {
  const key = state === "open"
    ? "orientationOpen"
    : state === "resting"
      ? "orientationResting"
      : "orientationClosed";
  return wcopy(locale, key);
}

function OrientationCard({
  orientation,
  locale,
}: {
  orientation: LivingOrientationView;
  locale: Locale;
}) {
  const nextEligible = orientation.nextEligibleAt === null
    ? orientation.state === "open" ? wcopy(locale, "orientationReady") : "—"
    : formatMoment(orientation.nextEligibleAt, locale);
  return (
    <article className={`pw-orientation-card state-${orientation.state}`}>
      <header>
        <div>
          <span className="pw-orientation-state">{orientationStateLabel(orientation.state, locale)}</span>
          <small>rev {orientation.revision}</small>
        </div>
        <small className="pw-orientation-owner">
          {wcopy(locale, "orientationOwner")} · {shortSignature(orientation.ownerEngramId)}
        </small>
      </header>
      <p>{orientation.content}</p>
      {orientation.state === "resting" && (
        <div className="pw-orientation-resting-note">
          <Icon name="clock" size={13} />
          <span>{wcopy(locale, "orientationRestingHint")}</span>
        </div>
      )}
      <footer>
        <span>
          <Icon name="spark" size={12} />
          {orientation.engagementCount} {wcopy(locale, "orientationEngagementCount")}
        </span>
        <span>
          <Icon name="clock" size={12} />
          {wcopy(locale, "orientationLastEngagement")} · {formatMoment(orientation.lastEngagedAt, locale)}
        </span>
        <span>
          <Icon name="route" size={12} />
          {wcopy(locale, "orientationNextEligible")} · {nextEligible}
        </span>
        <code title={orientation.id}>{shortSignature(orientation.id)}</code>
      </footer>
    </article>
  );
}

function LivingOrientationSection({
  orientations,
  total,
  truncated,
  locale,
}: {
  orientations: LivingOrientationView[];
  total: number;
  truncated: boolean;
  locale: Locale;
}) {
  const current = orientations.filter((orientation) => orientation.state !== "closed");
  const closed = orientations.filter((orientation) => orientation.state === "closed");
  return (
    <section className="pw-life-section pw-orientation-section" aria-label={wcopy(locale, "livingOrientation")}>
      <header>
        <div>
          <span>{wcopy(locale, "livingOrientation")}</span>
          <small>{wcopy(locale, "currentLivingOrientation")}</small>
        </div>
        <strong>{total}</strong>
      </header>
      {current.length === 0 ? (
        <div className="pw-life-empty pw-orientation-empty">
          <Icon name="spark" size={18} />
          <span>{wcopy(locale, "noLivingOrientation")}</span>
        </div>
      ) : (
        <div className="pw-orientation-grid">
          {current.map((orientation) => (
            <OrientationCard key={orientation.id} orientation={orientation} locale={locale} />
          ))}
        </div>
      )}
      {truncated && (
        <small className="pw-life-truncated">
          {locale === "zh-CN"
            ? (zhText("workbench.LivingCenterPane.line201.head") + String(orientations.length) + zhText("workbench.LivingCenterPane.line201.tail1") + String(total) + zhText("workbench.LivingCenterPane.line201.tail2"))
            : `Showing ${orientations.length} of ${total} orientations.`}
        </small>
      )}
      {closed.length > 0 ? (
        <details className="pw-orientation-history">
          <summary>
            <span>{wcopy(locale, "orientationHistory")}</span>
            <small>{closed.length} · {wcopy(locale, "orientationHistoryHint")}</small>
          </summary>
          <div className="pw-orientation-history-grid">
            {closed.map((orientation) => (
              <OrientationCard key={orientation.id} orientation={orientation} locale={locale} />
            ))}
          </div>
        </details>
      ) : !truncated && orientations.length > 0 && current.length > 0 ? (
        <small className="pw-orientation-no-history">{wcopy(locale, "orientationNoHistory")}</small>
      ) : null}
    </section>
  );
}

function portfolioStateLabel(
  state: LivingPortfolioState,
  locale: Locale,
): string {
  const key = state === "active"
    ? "portfolioActive"
    : state === "quiet"
      ? "portfolioQuiet"
      : state === "parked"
        ? "portfolioParked"
        : state === "completed"
          ? "portfolioCompleted"
          : "portfolioArchived";
  return wcopy(locale, key);
}

function portfolioRelationLabel(
  relation: LivingPortfolioRelation,
  locale: Locale,
): string {
  return wcopy(
    locale,
    relation === "focal"
      ? "relationFocal"
      : relation === "participant"
        ? "relationParticipant"
        : "relationShared",
  );
}

function portfolioOriginLabel(origin: string, locale: Locale): string {
  const key = origin === "self"
    ? "originSelf"
    : origin === "shared"
      ? "originShared"
      : origin === "system"
        ? "originSystem"
        : "originUser";
  return wcopy(locale, key);
}

function portfolioKindLabel(kind: string, locale: Locale): string {
  const key = kind === "hobby"
    ? "hobby"
    : kind === "life_project"
      ? "lifeProject"
      : kind === "relationship"
        ? "relationship"
        : kind === "exploration"
          ? "exploration"
          : kind === "practice"
            ? "practice"
            : kind === "expression"
              ? "expression"
              : kind === "rest"
                ? "rest"
                : "other";
  return wcopy(locale, key);
}

function PurposeRevisionRow({
  revision,
  locale,
  current,
}: {
  revision: PurposeRevisionView;
  locale: Locale;
  current: boolean;
}) {
  const amendment = locale === "zh-CN"
    ? revision.amendment_kind === "establish"
      ? zhText("workbench.LivingCenterPane.line295")
      : revision.amendment_kind === "amend"
        ? zhText("workbench.LivingCenterPane.line297")
        : zhText("workbench.LivingCenterPane.line298")
    : revision.amendment_kind === "establish"
      ? "established"
      : revision.amendment_kind === "amend"
        ? "amended"
        : "withdrawn";
  return (
    <article className={`pw-purpose-revision${current ? " is-current" : ""}`}>
      <header>
        <span>rev {revision.revision} · {amendment}</span>
        <time>{formatMoment(revision.created_at, locale)}</time>
      </header>
      <p>
        {revision.content ?? (
          locale === "zh-CN"
            ? zhText("workbench.LivingCenterPane.line313")
            : "The subject withdrew the purpose held before this revision."
        )}
      </p>
      <footer>
        <span>{current ? (locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line318") : "held now") : revision.state}</span>
        <code title={revision.author_engram_id}>
          {locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line320") : "author"} · {shortSignature(revision.author_engram_id)}
        </code>
      </footer>
    </article>
  );
}

function purposeAttemptStateLabel(
  state: PurposeAmendmentAttempt["state"],
  locale: Locale,
): string {
  const labels = locale === "zh-CN"
    ? {
        pending: zhText("workbench.LivingCenterPane.line333"),
        committed: zhText("workbench.LivingCenterPane.line334"),
        rejected: zhText("workbench.LivingCenterPane.line335"),
        uncertain: zhText("workbench.LivingCenterPane.line336"),
        conflicted: zhText("workbench.LivingCenterPane.line337"),
      }
    : {
        pending: "Awaiting turn settlement",
        committed: "Adopted",
        rejected: "Turn failed · not adopted",
        uncertain: "Uncertain · not adopted",
        conflicted: "Conflict · did not overwrite",
      };
  return labels[state];
}

function purposeAttemptExplanation(
  attempt: PurposeAmendmentAttempt,
  locale: Locale,
  current: boolean,
): string {
  if (locale === "zh-CN") {
    return {
      pending: zhText("workbench.LivingCenterPane.line356"),
      committed: current
        ? zhText("workbench.LivingCenterPane.line358")
        : zhText("workbench.LivingCenterPane.line359"),
      rejected: zhText("workbench.LivingCenterPane.line360"),
      uncertain: zhText("workbench.LivingCenterPane.line361"),
      conflicted: zhText("workbench.LivingCenterPane.line362"),
    }[attempt.state];
  }
  return {
    pending: "The subject proposed this, but it can become current only after the same Harness turn settles successfully.",
    committed: current
      ? "The same turn settled and its causal and revision checks passed; the subject holds this purpose now."
      : "The same turn settled and the subject adopted this earlier; it remains in the purpose lineage.",
    rejected: "The proposing Harness turn failed, so this did not shape the subject.",
    uncertain: "The turn outcome is unconfirmed; the system leaves it unadopted for evidence review.",
    conflicted: "The turn settled after the holder or purpose revision changed; the older proposal did not overwrite newer fact.",
  }[attempt.state];
}

function PurposeAmendmentAttemptRow({
  attempt,
  locale,
  current,
}: {
  attempt: PurposeAmendmentAttempt;
  locale: Locale;
  current: boolean;
}) {
  const amendment = locale === "zh-CN"
    ? {
        establish: zhText("workbench.LivingCenterPane.line387"),
        amend: zhText("workbench.LivingCenterPane.line388"),
        withdraw: zhText("workbench.LivingCenterPane.line389"),
      }[attempt.amendment_kind]
    : attempt.amendment_kind;
  return (
    <article className={`pw-purpose-attempt state-${attempt.state}`}>
      <header>
        <span className={`pw-purpose-attempt-state state-${attempt.state}`}>
          {purposeAttemptStateLabel(attempt.state, locale)}
        </span>
        <time>{formatMoment(attempt.created_at, locale)}</time>
      </header>
      <p className="pw-purpose-attempt-content">
        {attempt.content ?? (
          locale === "zh-CN"
            ? zhText("workbench.LivingCenterPane.line403")
            : "The subject proposed withdrawing the current purpose."
        )}
      </p>
      <p className="pw-purpose-attempt-explanation">
        {purposeAttemptExplanation(attempt, locale, current)}
      </p>
      <footer>
        <span>{amendment}</span>
        <code title={attempt.harness_turn_id}>
          turn · {shortSignature(attempt.harness_turn_id)}
        </code>
      </footer>
    </article>
  );
}

function PortfolioItemCard({
  item,
  locale,
  currentCenterId,
  subjectEngramId,
  onSelectLife,
}: {
  item: LivingPortfolioItem;
  locale: Locale;
  currentCenterId: string;
  subjectEngramId: string;
  onSelectLife: (centerId: string, subjectEngramId: string) => void;
}) {
  const isCurrentView = item.center.id === currentCenterId;
  return (
    <button
      type="button"
      className={`pw-portfolio-card state-${item.portfolio_state}${isCurrentView ? " is-viewing" : ""}`}
      aria-current={isCurrentView ? "page" : undefined}
      disabled={isCurrentView}
      onClick={() => onSelectLife(item.center.id, subjectEngramId)}
    >
      <span className="pw-portfolio-card-topline">
        <span className={`pw-portfolio-state state-${item.portfolio_state}`}>
          {portfolioStateLabel(item.portfolio_state, locale)}
        </span>
        {isCurrentView && <span className="pw-portfolio-viewing">{wcopy(locale, "viewingCenter")}</span>}
      </span>
      <strong>{item.center.title}</strong>
      <span className="pw-portfolio-description">
        {item.center.description || (
          locale === "zh-CN"
            ? zhText("workbench.LivingCenterPane.line452")
            : "No description is written; it may still exist quietly."
        )}
      </span>
      <span className="pw-portfolio-card-meta">
        <span>{portfolioKindLabel(item.center.kind, locale)}</span>
        <span>{portfolioRelationLabel(item.relation, locale)}</span>
        <span>{portfolioOriginLabel(item.center.origin, locale)}</span>
      </span>
      <span className="pw-portfolio-card-footer">
        <span>{formatMoment(item.center.updated_at, locale)}</span>
        <span>{isCurrentView ? wcopy(locale, "viewingCenter") : wcopy(locale, "openLifeCenter")}</span>
      </span>
    </button>
  );
}

function LivingPortfolioSection({
  portfolio,
  purposeAmendments,
  purposeAmendmentsState,
  purposeAmendmentsError,
  state,
  error,
  hasHolder,
  currentCenterId,
  locale,
  onRetry,
  onSelectLife,
}: {
  portfolio: LivingPortfolio | null;
  purposeAmendments: PurposeAmendmentsProjection | null;
  purposeAmendmentsState: "loading" | "ready" | "error";
  purposeAmendmentsError: string | null;
  state: "loading" | "ready" | "error";
  error: string | null;
  hasHolder: boolean;
  currentCenterId: string;
  locale: Locale;
  onRetry: () => void;
  onSelectLife: (centerId: string, subjectEngramId: string) => void;
}) {
  const currentPurpose = portfolio?.purpose.current ?? null;
  const attentionAttempts = purposeAmendments?.attempts.filter(
    (attempt) => attempt.state === "pending" ||
      attempt.state === "uncertain" ||
      attempt.state === "conflicted",
  ).length ?? 0;
  const hasAttentionAttempts = attentionAttempts > 0;
  const [purposeAttemptsOpen, setPurposeAttemptsOpen] = useState(false);
  useEffect(() => {
    if (purposeAmendmentsState === "loading") {
      setPurposeAttemptsOpen(false);
    } else if (purposeAmendmentsState === "ready" && hasAttentionAttempts) {
      setPurposeAttemptsOpen(true);
    }
  }, [hasAttentionAttempts, purposeAmendmentsState]);
  return (
    <section className="pw-life-section pw-portfolio-section" aria-label={wcopy(locale, "livingPortfolio")}>
      <header>
        <div>
          <span>{wcopy(locale, "livingPortfolio")}</span>
          <small>{wcopy(locale, "livingPortfolioHint")}</small>
        </div>
        {portfolio !== null && <strong>{portfolio.item_count}</strong>}
      </header>

      {state === "loading" && portfolio === null && (
        <div className="pw-portfolio-loading">
          <span className="pw-send-spinner" />
          <span>{wcopy(locale, "portfolioLoading")}</span>
        </div>
      )}

      {state === "error" && (
        <div className={`pw-portfolio-error${portfolio !== null ? " is-stale" : ""}`}>
          <Icon name="info" size={15} />
          <div>
            <strong>{wcopy(locale, "portfolioRefreshFailed")}</strong>
            <span>
              {!hasHolder
                ? wcopy(locale, "portfolioNoHolder")
                : error ?? wcopy(locale, "runtimeUnavailable")}
              {portfolio !== null ? ` ${wcopy(locale, "portfolioShowingLastSnapshot")}` : ""}
            </span>
          </div>
          {hasHolder && <button type="button" onClick={onRetry}>{wcopy(locale, "retry")}</button>}
        </div>
      )}

      {portfolio !== null && (
        <>
          <div className="pw-portfolio-purpose">
            <div className="pw-portfolio-subhead">
              <div>
                <span>{wcopy(locale, "subjectPurpose")}</span>
                <small>{wcopy(locale, "subjectPurposeHint")}</small>
              </div>
              <code title={portfolio.subject.current_engram_id}>
                {wcopy(locale, "generation")} {portfolio.subject.generation} · {shortSignature(portfolio.subject.current_engram_id)}
              </code>
            </div>
            {portfolio.subject.lineage_state === "unestablished" ? (
              <div className="pw-purpose-empty">
                <Icon name="spark" size={18} />
                <div>
                  <strong>{wcopy(locale, "purposeUnestablished")}</strong>
                  <span>{wcopy(locale, "purposeUnestablishedHint")}</span>
                </div>
              </div>
            ) : currentPurpose === null ? (
              <div className="pw-purpose-empty">
                <Icon name="spark" size={18} />
                <div>
                  <strong>{wcopy(locale, "purposeUnwritten")}</strong>
                  <span>{wcopy(locale, "purposeUnwrittenHint")}</span>
                </div>
              </div>
            ) : (
              <div className="pw-purpose-current">
                <span>{locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line572") : "Held now"}</span>
                <p>{currentPurpose.content}</p>
                <small>
                  rev {currentPurpose.revision} · {formatMoment(currentPurpose.created_at, locale)}
                </small>
              </div>
            )}
            {purposeAmendmentsState === "loading" && purposeAmendments === null && (
              <div className="pw-purpose-attempts-loading">
                <span className="pw-mini-spinner" />
                <span>
                  {locale === "zh-CN"
                    ? zhText("workbench.LivingCenterPane.line584")
                    : "Checking which reflections settled and were adopted…"}
                </span>
              </div>
            )}
            {purposeAmendmentsState === "error" && (
              <div className="pw-purpose-attempts-error" role="status">
                <Icon name="info" size={14} />
                <span>
                  {locale === "zh-CN"
                    ? (zhText("workbench.LivingCenterPane.line594.head") + String(purposeAmendmentsError === null ? "" : ` ${purposeAmendmentsError}`) + "")
                    : `Purpose reflection records could not be verified.${purposeAmendmentsError === null ? "" : ` ${purposeAmendmentsError}`}`}
                </span>
              </div>
            )}
            {purposeAmendments !== null && (
              <>
                {purposeAmendments.settlement.health === "degraded" && (
                  <div className="pw-purpose-settlement-degraded" role="alert">
                    <Icon name="info" size={14} />
                    <span>
                      {locale === "zh-CN"
                        ? zhText("workbench.LivingCenterPane.line606")
                        : "A recent purpose settlement check failed; unresolved proposals remain unadopted pending raw-turn review."}
                    </span>
                  </div>
                )}
                {purposeAmendments.attempts.length > 0 && (
                  <details
                    className="pw-purpose-attempts"
                    open={purposeAttemptsOpen}
                    onToggle={(event) => {
                      setPurposeAttemptsOpen(event.currentTarget.open);
                    }}
                  >
                    <summary>
                      <span>
                        {locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line621") : "Reflection and adoption record"}
                      </span>
                      <small>
                        {purposeAmendments.attempt_count}
                        {attentionAttempts > 0
                          ? ` · ${attentionAttempts} ${locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line626") : "need attention"}`
                          : ` · ${locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line627") : "all terminal"}`}
                      </small>
                    </summary>
                    <div className="pw-purpose-attempt-list">
                      {purposeAmendments.attempts.map((attempt) => (
                        <PurposeAmendmentAttemptRow
                          key={attempt.proposal_id}
                          attempt={attempt}
                          locale={locale}
                          current={
                            purposeAmendments.current_purpose_revision_id ===
                            attempt.committed_revision_id
                          }
                        />
                      ))}
                    </div>
                  </details>
                )}
              </>
            )}
            {(portfolio.purpose.history.length > 0 || portfolio.purpose.history_truncated) && (
              <details className="pw-purpose-history">
                <summary>
                  <span>{wcopy(locale, "purposeHistory")}</span>
                  <small>{portfolio.purpose.history.length} · {wcopy(locale, "purposeHistoryHint")}</small>
                </summary>
                <div className="pw-purpose-history-list">
                  {portfolio.purpose.history.map((revision) => (
                    <PurposeRevisionRow
                      key={revision.purpose_revision_id}
                      revision={revision}
                      locale={locale}
                      current={revision.purpose_revision_id === currentPurpose?.purpose_revision_id}
                    />
                  ))}
                </div>
                {portfolio.purpose.history_truncated && (
                  <small className="pw-life-truncated">
                    {locale === "zh-CN"
                      ? zhText("workbench.LivingCenterPane.line666")
                      : "Lineage history is truncated; raise the read limit to inspect earlier revisions."}
                  </small>
                )}
              </details>
            )}
          </div>

          <div className="pw-portfolio-areas">
            <div className="pw-portfolio-subhead">
              <div>
                <span>{wcopy(locale, "portfolioAreas")}</span>
                <small>{wcopy(locale, "portfolioAreasHint")}</small>
              </div>
              <div className="pw-portfolio-state-counts" aria-label={locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line680") : "Life state counts"}>
                {(["active", "quiet", "parked", "completed", "archived"] as LivingPortfolioState[])
                  .filter((portfolioState) => portfolio.state_counts[portfolioState] > 0)
                  .map((portfolioState) => (
                    <span key={portfolioState} className={`state-${portfolioState}`}>
                      {portfolioStateLabel(portfolioState, locale)} · {portfolio.state_counts[portfolioState]}
                    </span>
                  ))}
              </div>
            </div>
            {portfolio.items.length === 0 ? (
              <div className="pw-life-empty pw-portfolio-empty">
                <HexMark size={30} />
                <span>{wcopy(locale, "portfolioEmpty")}</span>
              </div>
            ) : (
              <div className="pw-portfolio-grid">
                {portfolio.items.map((item) => (
                  <PortfolioItemCard
                    key={item.center.id}
                    item={item}
                    locale={locale}
                    currentCenterId={currentCenterId}
                    subjectEngramId={portfolio.subject.current_engram_id}
                    onSelectLife={onSelectLife}
                  />
                ))}
              </div>
            )}
          </div>
        </>
      )}
    </section>
  );
}

function CenterMoment({
  message,
  centerTitle,
}: {
  message: CenterMessageView;
  centerTitle: string;
}) {
  const { locale } = useI18n();
  const orientationEngagement = message.metadata.reason_code === "living_orientation_engagement";
  const reentry = message.metadata.reason === "living_concern_reentry" ||
    message.metadata.reason_code === "living_concern_reentry";
  const fromCenter = message.role === "assistant";
  const label = orientationEngagement
    ? locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line729") : "Living orientation engagement"
    : fromCenter
      ? centerTitle
      : reentry
        ? locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line733") : "Living concern re-entry"
        : message.source === "spontaneous"
          ? locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line735") : "Spontaneous life"
          : locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line736") : "You";
  return (
    <article className={`pw-message pw-center-moment pw-message-${fromCenter ? "engram" : "user"}${reentry ? " is-reentry" : ""}${orientationEngagement ? " is-orientation-engagement" : ""}`}>
      <div className="pw-message-avatar">
        {fromCenter
          ? <HexMark tone="pulse" size={31} />
          : orientationEngagement
            ? <span className="pw-orientation-avatar"><Icon name="spark" size={14} /></span>
          : reentry
            ? <span className="pw-reentry-avatar"><Icon name="route" size={14} /></span>
            : <span>{locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line746") : "Y"}</span>}
      </div>
      <div className="pw-message-body">
        <div className="pw-message-meta">
          <strong>{label}</strong>
          <time>{formatMoment(message.timestamp, locale)}</time>
          <span className="pw-message-role">#{message.seq} · {message.kind} · {message.status}</span>
        </div>
        <MessageContent content={message.content} />
      </div>
    </article>
  );
}

type OfferMutationAction = "remind" | "withdraw" | "revise";

interface OfferMutationState {
  offerId: string;
  action: OfferMutationAction;
}

interface OfferActionError {
  offerId: string;
  message: string;
  conflict: boolean;
}

function taskOfferStatusLabel(status: TaskOfferStatus, locale: Locale): string {
  switch (status) {
    case "pending":
      return wcopy(locale, "taskOfferPending");
    case "changes_requested":
      return wcopy(locale, "taskOfferChangesRequested");
    case "accepted":
      return wcopy(locale, "taskOfferAccepted");
    case "refused":
      return wcopy(locale, "taskOfferRefused");
    case "withdrawn":
      return wcopy(locale, "taskOfferWithdrawn");
  }
}

function taskOfferStatusHelp(status: TaskOfferStatus, locale: Locale): string {
  switch (status) {
    case "pending":
      return wcopy(locale, "taskOfferPendingHelp");
    case "changes_requested":
      return wcopy(locale, "taskOfferChangesRequestedHelp");
    case "accepted":
      return wcopy(locale, "taskOfferAcceptedHelp");
    case "refused":
      return wcopy(locale, "taskOfferRefusedHelp");
    case "withdrawn":
      return wcopy(locale, "taskOfferWithdrawnHelp");
  }
}

function TaskOfferCard({
  offer,
  locale,
  mutation,
  error,
  onRemind,
  onWithdraw,
  onRevise,
  onOpenTask,
}: {
  offer: TaskOfferSummary;
  locale: Locale;
  mutation: OfferMutationState | null;
  error: OfferActionError | null;
  onRemind: (offer: TaskOfferSummary) => void;
  onWithdraw: (offer: TaskOfferSummary) => void;
  onRevise: (offer: TaskOfferSummary, content: string) => void;
  onOpenTask: (frontId: string) => void;
}) {
  const { taskOffer, currentRevision } = offer;
  const [revisionDraft, setRevisionDraft] = useState(currentRevision.content);
  const busyAction = mutation?.offerId === taskOffer.id ? mutation.action : null;
  const actionError = error?.offerId === taskOffer.id ? error : null;
  const canRevise = revisionDraft.trim() !== "" && busyAction === null;

  useEffect(() => {
    setRevisionDraft(currentRevision.content);
  }, [currentRevision.content, currentRevision.revision]);

  return (
    <article
      className={`pw-task-offer-card is-${taskOffer.status}`}
      data-task-offer-id={taskOffer.id}
      data-task-offer-status={taskOffer.status}
    >
      <header>
        <div>
          <span className="pw-task-offer-status">
            {taskOfferStatusLabel(taskOffer.status, locale)}
          </span>
          <h3>{currentRevision.title}</h3>
        </div>
        <span className="pw-task-offer-revision">
          {wcopy(locale, "taskOfferRevision")} {currentRevision.revision}
        </span>
      </header>

      <p className="pw-task-offer-help">
        {taskOfferStatusHelp(taskOffer.status, locale)}
      </p>

      <div className="pw-task-offer-terms">
        <span>{wcopy(locale, "taskOfferTerms")}</span>
        <p>{currentRevision.content}</p>
      </div>

      {currentRevision.subject_response !== null && (
        <div className="pw-task-offer-response">
          <span>{wcopy(locale, "taskOfferSubjectResponse")}</span>
          <p>{currentRevision.subject_response}</p>
        </div>
      )}

      {taskOffer.status === "changes_requested" && (
        <div className="pw-task-offer-revise">
          <label htmlFor={`task-offer-revision-${taskOffer.id}`}>
            {wcopy(locale, "taskOfferRevisePlaceholder")}
          </label>
          <textarea
            id={`task-offer-revision-${taskOffer.id}`}
            rows={4}
            maxLength={12_000}
            value={revisionDraft}
            disabled={busyAction !== null}
            placeholder={wcopy(locale, "taskOfferRevisePlaceholder")}
            onChange={(event) => setRevisionDraft(event.target.value)}
          />
          <button
            type="button"
            className="is-primary"
            disabled={!canRevise}
            onClick={() => onRevise(offer, revisionDraft.trim())}
          >
            <Icon name="send" size={13} />
            {busyAction === "revise"
              ? wcopy(locale, "taskOfferRevising")
              : wcopy(locale, "taskOfferRevise")}
          </button>
        </div>
      )}

      <footer>
        <time>{formatMoment(taskOffer.updated_at, locale)}</time>
        <div className="pw-task-offer-actions">
          {taskOffer.status === "pending" && (
            <button
              type="button"
              disabled={busyAction !== null}
              onClick={() => onRemind(offer)}
            >
              <Icon name="refresh" size={13} />
              {busyAction === "remind"
                ? wcopy(locale, "taskOfferReminding")
                : wcopy(locale, "taskOfferRemind")}
            </button>
          )}
          {(taskOffer.status === "pending" || taskOffer.status === "changes_requested") && (
            <button
              type="button"
              className="is-danger"
              disabled={busyAction !== null}
              onClick={() => onWithdraw(offer)}
            >
              <Icon name="x" size={13} />
              {busyAction === "withdraw"
                ? wcopy(locale, "taskOfferWithdrawing")
                : wcopy(locale, "taskOfferWithdraw")}
            </button>
          )}
          {taskOffer.status === "accepted" && taskOffer.task_front_id !== null && (
            <button
              type="button"
              className="is-primary"
              onClick={() => onOpenTask(taskOffer.task_front_id as string)}
            >
              <Icon name="message" size={13} />
              {wcopy(locale, "taskOfferOpenTask")}
            </button>
          )}
        </div>
      </footer>

      {taskOffer.status === "accepted" && taskOffer.task_front_id === null && (
        <div className="pw-task-offer-error" role="alert">
          <Icon name="info" size={13} />
          <span>{wcopy(locale, "taskOfferAcceptedBroken")}</span>
        </div>
      )}
      {actionError !== null && (
        <div className={`pw-task-offer-error${actionError.conflict ? " is-conflict" : ""}`} role="alert">
          <Icon name="info" size={13} />
          <span>{actionError.message}</span>
        </div>
      )}
    </article>
  );
}

export function LivingCenterPane({
  base,
  center,
  subjectEngramId,
  onOpenSidebar,
  onOpenRail,
  onDirectoryRefresh,
  onSelectLife,
  onNewTaskForSubject,
  onSelectTask,
}: {
  base: string;
  center: ActivityCenterSummary;
  subjectEngramId: string | null;
  onOpenSidebar: () => void;
  onOpenRail: () => void;
  onDirectoryRefresh: () => void;
  onSelectLife: (centerId: string, subjectEngramId?: string) => void;
  onNewTaskForSubject: (subjectEngramId: string) => void;
  onSelectTask: (frontId: string) => void;
}) {
  const { locale } = useI18n();
  const [detail, setDetail] = useState<ActivityCenterDetail | null>(null);
  const [detailState, setDetailState] = useState<"loading" | "ready" | "error">("loading");
  const [detailError, setDetailError] = useState<string | null>(null);
  const [portfolio, setPortfolio] = useState<LivingPortfolio | null>(null);
  const [portfolioState, setPortfolioState] = useState<"loading" | "ready" | "error">("loading");
  const [portfolioError, setPortfolioError] = useState<string | null>(null);
  const [purposeAmendments, setPurposeAmendments] = useState<PurposeAmendmentsProjection | null>(null);
  const [purposeAmendmentsState, setPurposeAmendmentsState] = useState<"loading" | "ready" | "error">("loading");
  const [purposeAmendmentsError, setPurposeAmendmentsError] = useState<string | null>(null);
  const [taskOffers, setTaskOffers] = useState<TaskOfferSummary[]>([]);
  const [offersState, setOffersState] = useState<"loading" | "ready" | "error">("loading");
  const [offersError, setOffersError] = useState<string | null>(null);
  const [offerMutation, setOfferMutation] = useState<OfferMutationState | null>(null);
  const [offerActionError, setOfferActionError] = useState<OfferActionError | null>(null);
  const [pendingUpdate, setPendingUpdate] = useState<string | null>(null);
  const [updateError, setUpdateError] = useState<string | null>(null);
  const [autonomyDraft, setAutonomyDraft] = useState(center.autonomy);
  const [draft, setDraft] = useState("");
  const [sending, setSending] = useState(false);
  const [sendError, setSendError] = useState<string | null>(null);
  const controllerRef = useRef<AbortController | null>(null);
  const portfolioControllerRef = useRef<AbortController | null>(null);
  const visibleCenter = detail?.activityCenter.id === center.id
    ? detail.activityCenter
    : center;
  const portfolioEngramId = subjectEngramId;
  const taskSubjectEngramId = portfolio?.subject.current_engram_id ?? subjectEngramId;

  const load = useCallback(async () => {
    controllerRef.current?.abort();
    const controller = new AbortController();
    controllerRef.current = controller;
    setDetailState((current) => current === "ready" ? "ready" : "loading");
    setDetailError(null);
    try {
      const next = await fetchActivityCenterDetail(base, center.id, controller.signal);
      if (controller.signal.aborted) return;
      setDetail(next);
      setDetailState("ready");
    } catch (cause) {
      if (controller.signal.aborted) return;
      setDetailState("error");
      setDetailError(faultText(cause));
    }
  }, [base, center.id]);

  const loadSubjectContext = useCallback(async () => {
    if (portfolioEngramId === null) {
      portfolioControllerRef.current?.abort();
      setPortfolioState("error");
      setPortfolioError(null);
      setPurposeAmendmentsState("error");
      setPurposeAmendmentsError(null);
      setOffersState("error");
      setOffersError(null);
      return;
    }
    portfolioControllerRef.current?.abort();
    const controller = new AbortController();
    portfolioControllerRef.current = controller;
    setPortfolioState((current) => current === "ready" ? "ready" : "loading");
    setPurposeAmendmentsState((current) => current === "ready" ? "ready" : "loading");
    setOffersState((current) => current === "ready" ? "ready" : "loading");
    setPortfolioError(null);
    setPurposeAmendmentsError(null);
    setOffersError(null);
    const portfolioRequest = fetchLivingPortfolio(
      base,
      portfolioEngramId,
      controller.signal,
    );
    const initialOffersRequest = fetchTaskOffers(
      base,
      portfolioEngramId,
      controller.signal,
    );
    const purposeAmendmentsRequest = fetchPurposeAmendments(
      base,
      portfolioEngramId,
      controller.signal,
    );
    const [
      portfolioResult,
      initialOffersResult,
      purposeAmendmentsResult,
    ] = await Promise.allSettled([
      portfolioRequest,
      initialOffersRequest,
      purposeAmendmentsRequest,
    ]);
    if (controller.signal.aborted) return;

    if (portfolioResult.status === "fulfilled") {
      setPortfolio(portfolioResult.value);
      setPortfolioState("ready");
    } else {
      setPortfolioState("error");
      setPortfolioError(faultText(portfolioResult.reason));
    }

    if (purposeAmendmentsResult.status === "fulfilled") {
      const holderMatches = portfolioResult.status !== "fulfilled" || (
        purposeAmendmentsResult.value.subject.current_engram_id ===
        portfolioResult.value.subject.current_engram_id
      );
      if (holderMatches) {
        setPurposeAmendments(purposeAmendmentsResult.value);
        setPurposeAmendmentsState("ready");
      } else {
        setPurposeAmendmentsState("error");
        setPurposeAmendmentsError(
          locale === "zh-CN"
            ? zhText("workbench.LivingCenterPane.line1085")
            : "The subject changed generation during refresh; waiting for a consistent snapshot.",
        );
      }
    } else {
      setPurposeAmendmentsState("error");
      setPurposeAmendmentsError(faultText(purposeAmendmentsResult.reason));
    }

    let offersResult = initialOffersResult;
    const currentSubjectEngramId = portfolioResult.status === "fulfilled"
      ? portfolioResult.value.subject.current_engram_id
      : portfolioEngramId;
    if (currentSubjectEngramId !== portfolioEngramId) {
      try {
        offersResult = {
          status: "fulfilled",
          value: await fetchTaskOffers(
            base,
            currentSubjectEngramId,
            controller.signal,
          ),
        };
      } catch (cause) {
        offersResult = { status: "rejected", reason: cause };
      }
    }
    if (controller.signal.aborted) return;
    if (offersResult.status === "fulfilled") {
      setTaskOffers(offersResult.value);
      setOffersState("ready");
    } else {
      setOffersState("error");
      setOffersError(faultText(offersResult.reason));
    }
  }, [base, locale, portfolioEngramId]);

  useEffect(() => {
    setDetail(null);
    setDetailState("loading");
    setUpdateError(null);
    setSendError(null);
    setDraft("");
    void load();
    const timer = window.setInterval(() => void load(), 3_000);
    return () => {
      window.clearInterval(timer);
      controllerRef.current?.abort();
    };
  }, [load]);

  useEffect(() => {
    setPortfolio(null);
    setPortfolioError(null);
    setPurposeAmendments(null);
    setPurposeAmendmentsError(null);
    setTaskOffers([]);
    setOffersError(null);
    setOfferActionError(null);
    if (portfolioEngramId === null) {
      setPortfolioState("error");
      setPurposeAmendmentsState("error");
      setOffersState("error");
      return () => portfolioControllerRef.current?.abort();
    }
    setPortfolioState("loading");
    setPurposeAmendmentsState("loading");
    setOffersState("loading");
    void loadSubjectContext();
    const timer = window.setInterval(() => void loadSubjectContext(), 3_000);
    return () => {
      window.clearInterval(timer);
      portfolioControllerRef.current?.abort();
    };
  }, [loadSubjectContext, portfolioEngramId]);

  useEffect(() => {
    setAutonomyDraft(visibleCenter.autonomy);
  }, [visibleCenter.autonomy, visibleCenter.id]);

  const concerns = useMemo(() => {
    const rows = detail?.livingConcerns ?? [];
    return [...rows].sort((left, right) => {
      const rank = { revisit: 0, quiet: 1, resolved: 2 } as Record<string, number>;
      return (rank[left.disposition] ?? 3) - (rank[right.disposition] ?? 3) ||
        (right.updated_at ?? "").localeCompare(left.updated_at ?? "");
    });
  }, [detail?.livingConcerns]);

  const mutate = useCallback(async (label: string, updates: ActivityCenterUpdate) => {
    if (pendingUpdate !== null) return;
    setPendingUpdate(label);
    setUpdateError(null);
    try {
      const updated = await updateActivityCenter(base, center.id, updates);
      setDetail((current) => current === null
        ? current
        : { ...current, activityCenter: updated });
      onDirectoryRefresh();
      await Promise.all([load(), loadSubjectContext()]);
    } catch (cause) {
      setUpdateError(faultText(cause));
    } finally {
      setPendingUpdate(null);
    }
  }, [base, center.id, load, loadSubjectContext, onDirectoryRefresh, pendingUpdate]);

  const runOfferMutation = useCallback(async (
    offer: TaskOfferSummary,
    action: OfferMutationAction,
    operation: () => Promise<unknown>,
  ) => {
    if (offerMutation !== null) return;
    const offerId = offer.taskOffer.id;
    setOfferMutation({ offerId, action });
    setOfferActionError(null);
    try {
      await operation();
      await loadSubjectContext();
      onDirectoryRefresh();
    } catch (cause) {
      const raw = faultText(cause);
      const normalized = raw.toLowerCase();
      const conflict = normalized.includes("task_offer_revision_conflict") ||
        (normalized.includes("revision") && normalized.includes("conflict")) ||
        normalized.includes("expected revision");
      setOfferActionError({
        offerId,
        conflict,
        message: conflict ? `${wcopy(locale, "taskOfferConflict")} ${raw}` : raw,
      });
      await loadSubjectContext();
    } finally {
      setOfferMutation(null);
    }
  }, [loadSubjectContext, locale, offerMutation, onDirectoryRefresh]);

  const remindOffer = useCallback((offer: TaskOfferSummary) => {
    void runOfferMutation(offer, "remind", () => remindTaskOffer(
      base,
      offer.taskOffer.id,
      offer.taskOffer.current_revision,
    ));
  }, [base, runOfferMutation]);

  const withdrawOffer = useCallback((offer: TaskOfferSummary) => {
    void runOfferMutation(offer, "withdraw", () => withdrawTaskOffer(
      base,
      offer.taskOffer.id,
      offer.taskOffer.current_revision,
    ));
  }, [base, runOfferMutation]);

  const reviseOffer = useCallback((offer: TaskOfferSummary, content: string) => {
    void runOfferMutation(offer, "revise", () => reviseTaskOffer(
      base,
      offer.taskOffer.id,
      {
        expectedRevision: offer.taskOffer.current_revision,
        content,
        title: offer.currentRevision.title,
        projectId: offer.currentRevision.project_id,
      },
    ));
  }, [base, runOfferMutation]);

  const send = useCallback(async () => {
    const content = draft.trim();
    if (content === "" || sending) return;
    setSending(true);
    setSendError(null);
    try {
      await sendActivityCenterMessage(base, center.id, content);
      setDraft("");
      onDirectoryRefresh();
      await load();
    } catch (cause) {
      setSendError(faultText(cause));
    } finally {
      setSending(false);
    }
  }, [base, center.id, draft, load, onDirectoryRefresh, sending]);

  const onComposerKey = (event: KeyboardEvent<HTMLTextAreaElement>) => {
    if (event.key === "Enter" && !event.shiftKey && !event.nativeEvent.isComposing) {
      event.preventDefault();
      void send();
    }
  };

  const writable = visibleCenter.status === "active" || visibleCenter.status === "dormant";
  const summary = detail?.activitySummary ?? null;

  return (
    <section className="pw-conversation pw-life-center-pane">
      <header className="pw-conversation-head pw-life-center-head">
        <button className="pw-icon-button pw-mobile-toggle" aria-label={wcopy(locale, "expandSidebar")} onClick={onOpenSidebar}>
          <Icon name="panelLeft" />
        </button>
        <div className="pw-session-heading">
          <Icon name="spark" size={17} />
          <div>
            <span className="pw-session-title">{visibleCenter.title}</span>
            <span className="pw-session-nickname">{locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line1288") : "Life Center"}</span>
          </div>
        </div>
        <div className="pw-environment">
          <span><Icon name="globe" size={15} />Center</span>
          <span>{Math.round(visibleCenter.autonomy * 100)}% {locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line1293") : "rhythm weight"}</span>
          <span className={`pw-session-status pw-status-${visibleCenter.status}`}>
            {statusLabel(locale, visibleCenter.status)}
          </span>
        </div>
        <button className="pw-icon-button pw-mobile-toggle" aria-label={wcopy(locale, "expandRail")} onClick={onOpenRail}>
          <Icon name="panelRight" />
        </button>
      </header>

      <div className="pw-conversation-scroll">
        <div className="pw-life-center-inner">
          {detailState === "loading" && detail === null && (
            <div className="pw-conversation-loading"><span /><span /><span /></div>
          )}
          {detailState === "error" && detail === null && (
            <div className="pw-conversation-error">
              <Icon name="info" />
              <div><strong>{wcopy(locale, "runtimeUnavailable")}</strong><span>{detailError}</span></div>
              <button onClick={() => void load()}>{wcopy(locale, "retry")}</button>
            </div>
          )}
          {detailState === "error" && detail !== null && (
            <div className="pw-center-refresh-error">
              <Icon name="info" size={14} />
              <span>{detailError}</span>
              <button onClick={() => void load()}>{wcopy(locale, "retry")}</button>
            </div>
          )}
          {detail !== null && (
            <>
              <section className="pw-life-hero">
                <div className="pw-life-hero-copy">
                  <span className="pw-life-eyebrow">{visibleCenter.kind.replaceAll("_", " ")} · {visibleCenter.origin}</span>
                  <h1>{visibleCenter.title}</h1>
                  <p>{visibleCenter.description || (locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line1328") : "This Center has no written meaning yet; it may still exist quietly.")}</p>
                </div>
                <div className="pw-life-controls" aria-label={locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line1330") : "Life Center controls"}>
                  <button
                    type="button"
                    className="pw-subject-task-action is-primary"
                    disabled={taskSubjectEngramId === null}
                    title={taskSubjectEngramId === null
                      ? wcopy(locale, "taskSubjectUnavailable")
                      : wcopy(locale, "giveSubjectTask")}
                    onClick={() => {
                      if (taskSubjectEngramId !== null) {
                        onNewTaskForSubject(taskSubjectEngramId);
                      }
                    }}
                  >
                    <Icon name="message" size={13} />
                    {wcopy(locale, "giveSubjectTask")}
                  </button>
                  {visibleCenter.status === "active" ? (
                    <>
                      <button disabled={pendingUpdate !== null} onClick={() => void mutate("paused", { status: "paused" })}>{locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line1349") : "Pause"}</button>
                      <button disabled={pendingUpdate !== null} onClick={() => void mutate("dormant", { status: "dormant" })}>{locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line1350") : "Dormant"}</button>
                      <button disabled={pendingUpdate !== null} onClick={() => void mutate("completed", { status: "completed" })}>{locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line1351") : "Complete phase"}</button>
                    </>
                  ) : visibleCenter.status !== "archived" ? (
                    <button className="is-primary" disabled={pendingUpdate !== null} onClick={() => void mutate("active", { status: "active" })}>{locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line1354") : "Resume"}</button>
                  ) : null}
                  <label
                    className="pw-autonomy-control"
                    title={locale === "zh-CN"
                      ? zhText("workbench.LivingCenterPane.line1359")
                      : "Adjusts spontaneous activity rhythm; it does not measure agency."}
                  >
                    <span>{locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line1362") : "Spontaneous rhythm weight"}</span>
                    <input
                      type="range"
                      min={0}
                      max={1}
                      step={0.01}
                      value={autonomyDraft}
                      aria-label={locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line1369") : "Spontaneous rhythm weight"}
                      onChange={(event) => setAutonomyDraft(Number(event.target.value))}
                    />
                    <output>{Math.round(autonomyDraft * 100)}%</output>
                    <button
                      disabled={pendingUpdate !== null || autonomyDraft === visibleCenter.autonomy}
                      onClick={() => void mutate("autonomy", { autonomy: autonomyDraft })}
                    >{locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line1376") : "Apply"}</button>
                  </label>
                </div>
                {updateError !== null && <div className="pw-inline-error">{updateError}</div>}
              </section>

              <div className="pw-center-scope-note">
                <Icon name="globe" size={15} />
                <span>
                  {locale === "zh-CN"
                    ? (zhText("workbench.LivingCenterPane.line1386.head") + (detail.unattributedHistoryCount > 0
                      ? zhText("workbench.LivingCenterPane.unattributedHistoryPrefix")
                        + detail.unattributedHistoryCount
                        + zhText("workbench.LivingCenterPane.unattributedHistorySuffix")
                      : zhText("workbench.LivingCenterPane.noRelabeledHistory")))
                    : `Only durable moments attributed to this Center appear here.${detail.unattributedHistoryCount > 0 ? ` ${detail.unattributedHistoryCount} older unattributed Engram moments remain separate.` : " Other Engram life is not relabeled as Center history."}`}
                </span>
              </div>

              <LivingPortfolioSection
                portfolio={portfolio}
                purposeAmendments={purposeAmendments}
                purposeAmendmentsState={purposeAmendmentsState}
                purposeAmendmentsError={purposeAmendmentsError}
                state={portfolioState}
                error={portfolioError}
                hasHolder={portfolioEngramId !== null}
                currentCenterId={visibleCenter.id}
                locale={locale}
                onRetry={() => void loadSubjectContext()}
                onSelectLife={onSelectLife}
              />

              <section
                className="pw-life-section pw-task-offers-section"
                data-task-offer-limit="50"
              >
                <header>
                  <div>
                    <span>{wcopy(locale, "taskOffers")}</span>
                    <small>{wcopy(locale, "taskOffersHint")}</small>
                  </div>
                  <strong>{taskOffers.length}</strong>
                </header>
                <div className="pw-task-offer-boundary">
                  <Icon name="check" size={14} />
                  <span>{wcopy(locale, "taskOfferNoPrework")}</span>
                </div>
                {offersState === "loading" && taskOffers.length === 0 && (
                  <div className="pw-portfolio-loading">
                    <span className="pw-mini-spinner" />
                    <span>{wcopy(locale, "taskOffersLoading")}</span>
                  </div>
                )}
                {offersState === "error" && (
                  <div className={`pw-portfolio-error${taskOffers.length > 0 ? " is-stale" : ""}`} role="alert">
                    <Icon name="info" size={15} />
                    <div>
                      <strong>{wcopy(locale, "taskOffersRefreshFailed")}</strong>
                      <span>
                        {taskOffers.length > 0
                          ? wcopy(locale, "taskOffersShowingLastSnapshot")
                          : offersError}
                      </span>
                    </div>
                    <button type="button" onClick={() => void loadSubjectContext()}>
                      {wcopy(locale, "taskOfferReload")}
                    </button>
                  </div>
                )}
                {offersState === "ready" && taskOffers.length === 0 && (
                  <div className="pw-life-empty">
                    <Icon name="message" size={18} />
                    <span>{wcopy(locale, "noTaskOffers")}</span>
                  </div>
                )}
                {taskOffers.length > 0 && (
                  <div className="pw-task-offer-list">
                    {taskOffers.map((offer) => (
                      <TaskOfferCard
                        key={`${offer.taskOffer.id}:${offer.taskOffer.current_revision}`}
                        offer={offer}
                        locale={locale}
                        mutation={offerMutation}
                        error={offerActionError}
                        onRemind={remindOffer}
                        onWithdraw={withdrawOffer}
                        onRevise={reviseOffer}
                        onOpenTask={onSelectTask}
                      />
                    ))}
                  </div>
                )}
              </section>

              <section className="pw-life-summary" aria-label={locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line1467") : "Recent activity"}>
                <div><span>{locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line1468") : "Last activity"}</span><strong>{formatMoment(summary?.last_event_at ?? null, locale)}</strong></div>
                <div><span>{locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line1469") : "In motion"}</span><strong>{(summary?.queued ?? 0) + (summary?.running ?? 0)}</strong></div>
                <div><span>{locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line1470") : "Living concerns"}</span><strong>{detail.livingConcernsTotal}</strong></div>
                <div><span>{locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line1471") : "Recent source"}</span><strong>{summary?.recent_source ?? "—"}</strong></div>
              </section>

              {(summary?.uncertain ?? 0) > 0 && (
                <div className="pw-needs-decision">
                  <Icon name="info" size={17} />
                  <div>
                    <strong>{locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line1478") : "This Center needs a decision"}</strong>
                    <span>{locale === "zh-CN" ? ("" + String(summary?.uncertain) + zhText("workbench.LivingCenterPane.line1479.tail1")) : `${summary?.uncertain} external outcomes remain uncertain; reconcile them in the Center-scoped timeline.`}</span>
                  </div>
                </div>
              )}

              <LivingOrientationSection
                orientations={detail.livingOrientations}
                total={detail.livingOrientationsTotal}
                truncated={detail.livingOrientationsTruncated}
                locale={locale}
              />

              <section className="pw-life-section">
                <header><div><span>{locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line1492") : "Living concerns"}</span><small>{locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line1492.2") : "Natural language the agent chose to keep carrying"}</small></div><strong>{detail.livingConcernsTotal}</strong></header>
                {concerns.length === 0 ? (
                  <div className="pw-life-empty"><Icon name="spark" size={18} /><span>{locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line1494") : "No concerns have been held by the agent yet."}</span></div>
                ) : (
                  <div className="pw-concern-grid">{concerns.map((concern) => <ConcernCard key={concern.id} concern={concern} />)}</div>
                )}
                {detail.livingConcernsTruncated && <small className="pw-life-truncated">{locale === "zh-CN" ? (zhText("workbench.LivingCenterPane.line1498.head") + String(concerns.length) + zhText("workbench.LivingCenterPane.line1498.tail1") + String(detail.livingConcernsTotal) + zhText("workbench.LivingCenterPane.line1498.tail2")) : `Showing ${concerns.length} of ${detail.livingConcernsTotal} concerns.`}</small>}
              </section>

              <section className="pw-life-section pw-life-moments">
                <header><div><span>{locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line1502") : "Living moments"}</span><small>{locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line1502.2") : "Stimulus, spontaneity, propagation, and response"}</small></div><strong>{detail.messages.length}</strong></header>
                {detail.messages.length === 0 ? (
                  <div className="pw-life-empty"><HexMark size={34} /><span>{locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line1504") : "It exists quietly; new stimuli or autonomous activity will leave moments here."}</span></div>
                ) : detail.messages.map((message) => <CenterMoment key={message.event_id} message={message} centerTitle={visibleCenter.title} />)}
              </section>
            </>
          )}
        </div>
      </div>

      <div className="pw-composer-zone">
        <div className={`pw-composer${sendError !== null ? " has-error" : ""}`}>
          <textarea
            rows={2}
            value={draft}
            disabled={sending || !writable}
            placeholder={writable
              ? locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line1519") : "Offer this Life Center a new stimulus…"
              : locale === "zh-CN" ? zhText("workbench.LivingCenterPane.line1520") : "Resume this Center before adding a stimulus"}
            onChange={(event) => setDraft(event.target.value)}
            onKeyDown={onComposerKey}
          />
          <div className="pw-composer-foot">
            <div className="pw-composer-tools">
              <span><Icon name="spark" size={15} />{wcopy(locale, "explicitStimulus")}</span>
              <span className="pw-composer-hint">{sendError ?? wcopy(locale, "composerHint")}</span>
            </div>
            <button className="pw-send-button" aria-label={sending ? wcopy(locale, "sending") : wcopy(locale, "send")} disabled={draft.trim() === "" || sending || !writable} onClick={() => void send()}>
              {sending ? <span className="pw-send-spinner" /> : <Icon name="send" size={17} />}
            </button>
          </div>
        </div>
      </div>
    </section>
  );
}
