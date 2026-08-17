import { useEffect, useRef, useState, type FormEvent } from "react";
import { useI18n } from "../i18n";
import { faultText } from "../pulse";
import {
  LIFE_KINDS,
  createActivityCenter,
  type ActivityCenterSummary,
  type ActivityKind,
} from "../world";
import { HexMark, Icon } from "./Icons";
import { wcopy, type WorkbenchCopyKey } from "./model";

const KIND_COPY: Record<Exclude<ActivityKind, "task">, WorkbenchCopyKey> = {
  hobby: "hobby",
  life_project: "lifeProject",
  relationship: "relationship",
  exploration: "exploration",
  practice: "practice",
  expression: "expression",
  rest: "rest",
  other: "other",
};

export function LifeCenterDialog({
  base,
  onClose,
  onCreated,
}: {
  base: string;
  onClose: () => void;
  onCreated: (center: ActivityCenterSummary) => void;
}) {
  const { locale } = useI18n();
  const [kind, setKind] = useState<Exclude<ActivityKind, "task">>("hobby");
  const [title, setTitle] = useState("");
  const [description, setDescription] = useState("");
  const [stimulus, setStimulus] = useState("");
  const [sending, setSending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const titleRef = useRef<HTMLInputElement>(null);

  useEffect(() => titleRef.current?.focus(), []);

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    if (title.trim() === "" || sending) return;
    setSending(true);
    setError(null);
    try {
      const created = await createActivityCenter(base, {
        kind,
        title: title.trim(),
        ...(description.trim() === "" ? {} : { description: description.trim() }),
        ...(stimulus.trim() === "" ? {} : { stimulus: stimulus.trim() }),
      });
      onCreated(created.activityCenter);
    } catch (cause) {
      setError(faultText(cause));
    } finally {
      setSending(false);
    }
  };

  return (
    <div
      className="pw-dialog-backdrop"
      role="presentation"
      onMouseDown={(event) => {
        if (event.target === event.currentTarget && !sending) onClose();
      }}
    >
      <form
        className="pw-life-dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby="pw-life-dialog-title"
        onSubmit={(event) => void submit(event)}
      >
        <header>
          <HexMark tone="violet" size={38} label="∞" />
          <div>
            <h2 id="pw-life-dialog-title">{wcopy(locale, "createLifeTitle")}</h2>
            <p>{wcopy(locale, "createLifeHelp")}</p>
          </div>
          <button
            type="button"
            className="pw-icon-button"
            aria-label={wcopy(locale, "close")}
            disabled={sending}
            onClick={onClose}
          >
            <Icon name="x" size={16} />
          </button>
        </header>

        <div className="pw-life-form-grid">
          <label>
            <span>{wcopy(locale, "lifeKind")}</span>
            <select
              value={kind}
              onChange={(event) =>
                setKind(event.target.value as Exclude<ActivityKind, "task">)
              }
            >
              {LIFE_KINDS.map((value) => (
                <option key={value} value={value}>
                  {wcopy(locale, KIND_COPY[value])}
                </option>
              ))}
            </select>
          </label>
          <label>
            <span>{wcopy(locale, "lifeTitle")}</span>
            <input
              ref={titleRef}
              value={title}
              maxLength={120}
              required
              onChange={(event) => setTitle(event.target.value)}
            />
          </label>
          <label className="is-wide">
            <span>{wcopy(locale, "lifeDescription")}</span>
            <textarea
              rows={3}
              value={description}
              onChange={(event) => setDescription(event.target.value)}
            />
          </label>
          <label className="is-wide">
            <span>{wcopy(locale, "lifeStimulus")}</span>
            <textarea
              rows={3}
              value={stimulus}
              onChange={(event) => setStimulus(event.target.value)}
            />
          </label>
        </div>

        {error !== null && <div className="pw-inline-error">{error}</div>}
        <footer>
          <button type="button" disabled={sending} onClick={onClose}>
            {wcopy(locale, "cancel")}
          </button>
          <button
            type="submit"
            className="is-primary"
            disabled={sending || title.trim() === ""}
          >
            {sending ? wcopy(locale, "sending") : wcopy(locale, "create")}
          </button>
        </footer>
      </form>
    </div>
  );
}
