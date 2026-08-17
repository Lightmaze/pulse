import { useRef } from "react";
import { useI18n } from "../i18n";
import { useViewer } from "../store";

const SPEEDS = [1, 5, 20, 60, 300, 1200];
const SCRUB_STEPS = 10_000;

function fmtDuration(ms: number): string {
  const totalSec = Math.floor(ms / 1000);
  const h = Math.floor(totalSec / 3600);
  const m = Math.floor((totalSec % 3600) / 60);
  const s = totalSec % 60;
  const mmss = `${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
  return h > 0 ? `${h}:${mmss}` : mmss;
}

export function Controls({ onFile }: { onFile: (file: File) => void }) {
  const { t } = useI18n();
  const fileRef = useRef<HTMLInputElement>(null);
  const playing = useViewer((s) => s.playing);
  const speed = useViewer((s) => s.speed);
  const hasRun = useViewer((s) => s.run !== null);
  const mode = useViewer((s) => s.mode);
  const liveStatus = useViewer((s) => s.liveStatus);
  const liveUrl = useViewer((s) => s.liveUrl);
  const replayWindow = useViewer((s) => s.replayWindow);
  const following = useViewer((s) => s.following);
  const live = mode === "live";
  const scrub = useViewer((s) => {
    const r = s.run;
    if (r === null) return 0;
    const span = Math.max(r.tMaxMs - r.tMinMs, 1);
    return Math.round(((s.cursorMs - r.tMinMs) / span) * SCRUB_STEPS);
  });
  const clock = useViewer((s) => {
    const r = s.run;
    if (r === null) return "";
    const wall = new Date(s.cursorMs).toISOString().slice(11, 21);
    return `${wall}Z · T+${fmtDuration(s.cursorMs - r.tMinMs)} / ${fmtDuration(r.tMaxMs - r.tMinMs)}`;
  });

  const liveLabel: Record<string, string> = {
    off: t("controls.offline"),
    connecting: t("controls.connecting"),
    open: "● LIVE",
    error: t("controls.reconnecting"),
  };

  return (
    <div className="controls">
      <button onClick={() => fileRef.current?.click()}>{t("controls.open")}</button>
      <input
        ref={fileRef}
        type="file"
        accept=".jsonl,.log,.txt,application/jsonl"
        style={{ display: "none" }}
        onChange={(e) => {
          const file = e.target.files?.[0];
          if (file !== undefined) onFile(file);
          e.target.value = "";
        }}
      />
      {live ? (
        <>
          <span className={`live-badge ${liveStatus}`}>
            {liveLabel[liveStatus] ?? liveStatus}
          </span>
          {replayWindow?.truncated === true ? (
            <span
              className="replay-badge"
              title={`${replayWindow.start_offset}–${replayWindow.end_offset} / ${replayWindow.file_size} bytes`}
            >
              {t("controls.partialHistory")}
            </span>
          ) : null}
          <button
            className={following ? "follow on" : "follow"}
            onClick={() => useViewer.getState().setFollowing(!following)}
          >
            {following ? t("controls.follow") : t("controls.parked")}
          </button>
          <button onClick={() => useViewer.getState().disconnectLive()}>
            {t("controls.disconnect")}
          </button>
        </>
      ) : (
        <>
          <button
            onClick={() => useViewer.getState().connectLive(liveUrl)}
            title={liveUrl}
          >
            {t("controls.connectLive")}
          </button>
          <button
            disabled={!hasRun}
            onClick={() => useViewer.getState().setPlaying(!playing)}
          >
            {playing ? t("controls.pause") : t("controls.play")}
          </button>
          <select
            value={speed}
            onChange={(e) => useViewer.getState().setSpeed(Number(e.target.value))}
          >
            {SPEEDS.map((s) => (
              <option key={s} value={s}>
                {s}×
              </option>
            ))}
          </select>
        </>
      )}
      <input
        className="scrub"
        type="range"
        min={0}
        max={SCRUB_STEPS}
        value={scrub}
        disabled={!hasRun}
        onChange={(e) => {
          const s = useViewer.getState();
          if (s.run === null) return;
          const span = s.run.tMaxMs - s.run.tMinMs;
          s.seek(s.run.tMinMs + (Number(e.target.value) / SCRUB_STEPS) * span);
        }}
      />
      <span className="clock">{clock}</span>
    </div>
  );
}
