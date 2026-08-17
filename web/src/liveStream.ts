/** One EventSource per normalized URL and browser tab.
 *
 * Viewer state and RuntimeRail both consume /events.  They subscribe here so
 * mounting the rail cannot create a second socket or a second replay.  The
 * entry owns the native EventSource until its final subscriber releases it.
 */

export type SharedStreamState = "connecting" | "open" | "error";

export interface LiveFrame {
  event: string;
  data: string;
  lastEventId: string;
}

export interface LiveStreamSubscriber {
  onFrame(frame: LiveFrame): void;
  onStatus?(state: SharedStreamState): void;
}

interface StreamEntry {
  source: EventSource;
  state: SharedStreamState;
  subscribers: Set<LiveStreamSubscriber>;
}

const FRAME_NAMES = [
  "replay",
  "snapshot",
  "append",
  "pulse",
  "propagate",
  "heartbeat",
  "tuning_applied",
  "delegation",
];

const streams = new Map<string, StreamEntry>();

function normalizeUrl(url: string): string {
  return new URL(url, window.location.href).href;
}

function notifyStatus(entry: StreamEntry, state: SharedStreamState): void {
  entry.state = state;
  for (const subscriber of entry.subscribers) subscriber.onStatus?.(state);
}

function notifyFrame(entry: StreamEntry, event: string, raw: MessageEvent): void {
  const frame: LiveFrame = {
    event,
    data: raw.data,
    lastEventId: raw.lastEventId,
  };
  for (const subscriber of entry.subscribers) subscriber.onFrame(frame);
}

function createEntry(url: string): StreamEntry {
  const source = new EventSource(url);
  const entry: StreamEntry = {
    source,
    state: "connecting",
    subscribers: new Set(),
  };
  source.onopen = () => notifyStatus(entry, "open");
  source.onerror = () => notifyStatus(entry, "error");
  source.onmessage = (event) => notifyFrame(entry, "message", event);
  for (const name of FRAME_NAMES) {
    source.addEventListener(name, (event) =>
      notifyFrame(entry, name, event as MessageEvent),
    );
  }
  return entry;
}

/** Subscribe to a tab-local shared stream; release is idempotent. */
export function subscribeLiveStream(
  url: string,
  subscriber: LiveStreamSubscriber,
): () => void {
  const key = normalizeUrl(url);
  let entry = streams.get(key);
  if (entry === undefined) {
    entry = createEntry(key);
    streams.set(key, entry);
  }
  entry.subscribers.add(subscriber);
  subscriber.onStatus?.(entry.state);

  let released = false;
  return () => {
    if (released) return;
    released = true;
    entry?.subscribers.delete(subscriber);
    if (entry !== undefined && entry.subscribers.size === 0) {
      entry.source.close();
      if (streams.get(key) === entry) streams.delete(key);
    }
  };
}
