/**
 * Deterministic, one-shot timer seam for mutable Pi gateway requests.
 *
 * The caller owns the timeout side effect (normally a /v1/tools/cancel
 * request).  Returning an idempotent cleanup function lets a completed fetch
 * cancel the timer without racing a late callback.
 */

type TimerHandle = ReturnType<typeof globalThis.setTimeout>;

export type TimerScheduler = Pick<typeof globalThis, "setTimeout" | "clearTimeout">;

export function scheduleActionTimeout(
	timeoutMs: number,
	onTimeout: () => void,
	timers: TimerScheduler = globalThis,
): () => void {
	let active = true;
	const handle: TimerHandle = timers.setTimeout(() => {
		if (!active) return;
		active = false;
		onTimeout();
	}, timeoutMs);
	return () => {
		if (!active) return;
		active = false;
		timers.clearTimeout(handle);
	};
}
