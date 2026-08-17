import assert from "node:assert/strict";

const { scheduleActionTimeout } = await import(
	new URL("../../src/pulse_system/agent/extensions/pulse-timeout.ts", import.meta.url).href,
);

let timerCallback;
let cleared = false;
const timers = {
	setTimeout(callback, timeoutMs) {
		assert.equal(timeoutMs, 17);
		timerCallback = callback;
		return "timer-1";
	},
	clearTimeout(handle) {
		assert.equal(handle, "timer-2");
		cleared = true;
	},
};

const cancellationReasons = [];
const cleanup = scheduleActionTimeout(
	17,
	() => cancellationReasons.push("mutable-fetch-timeout-cancel"),
	{
		setTimeout: (callback, timeoutMs) => {
			assert.equal(timeoutMs, 17);
			timerCallback = callback;
			return "timer-2";
		},
		clearTimeout: timers.clearTimeout,
	},
);
timerCallback();
cleanup();
assert.deepEqual(cancellationReasons, ["mutable-fetch-timeout-cancel"]);
assert.equal(cleared, false);

let secondCallback;
let secondCleared = false;
const secondCleanup = scheduleActionTimeout(
	17,
	() => cancellationReasons.push("should-not-fire"),
	{
		setTimeout: (callback) => {
			secondCallback = callback;
			return "timer-3";
		},
		clearTimeout: (handle) => {
			assert.equal(handle, "timer-3");
			secondCleared = true;
		},
	},
);
secondCleanup();
secondCallback();
assert.equal(secondCleared, true);
assert.deepEqual(cancellationReasons, ["mutable-fetch-timeout-cancel"]);

console.log("pulse_extension_timeout_ok");
