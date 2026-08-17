// @ts-nocheck

import assert from "node:assert/strict";
import test from "node:test";

const {
  shutdownEvidenceStateLabel,
  successionCapacityPresentation,
} = await import("./model.ts");

const capacity = {
  succession_worker_limit: 4,
  succession_workers_running: 2,
  succession_subjects_pending: 3,
  succession_subjects_blocked: 1,
};

test("succession capacity presentation exposes only aggregate execution-domain counts", () => {
  assert.deepEqual(successionCapacityPresentation("en", capacity), {
    primary: "2 / 4",
    secondary: "running / limit · 3 pending · 1 blocked",
    ariaLabel: "Succession execution domain: 2 running, 4 limit, 3 pending, 1 blocked",
  });
});

test("succession capacity presentation keeps the compact Chinese labels", () => {
  assert.deepEqual(successionCapacityPresentation("zh-CN", capacity), {
    primary: "2 / 4",
    secondary: "运行 / 上限 · 3 待执行 · 1 受阻",
    ariaLabel: "继承执行域: 2 运行, 4 上限, 3 待执行, 1 受阻",
  });
});

test("shutdown evidence labels keep root exit and uncertainty explicit in both locales", () => {
  assert.deepEqual(
    ["open", "closing", "closed"].map((state) => shutdownEvidenceStateLabel("en", state)),
    ["Open", "Closing", "Closed"],
  );
  assert.deepEqual(
    ["open", "closing", "closed"].map((state) => shutdownEvidenceStateLabel("zh-CN", state)),
    ["运行中", "关停中", "已关闭"],
  );
  assert.equal(shutdownEvidenceStateLabel("en", "uncertain"), "Uncertain");
  assert.equal(shutdownEvidenceStateLabel("en", "root_exit_only"), "Root exit only");
  assert.equal(shutdownEvidenceStateLabel("zh-CN", "uncertain"), "不确定");
  assert.equal(shutdownEvidenceStateLabel("zh-CN", "root_exit_only"), "仅根进程退出");
});
