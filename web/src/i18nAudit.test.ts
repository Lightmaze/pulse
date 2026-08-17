// @ts-nocheck
import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import test from "node:test";
import { enMessages } from "./locales/en.ts";
import { zhCNMessages } from "./locales/zh-CN.ts";
import { zhUi } from "./locales/zh-ui.ts";
import { workbenchEn } from "./workbench/locales/en.ts";
import { workbenchZhCN } from "./workbench/locales/zh-CN.ts";

const read = (relative: string) =>
  readFileSync(new URL(relative, import.meta.url), "utf8").replace(/\r\n?/g, "\n");

const sources = {
  i18n: read("./i18n.tsx"),
  globalEn: read("./locales/en.ts"),
  globalZhCN: read("./locales/zh-CN.ts"),
  componentZhCN: read("./locales/zh-ui.ts"),
  workbenchModel: read("./workbench/model.ts"),
  workbenchEn: read("./workbench/locales/en.ts"),
  workbenchZhCN: read("./workbench/locales/zh-CN.ts"),
  settings: read("./pages/SettingsPage.tsx"),
  cost: read("./pages/CostPage.tsx"),
  sidebar: read("./components/Sidebar.tsx"),
  statusBar: read("./components/StatusBar.tsx"),
  inspector: read("./components/Inspector.tsx"),
  timeline: read("./components/Timeline.tsx"),
  runtimeRail: read("./workbench/RuntimeRail.tsx"),
  app: read("./App.tsx"),
};

function uiSources(directory = new URL("./", import.meta.url)): string[] {
  const output: string[] = [];
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    if (entry.name === "locales" || entry.name.endsWith(".test.ts") || entry.name.endsWith(".test.tsx")) continue;
    const target = new URL(entry.name + (entry.isDirectory() ? "/" : ""), directory);
    if (entry.isDirectory()) output.push(...uiSources(target));
    else if (entry.name.endsWith(".ts") || entry.name.endsWith(".tsx")) output.push(readFileSync(target, "utf8"));
  }
  return output;
}

function placeholders(value: string): string[] {
  return [...value.matchAll(/\{([A-Za-z][A-Za-z0-9]*)\}/g)]
    .map((match) => match[1])
    .sort();
}

function assertCatalogParity(
  english: Record<string, string>,
  chinese: Record<string, string>,
  label: string,
): void {
  assert.deepEqual(Object.keys(english).sort(), Object.keys(chinese).sort(), `${label} keys`);
  for (const key of Object.keys(english)) {
    assert.deepEqual(placeholders(english[key]), placeholders(chinese[key]), `${label}.${key}`);
  }
}

test("English and Chinese catalogs are physically separate and structurally equal", () => {
  assertCatalogParity(enMessages, zhCNMessages, "global catalog");
  assertCatalogParity(workbenchEn, workbenchZhCN, "Workbench catalog");
  assert.doesNotMatch(sources.globalEn, /[\u3400-\u9fff]/u);
  assert.doesNotMatch(sources.workbenchEn, /[\u3400-\u9fff]/u);
  assert.match(sources.globalZhCN, /[\u3400-\u9fff]/u);
  assert.match(sources.workbenchZhCN, /[\u3400-\u9fff]/u);
  assert.doesNotMatch(sources.i18n, /[\u3400-\u9fff]/u);
  assert.doesNotMatch(sources.workbenchModel, /[\u3400-\u9fff]/u);
});

test("component Chinese catalog has exact literal usage", () => {
  const calls = new Set<string>();
  for (const source of uiSources()) {
    for (const match of source.matchAll(/zhText\("([^"]+)"\)/g)) calls.add(match[1]);
  }
  assert.deepEqual([...calls].sort(), Object.keys(zhUi).sort());
});

test("localized sources are valid text without common mojibake markers", () => {
  const mojibake = /\uFFFD|\u951F\u65A4\u62F7|\u00C3.|\u00C2.|\u00E2\u20AC/u;
  for (const [name, source] of Object.entries(sources)) {
    assert.doesNotMatch(source, mojibake, `${name} contains a likely encoding error`);
  }
});

test("language selectors use the active locale instead of simultaneous autonyms", () => {
  for (const [name, source] of [
    ["Settings", sources.settings],
    ["Sidebar", sources.sidebar],
  ] as const) {
    assert.doesNotMatch(source, /<option[^>]*>\s*English\s*<\/option>/u, name);
    assert.doesNotMatch(source, /<option[^>]*>\s*简体中文\s*<\/option>/u, name);
    assert.match(source, /t\("locale\.english"\)/u);
    assert.match(source, /t\("locale\.chinese"\)/u);
  }
  assert.equal(enMessages["locale.chinese"], "Simplified Chinese");
  assert.equal(zhCNMessages["locale.english"], "英语");
});

test("document titles and product names come from the active catalog", () => {
  assert.doesNotMatch(sources.app, /locale\s*===\s*"zh-CN"\s*\?/u);
  assert.match(sources.app, /t\("document\.settingsTitle"\)/u);
  assert.match(sources.app, /t\("document\.appTitle"\)/u);
  assert.match(sources.settings, /t\("settings\.product\.name"\)/u);
});

test("ordinary telemetry labels come from the active locale", () => {
  for (const key of [
    "cost.seriesInput",
    "cost.seriesCached",
    "cost.seriesOutput",
    "cost.inputTokens",
    "cost.cachedTokens",
    "cost.outputTokens",
    "cost.engram",
    "cost.pulses",
    "cost.input",
    "cost.cached",
    "cost.output",
  ]) {
    assert.ok(sources.cost.includes(`t("${key}")`), key);
  }
  for (const key of [
    "status.ratio",
    "status.coherent",
    "status.breadth",
    "status.pending",
    "status.pulses",
    "status.tokenInput",
    "status.tokenOutput",
    "status.cached",
    "status.engrams",
    "status.events",
  ]) {
    assert.ok(sources.statusBar.includes(`t("${key}")`), key);
  }
  assert.match(sources.inspector, /t\("inspector\.pulseCount"/u);
  assert.match(sources.inspector, /t\("inspector\.messageCount"/u);
  for (const key of [
    "timeline.heartbeatSummary",
    "timeline.coherentPart",
    "timeline.pulseSummary",
    "timeline.propagateSummary",
    "timeline.inhibitedPart",
    "timeline.resonanceSummary",
    "timeline.decaySummary",
    "timeline.pendingSummary",
  ]) {
    assert.ok(sources.timeline.includes(`translate(locale, "${key}"`), key);
  }
  assert.match(sources.runtimeRail, /wcopy\(locale, "advisorySubtitle"\)/u);
  assert.match(sources.runtimeRail, /wcopy\(locale, "companionIntervention"\)/u);
  assert.match(sources.runtimeRail, /evidenceDetail\(row, locale\)/u);
  assert.doesNotMatch(sources.runtimeRail, />Steering · companion intervention</u);
  assert.doesNotMatch(sources.runtimeRail, /detail: str\(payload\.advisory_id\) \?\? "companion intervention"/u);
});

test("Settings copy keeps browser-local preference storage honest", () => {
  assert.doesNotMatch(sources.globalEn, /Saved locally/u);
  assert.doesNotMatch(sources.globalZhCN, /已保存在本地/u);
  assert.equal(enMessages["settings.storage.localOnly"], "Browser only");
  assert.equal(zhCNMessages["settings.storage.localOnly"], "仅限浏览器");
  assert.match(sources.settings, /t\("settings\.storage\.localOnly"\)/u);
});

test("mobile workspace reserves a non-overlapping security profile strip", () => {
  const workbenchCss = read("./workbench.css");
  assert.match(workbenchCss, /\.pw-security-profile \{[\s\S]*?z-index: 80;/u);
  assert.match(
    workbenchCss,
    /@media \(max-width: 790px\)[\s\S]*?--pw-mobile-security-strip-height: 48px;[\s\S]*?\.pulse-workbench:not\(\.is-settings-page\) \.pw-main \{[\s\S]*?padding-top: var\(--pw-mobile-security-strip-height\);/u,
  );
  assert.match(
    workbenchCss,
    /\.pulse-workbench:not\(\.is-sidebar-collapsed\) \.pw-security-profile,[\s\S]*?\.pulse-workbench:not\(\.is-rail-collapsed\) \.pw-security-profile \{[\s\S]*?visibility: hidden;[\s\S]*?pointer-events: none;/u,
  );
});
