#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");

const root = path.resolve(process.argv[2] || process.cwd());
const ts = require(path.join(root, "web", "node_modules", "typescript"));
const sourceRoot = path.join(root, "web", "src");
const han = /[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]/;
const errors = [];

function visitFile(file) {
  const text = fs.readFileSync(file, "utf8");
  const source = ts.createSourceFile(
    file,
    text,
    ts.ScriptTarget.Latest,
    true,
    file.endsWith(".tsx") ? ts.ScriptKind.TSX : ts.ScriptKind.TS,
  );
  function visit(node) {
    const isVisibleText =
      ts.isStringLiteral(node)
      || ts.isNoSubstitutionTemplateLiteral(node)
      || ts.isTemplateHead(node)
      || ts.isTemplateMiddle(node)
      || ts.isTemplateTail(node)
      || ts.isJsxText(node);
    if (isVisibleText && han.test(node.text)) {
      const point = source.getLineAndCharacterOfPosition(node.getStart(source));
      errors.push(`${path.relative(root, file)}:${point.line + 1}`);
    }
    ts.forEachChild(node, visit);
  }
  visit(source);
}

function walk(directory) {
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const target = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      if (entry.name !== "locales") walk(target);
    } else if (
      (target.endsWith(".ts") || target.endsWith(".tsx"))
      && !target.endsWith(".test.ts")
      && !target.endsWith(".test.tsx")
    ) {
      visitFile(target);
    }
  }
}

walk(sourceRoot);
if (errors.length) {
  for (const error of errors) console.error(`localized text outside locale resources: ${error}`);
  process.exit(1);
}
console.log("UI language boundary passed");
