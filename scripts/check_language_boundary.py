#!/usr/bin/env python3
"""Check structural language boundaries and inventory terms needing judgment."""

from __future__ import annotations

import argparse
from pathlib import Path
import re
import sys


HAN = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]")
LATIN_WORD = re.compile(r"[A-Za-z][A-Za-z0-9+.#_-]*")
MOJIBAKE = re.compile(r"\ufffd|\u951f\u65a4\u62f7|\u00c3.|\u00c2.|\u00e2\u20ac")
FENCE = re.compile(r"^\s*(```|~~~)")
INLINE_CODE = re.compile(r"`[^`]*`")
LINK_TARGET = re.compile(r"\]\([^)]*\)")
QUOTED = re.compile(r'"((?:[^"\\]|\\.)*)"|\'((?:[^\'\\]|\\.)*)\'')


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--pair", action="append", default=[], metavar="EN=ZH")
    parser.add_argument("--english-resource", action="append", default=[])
    parser.add_argument("--chinese-resource", action="append", default=[])
    parser.add_argument("--shared-ui", action="append", default=[])
    parser.add_argument("--allow-term", action="append", default=[])
    parser.add_argument("--strict-chinese-terms", action="store_true")
    return parser.parse_args()


def read_text(root: Path, relative: str, errors: list[str]) -> str:
    path = root / relative
    if not path.is_file():
        errors.append(f"missing file: {relative}")
        return ""
    raw = path.read_bytes()
    if raw.startswith(b"\xef\xbb\xbf") or b"\x00" in raw:
        errors.append(f"not UTF-8 without BOM/NUL: {relative}")
        return ""
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        errors.append(f"invalid UTF-8: {relative}: {exc}")
        return ""
    if MOJIBAKE.search(text):
        errors.append(f"likely mojibake: {relative}")
    return text


def prose_lines(text: str):
    fenced = False
    for number, line in enumerate(text.splitlines(), 1):
        if FENCE.match(line):
            fenced = not fenced
            continue
        if fenced:
            continue
        yield number, LINK_TARGET.sub("]()", INLINE_CODE.sub("", line))


def check_english(relative: str, text: str, errors: list[str], markdown: bool) -> None:
    lines = prose_lines(text) if markdown else enumerate(text.splitlines(), 1)
    for number, line in lines:
        if HAN.search(line):
            errors.append(f"Han text in English surface: {relative}:{number}")


def check_chinese_doc(
    relative: str,
    text: str,
    errors: list[str],
    reviews: list[str],
    strict: bool,
) -> None:
    if text and not HAN.search(text):
        errors.append(f"Chinese surface has no Han text: {relative}")
    for number, line in prose_lines(text):
        words = LATIN_WORD.findall(line)
        if len(words) >= 5:
            message = f"English prose in Chinese surface: {relative}:{number}"
            (errors if strict else reviews).append(message)


def check_chinese_resource(
    relative: str,
    text: str,
    errors: list[str],
    reviews: list[str],
    allowed: set[str],
    strict: bool,
) -> None:
    if text and not HAN.search(text):
        errors.append(f"Chinese resource has no Han text: {relative}")
    for number, line in enumerate(text.splitlines(), 1):
        for match in QUOTED.finditer(line):
            value = match.group(1) if match.group(1) is not None else match.group(2)
            if not value or not HAN.search(value):
                continue
            visible = re.sub(r"\{[A-Za-z][A-Za-z0-9]*\}", "", value)
            visible = re.sub(r"(?:https?://|\?)[^\s\"']+", "", visible)
            visible = re.sub(r"(?:--|/)[A-Za-z0-9_./?=&-]+", "", visible)
            words = {word.casefold() for word in LATIN_WORD.findall(visible)}
            unexpected = sorted(words - allowed)
            if unexpected:
                message = (
                    f"Latin terms in Chinese UI value: {relative}:{number}: "
                    + ", ".join(unexpected)
                )
                (errors if strict else reviews).append(message)


def main() -> int:
    args = parse_args()
    root = args.root.resolve()
    errors: list[str] = []
    reviews: list[str] = []
    allowed = {
        word.casefold()
        for term in args.allow_term
        for word in LATIN_WORD.findall(term)
    }

    for pair in args.pair:
        if "=" not in pair:
            errors.append(f"invalid pair (expected EN=ZH): {pair}")
            continue
        english, chinese = pair.split("=", 1)
        check_english(english, read_text(root, english, errors), errors, True)
        check_chinese_doc(
            chinese,
            read_text(root, chinese, errors),
            errors,
            reviews,
            args.strict_chinese_terms,
        )

    for relative in args.english_resource:
        check_english(relative, read_text(root, relative, errors), errors, False)
    for relative in args.chinese_resource:
        check_chinese_resource(
            relative,
            read_text(root, relative, errors),
            errors,
            reviews,
            allowed,
            args.strict_chinese_terms,
        )
    for relative in args.shared_ui:
        text = read_text(root, relative, errors)
        if HAN.search(text):
            reviews.append(f"localized text remains in shared UI source: {relative}")
        if re.search(r"<option[^>]*>\s*(?:English|Simplified Chinese)\s*</option>", text):
            errors.append(f"hard-coded language selector in shared UI source: {relative}")

    for review in reviews:
        print(f"REVIEW: {review}")
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print(f"language boundary check passed ({len(reviews)} review items)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
