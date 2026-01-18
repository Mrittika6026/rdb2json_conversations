#!/usr/bin/env python3
"""
Convert dump.json (exported from Redis RDB) directly into conversations.json.

Output shape:
[
  {"session_id": "...", "conversation": [ {"role": "...", "content": "..."}, ... ]},
  ...
]

We only include keys that look like chat sessions:
  chat:session_<session_id>

Usage:
  python3 dump_to_conversations.py --in dump.json --out conversations.json
"""

from __future__ import annotations

import argparse
import json
import re
from typing import Any, Dict, List, Optional, Tuple


_SESSION_KEY_RE = re.compile(r"^chat:session_(?P<sid>.+)$")


def _get_key_string(entry: Dict[str, Any]) -> Optional[str]:
    k = entry.get("key")
    if isinstance(k, dict):
        enc = k.get("encoding")
        data = k.get("data")
        if enc == "utf-8" and isinstance(data, str):
            return data
        if enc == "base64" and isinstance(data, str):
            # We can't safely decode arbitrary bytes to a string key.
            return f"base64:{data}"
    if isinstance(k, str):
        return k
    return None


def _get_utf8_value_string(entry: Dict[str, Any]) -> Optional[str]:
    v = entry.get("value")
    if not isinstance(v, dict):
        return None
    enc = v.get("encoding")
    data = v.get("data")
    if enc == "utf-8" and isinstance(data, str):
        return data
    return None


def _looks_like_conversation(obj: Any) -> bool:
    if not isinstance(obj, list) or not obj:
        return False
    # Heuristic: messages are dicts with role/content.
    n = 0
    for item in obj[:10]:
        if isinstance(item, dict) and "role" in item and "content" in item:
            n += 1
    return n >= 1


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Convert dump.json to conversations.json with session_id.")
    ap.add_argument("--in", dest="in_path", default="dump.json")
    ap.add_argument("--out", dest="out_path", default="conversations.json")
    args = ap.parse_args(argv)

    with open(args.in_path, "r", encoding="utf-8") as f:
        doc = json.load(f)

    entries = doc.get("db", {}).get("0", {}).get("entries", [])
    if not isinstance(entries, list):
        raise SystemExit("Unexpected input: doc['db']['0']['entries'] is not a list")

    out: List[Dict[str, Any]] = []
    skipped = 0
    parsed_fail = 0
    not_session_key = 0
    not_conversation = 0

    for entry in entries:
        if not isinstance(entry, dict):
            skipped += 1
            continue

        key_str = _get_key_string(entry)
        if not key_str:
            skipped += 1
            continue

        m = _SESSION_KEY_RE.match(key_str)
        if not m:
            not_session_key += 1
            continue
        session_id = m.group("sid")

        raw = _get_utf8_value_string(entry)
        if raw is None:
            skipped += 1
            continue

        try:
            parsed = json.loads(raw)
        except Exception:
            parsed_fail += 1
            continue

        if not _looks_like_conversation(parsed):
            not_conversation += 1
            continue

        out.append({"session_id": session_id, "conversation": parsed})

    with open(args.out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"wrote: {args.out_path}")
    print(f"conversations: {len(out)}")
    print(f"skipped: {skipped}")
    print(f"not_session_key: {not_session_key}")
    print(f"parsed_fail: {parsed_fail}")
    print(f"not_conversation: {not_conversation}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


