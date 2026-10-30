#!/usr/bin/env python3
"""Apply exact literal replacements described by a JSON spec.

Usage: apply_edits.py SPEC.json
SPEC = [{"path": ..., "old_b64": ..., "new_b64": ..., "count": 1}, ...]
All edits are applied in order to an in-memory copy of each file, so several
entries may target the same path. Every entry must match exactly `count`
times (default 1) or the tool aborts without writing anything. File bytes are
preserved verbatim (no newline translation), so CRLF files stay CRLF.
"""
import base64
import json
import sys

spec = json.load(open(sys.argv[1], "r", encoding="utf-8"))
docs = {}
order = []
fail = False
for e in spec:
    p = e["path"]
    old = base64.b64decode(e["old_b64"]).decode("utf-8")
    new = base64.b64decode(e["new_b64"]).decode("utf-8")
    want = e.get("count", 1)
    if p not in docs:
        with open(p, "r", encoding="utf-8", newline="") as f:
            docs[p] = f.read()
        order.append(p)
    got = docs[p].count(old)
    if got != want:
        print("FAIL %s: expected %d match(es), found %d" % (p, want, got))
        fail = True
    else:
        docs[p] = docs[p].replace(old, new) if want > 1 else docs[p].replace(old, new, 1)
        print("OK %s: %d -> %d bytes" % (p, len(old), len(new)))

if fail:
    sys.exit(1)
for p in order:
    with open(p, "w", encoding="utf-8", newline="") as f:
        f.write(docs[p])