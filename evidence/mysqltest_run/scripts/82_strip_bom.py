#!/usr/bin/env python3
"""Strip UTF-8 BOM (EF BB BF) byte sequences from files.

Stray BOMs were injected into tracked sources and helper scripts by earlier
PowerShell edits; clang rejects a BOM in C++ sources and the kernel rejects a
BOM before a shebang. Usage: python3 82_strip_bom.py FILE [FILE ...]
"""
import sys

BOM = b"\xef\xbb\xbf"
for path in sys.argv[1:]:
    with open(path, "rb") as f:
        data = f.read()
    cnt = data.count(BOM)
    if cnt:
        with open(path, "wb") as f:
            f.write(data.replace(BOM, b""))
    print("%s: removed %d BOM(s)" % (path, cnt))