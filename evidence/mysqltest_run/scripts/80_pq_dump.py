import base64, sys, struct

path = sys.argv[1] if len(sys.argv) > 1 else r"\\wsl$\Ubuntu-24.04\home\hjy\work\seekdb\tools\deploy\mysql_test\test_suite\file_sql\data\simple.parquet.b64"
raw = open(path, "rb").read()
buf = base64.b64decode(raw) if not raw.startswith(b"PAR1") else raw
print("total bytes:", len(buf))

TYPES = {1: "BOOL_TRUE", 2: "BOOL_FALSE", 3: "BYTE", 4: "I16", 5: "I32", 6: "I64",
         7: "DOUBLE", 8: "BINARY", 9: "LIST", 10: "SET", 11: "MAP", 12: "STRUCT"}


class C:
    def __init__(self, b, pos=0, end=None):
        self.b = b
        self.p = pos
        self.end = len(b) if end is None else end

    def byte(self):
        v = self.b[self.p]
        self.p += 1
        return v

    def varint(self):
        v = 0
        s = 0
        while True:
            x = self.byte()
            v |= (x & 0x7f) << s
            if not (x & 0x80):
                return v
            s += 7

    def zigzag(self):
        u = self.varint()
        return (u >> 1) ^ -(u & 1)

    def binary(self):
        n = self.varint()
        v = self.b[self.p:self.p + n]
        self.p += n
        return v

    def struct_fields(self):
        last = 0
        while True:
            h = self.byte()
            if h == 0:
                return
            delta = (h >> 4) & 0xf
            t = h & 0xf
            if delta == 0:
                fid = self.zigzag()
            else:
                fid = last + delta
            last = fid
            yield fid, t

    def dump(self, indent=0, depth=0):
        pad = "  " * indent
        for fid, t in self.struct_fields():
            tn = TYPES.get(t, "?%d" % t)
            if t in (1, 2):
                print("%s%d: %s = %s" % (pad, fid, tn, "true" if t == 1 else "false"))
            elif t == 3:
                print("%s%d: BYTE = %d" % (pad, fid, self.byte()))
            elif t in (4, 5, 6):
                print("%s%d: %s = %d" % (pad, fid, tn, self.zigzag()))
            elif t == 7:
                v = struct.unpack("<d", self.b[self.p:self.p + 8])[0]
                self.p += 8
                print("%s%d: DOUBLE = %r" % (pad, fid, v))
            elif t == 8:
                v = self.binary()
                try:
                    s = v.decode("utf-8")
                    shown = repr(s)
                except Exception:
                    shown = v.hex()
                print("%s%d: BINARY(%d) = %s" % (pad, fid, len(v), shown))
            elif t in (9, 10):
                h = self.byte()
                n = (h >> 4) & 0xf
                et = h & 0xf
                if n == 15:
                    n = self.varint()
                print("%s%d: %s n=%d elem=%s" % (pad, fid, tn, n, TYPES.get(et, et)))
                for i in range(n):
                    if et in (1, 2):
                        print("%s  [%d] bool" % (pad, i))
                    elif et == 12:
                        print("%s  [%d] struct {" % (pad, i))
                        self.dump(indent + 2, depth + 1)
                        print("%s  }" % pad)
                    elif et == 8:
                        print("%s  [%d] %r" % (pad, i, self.binary()))
                    elif et in (4, 5, 6):
                        print("%s  [%d] %d" % (pad, i, self.zigzag()))
                    else:
                        print("%s  [%d] unhandled elem type %s" % (pad, i, et))
                        return
            elif t == 12:
                print("%s%d: STRUCT {" % (pad, fid))
                self.dump(indent + 1, depth + 1)
                print("%s}" % pad)
            else:
                print("%s%d: unhandled type %s" % (pad, fid, tn))
                return


# footer
assert buf[:4] == b"PAR1" and buf[-4:] == b"PAR1"
flen = struct.unpack("<i", buf[-8:-4])[0]
print("footer len:", flen, "footer at", len(buf) - 8 - flen)
c = C(buf, len(buf) - 8 - flen, len(buf) - 8)
print("=== FileMetaData ===")
c.dump()
print("consumed footer bytes:", c.p - (len(buf) - 8 - flen), "of", flen)
