#!/usr/bin/env python3
"""Cross-feature coexistence smoke test on the INTEGRATED binary (v3).

One server session: file SQL (COPY TO / read_csv / DESCRIBE) feeds an
AI COLUMN table (AI_COMPLETE column), results read back.
"""
import subprocess, sys, time, socket
import pymysql

SUB = "/home/hjy/work/seekdb-sub"
BIN = SUB + "/build/bin/src/observer/seekdb"
SDB = SUB + "/.github/script/seekdb/sdb.py"
MOCK = SUB + "/tools/deploy/mysql_test/test_suite/ai_column/include/ai_column_mock.py"
BASE = "/home/hjy/work/integ_verify/base_smoke"
PORT = 2887
MOCK_PORT = 19131
IN_CSV = "/tmp/coexist_in.csv"
OUT_CSV = "/tmp/coexist_out.csv"
ok, bad = [], []

def check(name, cond, detail=""):
    (ok if cond else bad).append(name)
    print(("PASS " if cond else "FAIL ") + name + ((" | " + str(detail)[:220]) if detail else ""))

def wait_port(p, t=90):
    d = time.time() + t
    while time.time() < d:
        try:
            socket.create_connection(("127.0.0.1", p), 1).close(); return True
        except OSError:
            time.sleep(1)
    return False

class Q:
    def __init__(self, db=None):
        self.c = pymysql.connect(host="127.0.0.1", port=PORT, user="root", password="",
                                 database=db, autocommit=True, connect_timeout=10)
    def run(self, q):
        cur = self.c.cursor()
        try:
            cur.execute(q)
            try: return cur.fetchall()
            except pymysql.err.MySQLError: return ()
        finally:
            cur.close()
    def try_(self, q):
        try: return None, self.run(q)
        except pymysql.err.MySQLError as e: return e, None

with open(IN_CSV, "w") as f:
    f.write("id,content\n1,alpha\n2,beta\n")
if subprocess.run(["rm", "-f", OUT_CSV]).returncode != 0:
    pass

print("=== boot integrated binary on a fresh base dir (default params) ===")
subprocess.run(["pkill", "-f", "base-dir=" + BASE])
subprocess.run([sys.executable, SDB, "destroy", "--base-dir", BASE], capture_output=True, text=True)
r = subprocess.run([sys.executable, SDB, "start", "--binary", BIN, "--base-dir", BASE,
                    "--port", str(PORT)], capture_output=True, text=True)
check("boot", wait_port(PORT), (r.stdout or "")[-160:])
time.sleep(3)

print("=== mock AI service on %d ===" % MOCK_PORT)
subprocess.run([sys.executable, MOCK, "start", str(MOCK_PORT)], capture_output=True, text=True)
check("mock-up", wait_port(MOCK_PORT))

q = Q()
q.run("create database if not exists smoke")
q.run("use smoke")
e, _ = q.try_("set session enable_file_sql = on")
check("enable_file_sql session", e is None, e)

print("=== feature 2 (file SQL), same session ===")
e, rows = q.try_("select id, content from read_csv('%s') order by id" % IN_CSV)
check("read_csv round-trip", e is None and rows == ((1, 'alpha'), (2, 'beta')), e or rows)
e, rows = q.try_("describe select * from read_csv('%s')" % IN_CSV)
check("describe file table", e is None and len(rows) == 2 and rows[0][0] == 'id', e or rows)
e, rows = q.try_("select count(*) from read_csv('%s')" % IN_CSV)
check("file-table aggregate", e is None and rows == ((2,),), e or rows)
e, rows = q.try_("copy (select id, content from read_csv('%s') order by id) to '%s'" % (IN_CSV, OUT_CSV))
check("copy-query-to-file", e is None, e)
try:
    body = open(OUT_CSV).read()
except OSError as exc:
    body = ""
    check("copy-output-readable", False, exc)
else:
    check("copy-output-readable", body.count("\n") >= 2 and "alpha" in body, repr(body[:80]))

print("=== feature 3 (AI COLUMN) + cross-feature INSERT..SELECT from the file table ===")
e, _ = q.try_("call DBMS_AI_SERVICE.CREATE_AI_MODEL('smoke_model', "
              "'{\"type\":\"completion\",\"model_name\":\"mock-complete\"}')")
check("create-ai-model", e is None, e)
e, _ = q.try_("call DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT('smoke_ep', "
              "'{\"ai_model_name\":\"smoke_model\",\"url\":\"http://127.0.0.1:%d/v1/chat/completions\","
              "\"access_key\":\"k\",\"provider\":\"openai\",\"request_model_name\":\"mock-complete\"}')" % MOCK_PORT)
check("create-ai-endpoint", e is None, e)
e, _ = q.try_("create table smoke.docs (id bigint primary key, content varchar(200), "
              "title varchar(200), summary varchar(400) AI COLUMN "
              "(AI_COMPLETE('smoke_model', concat('Summarize: ', content))) "
              "WITH (sync_mode='async', max_retries=8))")
check("create-ai-column-table", e is None, e)

e, _ = q.try_("insert into smoke.docs (id, content, title) "
              "select id, content, concat('t', id) from read_csv('%s')" % IN_CSV)
check("insert-select-from-file-table", e is None, e)
e, rows = q.try_("select count(*) from smoke.docs")
check("rows-inserted", e is None and rows == ((2,),), e or rows)

deadline, ready = time.time() + 120, 0
while time.time() < deadline:
    try: ready = q.run("select count(*) from smoke.docs where summary is not null")[0][0]
    except pymysql.err.MySQLError: ready = 0
    if ready == 2: break
    time.sleep(1)
check("ai-column-worker-produced-results", ready == 2, ready)
e, rows = q.try_("select id, content, summary from smoke.docs order by id")
check("ai-column-values-readable", e is None and len(rows) == 2 and all(r[2] for r in rows), e or rows)

e, _ = q.try_("set session ai_column_strict_read = on")
check("strict-read-guc-settable", e is None, e)

print()
print("SMOKE passed=%d failed=%d" % (len(ok), len(bad)))
if bad: print("FAILED:", bad)
subprocess.run([sys.executable, SDB, "stop", "--base-dir", BASE], capture_output=True, text=True)
subprocess.run(["pkill", "-f", "ai_column_mock.py"])
sys.exit(0 if not bad else 1)
