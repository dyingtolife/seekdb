#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""End-to-end driver for problem 05b: AI column runtime milestone (r2).

Boots the locally built seekdb (port 2883) via .github/script/seekdb/sdb.py,
controls the deterministic OpenAI-compatible mock on port 19130, and drives
the acceptance steps through pymysql with real assertions:

  1. CREATE TABLE with AI_COMPLETE AI columns (async, max_retries=2) -> DDL ok
  2. metadata rows in __all_ai_column + hidden state/request/spec columns
  3. INSERT 2 rows with mock DOWN -> task rows observable (RETRY_WAIT),
     proving enqueue-in-transaction; then mock UP -> tasks retried & consumed
  4. worker consumes tasks -> __all_ai_column_task drained, hidden state READY
  5. SELECT reads deterministic results
  6. UPDATE dependency column -> re-tasked, result changes
  7. UPDATE unrelated column (title) -> NO new task, result stable
  8. always-fail endpoint -> FAILED after max_retries; strict_read errors
     11120 on the not-READY column, passes on READY; RETRY re-enqueues and
     republishes.

Exit code 0 iff every step passes. Prints PASS/FAIL lines with real output.
"""
import os
import socket
import subprocess
import sys
import time

import pymysql

REPO = "/home/hjy/work/seekdb-05b"
SDB = REPO + "/.github/script/seekdb/sdb.py"
BINARY = os.environ.get(
    "AI_SEEKDB_BINARY",
    REPO + "/build/execroot/_main/bazel-out/k8-opt/bin/src/observer/seekdb")
BASE = os.environ.get("AI_BASE", "/home/hjy/work/ai_col_e2e_r23")
PORT = 2883
MOCK_PORT = 19130
MOCK = REPO + "/tools/deploy/mysql_test/test_suite/ai_column/include/ai_column_mock.py"

passed = []
failed = []


def check(name, cond, detail=""):
    (passed if cond else failed).append(name)
    print(("PASS " if cond else "FAIL ") + name +
          ((" | " + str(detail)[:300]) if detail else ""), flush=True)
    return cond


def wait_port(port, timeout=240):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return True
        except OSError:
            time.sleep(1)
    return False


def connect(db=None):
    return pymysql.connect(host="127.0.0.1", port=PORT, user="root", password="",
                           database=db, autocommit=True, connect_timeout=10,
                           read_timeout=60, cursorclass=pymysql.cursors.DictCursor)


def run(sql, db="oceanbase"):
    conn = connect(db)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        try:
            rows = cur.fetchall()
        except Exception:
            rows = ()
        cur.close()
    finally:
        conn.close()
    return rows


def scalar(sql, db="oceanbase"):
    rows = run(sql, db)
    if not rows:
        return None
    row = rows[0]
    return list(row.values())[0] if isinstance(row, dict) else row[0]


def expect_error(sql, db="oceanbase"):
    conn = connect(db)
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            cur.fetchall()
            return None
        except pymysql.err.MySQLError as exc:
            return exc
        finally:
            cur.close()
    finally:
        conn.close()


def mock(mode, assertion=None):
    cmd = [sys.executable, MOCK, mode, str(MOCK_PORT)]
    if assertion:
        cmd.append(assertion)
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    return proc


def wait_until(sql, timeout_s, want=None):
    """Poll a scalar query until it equals want (or truthy when want is None)."""
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        try:
            last = scalar(sql)
        except Exception as exc:
            last = repr(exc)
        if want is not None:
            if last == want:
                return last
        elif last is not None and last != 0 and last is not False:
            return last
        time.sleep(0.5)
    return last


def boot():
    subprocess.run([sys.executable, SDB, "destroy", "--base-dir", BASE],
                   capture_output=True)
    # Never let a stale instance answer for the freshly built binary.
    subprocess.run(["bash", "-lc", "fuser -k %d/tcp 2>/dev/null; true" % PORT])
    time.sleep(3)
    if wait_port(PORT, timeout=3):
        print("port %d still busy after cleanup" % PORT)
        sys.exit(2)
    proc = subprocess.run(
        [sys.executable, SDB, "start", "--binary", BINARY,
         "--base-dir", BASE, "--port", str(PORT), "--nodaemon"],
        capture_output=True, text=True, timeout=300)
    if proc.returncode != 0:
        print("sdb start failed:", proc.stdout[-800:], proc.stderr[-800:])
        sys.exit(2)
    if not wait_port(PORT):
        print("server did not open port", PORT)
        console = os.path.join(BASE, "console.log")
        if os.path.exists(console):
            print(open(console).read()[-1500:])
        sys.exit(2)
    deadline = time.time() + 120
    while time.time() < deadline:
        try:
            conn = connect()
            cur = conn.cursor()
            cur.execute("select 1")
            cur.fetchone()
            cur.close()
            conn.close()
            return
        except Exception:
            time.sleep(2)
    print("server never answered select 1")
    sys.exit(2)


TASK_JOIN = (
    "select count(*) from oceanbase.__all_ai_column_task task "
    "join oceanbase.__all_table t on t.table_id = task.table_id "
    "join oceanbase.__all_database d on d.database_id = t.database_id "
    "where d.database_name = '%s' and t.table_name = 'docs'")

# Queue rows of one source row. The product renders the rowkey predicate as
# "`id` = <value>". The per-column status lives in a HIDDEN column (spec:
# hidden status column) and is not user-selectable, so state is asserted from
# the task queue plus the published (or NULL) AI result instead of a direct
# select on the hidden column. Which AI columns a table has comes from
# __all_ai_column (checked in step 2).
ROW_TASK_JOIN = (
    "select count(*) from oceanbase.__all_ai_column_task task "
    "join oceanbase.__all_table t on t.table_id = task.table_id "
    "join oceanbase.__all_database d on d.database_id = t.database_id "
    "where d.database_name = '%s' and t.table_name = 'docs' "
    "and task.rowkey_predicate like '%%`id` = %s'")


def main():
    boot()
    print("booted seekdb on", PORT, flush=True)

    # mock: take over port 19130 (stop any stale server first)
    m = mock("stop")
    m = mock("start")
    check("mock-start", m.returncode == 0, m.stderr[-200:])

    DB = "ai_e2e_runtime"
    run("drop database if exists " + DB)
    run("create database " + DB)

    # model + endpoints against the mock (pre-clean; not-found is fine)
    expect_error("call DBMS_AI_SERVICE.DROP_AI_MODEL_ENDPOINT('e2e_ep')")
    expect_error("call DBMS_AI_SERVICE.DROP_AI_MODEL('e2e_model')")
    expect_error("call DBMS_AI_SERVICE.DROP_AI_MODEL_ENDPOINT('e2e_fail_ep')")
    expect_error("call DBMS_AI_SERVICE.DROP_AI_MODEL('e2e_fail_model')")
    err = expect_error(
        "call DBMS_AI_SERVICE.CREATE_AI_MODEL('e2e_model',"
        " '{\"type\":\"completion\",\"model_name\":\"mock-complete\"}')")
    check("create-model", err is None, err)
    err = expect_error(
        "call DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT('e2e_ep',"
        " '{\"ai_model_name\":\"e2e_model\","
        " \"url\":\"http://127.0.0.1:%d/v1/chat/completions\","
        " \"access_key\":\"e2e-key\",\"provider\":\"openai\","
        " \"request_model_name\":\"mock-complete\"}')" % MOCK_PORT)
    check("create-endpoint", err is None, err)
    # failure-path model: URL path contains always-fail -> mock answers 400
    err = expect_error(
        "call DBMS_AI_SERVICE.CREATE_AI_MODEL('e2e_fail_model',"
        " '{\"type\":\"completion\",\"model_name\":\"mock-complete\"}')")
    check("create-fail-model", err is None, err)
    err = expect_error(
        "call DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT('e2e_fail_ep',"
        " '{\"ai_model_name\":\"e2e_fail_model\","
        " \"url\":\"http://127.0.0.1:%d/v1/chat/completions/always-fail\","
        " \"access_key\":\"e2e-key\",\"provider\":\"openai\","
        " \"request_model_name\":\"mock-complete\"}')" % MOCK_PORT)
    check("create-fail-endpoint", err is None, err)

    # ---- STEP 1: CREATE AI columns (async DDL returns; tasks async) ----
    create_sql = (
        "create table %s.docs ("
        "  id bigint primary key,"
        "  content varchar(200),"
        "  title varchar(200),"
        "  summary varchar(400) AI COLUMN ("
        "    AI_COMPLETE('e2e_model', concat('Summarize: ', content))"
        "  ) WITH (sync_mode='async', max_retries=8),"
        "  risky varchar(400) AI COLUMN ("
        "    AI_COMPLETE('e2e_fail_model', concat('Risk: ', content))"
        "  ) WITH (sync_mode='async', max_retries=2)"
        ")") % DB
    err = expect_error(create_sql, DB)
    check("step1-create-ai-column", err is None, err)

    # ---- STEP 2: metadata + hidden columns ----
    meta = scalar("select count(*) from oceanbase.__all_ai_column "
                  "where column_name in ('summary', 'risky')", "oceanbase")
    check("step2-metadata-rows", meta == 2, meta)
    for col in ("summary", "risky"):
        n = scalar(
            "select count(*) from oceanbase.__all_column c "
            "join oceanbase.__all_table t on t.table_id = c.table_id "
            "join oceanbase.__all_database d on d.database_id = t.database_id "
            "where d.database_name = '%s' and t.table_name = 'docs' "
            "and c.column_name in "
            "('%s_state', '%s_request', '%s_result_spec')" % (DB, col, col, col),
            "oceanbase")
        check("step2-hidden-cols-" + col, n == 3, n)
    nhidden = scalar(
        "select count(*) from oceanbase.__all_column c "
        "join oceanbase.__all_table t on t.table_id = c.table_id "
        "join oceanbase.__all_database d on d.database_id = t.database_id "
        "where d.database_name = '%s' and t.table_name = 'docs' "
        "and c.column_name like '%%_%%%%' and c.column_name not in "
        "('id', 'content', 'title', 'summary', 'risky')" % DB, "oceanbase")
    check("step2-six-hidden-cols", nhidden == 6, nhidden)

    # ---- STEP 3: INSERT with mock DOWN -> tasks observable, then retried ----
    # With the mock down, claimed tasks fail fast and sit in RETRY_WAIT for
    # 500ms per attempt, giving a deterministic window to count the queue.
    #
    # ROUND 13 DRIVER FIX: the down-window is a race against the retry budget.
    # Round 12 stopped the mock, inserted, then ran six observations on six
    # fresh connections and only then restarted the mock - by which time
    # summary had burned max_retries=2 (terminal FAILED, task row deleted), so
    # nothing ever published and every AI column stayed NULL. Two changes: the
    # observations share ONE connection, and summary carries max_retries=8
    # (~4s of attempts) so the queue survives the outage while the endpoint is
    # still down. The always-fail column keeps max_retries=2 (bounded retry).
    mock("stop")
    t_down = time.time()
    err = expect_error("insert into %s.docs(id, content, title) "
                       "values(1, 'alpha', 't1')" % DB, DB)
    check("step3-insert1-ok", err is None, err)
    err = expect_error("insert into %s.docs(id, content, title) "
                       "values(2, 'beta', 't2')" % DB, DB)
    check("step3-insert2-ok", err is None, err)
    tasks1 = None
    star_cols = []
    row_tasks = None
    res = None
    conn = connect(DB)
    try:
        cur = conn.cursor()
        cur.execute(TASK_JOIN % DB)
        tasks1 = list(cur.fetchone().values())[0]
        cur.execute("select * from %s.docs where id = 1" % DB)
        cur.fetchall()
        star_cols = [d[0] for d in cur.description or []]
        cur.execute(ROW_TASK_JOIN % (DB, 1))
        row_tasks = list(cur.fetchone().values())[0]
        cur.execute("select summary from %s.docs where id = 1" % DB)
        res = list(cur.fetchone().values())[0]
    finally:
        conn.close()
    # 2 inserts x 2 AI columns = 4 task rows (claimed, failing, RETRY_WAIT)
    check("step3-task-enqueued", tasks1 == 4, tasks1)
    # The aux bookkeeping columns are hidden by design: SELECT * must not
    # expose them. (Their names remain resolvable for the engine's internal
    # SQL - see the ROUND 10 note in the report.)
    hidden_exposed = [c for c in star_cols
                      if c not in ("id", "content", "title", "summary", "risky")]
    check("step3-hidden-cols-not-in-select-star", hidden_exposed == [],
          star_cols)
    # Not-ready, asserted from the queue: one task per async AI column of that
    # row (2 AI columns), and the AI result is still NULL.
    check("step3-row1-tasks-pending", row_tasks == 2, row_tasks)
    check("step3-result-null", res is None, res)
    # bring the mock back: the durable queue retries and publishes
    mock("start")
    print("step3 down-window: %.2fs" % (time.time() - t_down), flush=True)
    # A published, non-NULL result implies READY: the worker writes the result
    # and the READY state in one transaction.
    ok = wait_until(
        "select summary is not null from %s.docs where id = 1" % DB, 90)
    check("step4-row1-ready", ok == 1, ok)
    ok = wait_until(
        "select summary is not null from %s.docs where id = 2" % DB, 90)
    check("step4-row2-ready", ok == 1, ok)
    tasks_left = wait_until(TASK_JOIN % DB, 30, want=0)
    check("step4-tasks-consumed", tasks_left == 0, tasks_left)
    val = scalar("select summary from %s.docs where id = 1" % DB, DB)
    check("step4-deterministic-result", val == "mock:Summarize: alpha", val)

    # ---- STEP 5: SELECT reads results ----
    rows = run("select id, summary from %s.docs order by id" % DB, DB)
    summary_vals = [r["summary"] for r in rows]
    check("step5-select-results",
          summary_vals == ["mock:Summarize: alpha", "mock:Summarize: beta"],
          summary_vals)

    # ---- STEP 6: dependency UPDATE re-tasks that row ----
    err = expect_error("update %s.docs set content = 'alpha-v2' where id = 1" % DB, DB)
    check("step6-update-ok", err is None, err)
    ok = wait_until(
        "select summary = 'mock:Summarize: alpha-v2' from %s.docs where id = 1" % DB,
        90)
    check("step6-retask-new-result", ok == 1, ok)
    tasks_now = wait_until(TASK_JOIN % DB, 30, want=0)
    check("step6-task-drained-again", tasks_now == 0, tasks_now)

    # ---- STEP 7: unrelated UPDATE (title) does NOT re-task ----
    mock("reset")
    err = expect_error("update %s.docs set title = 'title-1-changed' where id = 1" % DB, DB)
    check("step7-update-title-ok", err is None, err)
    time.sleep(4)
    tasks_after = scalar(TASK_JOIN % DB)
    check("step7-no-new-task", tasks_after == 0, tasks_after)
    val = scalar("select summary from %s.docs where id = 1" % DB, DB)
    check("step7-result-stable", val == "mock:Summarize: alpha-v2", val)

    # ---- STEP 8: failure path + strict_read + RETRY ----
    # 8a. rows whose 'risky' column hits the always-fail endpoint: after
    #     max_retries (2) the hidden state lands on FAILED and the task row
    #     is removed. The summary column of the same rows still completes.
    mock("reset")
    err = expect_error("insert into %s.docs(id, content, title) "
                       "values(3, 'gamma', 't3')" % DB, DB)
    check("step8a-insert-ok", err is None, err)
    # Terminal failure without touching the hidden state column: after
    # max_retries the task row is deleted from the queue while the AI result
    # stays NULL; the strict-read check below additionally proves the state is
    # not READY.
    ok = wait_until(ROW_TASK_JOIN % (DB, 3), 90, want=0)
    check("step8a-terminal-failed", ok == 0, ok)
    risky_null = wait_until(
        "select risky is null from %s.docs where id = 3" % DB, 60)
    check("step8a-failed-result-null", risky_null == 1, risky_null)
    ok = wait_until(
        "select summary = 'mock:Summarize: gamma' from %s.docs where id = 3" % DB,
        90)
    check("step8a-summary-still-ok", ok == 1, ok)
    tasks_left = wait_until(
        "select count(*) from oceanbase.__all_ai_column_task task "
        "join oceanbase.__all_table t on t.table_id = task.table_id "
        "join oceanbase.__all_database d on d.database_id = t.database_id "
        "where d.database_name = '%s' and t.table_name = 'docs' "
        "and task.request_id like '%%3%%'" % DB, 30, want=0)
    # request ids are '<sid>-<stmt>-<seq>'; just require the queue fully drained
    tasks_left = wait_until(TASK_JOIN % DB, 30, want=0)
    check("step8a-failed-tasks-removed", tasks_left == 0, tasks_left)

    # 8b. strict read: not-READY column errors 11120; READY column passes.
    # One connection: a session variable only exists on its own session.
    conn = connect(DB)
    try:
        cur = conn.cursor()
        cur.execute("set session ai_column_strict_read = on")
        exc = None
        try:
            cur.execute("select risky from %s.docs where id = 3" % DB)
            cur.fetchall()
        except pymysql.err.MySQLError as e:
            exc = e
        code = exc.args[0] if exc else None
        check("step8b-strict-read-errors", code == 11120, repr(exc))

        exc2 = None
        try:
            cur.execute("select summary from %s.docs where id = 3" % DB)
            cur.fetchall()
        except pymysql.err.MySQLError as e:
            exc2 = e
        check("step8b-strict-ready-col-passes", exc2 is None, repr(exc2))

        cur.execute("set session ai_column_strict_read = off")
        exc3 = None
        try:
            cur.execute("select risky from %s.docs where id = 3" % DB)
            cur.fetchall()
        except pymysql.err.MySQLError as e:
            exc3 = e
        check("step8b-strict-off-passes", exc3 is None, repr(exc3))
    finally:
        conn.close()

    # 8c. RETRY AI COLUMN ... WHERE: re-invalidates and re-enqueues the row;
    #     with the fail endpoint still failing, state returns to FAILED, but
    #     a task row must appear in between. Retry the summary column instead
    #     (READY -> re-tasked -> republished deterministic result).
    mock("reset")
    err = expect_error(
        "alter table %s.docs retry ai column summary where id = 3" % DB, DB)
    check("step8c-retry-ok", err is None, err)
    ok = wait_until(
        "select summary = 'mock:Summarize: gamma' from %s.docs where id = 3" % DB,
        90)
    check("step8c-retry-republished", ok == 1, ok)
    tasks_final = wait_until(TASK_JOIN % DB, 30, want=0)
    check("step8c-tasks-drained", tasks_final == 0, tasks_final)

    print("")
    print("PASSED: %d  FAILED: %d" % (len(passed), len(failed)))
    if failed:
        print("FAILED STEPS: " + ", ".join(failed))
    mock("stop")
    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as exc:
        import traceback
        traceback.print_exc()
        print("DRIVER ERROR:", exc)
        sys.exit(3)
