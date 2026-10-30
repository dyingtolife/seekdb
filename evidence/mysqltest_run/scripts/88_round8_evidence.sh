#!/bin/bash
# Round 8: capture the green run, record the .result from a real run, re-verify.
set -u
ROOT=/home/hjy/work/seekdb
cd "$ROOT" || exit 1
RES=$ROOT/tools/deploy/mysql_test/test_suite/file_sql/r/mysql/file_sql_basic.result

boot() {
  pkill -x seekdb
  sleep 2
  bash "$ROOT/evidence/mysqltest_run/scripts/78_boot_r6.sh" /home/hjy/work/fs_e2e/r13 2>&1 | grep -E 'PORT_UP|BOOT_FAILED|PROCESS_DIED'
}

echo "===== 1) green run (fresh observer, hand-authored .result) ====="
boot
bash "$ROOT/evidence/mysqltest_run/scripts/87_run_suite_r8.sh" > /tmp/suite_green.log 2>&1
cat /tmp/suite_green.log
cp "$RES" /tmp/result_before.txt

echo "===== 2) RECORD=1 (fresh observer) ====="
boot
RECORD=1 bash "$ROOT/evidence/mysqltest_run/scripts/87_run_suite_r8.sh" > /tmp/suite_record.log 2>&1
cat /tmp/suite_record.log
echo "--- diff hand-authored vs recorded ---"
diff -u /tmp/result_before.txt "$RES" || true
echo "--- recorded file ---"
cat "$RES"

echo "===== 3) re-verify RECORD=0 against the recorded file (fresh observer) ====="
boot
bash "$ROOT/evidence/mysqltest_run/scripts/87_run_suite_r8.sh" > /tmp/suite_verify.log 2>&1
cat /tmp/suite_verify.log
