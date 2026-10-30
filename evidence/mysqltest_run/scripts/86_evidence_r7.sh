#!/bin/bash
# Round 7 evidence capture. Order matters: the observer is restarted first (the
# plan cache stores the file signature taken at resolve time, so re-copying the
# fixtures and re-running without a restart reports 1210 "file changed between
# resolve and execution"), then the suite installs the fixtures itself over the
# ssh/scp shims, then the read-only probes reuse the same directory.
set -u
ROOT=/home/hjy/work/seekdb
CLI=$ROOT/deps/3rd/u01/obclient/bin/obclient
BASE=/home/hjy/work/fs_e2e/r13
D=$BASE/data_for_mysqltest_file_sql
EV=$ROOT/evidence/mysqltest_run
BIN=$ROOT/build/bin/src/observer/seekdb

pkill -f 'seekdb --base-dir' 2>/dev/null
sleep 2
bash "$EV/scripts/78_boot_r6.sh" "$BASE" > /tmp/boot_r7.txt 2>&1
grep -E 'PORT_UP|BOOT_FAILED' /tmp/boot_r7.txt

bash "$EV/scripts/85_run_suite_r7.sh" > "$EV/21_suite_round7.txt" 2>&1
echo "== suite tail =="
tail -5 "$EV/21_suite_round7.txt"

{
  echo "# 20 - Round 7 verification: DESCRIBE metadata and Parquet decoding"
  echo
  echo "binary: $BIN"
  echo "sha256: $(sha256sum "$BIN" | cut -d' ' -f1)"
  echo "size:   $(stat -c %s "$BIN") bytes"
  echo "date:   $(date -Is)"
  echo "fixture simple.parquet: $(stat -c %s "$D/simple.parquet") bytes, sha256 $(sha256sum "$D/simple.parquet" | cut -d' ' -f1)"
  echo
  echo "## 1. DESCRIBE of a CSV file table (6 metadata columns, header from field metadata)"
  $CLI -h127.0.0.1 -P2886 -uadmin -padmin -Dtest --batch \
       -e "set session enable_file_sql=ON; describe select * from read_csv('$D/basic.csv')" 2>&1
  echo
  echo "## 2. DESCRIBE of a JSONL file table"
  $CLI -h127.0.0.1 -P2886 -uadmin -padmin -Dtest --batch \
       -e "set session enable_file_sql=ON; describe select * from read_json('$D/basic.jsonl', format = 'newline_delimited')" 2>&1
  echo
  echo "## 3. EXPLAIN still prints the text plan (the is_describe() marker is distinct)"
  $CLI -h127.0.0.1 -P2886 -uadmin -padmin -Dtest --batch \
       -e "set session enable_file_sql=ON; explain select * from read_csv('$D/basic.csv')" 2>&1
  echo
  echo "## 4. Parquet aggregate (expected: 7 / 0 / 6 / 21)"
  $CLI -h127.0.0.1 -P2886 -uadmin -padmin -Dtest --batch \
       -e "set session enable_file_sql=ON; select count(*) as row_count, min(c1) as min_c1, max(c1) as max_c1, sum(c1) as sum_c1 from read_parquet('$D/simple.parquet') p" 2>&1
  echo
  echo "## 5. Parquet full scan (expected: the seven INT32 values 0..6)"
  $CLI -h127.0.0.1 -P2886 -uadmin -padmin -Dtest --batch \
       -e "set session enable_file_sql=ON; select * from read_parquet('$D/simple.parquet') p" 2>&1
  echo
  echo "## 6. DESCRIBE of the parquet file table"
  $CLI -h127.0.0.1 -P2886 -uadmin -padmin -Dtest --batch \
       -e "set session enable_file_sql=ON; describe select * from read_parquet('$D/simple.parquet')" 2>&1
  echo
  echo "## 7. COPY is still unimplemented (expected 1235)"
  $CLI -h127.0.0.1 -P2886 -uadmin -padmin -Dtest --batch \
       -e "set session enable_file_sql=ON; copy (select 1) to '$D/x.csv' (format csv)" 2>&1
} > "$EV/20_describe_parquet_round7.txt" 2>&1
echo "wrote $EV/20_describe_parquet_round7.txt"