#!/bin/bash
# Round 7: install the file-sql fixtures and probe DESCRIBE / parquet / copy.
set -u
ROOT=/home/hjy/work/seekdb
CLI=$ROOT/deps/3rd/u01/obclient/bin/obclient
BASE=${BASE:-/home/hjy/work/fs_e2e/r13}
PORT=${PORT:-2886}
DATA=$ROOT/tools/deploy/mysql_test/test_suite/file_sql/data
D=$BASE/data_for_mysqltest_file_sql

if [ "${PREPARE:-1}" = "1" ]; then
  rm -rf "$D"; mkdir -p "$D"
  cp "$DATA"/* "$D"/
  base64 -d "$D/simple.parquet.b64" > "$D/simple.parquet"
  rm -f "$D/simple.parquet.b64"
  echo "FIXTURES:"; ls -l "$D"
fi

run() {
  echo "### $1"
  $CLI -h127.0.0.1 -P"$PORT" -uadmin -padmin -Dtest --batch \
       -e "set session enable_file_sql=ON; $1" 2>&1
  echo "### rc=$?"
}

run "describe select * from read_csv('$D/basic.csv')"
run "describe select * from read_csv('$D/basic.csv') /* 2nd call, plan cache */"
run "explain select * from read_csv('$D/basic.csv')"
run "describe select * from read_json('$D/basic.jsonl', format = 'newline_delimited')"
run "select count(*) as row_count, min(c1) as min_c1, max(c1) as max_c1, sum(c1) as sum_c1 from read_parquet('$D/simple.parquet') p"
run "select * from read_parquet('$D/simple.parquet') p"
run "copy (select 1) to '$D/x.csv' (format csv)"