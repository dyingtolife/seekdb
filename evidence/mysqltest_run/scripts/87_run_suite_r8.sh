#!/bin/bash
# Round 8 mysqltest runner for test_suite/file_sql.
# RECORD=0 (default) compares against r/mysql/file_sql_basic.result;
# RECORD=1 regenerates that file from the real run.
set -u
ROOT=/home/hjy/work/seekdb
BASE=${BASE:-/home/hjy/work/fs_e2e/r13}
PORT=${PORT:-2886}
RECORD=${RECORD:-0}
MT=$ROOT/deps/3rd/u01/obclient/bin/mysqltest
WORK=${WORK:-/home/hjy/work/fs_e2e/run8}
rm -rf "$WORK"; mkdir -p "$WORK"
ln -s "$ROOT/tools/deploy/mysql_test" "$WORK/mysql_test"
cd "$WORK" || exit 1
export PATH="$ROOT/evidence/mysqltest_run/scripts:$PATH"
export OBMYSQL_MS0=127.0.0.1
export OBMYSQL_PORT=$PORT
export OBMYSQL_USR=admin
export OBMYSQL_PWD=admin
export OBSERVER_DIR=$BASE
export IS_BUSINESS=false
ARGS=""
if [ "$RECORD" = "1" ]; then ARGS="--record"; fi
"$MT" --user=admin --password=admin --host=127.0.0.1 --port=$PORT \
      --test-file=mysql_test/test_suite/file_sql/t/file_sql_basic.test \
      --result-file=mysql_test/test_suite/file_sql/r/mysql/file_sql_basic.result \
      $ARGS 2>&1
echo "MYSQLTEST_EXIT=$?"