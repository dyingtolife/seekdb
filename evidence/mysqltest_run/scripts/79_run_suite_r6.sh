#!/bin/bash
# Round 6 mysqltest runner for test_suite/file_sql.
# Contract expected by the suite: OBMYSQL_MS0 / OBMYSQL_PORT / OBMYSQL_USR /
# OBMYSQL_PWD / OBSERVER_DIR / IS_BUSINESS, a `mysql_test` symlink in the CWD and
# ssh/scp shims (the fixture host is this machine).
set -u
ROOT=/home/hjy/work/seekdb
BASE=${BASE:-/home/hjy/work/fs_e2e/r13}
PORT=${PORT:-2886}
MT=$ROOT/deps/3rd/u01/obclient/bin/mysqltest
WORK=/home/hjy/work/fs_e2e/run6
rm -rf "$WORK"; mkdir -p "$WORK"
ln -s "$ROOT/tools/deploy/mysql_test" "$WORK/mysql_test"
cd "$WORK"
export PATH="$ROOT/evidence/mysqltest_run/scripts:$PATH"
export OBMYSQL_MS0=127.0.0.1
export OBMYSQL_PORT=$PORT
export OBMYSQL_USR=admin
export OBMYSQL_PWD=admin
export OBSERVER_DIR=$BASE
export IS_BUSINESS=false
"$MT" --user=admin --password=admin --host=127.0.0.1 --port=$PORT \
      --test-file=mysql_test/test_suite/file_sql/t/file_sql_basic.test \
      --result-file=mysql_test/test_suite/file_sql/r/mysql/file_sql_basic.result \
      --record "$@" 2>&1
echo "MYSQLTEST_EXIT=$?"
