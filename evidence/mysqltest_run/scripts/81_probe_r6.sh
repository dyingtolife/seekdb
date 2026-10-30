#!/bin/bash
# Probe the three round-6 targets on the running instance.
set -u
ROOT=/home/hjy/work/seekdb
CLI=$ROOT/deps/3rd/u01/obclient/bin/obclient
BASE=${BASE:-/home/hjy/work/fs_e2e/r13}
D=$BASE/data_for_mysqltest_file_sql
run() { echo "### $1"; $CLI -h127.0.0.1 -P2886 -uadmin -padmin -Dtest --batch -e "set session enable_file_sql=ON" -e "$1" 2>&1 | head -16; }
run "describe select * from read_csv('$D/basic.csv')"
run "describe select * from read_csv('$D/basic.csv') /* second call, plan cache */"
run "explain select * from read_csv('$D/basic.csv')"
run "select count(*) from read_parquet('$D/simple.parquet') p"
run "copy (select 1) to '$D/x.csv' (format csv)"
