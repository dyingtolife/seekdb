#!/bin/bash
# Round 8: probe COPY ... TO (CSV export) end to end on port 2886.
set -u
ROOT=/home/hjy/work/seekdb
CLI=$ROOT/deps/3rd/u01/obclient/bin/obclient
BASE=${BASE:-/home/hjy/work/fs_e2e/r13}
PORT=${PORT:-2886}
DATA=$ROOT/tools/deploy/mysql_test/test_suite/file_sql/data
D=$BASE/data_for_mysqltest_file_sql
E=$BASE/copy_out

if [ "${PREPARE:-1}" = "1" ]; then
  rm -rf "$D"; mkdir -p "$D"
  cp "$DATA"/* "$D"/
  base64 -d "$D/simple.parquet.b64" > "$D/simple.parquet"
  rm -f "$D/simple.parquet.b64"
fi
rm -rf "$E"; mkdir -p "$E"
printf 'a,b\n1,"unterminated\n' > "$E/bad.csv"

run() {
  echo "### $1"
  $CLI -h127.0.0.1 -P"$PORT" -uadmin -padmin -Dtest --batch \
       -e "set session enable_file_sql=ON; $1" 2>&1
  echo "### rc=$?"
}

echo "######## 1. ROUND TRIP: header + defaults"
rm -f "$E/rt.csv"
run "copy (select id, name, score from read_csv('$D/basic.csv') order by id) to '$E/rt.csv' (format csv, header)"
echo "--- bytes ---"; od -c "$E/rt.csv" | head -8
run "select id, name, score from read_csv('$E/rt.csv') order by id"
echo "--- source truth ---"
run "select id, name, score from read_csv('$D/basic.csv') order by id"

echo "######## 2. header=false"
rm -f "$E/nohdr.csv"
run "copy (select id, name, score from read_csv('$D/basic.csv') order by id) to '$E/nohdr.csv' (format csv, header=false)"
echo "--- bytes ---"; od -c "$E/nohdr.csv" | head -8
run "select id, name, score from read_csv('$E/nohdr.csv', header=false) order by id"

echo "######## 3. tricky text: embedded newline and doubled quotes"
rm -f "$E/note.csv"
run "copy (select id, note from read_csv('$D/basic.csv') order by id) to '$E/note.csv' (format csv, header)"
echo "--- bytes ---"; od -c "$E/note.csv" | head -14
run "select id, replace(note, char(10), '<NL>') as note from read_csv('$E/note.csv') order by id"

echo "######## 4. nullstr 'NULL'"
rm -f "$E/nul.csv"
run "copy (select id, day from read_csv('$D/basic.csv') order by id) to '$E/nul.csv' (format csv, header, nullstr 'NULL')"
echo "--- bytes ---"; od -c "$E/nul.csv" | head -8
run "select id, day from read_csv('$E/nul.csv', header=true, nullstr='NULL') order by id"
run "select count(*) as null_days from read_csv('$E/nul.csv', header=true, nullstr='NULL') where day is null"

echo "######## 5. custom delimiter"
rm -f "$E/pipe.csv"
run "copy (select id, name from read_csv('$D/basic.csv') order by id) to '$E/pipe.csv' (format csv, header, delimiter '|')"
echo "--- bytes ---"; od -c "$E/pipe.csv" | head -8
run "select id, name from read_csv('$E/pipe.csv', delimiter='|') order by id"

echo "######## 6. fail if the target already exists"
echo "--- existing target re-run (expect an error) ---"
run "copy (select id, name, score from read_csv('$D/basic.csv') order by id) to '$E/rt.csv' (format csv, header)"

echo "######## 7. cleanup when the query fails at execution time"
rm -f "$E/fail1.csv" "$E/fail2.csv"
echo "--- 7a scalar subquery returning more than one row ---"
run "copy (select id, (select 1 union all select 2) as x from read_csv('$D/basic.csv') order by id) to '$E/fail1.csv' (format csv, header)"
ls -l "$E/fail1.csv" 2>&1
echo "--- 7b malformed source csv ---"
run "copy (select * from read_csv('$E/bad.csv')) to '$E/fail2.csv' (format csv, header)"
ls -l "$E/fail2.csv" 2>&1

echo "######## 8. syntax guards"
run "copy (select 1) to '$E/x1.csv' (format parquet)"
run "copy (select 1) to '$E/x2.csv' (bogus_option 1)"
run "copy (select 1) to '/etc/seekdb_should_not_write.csv' (format csv)"

echo "######## 9. files produced"
ls -l "$E"
