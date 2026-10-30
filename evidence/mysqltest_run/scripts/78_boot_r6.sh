#!/bin/bash
# Round 6 boot: DEFAULT parameters only. Port 2886.
set -u
ROOT=/home/hjy/work/seekdb
BIN=$ROOT/build/bin/src/observer/seekdb
CLI=$ROOT/deps/3rd/u01/obclient/bin/obclient
BASE=${1:-/home/hjy/work/fs_e2e/r13}
PORT=2886
if (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null; then echo PORT_IN_USE; exit 91; fi
rm -rf "$BASE"; mkdir -p "$BASE"
nohup "$BIN" --base-dir="$BASE" --port=$PORT --nodaemon > "$BASE/boot.log" 2>&1 &
OPID=$!
echo "OBSERVER_PID=$OPID"
UP=0
for i in $(seq 1 300); do
  if ! kill -0 "$OPID" 2>/dev/null; then echo "PROCESS_DIED after ${i}s"; break; fi
  if (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null; then UP=1; echo "PORT_UP after ${i}s"; break; fi
  sleep 1
done
if [ $UP -ne 1 ]; then echo BOOT_FAILED; exit 92; fi
sleep 3
"$CLI" -h127.0.0.1 -P$PORT -uroot -e "select 1 as ok, @@version as ver, @@enable_file_sql as efs" 2>&1 | head -5
"$CLI" -h127.0.0.1 -P$PORT -uroot -e "create database if not exists test;" 2>&1 | head -3
"$CLI" -h127.0.0.1 -P$PORT -uroot -e "create user if not exists admin identified by 'admin';" 2>&1 | head -3
"$CLI" -h127.0.0.1 -P$PORT -uroot -e "grant all privileges on *.* to admin with grant option;" 2>&1 | head -3
"$CLI" -h127.0.0.1 -P$PORT -uadmin -padmin -Dtest -e "select 1 as admin_ok" 2>&1 | head -5
