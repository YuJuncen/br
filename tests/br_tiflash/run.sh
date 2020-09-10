#!/bin/sh
#
# Copyright 2020 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu
DB="${TEST_NAME}_DATABASE"
RECORD_COUNT=1000

tiflash_replica_ready() {
   i=0
   while ! grep "sum(AVAILABLE): 1" <(run_sql "select sum(AVAILABLE) from information_schema.tiflash_replica WHERE TABLE_NAME = $1"); do
     i=$((i+1))
     if [ "$i" -gt 10 ]; then
         echo "wait tiflash replica ready timeout"
         return 1
     fi
     sleep 30
     run_sql "select sum(AVAILABLE) from information_schema.tiflash_replica WHERE TABLE_NAME = $1"
   done
 }

run_sql "CREATE DATABASE $DB" 

run_sql "CREATE TABLE $DB.kv(k varchar(256), v int primary key)"
run_sql "ALTER TABLE $DB.kv SET TIFLASH REPLICA 1"

stmt="INSERT INTO $DB.kv(k, v) VALUES ('1-record', 1)"
for i in $(seq 2 $RECORD_COUNT); do
    stmt="$stmt,('$i-record', $i)"
done
run_sql "$stmt"

tiflash_replica_ready "'kv'"

rm -rf "/${TEST_DIR}/$DB"
run_br backup full -s "local://$TEST_DIR/$DB" --pd $PD_ADDR

run_sql "DROP DATABASE $DB"
run_br restore full -s "local://$TEST_DIR/$DB" --pd $PD_ADDR

tiflash_replica_ready "'kv'"

run_sql "SELECT /*+ read_from_storage(tiflash[$DB.kv]) */ count(*) FROM $DB.kv;"
AFTER_BR_COUNT=`run_sql "SELECT /*+ read_from_storage(tiflash[kv]) */ count(*) FROM $DB.kv;" | sed -n "s/[^0-9]//g;/^[0-9]*$/p" | tail -n1`
echo "before: $RECORD_COUNT; after: $AFTER_BR_COUNT"
if [ $AFTER_BR_COUNT != $RECORD_COUNT ]; then
    echo "...failed to restore!"
    exit 1
fi

run_sql "DROP DATABASE $DB"

echo "TEST $TEST_NAME passed!"