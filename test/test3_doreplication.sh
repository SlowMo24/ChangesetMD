#!/bin/sh
: ' 
changesetMD - Test3 - doReplication

Replicates  Planet Changesets in the postgreSQL database from the last sequence inserted.
The python option [-u] reduces memory usage as it forces the stdout and stderr streams to be unbuffered 

Linux SH script
@author Pierre Béland 2021
'
clear
#
# directory where the changesetMD python modules are stored
cd ".."
echo "==================== < changesetMD    ===================="
echo "--- test3_doreplication.sh                            ---"

echo "\nDelete tables rows prior to Test"
psql -h localhost -p 5432 -U osm -d changesetmd_test -c "delete from testfile.osm_changeset;"
psql -h localhost -p 5432 -U osm -d changesetmd_test -c "delete from testfile.osm_changeset_comment;"

echo "Update osm_changeset_state with sequence from today "
psql -h localhost -p 5432 -U osm -d changesetmd_test -c "update testfile.osm_changeset_state set update_in_progress = 0,  last_sequence = '4280420', last_timestamp = '2021-01-07 23:00:00'"
# Running the previous instruction, if log error «Relation do not exist» is reported, this might indicate that the postgres user do not have right to access the tables - if such case arize, you must consult your Db administrator

python3 -u -X utf8 changesetmd.py -H 'localhost' -P 5432 -u 'osm' -p 'osm' --database='changesetmd_test' --schema=testfile --replicate --geometry --bulkrows=200

echo "\n====================   changesetMD /> ===================="
#back to test directory
cd test
