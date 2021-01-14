#!/bin/sh
: ' 
changesetMD - Test2 - Parse file

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
echo "--- test2_parsefile.sh                                 ---"

echo "Prior to test, Drops schema if exists already"
psql -h localhost -p 5432 -U osm  -d changesetmd_test -c "drop schema if exists testfile cascade"

echo "Parses large file, osm or osm.bz2  (Insert without indexes)"
python3  -u -X utf8 changesetmd.py -H localhost -P 5432 -u osm -p osm -d changesetmd_test --schema=testfile --create --geometry --bulkrows=500 --logfile --file="test/changesets_testfile.osm"
# instruction to Parse latest bz2 file located in planet subdirectory
#python  -u -X utf8 changesetmd.py -H localhost -P 5432 -u osm -p osm -d changesetmd_test --schema=testfile --trunc --geometry --bulkrows=500000 --logfile --file=planet/changesets-latest.osm.bz2

echo "\n====================   changesetMD /> ===================="
#back to test directory
cd test
