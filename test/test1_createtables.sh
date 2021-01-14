#!/bin/sh
: ' 
changesetMD - Test1 - Creates PostgreSQL Database, schema and tables

NOTE : If --geometry option is added, Superuser Role is necessary to create the POSTGIS extension.
You can ask the Superuser to run this script to create the database

Linux SH script
@author Pierre Béland 2021
'
clear
#
# directory where the changesetMD python modules are stored
cd .. 												
echo "==================== < changesetMD    ===================="
echo "--- test1_createtables.ps1                       ---"
echo "--- Create database                              ---"

createdb -h localhost -p 5432 -U osm "changesetmd_test"

python3 changesetmd.py -H localhost -P 5432 -u osm -p osm -d changesetmd_test --schema=testfile --create --geometry

echo "\n====================   changesetMD /> ===================="
#back to test directory
cd test
