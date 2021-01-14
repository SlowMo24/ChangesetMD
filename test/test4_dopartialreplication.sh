#!/bin/sh
: ' 
Partially Replicates  Planet Changesets in the postgreSQL database wit parameters fromseq and toseq
The python option [-u] reduces memory usage as it forces the stdout and stderr streams to be unbuffered 

Linux SH script
@author Pierre Béland 2021
'

clear
# directory where the changesetMD python modules are stored
cd ".."
echo "==================== < changesetMD    ===================="
echo  "test4_dopartialreplication.ps1                            "
echo "\nDelete tables rows prior to Test"
psql -h localhost -p 5432 -U osm -d changesetmd_test -c "delete from testfile.osm_changeset;"
psql -h localhost -p 5432 -U osm -d changesetmd_test -c "delete from testfile.osm_changeset_comment;"

echo "Partial Replication "
python3 -u -X utf8 changesetmd.py -H 'localhost' -P 5432 -u 'osm' -p 'osm' --database='changesetmd_test' --schema=testfile --replicate --geometry --bulkrows=10000 --fromseq=4260811 --toseq=4261001

echo
echo "\n====================   changesetMD /> ===================="
cd test
