<# 
changesetMD - Test2 - Parse file

Parses XML OSM Changeset metadata Replication file in the postgreSQL database.
The python option [-u] reduces memory usage as it forces the stdout and stderr streams to be unbuffered 

Windows Powershell script
@author Pierre Béland 2021
#>
clear
$OutputEncoding = [Console]::OutputEncoding = [Text.UTF8Encoding]::UTF8
$PSDefaultParameterValues['*:Encoding'] = 'utf8'

#
# directory where the changesetMD python modules are stored
cd ..
Write-host "==================== < changesetMD    ===================="
Write-host -BackgroundColor white -ForegroundColor blue " test2_parsefile.ps1                                "
Write-host " "

Write-host -ForegroundColor cyan "Prior to test, Drops schema if exists already"
psql -h localhost -p 5432 -U osm -d changesetmd_test -c "drop schema if exists testfile cascade"

Write-host -ForegroundColor cyan "Parse large bz2 file  (Insert without indexes)"
python  -u -X utf8 changesetmd.py -H localhost -P 5432 -u osm -d changesetmd_test --schema=testfile --trunc --geometry --bulkrows=500 --logfile --file=test/changesets_testfile.osm
# instruction to Parse latest bz2 file located in planet subdirectory
#python  -u -X utf8 changesetmd.py -H localhost -P 5432 -u osm -d changesetmd_test --schema=testfile --trunc --geometry --bulkrows=500000 --logfile --file=planet/changesets-latest.osm.bz2

Write-host " "
Write-host -ForegroundColor cyan "Constraint and indexes added)"
Write-host "====================   changesetMD /> ===================="
#back to test directory
cd test
