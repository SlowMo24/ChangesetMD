<# 
changesetMD - Test3 - doReplication

Replicates  Planet Changesets in the postgreSQL database from the last sequence inserted.
The python option [-u] reduces memory usage as it forces the stdout and stderr streams to be unbuffered 

Windows Powershell script
@author Pierre Béland 2021
#>
clear
$OutputEncoding = [System.Console]::OutputEncoding = [System.Console]::InputEncoding = [System.Text.Encoding]::UTF8
$PSDefaultParameterValues['*:Encoding'] = 'utf8'
#
# directory where the changesetMD python modules are stored
cd ..

Write-host "==================== < changesetMD    ===================="
Write-host -BackgroundColor white -ForegroundColor blue "test3_doreplication.ps1                            "
Write-host " "

Write-host -ForegroundColor cyan "Delete tables rows prior to Test"
psql -h localhost -p 5432 -U osm -d changesetmd_test -c "delete from testfile.osm_changeset;"
psql -h localhost -p 5432 -U osm -d changesetmd_test -c "delete from testfile.osm_changeset_comment;"

Write-host -ForegroundColor cyan "Update osm_changeset_state with sequence from today "
psql -h localhost -p 5432 -U osm -d changesetmd_test -c "update testfile.osm_changeset_state set update_in_progress = 0,  last_sequence = '4270770', last_timestamp = '2021-01-07 23:00:00'"
# Running the previous instruction, if log error «Relation do not exist»is reported, this might indicate that the postgres user do not have right to access the tables - if such case arize, you must consult your Db administrator

python -u -X utf8 changesetMD.py -H 'localhost' -P 5432 -u 'osm' --database='changesetmd_test' --schema=testfile --replicate --geometry --bulkrows=200

Write-host
Write-host "====================   changesetMD /> ===================="
#back to test directory
cd test
