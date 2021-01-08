<# 
changesetMD - Test3 - doReplication

This Test script documents the various steps  to Parse an OSM Replication file in the database.

Windows Powershell script
@author Pierre Béland 2021
#>
clear
$OutputEncoding = [System.Console]::OutputEncoding = [System.Console]::InputEncoding = [System.Text.Encoding]::UTF8
$PSDefaultParameterValues['*:Encoding'] = 'utf8'
#
# specify directory where you stored the changesetMD python modules
Set-Location "D:\OsmContributorStats\changesetMD_p3\github"


Write-host "==================== < changesetMD    ===================="
Write-host -BackgroundColor white -ForegroundColor blue "test3_doreplication.ps1                            "
Write-host

Write-host -ForegroundColor cyan "Write-host -ForegroundColor cyan "Delete tables rows prior to Test" "
psql -h localhost -p 5432 -U osm -d changesetmd_test -c "delete from testfile.osm_changeset;"
psql -h localhost -p 5432 -U osm -d changesetmd_test -c "delete from testfile.osm_changeset_comment;"

Write-host -ForegroundColor cyan "Update osm_changeset_state with sequence from today "
psql -h localhost -p 5432 -U osm -d changesetmd_test -c "update testfile.osm_changeset_state set update_in_progress = 0,  last_sequence = '4270770', last_timestamp = '2021-01-07 23:00:00'"

# running the following line, if log error «Relation do not exist» might indicate that the postgres user do not have right to access the tables - if such case arize, you must consult your Db administrator
python changesetMD.py -H 'localhost' -P 5432 -u 'osm' --database='changesetmd_test' --schema=testfile --replicate --geometry --bulkrows=200


Write-host
Write-host "====================   changesetMD /> ===================="
