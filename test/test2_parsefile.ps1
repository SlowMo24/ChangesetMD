<# 
changesetMD - Test2 - Parse file

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
Write-host -BackgroundColor white -ForegroundColor blue " test2_parsefile.ps1                                "
Write-host

Write-host -ForegroundColor cyan "Write-host -ForegroundColor cyan "Delete tables rows prior to Test" "
psql -h localhost -p 5432 -U osm -d changesetmd_test -c "delete from testfile.osm_changeset;"
psql -h localhost -p 5432 -U osm -d changesetmd_test -c "delete from testfile.osm_changeset_comment;"

python changesetMD.py -H localhost -P 5432 -u osm -d changesetmd_test --schema=testfile --geometry --bulkrows=150 --file=test/changesets_testfile.osm

Write-host
Write-host "====================   changesetMD /> ===================="
