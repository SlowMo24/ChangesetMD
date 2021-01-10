<# 
changesetMD - Test4 - doPartialReplication

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
Write-host -BackgroundColor white -ForegroundColor blue "test4_dopartialreplication.ps1                            "
Write-host

Write-host -ForegroundColor cyan "Delete tables rows prior to Test"
psql -h localhost -p 5432 -U osm -d changesetmd_test -c "delete from testfile.osm_changeset;"
psql -h localhost -p 5432 -U osm -d changesetmd_test -c "delete from testfile.osm_changeset_comment;"

Write-host -ForegroundColor cyan "Partial Replication "
python -u -X utf8 changesetMD.py -H 'localhost' -P 5432 -u 'osm' --database='changesetmd_test' --schema=testfile --replicate --geometry --bulkrows=500 --fromseq=4260811 --toseq=4261001


Write-host
Write-host "====================   changesetMD /> ===================="
