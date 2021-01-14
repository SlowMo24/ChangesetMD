<# 
changesetMD - Test4 - doPartialReplication

Partially Replicates  Planet Changesets in the postgreSQL database wit parameters fromseq and toseq
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
Write-host -BackgroundColor white -ForegroundColor blue "test4_dopartialreplication.ps1                            "
Write-host " "

Write-host -ForegroundColor cyan "Delete tables rows prior to Test"
psql -h localhost -p 5432 -U osm -d changesetmd_test -c "delete from testfile.osm_changeset;"
psql -h localhost -p 5432 -U osm -d changesetmd_test -c "delete from testfile.osm_changeset_comment;"

Write-host -ForegroundColor cyan "Partial Replication "
python -u -X utf8 changesetmd.py -H 'localhost' -P 5432 -u 'osm' --database='changesetmd_test' --schema=testfile --replicate --geometry --bulkrows=500 --fromseq=4260811 --toseq=4261001

Write-host
Write-host "====================   changesetMD /> ===================="
