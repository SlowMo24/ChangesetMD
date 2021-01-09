<# 
changesetMD - Test2 - Parse file

This Test script documents the various steps  to Parse an OSM Replication file in the database.
To reduce memory consumption and delays with Powershell, we use 
python option [-u]. This force the stdout and stderr streams to be unbuffered 

Windows Powershell script
@author Pierre Béland 2021
#>
clear
$OutputEncoding = [Console]::OutputEncoding = [Text.UTF8Encoding]::UTF8
$PSDefaultParameterValues['*:Encoding'] = 'utf8'

# specify directory where you stored the changesetMD python modules
Set-Location "D:\OsmContributorStats\changesetMD_p3\github"

Write-host "==================== < changesetMD    ===================="
Write-host -BackgroundColor white -ForegroundColor blue " test2_parsefile.ps1                                "
Write-host " "

Write-host -ForegroundColor cyan "Truncate tables (Fast row delete) and remove constraint and indexes"
Write-host -ForegroundColor cyan "Parse large bz2 file  (Insert without indexes)"
#python  -u -X utf8 changesetMD.py -H localhost -P 7513 -u osm -d changesetmd_test --schema=testfile --trunc --geometry --bulkrows=500000 --logfile --file=test/changesets-2016-06-04-10-52.osm.bz2
python  -u -X utf8 changesetMD.py -H localhost -P 7513 -u osm -d changesetmd_test --schema=testfile --trunc --geometry --bulkrows=500 --logfile --file=test/changesets_testfile.osm
Write-host " "
Write-host "====================   changesetMD /> ===================="
