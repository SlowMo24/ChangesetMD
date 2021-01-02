<# 
Test Create schema and do Partial Replicate from Planet Changesets repository using fromseq and toseq parameters
Windows Powershell script
@author Pierre Béland 2021
#>
clear
$OutputEncoding = [System.Console]::OutputEncoding = [System.Console]::InputEncoding = [System.Text.Encoding]::UTF8
$PSDefaultParameterValues['*:Encoding'] = 'utf8'

Write-host "==================== < changesetMD    ===================="
Write-host

psql -h localhost -p 7513 -U osm -c "CREATE DATABASE IF NOT EXISTS changesetMD WITH  OWNER = osm"
psql -h localhost -p 7513 -U osm -d changesetMD -c "CREATE SCHEMA IF NOT EXISTS test_dopartial"

python changesetMD.py -H localhost -P 7513 -u osm -d changesetMD --schema=test_dopartial --create --geometry

<#t
test Partial replication
we suggest a difference of 50 between fromseq and toseq
osm_changeset_state table is not used and modified by running this script
---
Example: sequences from 4263076 to 4263126
---
The bulkrows «throttle» parameter is for Bulk rows insert / commit to reduce acess to the PostgreSQL Db. Data is kep in Arrays. Check for any impact on the memory used by the application.
#>

python changesetMD.py -H localhost -P 7513 -u osm -d OsmContributorStat --schema=test_dopartial --replicate --geometry --bulkrows=1000 --fromseq=4263076 --toseq=4263126

Write-host
Write-host "====================   changesetMD /> ===================="
