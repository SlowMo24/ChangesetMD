<# 
Test Create schema and replicate from Planet Changesets repository
Windows Powershell script
@author Pierre Béland 2021
#>
clear
$OutputEncoding = [System.Console]::OutputEncoding = [System.Console]::InputEncoding = [System.Text.Encoding]::UTF8
$PSDefaultParameterValues['*:Encoding'] = 'utf8'

Write-host "==================== < changesetMD    ===================="
Write-host

psql -h localhost -p 7513 -U osm -c "CREATE DATABASE IF NOT EXISTS changesetMD WITH  OWNER = osm"
psql -h localhost -p 7513 -U osm -d changesetMD -c "CREATE SCHEMA IF NOT EXISTS test_doreplication"

python changesetMD.py -H localhost -P 7513 -u osm -d changesetMD --schema=test_doreplication --create --geometry

<#t
o test replication, we suggest you look at file https://planet.openstreetmap.org/replication/changesets/state.yaml for sequence value. 
Example
---
last_run: 2021-01-02 18:17:02.297008000 +00:00
sequence: 4263126
If you remove 50 from this sequence (ex. 4263076), you will be able to test with a small subset of the Planet Replication site
#>
psql -h localhost -p 7513 -U osm -d changesetMD -c "update test_doreplication.osm_changeset_state set update_in_progress = 0,  last_sequence = '4263076', last_timestamp = '2021-01-02 18:17:02.297008000'"

python changesetMD.py -H localhost -P 7513 -u osm -d OsmContributorStat --schema=test_doreplication --replicate --geometry --bulkrows=1000

Write-host
Write-host "====================   changesetMD /> ===================="
