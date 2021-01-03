<# 
Test Create schema and replicate from Planet Changesets repository
Windows Powershell script
@author Pierre Béland 2021

For documentation, lines below and in changesetMD.py contain Db Instructions to create database, extensions and schemas. 
Your Db user account need to have have the rights to create these various objects. 
If you encounter problems, you should contact your Db administrator to provide you rights or run the script to create these objects.

#>
clear
$OutputEncoding = [System.Console]::OutputEncoding = [System.Console]::InputEncoding = [System.Text.Encoding]::UTF8
$PSDefaultParameterValues['*:Encoding'] = 'utf8'

Write-host "==================== < changesetMD    ===================="
Write-host

# note if restrictions to create on your Database, assure that the Admin creates / gives access authorisation

# comment the next two lines after the db, schema and tables created
createdb -h localhost -p 7513 -U osm "changesetMD"
python changesetMD.py -H localhost -P 7513 -u osm -d changesetMD --schema=test_dopartial --create --geometry



<#
to test replication, we suggest you look at file https://planet.openstreetmap.org/replication/changesets/state.yaml for sequence value. 
Example
---
last_run: 2021-01-02 18:17:02.297008000 +00:00
sequence: 4263126
If you remove 50 from this sequence (ex. 4263076), you will be able to test with a small subset of the Planet Replication site
#>
psql -h localhost -p 7513 -U osm -d changesetMD -c "update test_doreplication.osm_changeset_state set update_in_progress = 0,  last_sequence = '4263076', last_timestamp = '2021-01-02 18:17:02.297008000'"

# running the following line, if log error «Relation do not exist» might indicate that the postgres user do not have right to access the tables - if such case arize, you must consult your Db administrator
python changesetMD.py -H 'localhost' -P 7513 -u 'osm' --database='changesetMd' --schema=test_doreplication --replicate --geometry --bulkrows=1000

Write-host
Write-host "====================   changesetMD /> ===================="
