<# 
Test to Create PosgreSQL Db, extensions, schema and do Partial Replicate from Planet Changesets repository using fromseq and toseq parameters
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
to test Partial replication
we suggest a difference of 50 between fromseq and toseq
osm_changeset_state table is not used and modified by running this script
---
Example: sequences from 4263076 to 4263126
---
The bulkrows Â«throttleÂ» parameter is for Bulk rows insert / commit to reduce acess to the PostgreSQL Db. Data is kep in Arrays. Check for any impact on the memory used by the application.
#>

# running the following line, if log error «Relation do not exist» might indicate that the postgres user do not have right to access the tables - if such case arize, you must consult your Db administrator
python changesetMD.py -H 'localhost' -P 7513 -u 'osm' --database='changesetMd' --schema=test_dopartial --replicate --geometry --bulkrows=1000 --fromseq=4263076 --toseq=4263126

Write-host
Write-host "====================   changesetMD /> ===================="
