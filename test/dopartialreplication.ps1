<# 
Test Create schema and do Partial Replicate from Planet Changesets repository using fromseq and toseq parameters
Windows Powershell script
@author Pierre BÃ©land 2021
	-- createdb osm_qual
	-- psql -d osm_qual -c "CREATE EXTENSION postgis"
	-- psql -d osm_qual -c "CREATE EXTENSION hstore"
#>
clear
$OutputEncoding = [System.Console]::OutputEncoding = [System.Console]::InputEncoding = [System.Text.Encoding]::UTF8
$PSDefaultParameterValues['*:Encoding'] = 'utf8'

Write-host "==================== < changesetMD    ===================="
Write-host

# note if restrictions to create on your Database, assure that the Admin creates / gives access authorisation

# comment the following two lines after the Database, schema and tables are created
createdb -h localhost -p 7513 -U osm "changesetMD"
python changesetMD.py -H localhost -P 7513 -u osm -d changesetMD --schema=t_dopartial --create --geometry

<#
test Partial replication
we suggest a difference of 50 between fromseq and toseq
osm_changeset_state table is not used and modified by running this script
---
Example: sequences from 4263076 to 4263126
---
The bulkrows Â«throttleÂ» parameter is for Bulk rows insert / commit to reduce acess to the PostgreSQL Db. Data is kep in Arrays. Check for any impact on the memory used by the application.
#>

python changesetMD.py -H localhost -P 7513 -u osm -d changesetMD --schema=t_dopartial --replicate --geometry --bulkrows=1000 --fromseq=4263076 --toseq=4263166

Write-host
Write-host "====================   changesetMD /> ===================="
