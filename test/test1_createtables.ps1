<# 
changesetMD - Test1 - Creates PostgreSQL Database, schema and tables

NOTE : If --geometry option is added, Superuser Role is necessary to create the POSTGIS extension.
You can ask the Superuser to run this script to create the database

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
Write-host -BackgroundColor white -ForegroundColor blue " test1_createtables.ps1                             "
Write-host
Write-host -ForegroundColor white " Create database                                    "

$error.clear()
try   {createdb -h localhost -p 5432 -U osm "changesetmd_test"}
catch {Write-host -BackgroundColor white -ForegroundColor red " Create database error occured: " $_.Exception.Message }

python changesetMD.py -H localhost -P 5432 -u osm -d changesetmd_test --schema=testfile --create --geometry

Write-host
Write-host "====================   changesetMD /> ===================="
#back to test directory
cd test
