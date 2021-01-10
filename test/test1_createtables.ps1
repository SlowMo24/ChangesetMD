<# 
changesetMD - Create PostgreSQL Database, schema and tables

This Test script documents the various steps  to create database, extensions and schemas.
NOTE : If you add --geometry option, Superuser Role is necessary to create the POSTGIS extension.
You can ask the Superuser to run this script for you

Windows Powershell script
@author Pierre Béland 2021
#>
clear
$OutputEncoding = [System.Console]::OutputEncoding = [System.Console]::InputEncoding = [System.Text.Encoding]::UTF8
$PSDefaultParameterValues['*:Encoding'] = 'utf8'
#
# specify directory where you stored the changesetMD python modules
#SET PATH="C:/Program Files/PostgreSQL/13/bin";%PATH%
Set-Location "D:\OsmContributorStats\changesetMD_p3\github"


Write-host "==================== < changesetMD    ===================="
Write-host -BackgroundColor white -ForegroundColor blue " test1_createtables.ps1                             "
Write-host
Write-host -ForegroundColor white " Create database                                    "

$error.clear()
try   {createdb -h localhost -p 5432 -U osm "changesetmd_test"}
catch {Write-host -BackgroundColor white -ForegroundColor red " Create database error occured: " $_.Exception.Message }

python -u -X utf8 changesetMD.py -H localhost -P 5432 -u osm -d changesetmd_test --schema=testfile --create --geometry

Write-host
Write-host "====================   changesetMD /> ===================="
