#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
create user changesets with password 'changesets';
create database changesets owner changesets;
\c changesets
create extension postgis;
EOSQL

