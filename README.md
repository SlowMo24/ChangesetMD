ChangesetMD
=========

ChangesetMD is a simple XML parser written in python that takes the weekly changeset metadata dump file
from http://planet.openstreetmap.org/ and shoves the data into a simple postgres database so it can be queried.

It can also keep a database created with a weekly dump file up to date using minutely changeset diff files available
at [http://planet.osm.org/replication/changesets/](http://planet.osm.org/replication/changesets/)

Setup
------------

ChangesetMD works with Python 3.8.

Aside from postgresql (9.5+), ChangesetMD depends on the python libraries psycopg2 and lxml.
On Debian-based systems this means installing the python-psycopg2 and python-lxml packages.

If you are using `pip` and `virtualenv`, you can install all dependencies with `pip install -r requirements.txt`.

If you want to parse the changeset file without first unzipping it, you will also need to install
the [bz2file library](http://pypi.python.org/pypi/bz2file) since the built-in bz2 library can not handle multi-stream
bzip files.

Two ```postgis``` extensions need to be [installed](http://postgis.net/install) in your PostgreSQL database:

- ```geometry``` for building geometries,

ChangesetMD expects a postgres database to be set up for it. It can likely co-exist within another database if desired.
Otherwise, As the postgres user execute:

    createdb changesets

It is easiest if your OS user has access to this database. I just created a user and made myself a superuser. Probably
not best practices.

    createuser <username>

While for production, you don't want to have many versions of this huge database,  ```schema``` option lets you
replicate subsets for analysis or tests. This ```schema``` option needs to be added to the various instructions.

For the various steps below, test files (Linux SH and Windows Powershell) are provided to better document the steps and
options necessary.

Execution
------------
The first time you run it, you will need to include the -c | --create option to create the table. To use the "public"
schema speficy `-s public` below.

    python changesetmd.py -d <database> -c [-s <schema>] (schema "none" by default)

> Script test1_createtables : [Linux](test/test1_createtables.sh) , [Windows](test/test1_createtables.ps1)

The create function can be combined with the file option to immediately parse a file.

To parse a dump file, use the -f | --file option. The -b |
--bulkrows «throttle» option (default value is 100000), let's specify the size of the commit (i.e. simultaneous number of
lines inserted / committed to the database). This reduces write access to the database. For computers with less memory,
it is possible to add the -B | --bz2buffer option to control the memory read buffer size parsing the
changesets-latest.osm.bz2 file.

    python changesetmd.py -d <database> [-s <schema>] [-b <bulkrows>] -f /tmp/changeset-latest.osm
    python changesetmd.py -d <database> [-s <schema>] [-b <bulkrows>] [-B <bz2buffer>] -f /tmp/changeset-latest.osm.bz2

> Script test2_parsefile : [Linux](test/test2_parsefile.sh) , [Windows](test/test2_parsefile.ps1)

If no other arguments are given on linux os, it will access postgres using the default settings of the postgres client,
typically connecting on the unix socket as the current OS user. Use the ```--help``` argument to see optional arguments
for connecting to postgres.

You can add the `-g` | `--geometry` option to build polygon geometries (the database also needs to be created with this
option).

Replication
------------
After you have parsed a weekly dump file into the database, the database can be kept up to date using changeset diff
files that are generated on the OpenStreetMap planet server every minute. This is also possible after you used Partial
Replication. If you don't need all the history from 2004, you could for example start from 2021-01-01 and replicate
from the last sequence you already processed.

To initiate the replication system you will need to find out which minutely sequence number you need to start with and
update the ```osm_changeset_state``` table so that ChangesetMD knows where to start. Unfortunately there isn't an easy
way to get the needed sequence number from the dump file. Here is the process to find it:

The Weekly dump file contains all changesets metadata from 2004 (some 100 million records, 4Gb, early 2021). It is
available from [http://planet.osm.org/](http://planet.osm.org/) and some Replication sites across the world.

First, determine the timestamp present in the first line of XML in the dump file. Assuming you are starting from the
.bzip2 file, use this command:

    bunzip2 -c discussions-latest.osm.bz2 | head

Look for this line:

    <osm license="http://opendatacommons.org/licenses/odbl/1-0/" copyright="OpenStreetMap and contributors" version="0.6" generator="planet-dump-ng 1.1.2" attribution="http://www.openstreetmap.org/copyright" timestamp="2015-11-16T01:59:54Z">

Note the timestamp at the end of it. In this case, just before 02:00 on November 16th, 2015. Now browse
to [http://planet.osm.org/replication/changesets/](http://planet.osm.org/replication/changesets/) and navigate the
directories until you find files with a similar timestamp as the one from the dump file. Each second level directory
contains 1,000 diffs so there is generally one directory per day with one day occasionally crossing two directories.

Unfortunately there is no metadata file that goes along with the changeset diff files (like there is with the map data
diff files) so there isn't a way to narrow it down to one specific file. However, it is safe to apply older diffs to the
database since it will just update the data to its current state again. So just go back 2 or 3 hours from the timestamp
in the dump file and start there. This will ensure that any time zone setting or daylight savings time will be accounted
for. So in the example from above, look for the file with a timestamp around November 15th at 23:00 since that is 3
hours before the given timestamp in the dump file of 02:00 on November 16th.

This gives the file 048.osm.gz in the
directory [http://planet.osm.org/replication/changesets/001/582/](http://planet.osm.org/replication/changesets/001/582/).
Now take the numbers of all the directories and the file and remove the slashes. So 001/582/048.osm.gz becomes: 1582048.
This is the sequence to start replication at. To set this, run the following SQL query in postgres:

    update osm_changeset_state set last_sequence = 1582048;

Now you are ready to start consuming the replication diffs with the following command:

    python changesetmd.py -d <database> [-s <schema>] [-b <bulkrows>]  -r

> Script test3_doreplication : [Linux](test/test3_doreplication.sh) , [Windows](test/test3_doreplication.ps1)

Run this command as often as you wish to keep your database up to date with OSM. You can put it in a cron job that runs
every minute if you like. The first run may take a few minutes to catch up but each subsequent run should only take a
few seconds to finish.

Partial Replication
------------
For Partial Replication, you manually control everything. In such case, the Partial Replication function don't
synchronize (read and write) with the ```osm_changeset_state``` table. It is up to you to control the Changeset
sequences. First, you need to
consult  [http://planet.osm.org/replication/changesets](http://planet.osm.org/replication/changesets/) to determine the
FromSeq and ToSeq of your Partial Replication request. For example, FromSeq=4260811 and ToSeq=4262246 will extract
changeset metadatas for 2001-01-01 (utc time). These two sequences are added to the instructions to specify Partial
Replication.

    python changesetmd.py -d <database> [-s <schema>] [-b <bulkrows>]  -r -F --fromseq=4260811 --toseq=4262246

Once this Partial replication is done, you could update manually the ```osm_changeset_state``` table to document the
last sequence and timestamp inserted in this schema and then Replicate from this point. For example, we could continue
the Replicate for a weekly project to follow from this point.

    python changesetmd.py -d <database> [-s <schema>] [-b <bulkrows>]  -r

> Script
> test4_dopartialreplication : [Linux](test/test4_dopartialreplication.sh) , [Windows](test/test4_dopartialreplication.ps1)

Logging
------------
Status messages are printed every after each Batch ```bulkrow``` is inserted / committed in the Db. Adding the option
```----logfile``` will append messages to the file ChangesetMD_YYYY-MM-DD.log. Note that it is safe to start from the
last sequence reported in the log since the report is printed after insert / commit of the block of lines.

The log shows the Db Insert Rate (Records per second) for each ```bulkrow```. This will vary a lot based on your
computer (laptop or server), type of disk and tuning of your PostgreSQL database. The replication log below for 2021-01-07 is from a laptop. We see that the number of sequences (files downloaded from the Planet server) and time-cost for
each bulkrow vary significatively. Comparatively, when parsing a planet/changesets-latest.osm.bz2 already saved on
disk (no internet download and no constraint and indexes), Db Insert rate increases to around 3,000 Recs/sec.

| Log example - Partial Replication for 2021-01-01                                                                     |
|----------------------------------------------------------------------------------------------------------------------| 
| 2021-01-07 22:36:46     doPartialReplication New                                                                     |  
| 2021-01-07 22:36:46  doPartialReplication try                                                                        |
| 2021-01-07 22:36:46  Partial replication (https://planet.openstreetmap.org/replication/changesets/) to PostgreSQL Db |
| 2021-01-07 22:36:46  From seq=4260811  to seq=4262246                                                                |

| 2021-01-07 22:36:46      |                               Last Db Sequence | Db Insert Rate Recs / sec. | Changeset Metadata Recs in Batch | Cum Recs | Last Db (UTC)            |
|--------------------------|-----------------------------------------------:|---------------------------:|---------------------------------:|---------:|--------------------------|
| 2021-01-07&nbsp;22:36:46 |                                      4,260,811 |                            |                                  |          |                          |
| 2021-01-07 22:40:41      |                                      4,261,391 |                      42.75 |                           10,044 |   10,044 | 2021-01-01&nbsp;09:44:00 |
| 2021-01-07 22:42:22      |                                      4,261,629 |                      98.46 |                           10,004 |   20,048 | 2021-01-01 13:42:00      |
| 2021-01-07 22:43:36      |                                      4,261,798 |                     135.73 |                           10,049 |   30,097 | 2021-01-01 16:30:59      |
| 2021-01-07 22:45:31      |                                      4,262,067 |                      87.34 |                           10,027 |   40,124 | 2021-01-01 20:59:54      |
| 2021-01-07 22:46:46      |                                      4,262,246 |                      62.88 |                            4,682 |   44,806 | 2021-01-01 23:58:57      |
| 2021-01-07 22:46:46      | finished with Partial replication as requested |                            |                                  |          |                          |
| 2021-01-07 22:46:46      |                             Time-Cost HH:MM:SS |                            |                                  |          |                          |
| 2021-01-07 22:46:46      |                                     0:09:59.86 |                      74.69 |                           44,806 |          |                          |
| 2021-01-07 22:46:46      |                       doPartialReplication End |                            |                                  |          |                          |

Notes
------------

- Takes 2-3 hours to import the current dump on a decent home computer, up to 8 hours on a laptop.
- Might be faster to process the XML into a flat file and then use the postgres COPY command to do a bulk load but this
  would make incremental updates a little harder
- Fields indexed have commonly been queried . Depending on what you want to do, you may need more indexes.
- Changesets can be huge in extent, so you may wish to filter them by area before any visualization. 225 square km seems
  to be a fairly decent threshold to get the actual spatial footprint of edits.
  `WHERE ST_Area(ST_Transform(geom, 3410)) < 225000000` will do the trick.
- Some changesets have bounding latitudes outside the range of [-90;90] range. Make sure you handle them right before
  projecting (e.g. for area checks).

Table Structure
------------
ChangesetMD populates two tables with the following structure:

osm\_changeset:  
Primary table of all changesets with the following columns:

- `id`: changeset ID
- `created_at/closed_at`: create/closed time
- `num_changes`: number of objects changed
- `min_lat/max_lat/min_lon/max_lon`: description of the changeset bbox in decimal degrees
- `user_name`: OSM username
- `user_id`: numeric OSM user ID
- `tags`: a jsonb column holding all the tags of the changeset
- `geom`: [optional] a postgis geometry column of `Polygon` type (SRID: 4326)

Note that all fields except for id and created\_at can be null.

osm\_changeset\_comment:
All comments made on changesets via the new commenting system

- `comment_changeset_id`: Foreign key to the changeset ID
- `comment_user_id`: numeric OSM user ID
- `comment_user_name`: OSM username
- `comment_date`: timestamp of when the comment was created

Example queries
------------
Count how many changesets have a comment tag:

    SELECT COUNT(*)
    FROM osm_changeset
    WHERE tags ? 'comment';

Find all changesets that were created by JOSM:

    SELECT COUNT(*)
    FROM osm_changeset
    WHERE tags -> 'created_by' LIKE 'JOSM%';

Find all changesets that were created in Liberty Island:

    SELECT count(id)
    FROM osm_changeset c, (SELECT ST_SetSRID(ST_MakeEnvelope(-74.0474545,40.6884971,-74.0433990,40.6911817),4326) AS geom) s
    WHERE ST_CoveredBy(c.geom, s.geom);

License
------------
Copyright (C) 2012 Toby Murray
2021 Pierre Béland

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public
License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

See the GNU Affero General Public License for more details: http://www.gnu.org/licenses/agpl.txt
