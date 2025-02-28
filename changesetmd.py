#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
ChangesetMD is a simple XML parser to read the weekly changeset metadata dumps
from OpenStreetmap into a postgres database for querying.
@author: Toby Murray 2012
         Pierre Béland 2021
"""

from __future__ import print_function

import argparse
import gzip
import logging
import sys
import time
from datetime import datetime
from typing import List

import psycopg2
import psycopg2.extras
import requests
import tqdm
import urllib3
import yaml
from lxml import etree

import queries

urllib3.disable_warnings()

try:
    from bz2file import BZ2File

    bz2Support = True
except ImportError:
    bz2Support = False

from psycopg2.extras import Json
from psycopg2.extensions import register_adapter

register_adapter(dict, Json)

# Block of changesets - nb to commit
DEFAULT_BULK_COPY_SIZE = 100000
BASE_REPL_URL = "https://planet.openstreetmap.org/replication/changesets/"


class ChangesetMD:
    logging.basicConfig(level=logging.INFO, filename="data/changesetmd_" + datetime.now().strftime('%Y-%m-%d') + ".log",
                        filemode="a+",
                        format="%(asctime)-15s %(levelname)-8s %(message)s")

    def __init__(self, create_geometry, schema="public", bulkrows=DEFAULT_BULK_COPY_SIZE, logfile=True,
                 contributors: List = None):
        self.parsedFileCount = 0
        if schema == "public":
            self.schema = "public"
            self.search_path = "public, postgis"
        else:
            self.schema = str(schema)
            self.search_path = str(schema) + ", public, postgis"
        self.createGeometry = create_geometry
        if bulkrows is None:
            self.bulkrows = DEFAULT_BULK_COPY_SIZE
        else:
            self.bulkrows = int(bulkrows)
        self.isLogging = logfile
        self.currentTimestamp = datetime.now()
        self.BatchstartTime = datetime.now()
        self.parsedCount = 0
        self.changesetsToProcess = 0
        self.beginTime = datetime.now()
        self.endTime = None
        self.contributors = contributors

    def msg_report(self, messsage: str):
        print("{0}  {1}".format(time.strftime('%Y-%m-%d %H:%M:%S'), messsage))
        if self.isLogging: logging.info(messsage)

    def report_header(self):
        self.beginTime = datetime.now()
        self.msg_report(
            "{0:^12}    {1:^8}    {2:^24}      {3:^25}".format("Last Db", "Db Insert Rate", "Changeset Metadata",
                                                               "Last Db timestamp"))
        self.msg_report(
            "{0:^12}    {1:^8}    {2:^12}    {3:^12}    {4:^25}".format("Sequence", "Recs / sec.", "Recs in Batch",
                                                                        "Cum Recs", "(UTC)"))
        self.msg_report('-' * 85)

    def report_bottom(self):
        self.endTime = datetime.now()
        time_cost, recs_second = self.calc_timecost(self.beginTime, self.endTime, self.parsedCount)
        self.msg_report("Time-Cost HH:MM:SS")
        self.msg_report("{0:16}  {1:>8}  {2:12,}".format(str(time_cost)[:10], recs_second, self.parsedCount))

    def report_progress(self, current_sequence: int, current_timestamp: datetime):
        time_cost, recs_second = self.calc_timecost(self.BatchstartTime, datetime.now(), self.changesetsToProcess)
        self.msg_report("{0:12,}    {1:>10}  {2:12,}  {3:12,}{4:10}{5:>19}    ".format(current_sequence, recs_second,
                                                                                       self.changesetsToProcess,
                                                                                       self.parsedCount, " ",
                                                                                       current_timestamp.strftime(
                                                                                           '%Y-%m-%d %H:%M:%S')))
        self.changesetsToProcess = 0
        self.BatchstartTime = datetime.now()

    @staticmethod
    def calc_timecost(begin_time, end_time, records):
        time_cost = end_time - begin_time
        recs_second = 0
        if time_cost.total_seconds() > 0:
            seconds = time_cost.total_seconds()
            recs_second = round((records / seconds), 2)
        return time_cost, recs_second

    def truncate_tables(self, db_connection):
        self.msg_report('truncating tables')
        db_cursor = db_connection.cursor()
        db_cursor.execute("TRUNCATE TABLE {0}.osm_changeset_comment CASCADE;".format(self.schema, ))
        db_cursor.execute("TRUNCATE TABLE {0}.osm_changeset CASCADE;".format(self.schema, ))
        db_cursor.execute(queries.dropIndexes.format(self.schema, ))
        db_cursor.execute(
            "UPDATE {0}.osm_changeset_state set last_sequence = -1, last_timestamp = null, update_in_progress = 0".format(
                self.schema, ))
        db_connection.commit()

    def create_tables(self, db_connection):
        local_cursor = db_connection.cursor()
        self.msg_report("--- createTables, schema = {0} ---".format(self.schema, ))
        sql = "create schema if not exists {0};".format(self.schema, )
        self.msg_report(sql)
        try:
            local_cursor.execute(sql)
        except psycopg2.OperationalError as err:
            self.msg_report("error {0}\n{1}".format(sql, err))
            return 1
        try:
            local_cursor.execute(queries.createChangesetTable.format(self.schema, ))
        except psycopg2.OperationalError as err:
            self.msg_report("error queries.createChangesetTable {0}".format(err, ))
            return 1
        self.msg_report("queries.initStateTable")
        try:
            local_cursor.execute(queries.initStateTable.format(self.schema, ))
        except psycopg2.OperationalError as err:
            self.msg_report("error queries.createChangesetTable {0}".format(err, ))
            return 1
        db_connection.commit()

        if self.createGeometry:
            self.msg_report("queries.createGeometryColumn")
            try:
                # modify column if exists, requires postgresql 9.6+
                local_cursor.execute(queries.createGeometryColumn.format(self.schema, ))
            except psycopg2.errors.DuplicateColumn:
                self.msg_report("Create geometry duplicate error : var geom already exist")
                return 1
            db_connection.commit()

    def insert_new_batch(self, db_connection, data_arr, is_replicate: bool):
        local_cursor = db_connection.cursor()
        if self.createGeometry:
            if is_replicate:
                sql = '''INSERT into {0}.osm_changeset AS t1
                    (id, user_id, created_at, min_lat, max_lat, min_lon, max_lon, closed_at, open, num_changes, user_name, tags, geom)
                    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,ST_MakeValid(ST_MakeEnvelope(%s,%s,%s,%s,4326)))
                    ON CONFLICT(id) DO UPDATE
                    SET created_at=excluded.created_at, min_lat=excluded.min_lat, max_lat=excluded.max_lat, min_lon=excluded.min_lon, max_lon=excluded.max_lon, closed_at=excluded.closed_at, open=excluded.open, num_changes=excluded.num_changes, user_name=excluded.user_name, tags=excluded.tags, geom=excluded.geom
                    WHERE t1.user_id=excluded.user_id'''.format(self.schema, )
            else:
                sql = '''INSERT into {0}.osm_changeset
                    (id, user_id, created_at, min_lat, max_lat, min_lon, max_lon, closed_at, open, num_changes, user_name, tags, geom)
                    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,ST_MakeValid(ST_MakeEnvelope(%s,%s,%s,%s,4326)))'''.format(
                    self.schema, )
        else:
            if is_replicate:
                sql = '''INSERT into {0}.osm_changeset AS t1
                    (id, user_id, created_at, min_lat, max_lat, min_lon, max_lon, closed_at, open, num_changes, user_name, tags)
                    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT(id) DO UPDATE
                    SET created_at=excluded.created_at, min_lat=excluded.min_lat, max_lat=excluded.max_lat, min_lon=excluded.min_lon, max_lon=excluded.max_lon, closed_at=excluded.closed_at, open=excluded.open, num_changes=excluded.num_changes, user_name=excluded.user_name, tags=excluded.tags
                    WHERE t1.id=excluded.id'''.format(self.schema, )
            else:
                sql = '''INSERT into {0}.osm_changeset
                    (id, user_id, created_at, min_lat, max_lat, min_lon, max_lon, closed_at, open, num_changes, user_name, tags)
                    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''.format(self.schema, )
        psycopg2.extras.execute_batch(local_cursor, sql, data_arr)
        local_cursor.close()

    def insert_new_batch_comment(self, db_connection, data_arr):
        local_cursor = db_connection.cursor()
        sql = '''INSERT into {0}.osm_changeset_comment
                    (comment_changeset_id, comment_user_id, comment_user_name, comment_date, comment_text)
                    values (%s,%s,%s,%s,%s)'''.format(self.schema, )
        psycopg2.extras.execute_batch(local_cursor, sql, data_arr)
        local_cursor.close()

    def delete_existing(self, db_connection, changeset_id):
        local_cursor = db_connection.cursor()
        local_cursor.execute('''DELETE FROM {0}.osm_changeset_comment
                          WHERE comment_changeset_id = {1}'''.format(self.schema, changeset_id, ))

    def parse_file(self, db_connection, current_sequence, changeset_file, is_replicate, changeset_list, comment_list):
        if not is_replicate:
            self.parsedCount = 0
            self.changesetsToProcess = 0
            self.report_header()
        context = etree.iterparse(changeset_file)
        _, _ = next(context)
        current_timestamp = datetime.strptime('2004-01-01T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ')
        for action, elem in context:
            if elem.tag != 'changeset':
                continue

            if self.contributors and elem.attrib.get('uid') not in self.contributors:  # != '1822133':
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]
                continue

            self.parsedFileCount += 1
            self.changesetsToProcess += 1
            tags = {}
            for tag in elem.iterchildren(tag='tag'):
                tags[tag.attrib['k']] = tag.attrib['v']

            for discussion in elem.iterchildren(tag='discussion'):
                for commentElement in discussion.iterchildren(tag='comment'):
                    text = None
                    for text in commentElement.iterchildren(tag='text'):
                        text = text.text
                    comment = (elem.attrib['id'], commentElement.attrib.get('uid'), commentElement.attrib.get('user'),
                               commentElement.attrib.get('date'), text)
                    comment_list.append(comment)

            if is_replicate:
                self.delete_existing(db_connection, elem.attrib['id'])

            if self.createGeometry:
                changeset_list.append((elem.attrib['id'], elem.attrib.get('uid', None), elem.attrib['created_at'],
                                       elem.attrib.get('min_lat', None),
                                       elem.attrib.get('max_lat', None), elem.attrib.get('min_lon', None),
                                       elem.attrib.get('max_lon', None), elem.attrib.get('closed_at', None),
                                       elem.attrib.get('open', None), elem.attrib.get('num_changes', None),
                                       elem.attrib.get('user', None), tags, elem.attrib.get('min_lon', None),
                                       elem.attrib.get('min_lat', None),
                                       elem.attrib.get('max_lon', None), elem.attrib.get('max_lat', None)))
            else:
                changeset_list.append((elem.attrib['id'], elem.attrib.get('uid', None), elem.attrib['created_at'],
                                       elem.attrib.get('min_lat', None),
                                       elem.attrib.get('max_lat', None), elem.attrib.get('min_lon', None),
                                       elem.attrib.get('max_lon', None), elem.attrib.get('closed_at', None),
                                       elem.attrib.get('open', None), elem.attrib.get('num_changes', None),
                                       elem.attrib.get('user', None), tags))
            if len(elem.attrib['created_at']) > 0: current_timestamp = datetime.strptime(elem.attrib['created_at'],
                                                                                         '%Y-%m-%dT%H:%M:%SZ')
            if self.changesetsToProcess >= self.bulkrows and is_replicate == False:
                # Bulkrows insert/commit for large files (isReplicate==False)
                self.parsedCount += self.changesetsToProcess
                self.insert_new_batch(db_connection, changeset_list, is_replicate)
                self.insert_new_batch_comment(db_connection, comment_list)
                # update current timestamp in table and use it for filtering on failed run
                self.report_progress(current_sequence, current_timestamp)
                # empty arrays after Bulk Db insert
                db_connection.commit()
                changeset_list = []
                comment_list = []
            # clear everything we don't need from memory to avoid leaking
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

        self.parsedCount += self.changesetsToProcess
        # uncomment next 2 lines to report each sequence wget in the log
        # msg="{0:12,}  {1:12,}  {2}      ( +{3:6}  Wait-list: {4} )".format(currentSequence, self.parsedCount,  currentTimestamp.strftime('%Y-%m-%d %H:%M:%S'), self.parsedFileCount,  self.changesetsToProcess)
        # if (self.isLogging): logging.info(msg)

        # Update whatever is left and empty arrays
        self.insert_new_batch(db_connection, changeset_list, is_replicate)
        self.insert_new_batch_comment(db_connection, comment_list)
        changeset_list = []
        comment_list = []
        # EOF commit Large files - if isReplicate==False
        if not is_replicate:
            db_connection.commit()
            self.report_progress(current_sequence, current_timestamp)
            self.report_bottom()
        return current_timestamp, changeset_list, comment_list

    @staticmethod
    def fetch_replication_file(sequence_number):
        sequence_number = str(sequence_number).zfill(9)
        topdir = str(sequence_number)[:3]
        subdir = str(sequence_number)[3:6]
        file_number = str(sequence_number)[-3:]
        file_url = BASE_REPL_URL + topdir + '/' + subdir + '/' + file_number + '.osm.gz'
        replication_file = requests.get(file_url, stream=True, verify=False)
        replication_data = replication_file.raw
        f = gzip.GzipFile(fileobj=replication_data)
        return f

    def do_replication(self, db_connection):
        global changesets, comments
        changesets = []
        comments = []
        current_timestamp = 0
        local_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            local_cursor.execute(
                'LOCK TABLE {0}.osm_changeset_state IN ACCESS EXCLUSIVE MODE NOWAIT'.format(self.schema, ))
        except psycopg2.OperationalError:
            self.msg_report("error getting lock on state table. Another process might be running")
            return 1
        local_cursor.execute('select * from {0}.osm_changeset_state'.format(self.schema, ))
        db_status = local_cursor.fetchone()
        last_db_sequence = db_status['last_sequence']
        last_db_timestamp = 0
        if db_status['last_timestamp'] is not None:
            last_db_timestamp = db_status['last_timestamp']
        self.msg_report("latest Timestamp in database: {0}".format(last_db_timestamp))
        if db_status['update_in_progress'] == 1:
            self.msg_report("concurrent update in progress. Bailing out!")
            return 1
        if last_db_sequence == -1:
            self.msg_report("replication state not initialized. You must set the sequence number first.")
            return 1
        sql = "update {0}.osm_changeset_state set update_in_progress = 1".format(self.schema, )
        local_cursor.execute(sql)
        db_connection.commit()
        self.msg_report("latest sequence from the database: {0}".format(last_db_sequence))

        # No matter what happens after this point, execution needs to reach the update statement
        # at the end of this method to unlock the database or an error will forever leave it locked
        return_status = 0
        self.msg_report("doReplication try")
        try:
            server_state = yaml.load(requests.get(BASE_REPL_URL + "state.yaml", verify=False).text,
                                     Loader=yaml.FullLoader)
            last_server_sequence = int(server_state['sequence'])
            current_sequence = last_server_sequence
            if self.isLogging: logging.info("got sequence")
            last_server_timestamp = server_state['last_run']
            if self.isLogging: logging.info("last timestamp on server: " + str(last_server_timestamp))
        except Exception:
            self.msg_report("error retrieving server state file. Bailing on replication\n")
            return_status = 2
        else:
            try:
                self.msg_report("latest sequence on OSM server: {0}".format(last_server_sequence))
                self.currentTimestamp = last_server_timestamp
                if last_server_sequence > last_db_sequence:
                    self.msg_report('-' * 85)
                    self.msg_report("Commencing Planet replication ({0}) to PostgreSQL Db".format(BASE_REPL_URL, ))
                    self.msg_report("From seq={0}    {1:19} to seq={2}    {3:19}".format(last_db_sequence,
                                                                                         last_db_timestamp.strftime(
                                                                                             '%Y-%m-%d %H:%M:%S'),
                                                                                         last_server_sequence,
                                                                                         last_server_timestamp.strftime(
                                                                                             '%Y-%m-%d %H:%M:%S')))
                    self.report_header()
                    self.msg_report("{0:^12}    {1:^8}    {2:^24}      {3:^25}".format("Last Db", "Db Insert Rate",
                                                                                       "Changeset Metadata",
                                                                                       "Last Db timestamp"))
                    self.msg_report(
                        "{0:^12}    {1:^8}    {2:^12}    {3:^12}    {4:^25}".format("Sequence", "Recs / sec.",
                                                                                    "Recs in Batch", "Cum Recs",
                                                                                    "(UTC)"))
                    self.msg_report('-' * 85)
                    self.msg_report(
                        "{0:12,}    {1:>10}  {2:12}  {3:12}    {4:>25}".format(last_db_sequence, " ", " ", " ",
                                                                               last_db_timestamp.strftime(
                                                                                   '%Y-%m-%d %H:%M:%S')))
                    current_sequence = last_db_sequence + 1
                    self.BatchstartTime = datetime.now()

                    pbar = tqdm.tqdm(total=last_server_sequence - current_sequence)
                    while current_sequence <= last_server_sequence:
                        (current_timestamp, changesets, comments) = self.parse_file(db_connection, current_sequence,
                                                                                    self.fetch_replication_file(
                                                                                        current_sequence), True,
                                                                                    changesets, comments)
                        # commit if doReplication==False or  Lines to process > bulkrows
                        if self.changesetsToProcess >= self.bulkrows:
                            local_cursor.execute(
                                "update {0}.osm_changeset_state set last_sequence={1}, last_timestamp='{2}'".format(
                                    self.schema, current_sequence, current_timestamp))
                            db_connection.commit()
                            self.report_progress(current_sequence, current_timestamp)
                            self.changesetsToProcess = 0
                        current_sequence += 1
                        pbar.update(n=1)

                # Process rows not yet inserted / committed
                current_sequence -= 1
                local_cursor.execute(
                    "update {0}.osm_changeset_state set last_sequence={1}, last_timestamp='{2}'".format(self.schema,
                                                                                                        current_sequence,
                                                                                                        current_timestamp))
                db_connection.commit()
                self.report_progress(current_sequence, current_timestamp)
                self.changesetsToProcess = 0
                # empty arrays for Bulk changesets and comments after Db Batch Rows insert is completed
                changesets = []
                comments = []
                self.msg_report("finished with replication. Clearing status record")
            except Exception as e:
                self.msg_report("error during replication")
                self.msg_report(str(e))
                return_status = 2

        sql = "update {0}.osm_changeset_state set update_in_progress = 0".format(self.schema, )
        local_cursor.execute(sql)
        db_connection.commit()
        self.report_bottom()
        self.msg_report('{0} doReplication End      {0}\n'.format('=' * 35))
        # clear from memory
        del changesets
        del comments
        return return_status

    def do_partial_replication(self, db_connection, from_seq, to_seq):
        global changesets, comments
        self.msg_report('{0} doPartialReplication New      {0}'.format('=' * 35))
        if (from_seq is None) or (to_seq is None):
            self.msg_report("both FromSeq and ToSeq must be specidied (integers > 0")
            return 1
        if (not isinstance(from_seq, int)) or (not isinstance(to_seq, int)):
            self.msg_report("FromSeq and ToSeq must be integers")
            return 1
        from_seq = int(from_seq)
        to_seq = int(to_seq)
        if (to_seq < from_seq) or (from_seq < 0):
            self.msg_report("Valid values : ( 0 < FromSeq < ToSeq )")
            return 1
        changesets = []
        comments = []
        current_sequence = 0
        current_timestamp = datetime.strptime('2004-01-01T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ')
        local_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        last_db_sequence = from_seq - 1
        last_server_sequence = to_seq
        last_server_timestamp = 0
        self.currentTimestamp = 0

        return_status = 0
        self.msg_report("doPartialReplication try")
        try:
            self.currentTimestamp = last_server_timestamp
            self.beginTime = datetime.now()
            if last_server_sequence > last_db_sequence:
                self.msg_report('-' * 85)
                self.msg_report("Partial replication ({0}) to PostgreSQL Db".format(BASE_REPL_URL, ))
                self.msg_report("From seq={0}  to seq={1}".format(from_seq, last_server_sequence))
                self.report_header()
                self.msg_report("{0:12,}".format(from_seq))
                current_sequence = last_db_sequence + 1
                self.BatchstartTime = datetime.now()
                while current_sequence <= last_server_sequence:
                    (current_timestamp, changesets, comments) = self.parse_file(db_connection, current_sequence,
                                                                                self.fetch_replication_file(
                                                                                    current_sequence), True, changesets,
                                                                                comments)
                    # commit if doReplication==False or  BulkRowsInsert > bulkrows
                    if self.changesetsToProcess >= self.bulkrows:
                        local_cursor.execute(
                            "update {0}.osm_changeset_state set last_sequence={1}, last_timestamp='{2}'".format(
                                self.schema, current_sequence, current_timestamp))
                        db_connection.commit()
                        self.report_progress(current_sequence, current_timestamp)
                        self.changesetsToProcess = 0
                    current_sequence += 1
            current_sequence -= 1
            # Process rows not yet inserted / committed
            db_connection.commit()
            self.report_progress(current_sequence, current_timestamp)
            self.changesetsToProcess = 0
            # empty arrays for Bulk changesets and comments after Db Batch Rows insert is completed
            changesets = []
            comments = []
            self.msg_report("{0:^100}".format("finished with Partial replication as requested"))
        except Exception as e:
            self.msg_report("error during replication")
            self.msg_report(str(e))
            return_status = 2

        self.report_bottom()
        self.msg_report('{0} doPartialReplication End      {0}\n'.format('=' * 35))
        # clear from memory
        del changesets
        del comments
        return return_status


if __name__ == '__main__':
    beginTime = datetime.now()
    endTime = None
    timeCost = None

    argParser = argparse.ArgumentParser(description="Parse OSM Changeset metadata into a database")
    argParser.add_argument('-t', '--trunc', action='store_true', default=False, dest='truncateTables',
                           help='Truncate existing tables (also drops indexes)')
    argParser.add_argument('-c', '--create', action='store_true', default=False, dest='createTables',
                           help='Create tables')
    argParser.add_argument('-H', '--host', action='store', dest='dbHost', help='Database hostname')
    argParser.add_argument('-P', '--port', action='store', dest='dbPort', default=None, help='Database port')
    argParser.add_argument('-u', '--user', action='store', dest='dbUser', default=None, help='Database username')
    argParser.add_argument('-p', '--password', action='store', dest='dbPass', default=None, help='Database password')
    argParser.add_argument('-d', '--database', action='store', dest='dbName', help='Target database', required=True)
    argParser.add_argument('-s', '--schema', action='store', dest='schema', help='Target schema (default=public)',
                           required=False)
    argParser.add_argument('-b', '--bulkrows', type=int, action='store', dest='bulkrows', default=50000,
                           help='Batch processing - Nb of records processed / commited (default=50000)', required=False)
    argParser.add_argument('-B', '--bz2buffer', type=int, action='store', dest='bz2buffer', default=None,
                           help='BZ2 Large file read buffer size (default=None)', required=False)
    argParser.add_argument('-f', '--file', action='store', dest='fileName', help='OSM changeset file to parse')
    argParser.add_argument('-k', '--contributors', nargs='+', action='store', dest='',
                           help='Filter changesets by user ids', required=False)
    argParser.add_argument('-r', '--replicate', action='store_true', dest='doReplication', default=False,
                           help='Apply a replication file to an existing database / schema')
    argParser.add_argument('-F', '--fromseq', type=int, action='store', dest='FromSeq',
                           help='FromSeq, To request Partial Replication (must be integer)', required=False)
    argParser.add_argument('-T', '--toseq', type=int, action='store', dest='ToSeq',
                           help='FromSeq, To request Partial Replication (must be integer > fromseq)', required=False)
    argParser.add_argument('-g', '--geometry', action='store_true', dest='createGeometry', default=False,
                           help='Build geometry of changesets (requires postgis)')
    argParser.add_argument('-L', '--logfile', action='store_true', dest='Logfile', default=False,
                           help='Log Messages written to Logfile')

    args = argParser.parse_args()

    connection = psycopg2.connect(database=args.dbName, user=args.dbUser, password=args.dbPass, host=args.dbHost,
                                  port=args.dbPort)

    md = ChangesetMD(create_geometry=args.createGeometry, schema=args.schema, bulkrows=args.bulkrows,
                     logfile=args.Logfile)

    print("Db Schema", md.schema)

    if args.Logfile:
        logging.info("---------- New ChangesetMD      ----------")

    if args.createTables:
        md.create_tables(connection)
        if args.doReplication:
            cursor = connection.cursor()
            md.msg_report('creating constraints')
            cursor.execute(queries.createConstraints.format(md.schema, ))

    if args.truncateTables:
        md.truncate_tables(connection)

    if args.doReplication:
        if args.FromSeq is None and args.ToSeq is None:
            returnStatus = md.do_replication(connection)
        else:
            returnStatus = md.do_partial_replication(connection, args.FromSeq, args.ToSeq)
        sys.exit(returnStatus)

    if not (args.fileName is None):

        if args.createGeometry:
            md.msg_report('parsing changeset file with geometries: {0}'.format(args.fileName))
        else:
            md.msg_report('parsing changeset file:{0}'.format(args.fileName))
        changesetFile = None
        if args.doReplication:

            changesetFile = gzip.open(args.fileName, 'rb')
        else:
            if args.fileName[-4:] == '.bz2':

                if bz2Support:
                    if args.bz2buffer:
                        changesetFile = BZ2File(args.fileName, 'rb', args.bz2buffer)
                        md.msg_report('bz2 file buffer : {0}'.format(args.bz2buffer))
                    else:
                        changesetFile = BZ2File(args.fileName)
                else:
                    md.msg_report('ERROR: bzip2 support not available. Unzip file first or install bz2file')
                    sys.exit(1)
            else:

                changesetFile = open(args.fileName, 'rb')

        if changesetFile is not None:
            changesets = []
            comments = []
            md.msg_report("ParseFile")
            (currentTimestamp, changesets, comments) = md.parse_file(connection, 0, changesetFile, False, changesets,
                                                                     comments)
            md.msg_report("parseFile completed")

        else:
            md.msg_report('ERROR: no changeset file opened. Something went wrong in processing args')
            sys.exit(1)

    if args.createTables:
        cursor = connection.cursor()
        if not args.doReplication:
            md.msg_report('creating constraints')
            cursor.execute(queries.createConstraints.format(md.schema, ))
        md.msg_report('creating indexes')
        cursor.execute(queries.createIndexes.format(md.schema, ))
        if args.createGeometry:
            md.msg_report('creating Geomindex')
            cursor.execute(queries.createGeomIndex.format(md.schema, ))
        cursor.execute(queries.create_tag_index.format(md.schema, ))
        connection.commit()

    connection.close()

    endTime = datetime.now()
    timeCost = (endTime - beginTime).total_seconds()
    print("-" * 85)
    if args.doReplication:
        if timeCost > 0:
            recsSecond = md.parsedCount / timeCost
        else:
            recsSecond = 0
        msg = "{0} Records inserted ({1} recs / second), Processing time is {2}".format(md.parsedCount, recsSecond,
                                                                                        timeCost)
    else:
        msg = "Processing time cost is {0}".format(timeCost, )
    md.msg_report(msg)

    md.msg_report('All done. Enjoy your (meta)data!')
