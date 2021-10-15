#!/usr/bin/env python
# -*- coding: utf-8 -*
'''
ChangesetMD is a simple XML parser to read the weekly changeset metadata dumps
from OpenStreetmap into a postgres database for querying.
@author: Toby Murray 2012
         Pierre Béland 2021
'''

from __future__ import print_function

import sys
import argparse
import gzip
from datetime import datetime
from datetime import timedelta
import time
import psycopg2
import psycopg2.extras
import queries
import requests
import yaml
import logging
from lxml import etree

try:
    from bz2file import BZ2File
    bz2Support = True
except ImportError:
    bz2Support = False

# Block of changesets - nb to commit
DEFAULT_BULK_COPY_SIZE = 100000
BASE_REPL_URL = "https://planet.openstreetmap.org/replication/changesets/"

class ChangesetMD():

    logging.basicConfig(level=logging.INFO, filename="changesetmd_"+datetime.now().strftime('%Y-%m-%d')+".log", filemode="a+",
                        format="%(asctime)-15s %(levelname)-8s %(message)s")

    def __init__(self, createGeometry, dbName="", dbUser="", dbHost="", dbPort="", dbPass=None, schema="public", bulkrows=DEFAULT_BULK_COPY_SIZE, Logfile=True):
        if schema=="public":
            self.schema="public"
            self.search_path="public, postgis"
        else:
            self.schema=str(schema)
            self.search_path=str(schema)+", public, postgis"
        self.createGeometry = createGeometry
        if (bulkrows is None):
            self.bulkrows = DEFAULT_BULK_COPY_SIZE
        else: self.bulkrows = int(bulkrows)
        self.isLogging=Logfile
        self.currentTimestamp=datetime.now()
        self.BatchstartTime=datetime.now()
        self.parsedCount = 0
        self.changesetsToProcess = 0
        self.beginTime = datetime.now()
        self.endTime = None
  
    def msg_Report(self, msg):
        print("{0}  {1}".format(time.strftime('%Y-%m-%d %H:%M:%S'), msg))
        if (self.isLogging): logging.info(msg)

    def report_header(self):
        self.beginTime = datetime.now()
        self.msg_Report ("{0:^12}    {1:^8}    {2:^24}      {3:^25}".format("Last Db", "Db Insert Rate", "Changeset Metadata", "Last Db timestamp"))
        self.msg_Report ("{0:^12}    {1:^8}    {2:^12}    {3:^12}    {4:^25}".format("Sequence", "Recs / sec.", "Recs in Batch", "Cum Recs", "(UTC)"))
        self.msg_Report ('-'*85)

    def report_bottom(self):
        self.endTime = datetime.now()
        (timeCost, recsSecond)= self.Calc_Timecost(self.beginTime, self.endTime, self.parsedCount)
        self.msg_Report("Time-Cost HH:MM:SS")
        self.msg_Report("{0:16}  {1:>8}  {2:12,}".format(str(timeCost)[:10], recsSecond, self.parsedCount))

    def report_progress(self, currentSequence, currentTimestamp):
        (timeCost, recsSecond)= self.Calc_Timecost(self.BatchstartTime, datetime.now(), self.changesetsToProcess)
        self.msg_Report("{0:12,}    {1:>10}  {2:12,}  {3:12,}{4:10}{5:>19}    ".format(currentSequence, recsSecond,  self.changesetsToProcess,  self.parsedCount, " ", currentTimestamp.strftime('%Y-%m-%d %H:%M:%S')))
        self.changesetsToProcess = 0
        self.BatchstartTime = datetime.now()

    def Calc_Timecost(self, beginTime, endTime, records):
        timeCost = endTime - beginTime
        recsSecond=0
        if (timeCost.total_seconds()>0):
            seconds=timeCost.total_seconds()
            recsSecond =  round((records / seconds), 2)
        return (timeCost, recsSecond)

    def truncateTables(self, connection):
        self.msg_Report('truncating tables')
        cursor = connection.cursor()
        cursor.execute("TRUNCATE TABLE {0}.osm_changeset_comment CASCADE;".format(self.schema,))
        cursor.execute("TRUNCATE TABLE {0}.osm_changeset CASCADE;".format(self.schema,))
        cursor.execute(queries.dropIndexes.format(self.schema,))
        cursor.execute("UPDATE {0}.osm_changeset_state set last_sequence = -1, last_timestamp = null, update_in_progress = 0".format(self.schema,))
        connection.commit()

    def createTables(self, connection):
        cursor = connection.cursor()
        self.msg_Report("--- createTables, schema = {0} ---".format(self.schema,))
        sql="create schema if not exists {0};".format(self.schema,)
        self.msg_Report(sql)
        try:
            cursor.execute(sql)
        except psycopg2.OperationalError as err:
            self.msg_Report("error {0}\n{1}".format(sql, err))
            return 1
        try:
            cursor.execute(queries.createChangesetTable.format(self.schema,))
        except psycopg2.OperationalError as err:
            self.msg_Report("error queries.createChangesetTable {0}".format(err,))
            return 1
        self.msg_Report("queries.initStateTable")
        try:
            cursor.execute(queries.initStateTable.format(self.schema,))
        except psycopg2.OperationalError as err:
            self.msg_Report("error queries.createChangesetTable {0}".format(err,))
            return 1
        connection.commit()

        if self.createGeometry:
            self.msg_Report("queries.createGeometryColumn")
            try:
                # modify column if exists, requires postgresql 9.6+
                cursor.execute(queries.createGeometryColumn.format(self.schema,))
            except psycopg2.errors.DuplicateColumn as errdup:
                self.msg_Report("Create geometry duplicate error : var geom already exist", errdup)
                return 1
            connection.commit()

    def insertNewBatch(self, connection, data_arr, isReplicate):
        psycopg2.extras.register_hstore(connection)
        cursor = connection.cursor()
        if self.createGeometry:
            if (isReplicate):
                sql = '''INSERT into {0}.osm_changeset AS t1
                    (id, user_id, created_at, min_lat, max_lat, min_lon, max_lon, closed_at, open, num_changes, user_name, tags, geom)
                    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,ST_SetSRID(ST_MakeEnvelope(%s,%s,%s,%s), 4326))
                    ON CONFLICT(id) DO UPDATE
                    SET created_at=excluded.created_at, min_lat=excluded.min_lat, max_lat=excluded.max_lat, min_lon=excluded.min_lon, max_lon=excluded.max_lon, closed_at=excluded.closed_at, open=excluded.open, num_changes=excluded.num_changes, user_name=excluded.user_name, tags=excluded.tags, geom=excluded.geom
                    WHERE t1.user_id=excluded.user_id'''.format(self.schema,)
            else:
                sql = '''INSERT into {0}.osm_changeset
                    (id, user_id, created_at, min_lat, max_lat, min_lon, max_lon, closed_at, open, num_changes, user_name, tags, geom)
                    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,ST_SetSRID(ST_MakeEnvelope(%s,%s,%s,%s), 4326))'''.format(self.schema,)
        else:
            if (isReplicate):
                sql = '''INSERT into {0}.osm_changeset AS t1
                    (id, user_id, created_at, min_lat, max_lat, min_lon, max_lon, closed_at, open, num_changes, user_name, tags)
                    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT(id) DO UPDATE
                    SET created_at=excluded.created_at, min_lat=excluded.min_lat, max_lat=excluded.max_lat, min_lon=excluded.min_lon, max_lon=excluded.max_lon, closed_at=excluded.closed_at, open=excluded.open, num_changes=excluded.num_changes, user_name=excluded.user_name, tags=excluded.tags
                    WHERE t1.id=excluded.id'''.format(self.schema,)
            else:
                sql = '''INSERT into {0}.osm_changeset
                    (id, user_id, created_at, min_lat, max_lat, min_lon, max_lon, closed_at, open, num_changes, user_name, tags)
                    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''.format(self.schema,)
        psycopg2.extras.execute_batch(cursor, sql, data_arr)
        cursor.close()

    def insertNewBatchComment(self, connection, data_arr):
        psycopg2.extras.register_hstore(connection)
        cursor=connection.cursor()
        sql = '''INSERT into {0}.osm_changeset_comment
                    (comment_changeset_id, comment_user_id, comment_user_name, comment_date, comment_text)
                    values (%s,%s,%s,%s,%s)'''.format(self.schema,)
        psycopg2.extras.execute_batch(cursor, sql, data_arr)
        cursor.close()

    def deleteExisting(self, connection, id):
        cursor = connection.cursor()
        cursor.execute('''DELETE FROM {0}.osm_changeset_comment
                          WHERE comment_changeset_id = {1}'''.format(self.schema, id,))

    def parseFile(self, connection, currentSequence, changesetFile, isReplicate, changesets, comments):
        self.parsedFileCount=0
        if not (isReplicate):
            self.parsedCount=0
            self.changesetsToProcess=0
            self.report_header()
        cursor = connection.cursor()
        context = etree.iterparse(changesetFile)
        action, root = next(context)
        currentTimestamp = datetime.strptime('2004-01-01T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ')
        for action, elem in context:
            if(elem.tag != 'changeset'):
                continue

            self.parsedFileCount += 1
            self.changesetsToProcess += 1
            tags = {}
            for tag in elem.iterchildren(tag='tag'):
                tags[tag.attrib['k']] = tag.attrib['v']

            for discussion in elem.iterchildren(tag='discussion'):
                for commentElement in discussion.iterchildren(tag='comment'):
                    for text in commentElement.iterchildren(tag='text'):
                       text = text.text
                    comment = (elem.attrib['id'], commentElement.attrib.get('uid'),  commentElement.attrib.get('user'), commentElement.attrib.get('date'), text)
                    comments.append(comment)

            if(isReplicate):
                self.deleteExisting(connection, elem.attrib['id'])

            if self.createGeometry:
                changesets.append((elem.attrib['id'], elem.attrib.get('uid', None),   elem.attrib['created_at'], elem.attrib.get('min_lat', None),
                 elem.attrib.get('max_lat', None), elem.attrib.get('min_lon', None),  elem.attrib.get('max_lon', None), elem.attrib.get('closed_at', None),
                 elem.attrib.get('open', None), elem.attrib.get('num_changes', None), elem.attrib.get('user', None), tags,elem.attrib.get('min_lon', None), elem.attrib.get('min_lat', None),
                 elem.attrib.get('max_lon', None), elem.attrib.get('max_lat', None)))
            else:
                changesets.append((elem.attrib['id'], elem.attrib.get('uid', None),   elem.attrib['created_at'], elem.attrib.get('min_lat', None),
                 elem.attrib.get('max_lat', None), elem.attrib.get('min_lon', None),  elem.attrib.get('max_lon', None), elem.attrib.get('closed_at', None),
                 elem.attrib.get('open', None), elem.attrib.get('num_changes', None), elem.attrib.get('user', None), tags))
            if len(elem.attrib['created_at'])>0: currentTimestamp=datetime.strptime(elem.attrib['created_at'], '%Y-%m-%dT%H:%M:%SZ')
            if(self.changesetsToProcess>=self.bulkrows and isReplicate==False):
                # Bulkrows insert/commit for large files (isReplicate==False)
                self.parsedCount+=self.changesetsToProcess
                self.insertNewBatch(connection, changesets, isReplicate)
                self.insertNewBatchComment(connection, comments )
                self.report_progress(currentSequence, currentTimestamp)
                # empty arrays after Bulk Db insert
                connection.commit()
                changesets = []
                comments = []
            #clear everything we don't need from memory to avoid leaking
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

        self.parsedCount += self.changesetsToProcess
        # uncomment next 2 lines to report each sequence wget in the log
        #msg="{0:12,}  {1:12,}  {2}      ( +{3:6}  Wait-list: {4} )".format(currentSequence, self.parsedCount,  currentTimestamp.strftime('%Y-%m-%d %H:%M:%S'), self.parsedFileCount,  self.changesetsToProcess)
        #if (self.isLogging): logging.info(msg)

        # Update whatever is left and empty arrays
        self.insertNewBatch(connection, changesets, isReplicate)
        self.insertNewBatchComment(connection, comments)
        changesets = []
        comments = []
        # EOF commit Large files - if isReplicate==False
        if (isReplicate==False):
            connection.commit()
            self.report_progress(currentSequence, currentTimestamp)
            self.report_bottom()
        return currentTimestamp, changesets, comments

    def fetchReplicationFile(self, sequenceNumber):
        sequenceNumber = str(sequenceNumber).zfill(9)
        topdir = str(sequenceNumber)[:3]
        subdir = str(sequenceNumber)[3:6]
        fileNumber = str(sequenceNumber)[-3:]
        fileUrl = BASE_REPL_URL + topdir + '/' + subdir + '/' + fileNumber + '.osm.gz'
        replicationFile = requests.get(fileUrl, stream=True)
        replicationData = replicationFile.raw
        f = gzip.GzipFile(fileobj=replicationData)
        return f

    def doReplication(self, connection):
        global changesets, comments
        changesets = []
        comments = []
        currentSequence=0
        currentTimestamp=0
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            cursor.execute('LOCK TABLE {0}.osm_changeset_state IN ACCESS EXCLUSIVE MODE NOWAIT'.format(self.schema,))
        except psycopg2.OperationalError as e:
            self.msg_Report("error getting lock on state table. Another process might be running")
            return 1
        cursor.execute('select * from {0}.osm_changeset_state'.format(self.schema,))
        dbStatus = cursor.fetchone()
        lastDbSequence = dbStatus['last_sequence']
        lastDbTimestamp = 0
        lastServerTimestamp = 0
        if(dbStatus['last_timestamp'] is not None):
            lastDbTimestamp = dbStatus['last_timestamp']
        self.msg_Report("latest Timestamp in database: {0}".format(lastDbTimestamp))
        if(dbStatus['update_in_progress'] == 1):
            self.msg_Report("concurrent update in progress. Bailing out!")
            return 1
        if(lastDbSequence == -1):
            self.msg_Report("replication state not initialized. You must set the sequence number first.")
            return 1
        sql="update {0}.osm_changeset_state set update_in_progress = 1".format(self.schema,)
        cursor.execute(sql)
        connection.commit()
        self.msg_Report("latest sequence from the database: {0}".format(lastDbSequence))

        #No matter what happens after this point, execution needs to reach the update statement
        #at the end of this method to unlock the database or an error will forever leave it locked
        returnStatus = 0
        self.msg_Report("doReplication try")
        try:
            serverState = yaml.load(requests.get(BASE_REPL_URL + "state.yaml").text, Loader=yaml.FullLoader)
            lastServerSequence = int(serverState['sequence'])
            currentSequence=lastServerSequence
            if (self.isLogging): logging.info("got sequence")
            lastServerTimestamp = serverState['last_run']
            if (self.isLogging): logging.info("last timestamp on server: " + str(lastServerTimestamp))
        except Exception as e:
            self.msg_Report("error retrieving server state file. Bailing on replication\n")
            returnStatus = 2
        else:
            try:
                self.msg_Report("latest sequence on OSM server: {0}".format(lastServerSequence))
                self.currentTimestamp=lastServerTimestamp
                if(lastServerSequence > lastDbSequence):
                    self.msg_Report('-'*85)
                    self.msg_Report("Commencing Planet replication ({0}) to PostgreSQL Db".format(BASE_REPL_URL, ))
                    self.msg_Report("From seq={0}    {1:19} to seq={2}    {3:19}".format(lastDbSequence, lastDbTimestamp.strftime('%Y-%m-%d %H:%M:%S'), lastServerSequence, lastServerTimestamp.strftime('%Y-%m-%d %H:%M:%S') ))
                    self.report_header()
                    self.msg_Report ("{0:^12}    {1:^8}    {2:^24}      {3:^25}".format("Last Db", "Db Insert Rate", "Changeset Metadata", "Last Db timestamp"))
                    self.msg_Report ("{0:^12}    {1:^8}    {2:^12}    {3:^12}    {4:^25}".format("Sequence", "Recs / sec.", "Recs in Batch", "Cum Recs", "(UTC)"))
                    self.msg_Report ('-'*85)
                    self.msg_Report ("{0:12,}    {1:>10}  {2:12}  {3:12}    {4:>25}".format(lastDbSequence, " ", " ", " ", lastDbTimestamp.strftime('%Y-%m-%d %H:%M:%S')))
                    currentSequence = lastDbSequence + 1
                    self.BatchstartTime = datetime.now()
                    while((currentSequence <= lastServerSequence)):
                        (currentTimestamp, changesets, comments)=self.parseFile(connection, currentSequence, self.fetchReplicationFile(currentSequence), True, changesets, comments)
                        # commit if doReplication==False or  Lines to process > bulkrows
                        if(self.changesetsToProcess>=self.bulkrows):
                            cursor.execute("update {0}.osm_changeset_state set last_sequence={1}, last_timestamp='{2}'".format(self.schema, currentSequence, currentTimestamp))
                            connection.commit()
                            self.report_progress(currentSequence, currentTimestamp)
                            self.changesetsToProcess=0
                        currentSequence += 1

                # Process rows not yet inserted / committed
                currentSequence -= 1
                cursor.execute("update {0}.osm_changeset_state set last_sequence={1}, last_timestamp='{2}'".format(self.schema, currentSequence, currentTimestamp))
                connection.commit()
                self.report_progress(currentSequence, currentTimestamp)
                self.changesetsToProcess=0
                # empty arrays for Bulk changesets and comments after Db Batch Rows insert is completed
                changesets = []
                comments = []
                self.msg_Report("finished with replication. Clearing status record")
            except Exception as e:
                self.msg_Report ("error during replication")
                self.msg_Report (e)
                returnStatus = 2

        sql="update {0}.osm_changeset_state set update_in_progress = 0".format(self.schema, )
        cursor.execute(sql)
        connection.commit()
        self.report_bottom()
        self.msg_Report('{0} doReplication End      {0}\n'.format('='*35))
        # clear from memory
        del (changesets)
        del (comments)
        return returnStatus

    def doPartialReplication(self, connection, FromSeq, ToSeq):
        global changesets, comments
        self.msg_Report('{0} doPartialReplication New      {0}'.format('='*35))
        if (FromSeq is None) or (ToSeq is None):
            self.msg_Report("both FromSeq and ToSeq must be specidied (integers > 0")
            return 1
        if (not isinstance(FromSeq, int)) or (not isinstance(ToSeq, int)):
            self.msg_Report("FromSeq and ToSeq must be integers")
            return 1
        FromSeq=int(FromSeq)
        ToSeq=int(ToSeq)
        if (ToSeq<FromSeq) or (FromSeq<0):
            self.msg_Report("Valid values : ( 0 < FromSeq < ToSeq )")
            return 1
        changesets = []
        comments = []
        currentSequence=0
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        lastDbSequence = FromSeq-1
        lastServerSequence=ToSeq
        lastDbTimestamp = 0
        lastServerTimestamp = 0
        self.currentTimestamp = 0

        returnStatus = 0
        self.msg_Report("doPartialReplication try")
        try:
            self.currentTimestamp=lastServerTimestamp
            self.beginTime = datetime.now()
            if(lastServerSequence > lastDbSequence):
                self.msg_Report('-'*85)
                self.msg_Report("Partial replication ({0}) to PostgreSQL Db".format(BASE_REPL_URL, ))
                self.msg_Report("From seq={0}  to seq={1}".format(FromSeq, lastServerSequence))
                self.report_header()
                self.msg_Report ("{0:12,}".format(FromSeq))
                currentSequence = lastDbSequence + 1
                self.BatchstartTime = datetime.now()
                while((currentSequence <= lastServerSequence)):
                    (currentTimestamp, changesets, comments)=self.parseFile(connection, currentSequence, self.fetchReplicationFile(currentSequence), True, changesets, comments)
                    # commit if doReplication==False or  BulkRowsInsert > bulkrows
                    if(self.changesetsToProcess>=self.bulkrows):
                        cursor.execute("update {0}.osm_changeset_state set last_sequence={1}, last_timestamp='{2}'".format(self.schema, currentSequence, currentTimestamp))
                        connection.commit()
                        self.report_progress(currentSequence, currentTimestamp)
                        self.changesetsToProcess=0
                    currentSequence += 1
            currentSequence -= 1
            # Process rows not yet inserted / committed
            connection.commit()
            self.report_progress(currentSequence, currentTimestamp)
            self.changesetsToProcess=0
            # empty arrays for Bulk changesets and comments after Db Batch Rows insert is completed
            changesets = []
            comments = []
            self.msg_Report("{0:^100}".format("finished with Partial replication as requested"))
        except Exception as e:
            self.msg_Report ("error during replication")
            self.msg_Report (e)
            returnStatus = 2

        self.report_bottom()
        self.msg_Report('{0} doPartialReplication End      {0}\n'.format('='*35))
        # clear from memory
        del (changesets)
        del (comments)
        return returnStatus

if __name__ == '__main__':
    beginTime = datetime.now()
    endTime = None
    timeCost = None

    argParser = argparse.ArgumentParser(description="Parse OSM Changeset metadata into a database")
    argParser.add_argument('-t', '--trunc', action='store_true', default=False, dest='truncateTables', help='Truncate existing tables (also drops indexes)')
    argParser.add_argument('-c', '--create', action='store_true', default=False, dest='createTables', help='Create tables')
    argParser.add_argument('-H', '--host', action='store', dest='dbHost', help='Database hostname')
    argParser.add_argument('-P', '--port', action='store', dest='dbPort', default=None, help='Database port')
    argParser.add_argument('-u', '--user', action='store', dest='dbUser', default=None, help='Database username')
    argParser.add_argument('-p', '--password', action='store', dest='dbPass', default=None, help='Database password')
    argParser.add_argument('-d', '--database', action='store', dest='dbName', help='Target database', required=True)
    argParser.add_argument('-s', '--schema', action='store', dest='schema', help='Target schema (default=public)', required=False)
    argParser.add_argument('-b', '--bulkrows', type=int, action='store', dest='bulkrows', default=50000, help='Batch processing - Nb of records processed / commited (default=50000)', required=False)
    argParser.add_argument('-B', '--bz2buffer', type=int, action='store', dest='bz2buffer', default=None, help='BZ2 Large file read buffer size (default=None)', required=False)
    argParser.add_argument('-f', '--file', action='store', dest='fileName', help='OSM changeset file to parse')
    argParser.add_argument('-r', '--replicate', action='store_true', dest='doReplication', default=False, help='Apply a replication file to an existing database / schema')
    argParser.add_argument('-F', '--fromseq', type=int, action='store', dest='FromSeq', help='FromSeq, To request Partial Replication (must be integer)', required=False)
    argParser.add_argument('-T', '--toseq', type=int, action='store', dest='ToSeq', help='FromSeq, To request Partial Replication (must be integer > fromseq)', required=False)
    argParser.add_argument('-g', '--geometry', action='store_true', dest='createGeometry', default=False, help='Build geometry of changesets (requires postgis)')
    argParser.add_argument('-L', '--logfile', action='store_true', dest='Logfile', default=False, help='Log Messages written to Logfile')

    args = argParser.parse_args()

    try:
        connection = psycopg2.connect(database=args.dbName, user=args.dbUser, password=args.dbPass, host=args.dbHost, port=args.dbPort)
    except psycopg2.OperationalError as err:
        print("PostgreSQL Db connection error", err)

    psycopg2.extras.register_hstore(connection)

    md = ChangesetMD(createGeometry=args.createGeometry, schema=args.schema, bulkrows=args.bulkrows, Logfile=args.Logfile)

    print ("Db Schema", md.schema)

    if (args.Logfile):
        logging.info("---------- New ChangesetMD      ----------")

    if args.createTables:
        md.createTables(connection)
        if (args.doReplication):
            cursor = connection.cursor()
            md.msg_Report('creating constraints')
            cursor.execute(queries.createConstraints.format(md.schema,))

    if args.truncateTables:
        md.truncateTables(connection)
								  

    if(args.doReplication):
        if (args.FromSeq == None and args.ToSeq == None):
            returnStatus = md.doReplication(connection)
        else:
            returnStatus = md.doPartialReplication(connection, args.FromSeq, args.ToSeq)
        sys.exit(returnStatus)

    if not (args.fileName is None):
										
        if args.createGeometry:
            md.msg_Report('parsing changeset file with geometries: {0}'.format(args.fileName))
        else:
            md.msg_Report('parsing changeset file:{0}'.format(args.fileName))
        changesetFile = None
        if(args.doReplication):
								
            changesetFile = gzip.open(args.fileName, 'rb')
        else:
            if(args.fileName[-4:] == '.bz2'):
																																			 
                if(bz2Support):
                    if (args.bz2buffer):
                        changesetFile = BZ2File(args.fileName,'rb',args.bz2buffer)
                        md.msg_Report('bz2 file buffer : {0}'.format(args.bz2buffer))
                    else:
                        changesetFile = BZ2File(args.fileName)
                else:
                    md.msg_Report('ERROR: bzip2 support not available. Unzip file first or install bz2file')
                    sys.exit(1)
            else:
																												
                changesetFile = open(args.fileName, 'rb')

        if(changesetFile != None):
            changesets=[]
            comments=[]
            md.msg_Report("ParseFile")
            (currentTimestamp, changesets, comments)=md.parseFile(connection, 0, changesetFile, False, changesets, comments)
            md.msg_Report("parseFile completed")
								 
        else:
            md.msg_Report('ERROR: no changeset file opened. Something went wrong in processing args')
            sys.exist(1)

    if args.createTables:
        cursor = connection.cursor()
        if not (args.doReplication):
            md.msg_Report('creating constraints')
            cursor.execute(queries.createConstraints.format(md.schema,))
        md.msg_Report('creating indexes')
        cursor.execute(queries.createIndexes.format(md.schema,))
        if args.createGeometry:
            md.msg_Report('creating Geomindex')
            cursor.execute(queries.createGeomIndex.format(md.schema,))
        connection.commit()
  
    connection.close()

    endTime = datetime.now()
    timeCost = endTime - beginTime
    print ("-"*85)
    if (args.doReplication):
        if timeCost>0:
            recsSecond =  md.parsedCount / timeCost
        else:  recsSecond = 0
        msg="{0} Records inserted ({1} recs / second), Processing time is {2}".format(md.parsedCount, recsSecond, timeCost)
    else: msg="Processing time cost is {0}".format(timeCost,)
    md.msg_Report(msg)

    md.msg_Report('All done. Enjoy your (meta)data!')
