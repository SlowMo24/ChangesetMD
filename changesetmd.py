#!/usr/bin/env python
'''
ChangesetMD is a simple XML parser to read the weekly changeset metadata dumps
from OpenStreetmap into a postgres database for querying.
@author: Toby Murray 2012
         Martijn Vexel 2019
         Pierre Béland 2021
'''

from __future__ import print_function

import argparse
import gzip
from datetime import datetime
from datetime import timedelta
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
DEFAULT_BULK_COPY_SIZE = 50000
BASE_REPL_URL = "https://planet.openstreetmap.org/replication/changesets/"

class ChangesetMD():

    logging.basicConfig(level=logging.INFO, filename="ChangesetMD_log", filemode="a+",
                        format="%(asctime)-15s %(levelname)-8s %(message)s")

    def __init__(self, createGeometry, dbName="", dbUser="", dbHost="", dbPort="", dbPass=None, Schema="public", BulkRows=DEFAULT_BULK_COPY_SIZE, Logfile=True):
        if Schema=="public":
            self.schema="public"
            self.search_path="public, postgis"
        else:
            self.schema=str(Schema)
            self.search_path=str(Schema)+", public, postgis"
        self.createGeometry = createGeometry
        if (BulkRows is None):
            self.BulkRowsInsert = DEFAULT_BULK_COPY_SIZE
        else: self.BulkRowsInsert = int(BulkRows)
        self.tocommit=False
        self.isLogging=Logfile
        self.currentSequence=0
        self.currentTimestamp=datetime.now()
        self.BatchstartTime=datetime.now()
        self.parsedCount = 0
        self.changesetsToProcess = 0
        self.beginTime = datetime.now()
        self.endTime = None
        #if (dbPass == None):  connection = psycopg2.connect(database=dbName, user=dbUser, host=dbHost, port=dbPort)
        #else: connection = psycopg2.connect(database=dbName, user=dbUser, password=dbPass, host=dbHost, port=dbPort)
        #psycopg2.extras.register_hstore(connection)

    def msg_Report(self, msg):
        print("{0}  {1}".format(time.strftime('%Y-%m-%d %H:%M:%S'), msg))
        if (self.isLogging): logging.info(msg)

    def Calc_Timecost(self, beginTime, endTime, records):
        timeCost = endTime - beginTime
        recsSecond=0
        if (timeCost.total_seconds()>0):
            seconds=timeCost.total_seconds()
            recsSecond =  round((records / seconds), 2)
        return (timeCost, recsSecond)

    def InsertAndCommit(self, connection, changesets, comments):
        cursor = connection.cursor()
        if (changesets):
            self.insertNewBatch(connection, changesets)
        if (comments):
            self.insertNewBatchComment(connection, comments)
        sql="update {0}.osm_changeset_state set last_sequence={1}, last_timestamp='{2}'".format(self.schema, self.currentSequence, self.currentTimestamp)
        try:
            cursor.execute(sql)
            connection.commit()
        except psycopg2.OperationalError as e:
            self.msg_Report("error updating the state table. Another process might be running")
            return 1
        (timeCost, recsSecond)= self.Calc_Timecost(self.BatchstartTime, datetime.now(), self.changesetsToProcess)
        self.msg_Report("{0:12,}    {1:>10}  {2:12,}  {3:12,}{4:10}{5:>19}    ".format(self.currentSequence, recsSecond,  self.changesetsToProcess,  self.parsedCount, " ", self.currentTimestamp.strftime('%Y-%m-%d %H:%M:%S')))
        self.changesetsToProcess = 0
        self.BatchstartTime = datetime.now()

    def truncateTables(self, connection):
        self.msg_Report('truncating tables')
        cursor = connection.cursor()
        cursor.execute("TRUNCATE TABLE {0}.osm_changeset_comment CASCADE;".format(self.schema,))
        cursor.execute("TRUNCATE TABLE {0}.osm_changeset CASCADE;".format(self.schema,))
        cursor.execute(queries.dropIndexes)
        cursor.execute("UPDATE {0}.osm_changeset_state set last_sequence = -1, last_timestamp = null, update_in_progress = 0".format(self.schema,))
        connection.commit()

    def createTables(self, connection):
        cursor = connection.cursor()
        self.msg_Report("creating extensions if not exists")
        cursor.execute("CREATE EXTENSION IF NOT EXISTS  hstore")
        cursor.execute("CREATE EXTENSION IF NOT EXISTS  postgis")
        sql="create schema if not exists {0}".format(self.schema,)
        self.msg_Report(sql)
        cursor.execute(sql)
        sql="set search_path to {0} ".format(self.search_path, )
        self.msg_Report(sql)
        cursor.execute(sql)
        connection.commit()
        self.msg_Report("creating tables")
        cursor.execute(queries.createChangesetTable)
        cursor.execute(queries.initStateTable)
        if self.createGeometry:
            self.msg_Report("creating Geometry column")
            cursor.execute(queries.createGeometryColumn)
        connection.commit()

    def insertNewBatch(self, connection, changesets):
        psycopg2.extras.register_hstore(connection)
        cursor = connection.cursor()
        if self.createGeometry:
            sql = '''INSERT into {0}.osm_changeset AS t1
                    (id, user_id, created_at, min_lat, max_lat, min_lon, max_lon, closed_at, open, num_changes, user_name, tags, geom)
                    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,ST_SetSRID(ST_MakeEnvelope(%s,%s,%s,%s), 4326))
                    ON CONFLICT(id) DO UPDATE
                    SET created_at=excluded.created_at, min_lat=excluded.min_lat, max_lat=excluded.max_lat, min_lon=excluded.min_lon, max_lon=excluded.max_lon, closed_at=excluded.closed_at, open=excluded.open, num_changes=excluded.num_changes, user_name=excluded.user_name, tags=excluded.tags, geom=excluded.geom
                    WHERE t1.user_id=excluded.user_id'''.format(self.schema,)
            psycopg2.extras.execute_batch(cursor, sql, changesets)
            cursor.close()
        else:
            sql = '''INSERT into {0}.osm_changeset AS t1
                    (id, user_id, created_at, min_lat, max_lat, min_lon, max_lon, closed_at, open, num_changes, user_name, tags)
                    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT(id) DO UPDATE
                    SET created_at=excluded.created_at, min_lat=excluded.min_lat, max_lat=excluded.max_lat, min_lon=excluded.min_lon, max_lon=excluded.max_lon, closed_at=excluded.closed_at, open=excluded.open, num_changes=excluded.num_changes, user_name=excluded.user_name, tags=excluded.tags
                    WHERE t1.user_id=excluded.user_id'''.format(self.schema,)
            psycopg2.extras.execute_batch(cursor, sql, changesets)
            cursor.close()

    def insertNewBatchComment(self, connection, comments):
        psycopg2.extras.register_hstore(connection)
        cursor=connection.cursor()
        sql = '''INSERT into {0}.osm_changeset_comment
                    (comment_changeset_id, comment_user_id, comment_user_name, comment_date, comment_text)
                    values (%s,%s,%s,%s,%s)'''.format(self.schema,)
        psycopg2.extras.execute_batch(cursor, sql, comments)
        cursor.close()

    def deleteExisting(self, connection, id):
        cursor = connection.cursor()
        cursor.execute('''DELETE FROM {0}.osm_changeset_comment
                          WHERE comment_changeset_id = {1}'''.format(self.schema, id,))
        cursor.execute('''DELETE FROM {0}.osm_changeset
                          WHERE id = {1}'''.format(self.schema, id,))

    def parseFile(self, connection, changesetFile, doReplication, changesets, comments):
        self.parsedFileCount=0
        cursor = connection.cursor()
        context = etree.iterparse(changesetFile)
        action, root = next(context)
        for action, elem in context:
            if(elem.tag != 'changeset'):
                continue

            self.parsedFileCount += 1

            tags = {}
            for tag in elem.iterchildren(tag='tag'):
                tags[tag.attrib['k']] = tag.attrib['v']

            for discussion in elem.iterchildren(tag='discussion'):
                for commentElement in discussion.iterchildren(tag='comment'):
                    for text in commentElement.iterchildren(tag='text'):
                       text = text.text
                    comment = (elem.attrib['id'], commentElement.attrib.get('uid'),  commentElement.attrib.get('user'), commentElement.attrib.get('date'), text)
                    comments.append(comment)

            if self.createGeometry:
                changesets.append((elem.attrib['id'], elem.attrib.get('uid', None),   elem.attrib['created_at'], elem.attrib.get('min_lat', None),
                 elem.attrib.get('max_lat', None), elem.attrib.get('min_lon', None),  elem.attrib.get('max_lon', None), elem.attrib.get('closed_at', None),
                 elem.attrib.get('open', None), elem.attrib.get('num_changes', None), elem.attrib.get('user', None), tags,elem.attrib.get('min_lon', None), elem.attrib.get('min_lat', None),
                 elem.attrib.get('max_lon', None), elem.attrib.get('max_lat', None)))
            else:
                changesets.append((elem.attrib['id'], elem.attrib.get('uid', None),   elem.attrib['created_at'], elem.attrib.get('min_lat', None),
                 elem.attrib.get('max_lat', None), elem.attrib.get('min_lon', None),  elem.attrib.get('max_lon', None), elem.attrib.get('closed_at', None),
                 elem.attrib.get('open', None), elem.attrib.get('num_changes', None), elem.attrib.get('user', None), tags))
            if len(elem.attrib['created_at'])>0: self.currentTimestamp=datetime.strptime(elem.attrib['created_at'], '%Y-%m-%dT%H:%M:%SZ')
            else: self.currentTimestamp=0
        self.changesetsToProcess += self.parsedFileCount
        self.parsedCount += self.parsedFileCount
        # uncomment next 2 lines to report each sequence wget in the log
        #msg="{0:12,}  {1:12,}  {2}      ( +{3:6}  Wait-list: {4} )".format(self.currentSequence, self.parsedCount,  self.currentTimestamp.strftime('%Y-%m-%d %H:%M:%S'), self.parsedFileCount,  self.changesetsToProcess)
        #if (self.isLogging): logging.info(msg)
        if(self.changesetsToProcess>=self.BulkRowsInsert):
            self.InsertAndCommit(connection, changesets, comments)
            # empty arrays for Bulk changesets and comments after Db Batch Rows insert is completed
            changesets = []
            comments = []
            #clear everything we don't need from memory to avoid leaking
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
        return changesets, comments

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
        self.currentTimestamp = 0
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
            self.currentSequence=lastServerSequence
            if (self.isLogging): logging.info("got sequence")
            lastServerTimestamp = serverState['last_run']
            if (self.isLogging): logging.info("last timestamp on server: " + str(lastServerTimestamp))
        except Exception as e:
            self.msg_Report("error retrieving server state file. Bailing on replication\n", e)
            returnStatus = 2
        else:
            try:
                self.msg_Report("latest sequence on OSM server: {0}".format(lastServerSequence))
                self.currentTimestamp=lastServerTimestamp
                self.beginTime = datetime.now()
                if(lastServerSequence > lastDbSequence):
                    self.msg_Report('-'*85)
                    self.msg_Report("Commencing Planet replication ({0}) to PostgreSQL Db".format(BASE_REPL_URL, ))
                    self.msg_Report("From seq={0}    {1:19} to seq={2}    {3:19}".format(lastDbSequence, lastDbTimestamp.strftime('%Y-%m-%d %H:%M:%S'), lastServerSequence, lastServerTimestamp.strftime('%Y-%m-%d %H:%M:%S') ))
                    self.msg_Report ("{0:^12}    {1:^8}    {2:^24}      {3:^25}".format("Last Db", "Db Insert Rate", "Changeset Metadata", "Last Db timestamp"))
                    self.msg_Report ("{0:^12}    {1:^8}    {2:^12}    {3:^12}    {4:^25}".format("Sequence", "Recs / sec.", "Recs in Batch", "Cum Recs", "(UTC)"))
                    self.msg_Report ('-'*85)
                    self.msg_Report ("{0:12,}    {1:>10}  {2:12}  {3:12}    {4:>25}".format(lastDbSequence, " ", " ", " ", lastDbTimestamp.strftime('%Y-%m-%d %H:%M:%S')))
                    self.currentSequence = lastDbSequence + 1
                    self.BatchstartTime = datetime.now()
                    while((self.currentSequence <= lastServerSequence)):
                        (changesets, comments)=self.parseFile(connection, self.fetchReplicationFile(self.currentSequence), True, changesets, comments)
                        self.currentSequence += 1
                self.msg_Report("finished with replication. Clearing status record")
            except Exception as e:
                self.msg_Report ("error during replication")
                self.msg_Report (e)
                returnStatus = 2
        self.endTime = datetime.now()
        (timeCost, recsSecond)= self.Calc_Timecost(self.beginTime, self.endTime, self.parsedCount)
        sql="update {0}.osm_changeset_state set update_in_progress = 0".format(self.schema, )
        cursor.execute(sql)
        connection.commit()
        self.msg_Report("Time-Cost HH:MM:SS")
        self.msg_Report("{0:16}  {1:>8}  {2:12,}".format(str(timeCost)[:10], recsSecond, self.parsedCount))
        self.msg_Report('{0} doPartialReplication End      {0}\n'.format('='*35))
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
                self.msg_Report("Commencing Partial Planet replication ({0}) to PostgreSQL Db".format(BASE_REPL_URL, ))
                self.msg_Report("From seq={0}  to seq={1}".format(FromSeq, lastServerSequence))
                self.msg_Report ("{0:^12}    {1:^8}    {2:^24}      {3:^25}".format("Last Db", "Db Insert Rate", "Changeset Metadata", "Last Db timestamp"))
                self.msg_Report ("{0:^12}    {1:^8}    {2:^12}    {3:^12}    {4:^25}".format("Sequence", "Recs / sec.", "Recs in Batch", "Cum Recs", "(UTC)"))
                self.msg_Report ('-'*85)
                self.msg_Report ("{0:12,}".format(FromSeq))
                self.currentSequence = lastDbSequence + 1
                self.BatchstartTime = datetime.now()
                while((self.currentSequence <= lastServerSequence)):
                    (changesets, comments)=self.parseFile(connection, self.fetchReplicationFile(self.currentSequence), True, changesets, comments)
                    self.currentSequence += 1
            self.msg_Report("{0:^100}".format("finished with Partial replication as requested"))
        except Exception as e:
            self.msg_Report ("error during replication")
            self.msg_Report (e)
            returnStatus = 2
        self.endTime = datetime.now()
        (timeCost, recsSecond)= self.Calc_Timecost(self.beginTime, self.endTime, self.parsedCount)
        self.msg_Report("Time-Cost HH:MM:SS")
        self.msg_Report("{0:16}  {1:>8}  {2:12,}".format(str(timeCost)[:10], recsSecond, self.parsedCount))
        self.msg_Report('{0} doPartialReplication End      {0}\n'.format('='*35))
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
    argParser.add_argument('-s', '--schema', action='store', dest='Schema', help='Target schema (default=public)', required=False)
    argParser.add_argument('-b', '--bulkrows', type=int, action='store', dest='BulkRows', default=50000, help='Batch processing - Nb of records processed / commited (default=50000)', required=False)
    argParser.add_argument('-f', '--file', action='store', dest='fileName', help='OSM changeset file to parse')
    argParser.add_argument('-r', '--replicate', action='store_true', dest='doReplication', default=False, help='Apply a replication file to an existing database / schema')
    argParser.add_argument('-F', '--fromseq', type=int, action='store', dest='FromSeq', help='FromSeq, To request Partial Replication (must be integer)', required=False)
    argParser.add_argument('-T', '--toseq', type=int, action='store', dest='ToSeq', help='FromSeq, To request Partial Replication (must be integer > fromseq)', required=False)
    argParser.add_argument('-g', '--geometry', action='store_true', dest='createGeometry', default=False, help='Build geometry of changesets (requires postgis)')
    argParser.add_argument('-L', '--logfile', action='store_true', dest='Logfile', default=True, help='Messages written to Logfile')

    args = argParser.parse_args()

    connection = psycopg2.connect(database=args.dbName, user=args.dbUser, password=args.dbPass, host=args.dbHost, port=args.dbPort)
    psycopg2.extras.register_hstore(connection)

    md = ChangesetMD(createGeometry=args.createGeometry, Schema=args.Schema, BulkRows=args.BulkRows, Logfile=args.Logfile)

    if (args.Logfile):
        logging.info("---------- New ChangesetMD      ----------")

    if args.truncateTables:
        md.truncateTables(connection)

    if args.createTables:
        md.createTables(connection)

    if(args.doReplication):
        if (args.FromSeq == None and args.ToSeq == None):
            returnStatus = md.doReplication(connection)
        else:
            returnStatus = md.doPartialReplication(connection, args.FromSeq, args.ToSeq)
        sys.exit(returnStatus)

    if not (args.fileName is None):
        if args.createGeometry:
            print('parsing changeset file with geometries')
            if (args.Logfile): logging.info('parsing changeset file with geometries')
        else:
            print('parsing changeset file')
            if (args.Logfile): logging.info("parsing changeset file")
        changesetFile = None
        if(args.doReplication):
            changesetFile = gzip.open(args.fileName, 'rb')
        else:
            if(args.fileName[-4:] == '.bz2'):
                if(bz2Support):
                    changesetFile = BZ2File(args.fileName)
                else:
                    print('ERROR: bzip2 support not available. Unzip file first or install bz2file')
                    if (args.Logfile): logging.info("ERROR: bzip2 support not available. Unzip file first or install bz2file")
                    sys.exit(1)
            else:
                changesetFile = open(args.fileName, 'rb')

        if(changesetFile != None):
            changesets=[]
            comments=[]
            (changesets, comments)=md.parseFile(connection, changesetFile, args.doReplication, changesets, comments)
        else:
            print('ERROR: no changeset file opened. Something went wrong in processing args')
            if (args.Logfile): logging.info("ERROR: no changeset file opened. Something went wrong in processing args")
            sys.exist(1)

    if(not args.doReplication):
        cursor = connection.cursor()
        print('creating constraints')
        if (args.Logfile): logging.info("creating constraints")
        cursor.execute(queries.createConstraints)
        print('creating indexes')
        if (args.Logfile): logging.info("creating indexes")
        cursor.execute(queries.createIndexes)
        if args.createGeometry:
            cursor.execute(queries.createGeomIndex)
        connection.commit()

        connection.close()

    endTime = datetime.now()
    timeCost = endTime - beginTime
    print ("-"*85)
    if (args.doReplication):
        print (md.parsedCount)
        if timeCost>0:
            recsSecond =  md.parsedCount / timeCost
        else:  recsSecond = 0
        msg="{0} Records inserted ({1} recs / seond), Processing time is {2}".format(md.parsedCount, recsSecond, timeCost)
    else: msg="Processing time cost is {0}".format(timeCost,)
    print(msg)
    if (args.Logfile): logging.info(msgCost)

    print('All done. Enjoy your (meta)data!')
    if (args.Logfile): logging.info("All done. Enjoy your (meta)data!")
