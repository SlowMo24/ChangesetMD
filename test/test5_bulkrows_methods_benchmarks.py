#!/usr/bin/env python
# -*- coding: utf-8 -*
'''
OSM Changeset-Meta - Test Parse-Xml and Write Db methods to find faster methods with lower memory usage
@author: Pierre Béland 2021
'''
from __future__ import print_function

from memory_profiler import profile


import os
import sys
import argparse
from datetime import datetime
from datetime import timedelta
import time
import json
import csv
#from osgeo import ogr
import postgis
import psycopg2
import psycopg2.extras
from pgcopy import CopyManager
from struct import pack
import io
import queries
import requests
import logging
from lxml import etree


#========================================================================
# parameters for tests

logging.basicConfig(level=logging.INFO, filename="changesetmdtest_"+datetime.now().strftime('%Y-%m-%d')+".log", filemode="a+", format="%(asctime)-15s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
isLogging='True'

connection2 = psycopg2.connect(database="changesetmd_test", user="osm", host="localhost", port=7513)

schema="testfile2"
postgis.psycopg.register(connection2)
psycopg2.extras.register_hstore(connection2, globally=True)
createGeometry=True
#========================================================================

try:
	from bz2file import BZ2File
	bz2Support = True
except ImportError:
	bz2Support = False


def bulk_execute_values(cur, sql, argslist, template=None, page_size=100, fetch=False):
    '''Copy of psycopg2.extras.execute_values where we take the list as is (_paginate already done)
    argslist == page
    '''
    from psycopg2.sql import Composable
    if isinstance(sql, Composable):
        sql = sql.as_string(cur)

    # we can't just use sql % vals because vals is bytes: if sql is bytes
    # there will be some decoding error because of stupid codec used, and Py3
    # doesn't implement % on bytes.
    if not isinstance(sql, bytes):
        sql = sql.encode(_ext.encodings[cur.connection.encoding])
    pre, post = _split_sql(sql)

    result = [] if fetch else None
    # _paginate already done
    #for page in _paginate(argslist, page_size=page_size):
    page=argslist
    if template is None:
        template = b'(' + b','.join([b'%s'] * len(page[0])) + b')'
    parts = pre[:]
    for args in page:
        parts.append(cur.mogrify(template, args))
        parts.append(b',')
    parts[-1:] = post
    cur.execute(b''.join(parts))
    if fetch:
        result.extend(cur.fetchall())
    return result

def cboolean( txt):
    #print(txt)
    if (txt==None): return None
    if (txt=='true'): return True
    if (txt=='false'): return False
    return None

def chstore( txt):
    txt=str(txt)
    #print(txt)
    if (txt == "None"): return None
    if (txt == "{}"): return None
    if txt.find("''"): return None
    return txt

def cint( txt):
    #print(txt)
    if (txt in (None, "", " ")): return None
    try: nb=int(txt)
    except ValueError:
        nb=None
    return nb

def cfloat( txt):
    if (txt==None): return 0.0
    try: nb=float(txt)
    except ValueError:
        return 0.0
    return nb

def ewktBBOX( min_lon, min_lat, max_lon, max_lat):
    """ewktBBOX Function output a geometry polygon with EWKT text standard -
    PostGIS will automatically translate to geometry when inserted into a geometry column
    PostGRES Function - public.pz_ewktBBOX
    NOTE: ; between params with no space
    format [LineString([(1, 2),(3, 4)], srid=4326)])
    See https://pypi.org/project/postgis/ , https://github.com/tilery/python-postgis
    """
    #IF (min_lon is None) OR (min_lat  is None) OR max_lon is None) OR max_lat: is None)

    m0=cfloat(min_lon)
    m1=cfloat(min_lat)
    m2=cfloat(max_lon)
    m3=cfloat(max_lat)
    if None in (m0,m1,m2,m3): return None
    if (m0==m2): m2+=0.000001
    if (m1==m3): m3+=0.000001
    ewkt= 'SRID=4326;POLYGON(({0} {1},{0} {3},{2} {3},{2} {1},{0} {1}))'.format(m0, m1, m2, m3)
    return ewkt

def Calc_Timecost( beginTime, endTime, records, secs_prepare):
    seconds = (endTime - beginTime)
    if seconds <=0: return (0,0,0)
    recsSecond =  round((1.0*records / seconds))
    recs_net   =  round((1.0*records / (secs_prepare + seconds)))
    #print ( ('records', records, 'sec', seconds, 'recs', recsSecond, 'recn', recs_net))
    return (seconds, recsSecond, recs_net)

def msg_Report( msg):
    print("{0}	{1}".format(time.strftime('%H:%M:%S'), msg))
    if (isLogging): logging.info(msg)

def report_header():
    # 15:19:24	|mem_type |mem_access|imethod     |catgeom  |page_size| sec.read   | sec.write-Db | read-rows/sec | rows_net     | bulkrows
    msg_Report("|{0:51}  |  {1:22} |  {2:41} | {3:14} |".format(" ", "seconds to process", "Bulk Read/Write : rows per second", "Proj. seconds"))
    msg_Report("|{0:51}  |  {1:22} |  {2:41} | {3:14} |".format(" ", "-"*21, "-"*36, " to process"))
    msg_Report("|{0:9}|{1:10}|{2:12}|{3:9}|{4:8}| {5:10} | {6:>10} | {7:>11}  | {8:>12} | {9:>12} | {10:15} | {11:>15}"
    .format("mem_type","mem_access","imethod","catgeom","page_size", "parse-read","write-Db", "parse-read","write-Db", "rows_net", "1Million Rows", "bulkrows"))

def report_method(connection, dttest, mem_type, mem_access, imethod, catgeom, page_size, beginTime, endTime, rows, secs_prepare):
    cursor = connection.cursor()
    (secs_write_db, recsec_writedb, recs_net) = Calc_Timecost(beginTime, endTime, rows, secs_prepare)
    recsec_read= round((rows*1.0 / secs_prepare),0)
    tsecs_write_db=round(secs_write_db,1)
    tsecs_prepare=round(secs_prepare,1)
    secs_net=secs_prepare+secs_write_db
    secs_prepare_1M = round((secs_net * (1000000.0/rows)),1)

    cat_bulk="|{0:9}|{1:10}|{2:12}|{3:9}|{4:8}".format(mem_type, mem_access, imethod, catgeom, page_size,)
    msg_Report("{0:40} | {1:10} | {2:10} | {3:12,} | {4:12,} | {5:12,} | {6:14} | {7:16,}".format(cat_bulk, tsecs_prepare, tsecs_write_db, recsec_read, recsec_writedb, recs_net, secs_prepare_1M, rows))
    #print (cat_bulk, secs_write_db, recsec_writedb, recs_net)
    #cat_bulk="|{0:9}|{1:10}|{2:12}|{3:9}|{4:8}".format(mem_type, mem_access, imethod, catgeom, page_size,)
    #print ('<', cat_bulk.split("|"), '>')
    (t, mem_type, mem_access, imethod, catgeom, page_size) = cat_bulk.split("|")
    #print ("catbulk", mem_type, mem_access, imethod, catgeom, page_size)

    sql="""insert into {0}.changeset_insert_benchmark
    (dttest, catgeom, imethod, page_size, rows, mem_type, mem_access, secs_prepare, secs_write_db)
    values ('{1}'::timestamp, '{2}', '{3}', '{4}', {5}, '{6}', '{7}', {8}, {9})
    """.format(schema, dttest, catgeom, imethod, page_size, rows, mem_type, mem_access, secs_prepare, secs_write_db)
    try:
        cursor.execute(sql)
    except psycopg2.OperationalError as err:
        msg_Report("error {0}\n{1}".format(sql, err))
        return 1
    connection.commit()
    cursor.close()

"""
def report_bottom(prefix):
    endTime = time.perf_counter()
    (timeCost, recsec_writedb)= Calc_Timecost(beginTime, endTime, parsedCount)
    msg_Report("Time-Cost HH:MM:SS")
    msg_Report("{0:20}	 {1:16}	 {2:>8}	 {3:12,}".format(prefix, str(timeCost)[:10], round(recsec_writedb,0), parsedCount))
"""

def createTables(connection):
    cursor = connection.cursor()
    msg_Report("--- createTables, schema = {0} ---".format(schema,))
    sql="create schema if not exists {0};".format(schema,)
    msg_Report(sql)
    try:
        cursor.execute(sql)
    except psycopg2.OperationalError as err:
        msg_Report("error {0}\n{1}".format(sql, err))
        return 1

    sql="""CREATE EXTENSION IF NOT EXISTS hstore;
      CREATE TABLE IF NOT EXISTS {0}.osm_changeset (
      id bigint,
      user_id bigint,
      created_at timestamp without time zone,
      min_lat numeric(10,7),
      max_lat numeric(10,7),
      min_lon numeric(10,7),
      max_lon numeric(10,7),
      closed_at timestamp without time zone,
      open boolean,
      num_changes integer,
      user_name varchar(255),
      tags hstore,
      geom geometry(Polygon,4326)
    );
      -- PostgreSQL12+ generated EWKT geometry column
      CREATE TABLE IF NOT EXISTS {0}.osm_changeset_dewkt (
      id bigint,
      user_id bigint,
      created_at timestamp without time zone,
      min_lat numeric(10,7),
      max_lat numeric(10,7),
      min_lon numeric(10,7),
      max_lon numeric(10,7),
      closed_at timestamp without time zone,
      open boolean,
      num_changes integer,
      user_name varchar(255),
      tags hstore,
      geom geometry(Polygon,4326) GENERATED ALWAYS AS
	 (public.pz_ewktBBOX( min_lon, min_lat, max_lon, max_lat)) STORED
    );
      -- PostgreSQL12+ generated BBOX geometry column
      CREATE TABLE IF NOT EXISTS {0}.osm_changeset_dbbox (
      id bigint,
      user_id bigint,
      created_at timestamp without time zone,
      min_lat numeric(10,7),
      max_lat numeric(10,7),
      min_lon numeric(10,7),
      max_lon numeric(10,7),
      closed_at timestamp without time zone,
      open boolean,
      num_changes integer,
      user_name varchar(255),
      tags hstore,
      geom geometry(Polygon,4326) GENERATED ALWAYS AS (ST_SetSRID(ST_MakeEnvelope(min_lon, min_lat,max_lon, max_lat),4326)) STORED
    );
      CREATE TABLE IF NOT EXISTS {0}.osm_changeset_ng (
      id bigint,
      user_id bigint,
      created_at timestamp without time zone,
      min_lat numeric(10,7),
      max_lat numeric(10,7),
      min_lon numeric(10,7),
      max_lon numeric(10,7),
      closed_at timestamp without time zone,
      open boolean,
      num_changes integer,
      user_name varchar(255),
      tags hstore
    );
      CREATE TABLE IF NOT EXISTS {0}.osm_changeset_ntg (
      id bigint,
      user_id bigint,
      created_at timestamp without time zone,
      min_lat numeric(10,7),
      max_lat numeric(10,7),
      min_lon numeric(10,7),
      max_lon numeric(10,7),
      closed_at timestamp without time zone,
      open boolean,
      num_changes integer,
      user_name varchar(255)
    );
    CREATE TABLE IF NOT EXISTS {0}.osm_changeset_comment (
      comment_changeset_id bigint not null,
      comment_user_id bigint not null,
      comment_user_name varchar(255) not null,
      comment_date timestamp without time zone not null,
      comment_text text not null
    );
    CREATE TABLE IF NOT EXISTS {0}.osm_changeset_state (
      last_sequence bigint,
      last_timestamp timestamp without time zone,
      update_in_progress smallint
    );
    """.format(schema,)
    try:
        cursor.execute(sql)
    except psycopg2.OperationalError as err:
        msg_Report("error CreateTables {0}".format(err,))
        return 1

    sqlfct="""
    -- FUNCTION: public.pz_ewktbbox(numeric, numeric, numeric, numeric)

    -- DROP FUNCTION public.pz_ewktbbox(numeric, numeric, numeric, numeric);

    CREATE OR REPLACE FUNCTION public.pz_ewktbbox(
    	min_lon numeric,
    	min_lat numeric,
    	max_lon numeric,
    	max_lat numeric)
        RETURNS geometry
        LANGUAGE 'plpgsql'
        COST 100
        IMMUTABLE PARALLEL UNSAFE
    AS $BODY$
    DECLARE
        ewkt geometry;
    BEGIN
        if abs(max_lon-min_lon)<0.000001 THEN max_lon:=max_lon+0.000001; END IF;
        if (max_lat-min_lat)<0.000001 THEN max_lat:=max_lat+0.000001; END IF;
        --     wkt= 'SRID=4326;POINT({0} {1}'.format(min_lon, min_lat)
        --else:
        --wkt= 'ST_SetSRID(ST_MakeEnvelope({0},{1},{2},{3}),4326)'.format(min_lon, min_lat, max_lon, max_lat)
        --ST_AsEWKT: "SRID=4326;POLYGON((0 1,0 3,2 3,2 1,0 1))"
        --ewkt= format('SRID=4326;POLYGON(({0} {1},{0} {3},{2} {3},{2} {1},{0} {1}))', m0, m1, m2, m3);
        ewkt:= format('SRID=4326;POLYGON((%1$s %2$s,%1$s %4$s,%3$s %4$s,%3$s %2$s,%1$s %2$s))', min_lon, min_lat, max_lon, max_lat);
    	-- ::geometry(Geometry,4326);
        RETURN ewkt;
    END
    $BODY$;

    ALTER FUNCTION public.pz_ewktbbox(numeric, numeric, numeric, numeric)
        OWNER TO osm;

    -- FUNCTION: public.pz_bbox(numeric, numeric, numeric, numeric)

    -- DROP FUNCTION public.pz_bbox(numeric, numeric, numeric, numeric);

    CREATE OR REPLACE FUNCTION public.pz_bbox(
    	min_lon numeric,
    	min_lat numeric,
    	max_lon numeric,
    	max_lat numeric)
        RETURNS geometry
        LANGUAGE 'plpgsql'
        COST 100
        IMMUTABLE PARALLEL UNSAFE
    AS $BODY$
    DECLARE
        bbox geometry;
    BEGIN
        if abs(max_lon-min_lon)<0.000001 THEN max_lon:=max_lon+0.000001; END IF;
        if (max_lat-min_lat)<0.000001 THEN max_lat:=max_lat+0.000001; END IF;
    	bbox:= ST_SetSRID(ST_MakeEnvelope(min_lon, min_lat, max_lon, max_lat), 4326);
        RETURN bbox;
    END
    $BODY$;

    ALTER FUNCTION public.pz_bbox(numeric, numeric, numeric, numeric)
        OWNER TO osm;
    """

    sql="""CREATE TABLE IF NOT EXISTS {0}.changeset_insert_benchmark (
    dttest timestamp,
    catgeom text,
    imethod text,
    page_size text,
    rows bigint,
    mem_type text,
    mem_access text,
    secs_prepare float4,
    secs_write_db float4);
    """.format(schema, )
    #print(sql)
    try:
        cursor.execute(sql)
    except psycopg2.OperationalError as err:
        msg_Report("error CreateTables {0}\n{1}".format(err, sql))
        return 1

    msg_Report("CreateTables completed")
    connection.commit()
    cursor.close()

def sqlInstr( connection, dttest, secs_prepare, data_arr, mem_type, mem_access, imethod, catgeom, page_size=0):
    """
    mem_type: array, buffer
    mem_access: rec, block
    imethod: cur.exec, exec_batch, exec_values, mogrify, copy (csv)
    catgeom: ng, bbox, ewkt, 1%s, n%s
    test methods summaries (no postgis geom)
    https://hakibenita.com/fast-load-data-python-postgresql https://naysan.ca/2020/05/09/pandas-to-postgresql-using-psycopg2-bulk-insert-performance-benchmark/
    copy_bynary     https://stackoverflow.com/questions/8144002/use-binary-copy-table-from-with-psycopg2
                    https://github.com/enomado/python-postgresql-load/blob/master/fill_db.py
    + geom ewkt     https://github.com/AlexImmer/ppygis3
    write bynary    https://www.code-learner.com/python-stringio-and-bytesio-example/
    """
    cursor=connection.cursor()
    cat_bulk="|{0:9}|{1:10}|{2:12}|{3:9}|{4:8}".format(mem_type, mem_access, imethod, catgeom, page_size,)

    #msg_Report("|{0:9}|{1:10}|{2:12}|{3:9}|{4:8}| {5:10} | {6:10} | {7:12} | {8:12}".format("mem_type","mem_access","imethod","catgeom","page_size","timecost","rows_sec","rows_net","bulkrows"))

    #print(cat_bulk)
    #vals="%0$s,%1$s,%2$s,%3$s,%4$s,%5$s,%6$s,%7$s,%8$s,%9$s,%10$s,%11$s"
    vals="%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s"
    template=""
    suffix=""
    if catgeom=="ng":
        suffix="_ng"
        vargeom =""
        vars="id,user_id,created_at,min_lat,max_lat,min_lon,max_lon,closed_at,open,num_changes,user_name,tags"
        valgeom=""
        values="%"
        values="({0})".format(vals, )
    elif catgeom=="ntg":
        suffix="_ntg"
        vargeom =""
        vars="id,user_id,created_at,min_lat,max_lat,min_lon,max_lon,closed_at,open,num_changes,user_name"
        valgeom=""
        values="%"
        values="({0})".format(vals, )
    elif catgeom=="dbbox":
        suffix="_dbbox"
        vargeom =""
        vars="id,user_id,created_at,min_lat,max_lat,min_lon,max_lon,closed_at,open,num_changes,user_name,tags"
        valgeom=""
        values="%"
        values="({0})".format(vals, )
    elif catgeom=="dewkt":
        suffix="_dewkt"
        vargeom =""
        vars="id,user_id,created_at,min_lat,max_lat,min_lon,max_lon,closed_at,open,num_changes,user_name,tags"
        valgeom=""
        values="%"
        values="({0})".format(vals, )
    elif catgeom=="bbox":
        vargeom=", geom"
        vars="id,user_id,created_at,min_lat,max_lat,min_lon,max_lon,closed_at,open,num_changes,user_name,tags,geom"
        valgeom=",(ST_SetSRID(ST_MakeEnvelope(%s,%s,%s,%s), 4326)::geometry)"
        #valgeom=",((public.pz_bbox(min_lon,min_lat,max_lon,max_lat)::geom))"
        values="({0}{1})".format(vals,valgeom, )
        template=values
    elif catgeom=="ewkt":
        vargeom=", geom"
        vars="id,user_id,created_at,min_lat,max_lat,min_lon,max_lon,closed_at,open,num_changes,user_name,tags,geom"
        valgeom=",ST_GeomFromEWKT(%s)"
        values="({0}{1})".format(vals,valgeom, )
        template=values
    elif catgeom=="newkt":  #ewkit - var ewkt not copied - test option - added after copy
        vargeom=""
        vars="id,user_id,created_at,min_lat,max_lat,min_lon,max_lon,closed_at,open,num_changes,user_name,tags"
        valgeom=""
        values="({0})".format(vals, )
        template=values
    elif catgeom=="1%s":
        vargeom=", geom"
        vars="id,user_id,created_at,min_lat,max_lat,min_lon,max_lon,closed_at,open,num_changes,user_name,tags,geom"
        valgeom=",%s"
        values="%s"
        template="({0}{1})".format(vals,valgeom, )
    elif catgeom=="n%s":
        vargeom=", geom"
        vars="id,user_id,created_at,min_lat,max_lat,min_lon,max_lon,closed_at,open,num_changes,user_name,tags,geom"
        valgeom=",%s"
        values="({0}{1})".format(vals,valgeom, )
        template=values
    else:
        print ("sqlInstr ERROR, invalid catgeom=", catgeom)
        return
    cols=tuple(vars.split(","))

    table="{0}.osm_changeset{1}".format(schema, suffix, )
    sql = '''INSERT into {0}
                ({1})
                values {2}'''.format(table, vars, values)
    sqlt = '''INSERT into {0}
                ({1})
                values %s'''.format(table, vars, )
    sqlv = '''INSERT into {0}
                ({1})'''.format(table, vars, )

    sqldel = '''TRUNCATE TABLE {0}'''.format(table, )
    try:
        cursor.execute(sqldel)
    except (psycopg2.Error) as err:
        msg_Report("--\n {0},  ERROR {1}\n{2}\n--".format(cat_bulk, err, sqldel))
        return

    beginTime = time.perf_counter()
    beginTime_ns = time.perf_counter_ns()
    if mem_type=="array" and mem_access=="rec":
        if imethod=="base":
            # cursor.execute does not support indexing, we need to loop
            for values in data_arr:
                try:
                    cursor.execute(sql, values)
                except (psycopg2.Error, TypeError, ValueError, IndexError, MemoryError) as err:
                    msg_Report("--\n ERROR {0}\n{1}\n{2}\n--".format(err, sql, values))
                    return
        elif imethod=="mogrify":
            # mogrify block of data then exec
            data_list = [cursor.mogrify("{0}".format(values,), row).decode('utf8') for row in data_arr]
            query  = sqlv + " values " + ",".join(data_list)
            try:
                cursor.execute(query)
            except (psycopg2.Error, TypeError, ValueError, IndexError, MemoryError) as err:
                msg_Report("--\n ERROR {0}\n{1}\n{2}\n--".format(err, sql, values))
                return
        else: msg_Report("--\n ERROR imethod={0}: not for single record insert".format(imethod,))
    elif mem_type=="array" and mem_access=="block":
        if imethod=="batch":
            try:
                if page_size>0:
                    psycopg2.extras.execute_batch(cursor, sql, data_arr, page_size=page_size)
                else:
                    #print ("execute_batch", sql, len(data_arr), data_arr[0])
                    psycopg2.extras.execute_batch(cursor, sql, data_arr)
            except psycopg2.Error as err:
                msg_Report("--\n ERROR {0}\n--".format(err,))
                return
            except ValueError as err:
                msg_Report("--\n ERROR {0}\n--".format(err,))
                return
            except TypeError as err:
                msg_Report("--\n ERROR {0}\n--\n{1}\n{2}".format(err, sql, data_arr[0]))
                return
        elif imethod=="values":
            try:
                if page_size>0:
                    if len(template)>0:
                        psycopg2.extras.execute_values(cursor, sqlt, data_arr, template, page_size=page_size)
                    else:
                        psycopg2.extras.execute_values(cursor, sqlt, data_arr, page_size=page_size)
                elif len(template)>0:
                    psycopg2.extras.execute_values(cursor, sqlt, data_arr, template)
                else:
                    psycopg2.extras.execute_values(cursor, sqlt, data_arr)
            except (psycopg2.errors.OutOfMemory, psycopg2.Error, psycopg2.errors.InFailedSqlTransaction, IndexError, MemoryError) as err:
                msg_Report("--\n ERROR {0}\n{1}\n{2}\n{3}\n--".format(err, cat_bulk, sql, values))
                return
            except ValueError as err:
                msg_Report("--\n ERROR {0}\n--".format(err))
                print (sqlt, data_arr)
                return
            except TypeError as err:
                msg_Report("--\n ERROR {0}\n--\n{1}\n{2}".format(err, sqlt, data_arr[0]))
                return
        elif imethod=="bvalues":
            try:
                if page_size>0:
                    if len(template)>0:
                        #psycopg2.extras.execute_values(cursor, sqlt, data_arr, template, page_size=page_size)
                        bulk_execute_values(cursor, sqlt, data_arr, template, page_size=page_size)
                    else:
                        psycopg2.extras.execute_values(cursor, sqlt, data_arr, page_size=page_size)
                elif len(template)>0:
                    psycopg2.extras.execute_values(cursor, sqlt, data_arr, template)
                else:
                    psycopg2.extras.execute_values(cursor, sqlt, data_arr)
            except (psycopg2.errors.OutOfMemory, psycopg2.Error, psycopg2.errors.InFailedSqlTransaction, IndexError, MemoryError, Error) as err:
                msg_Report("--\n ERROR {0}\n{1}\n{2}\n{3}\n--".format(err, cat_bulk, sql, values))
                return
            except ValueError as err:
                msg_Report("--\n ERROR {0}\n--".format(err))
                print (sqlt, data_arr)
                return
            except TypeError as err:
                msg_Report("--\n ERROR {0}\n--\n{1}\n{2}".format(err, sqlt, data_arr[0]))
                return
        elif imethod=="mogrify":

            data_list = [cursor.mogrify("{0}".format(values,), row).decode('utf8') for row in data_arr]
            query  = sqlv + " values " + ",".join(data_list)
            try:
                cursor.execute(query)
            except (psycopg2.Error, psycopg2.errors.OutOfMemory, ValueError, IndexError, MemoryError) as err:
                msg_Report("--\n ERROR {0}\n{1}\n{2}\n--".format(err, sql, values))
                return
    elif mem_type=="buffer" and mem_access=="block":
        if (imethod=="values"):
            data_arr.seek(0)
            print ("<data_arr>", data_arr.readline(), "</data_arr>\n")
            data_arr.seek(0)
            try:
                if page_size>0:
                    #psycopg2.extras.execute_values(cursor, sqlt, data_arr, page_size=page_size)
                    psycopg2.extras.execute_values(cursor, sqlt, ((values,  ) for values in data_arr), page_size=page_size)
                else:
                    psycopg2.extras.execute_values(cursor, sqlt, data_arr.getvalue())
            except (psycopg2.Error, ValueError, MemoryError, Error) as err:
                msg_Report("--\n psycopg2.Error {0}\n{1}\n--".format(err,sql))
                if hasattr(err, 'message'):
                    print(err.message)
                else:
                    print(err)
                return
        elif (imethod == "copy"):
            data_arr.seek(0)
            try:
                cursor.copy_from(data_arr, '{0} ({1})'.format(table, vars, ), sep='\t', null='None')
            except (psycopg2.Error, ValueError, MemoryError) as err:
                msg_Report("--\n psycopg2.Error {0}\n{1}\n--".format(err,sql))
                if hasattr(err, 'message'):
                    print(err.message)
                else:
                    print(err)
                return
    elif imethod in ("copy") and mem_type!="buffer":
        msg_Report("--\imethod={0}, mem_type={1}: copy method works only with mem_type=buffer")
        return
    else:
        msg_Report("--\n ERROR, mem_type={0}, mem_access={1},	imethod={2} : incorrect params combination".format(mem_type, mem_access, imethod, ))
        return


    connection.commit()
    endTime = time.perf_counter()
    endTime_ns = time.perf_counter_ns()

    cursor.execute("SELECT count(*)  as nb	FROM {0};".format(table, ))
    bulkrows = cursor.fetchone()[0]

    if catgeom == "ng":
        catgeom = "a."+catgeom;
    elif catgeom == "bbox":
        catgeom = "b."+catgeom;
    elif catgeom == "dbbox":
        catgeom = "c."+catgeom;
    elif catgeom == "ewkt":
        catgeom = "d."+catgeom;
    elif catgeom == "dewkt":
        catgeom = "e."+catgeom;
    elif catgeom == "ntg":
        catgeom = "f."+catgeom;
    else:
        catgeom = "z.err"+catgeom;

    if imethod  == "base":
        imethod  = "1."+imethod;
    elif imethod  == "batch":
        imethod  = "2."+imethod;
    elif imethod  == "mogrify":
        imethod  = "3."+imethod;
    elif imethod  == "values":
        imethod  = "4."+imethod;
    elif imethod  == "bvalues":
        imethod  = "5."+imethod;
    elif imethod  == "copy":
        imethod  = "6."+imethod;
    elif imethod  == "bcopy":
        imethod  = "7."+imethod;
    else:
        imethod  = "9.err"+imethod;

    report_method(connection, dttest, mem_type, mem_access, imethod, catgeom, page_size, beginTime, endTime, bulkrows, secs_prepare)

def insertTests( connection, dttest, secs_prepare, data_arr_ewkt):

    cursor = connection.cursor()

    # Arrays of changesets metadata with various formulation of the bbox geometry variable
    # To manage memory space, tests are grouped by section based on columns used

    # 1. data_arr_ng - No geography column - we remove data_arr_ewkt [12] == ewkt geometry
    data_arr_ng = []
    ioschangesets_ng = io.StringIO()
    #iobchangesets_ng = io.BytesIO()
    for changeset in data_arr_ewkt:
        changeset_ng=list(changeset)
        changeset_ng=changeset_ng[:-1]
        changeset_ng=tuple(changeset_ng)
        data_arr_ng.append(changeset_ng)
        row_ng="\t".join(map(str,(str(i) for i in changeset_ng)))+"\n"
        ioschangesets_ng.write(row_ng)

    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="rec", imethod="base", catgeom="ng")
    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="block", imethod="batch", catgeom="ng", page_size=100)
    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="block", imethod="batch", catgeom="ng", page_size=1000)
    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="block", imethod="batch", catgeom="ng", page_size=10000)
    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="block", imethod="batch", catgeom="ng", page_size=50000)
    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="block", imethod="mogrify", catgeom="ng")

    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="block", imethod="values", catgeom="ng", page_size=100)
    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="block", imethod="values", catgeom="ng", page_size=1000)
    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="block", imethod="values", catgeom="ng", page_size=10000)
    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="block", imethod="values", catgeom="ng", page_size=50000)

    sqlInstr(connection2, dttest, secs_prepare, ioschangesets_ng, mem_type="buffer", mem_access="block", imethod="copy", catgeom="ng")
    sqlInstr(connection2, dttest, secs_prepare, ioschangesets_ng, mem_type="buffer", mem_access="block", imethod="copy", catgeom="ng", page_size=100)
    sqlInstr(connection2, dttest, secs_prepare, ioschangesets_ng, mem_type="buffer", mem_access="block", imethod="copy", catgeom="ng", page_size=1000)

    sqlInstr(connection2, dttest, secs_prepare, ioschangesets_ng, mem_type="buffer", mem_access="block", imethod="copy", catgeom="ng", page_size=10000)
    sqlInstr(connection2, dttest, secs_prepare, ioschangesets_ng, mem_type="buffer", mem_access="block", imethod="copy", catgeom="ng", page_size=100000)

    #=========================================================
    print ("\n--------\t catgeom=dbbox, postgresql generated column with formula bbox  ", "-"*110)

    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="rec", imethod="base", catgeom="dbbox")

    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="block", imethod="batch", catgeom="dbbox", page_size=100)
    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="block", imethod="batch", catgeom="dbbox", page_size=1000)
    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="block", imethod="batch", catgeom="dbbox", page_size=10000)
    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="block", imethod="batch", catgeom="dbbox", page_size=50000)

    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="block", imethod="mogrify", catgeom="dbbox")

    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="block", imethod="values", catgeom="dbbox", page_size=100)
    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="block", imethod="values", catgeom="dbbox", page_size=1000)
    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="block", imethod="values", catgeom="dbbox", page_size=10000)
    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="block", imethod="values", catgeom="dbbox", page_size=50000)

    sqlInstr(connection2, dttest, secs_prepare, ioschangesets_ng, mem_type="buffer", mem_access="block", imethod="copy", catgeom="dbbox")
    sqlInstr(connection2, dttest, secs_prepare, ioschangesets_ng, mem_type="buffer", mem_access="block", imethod="copy", catgeom="dbbox", page_size=100)
    sqlInstr(connection2, dttest, secs_prepare, ioschangesets_ng, mem_type="buffer", mem_access="block", imethod="copy", catgeom="dbbox", page_size=1000)
    sqlInstr(connection2, dttest, secs_prepare, ioschangesets_ng, mem_type="buffer", mem_access="block", imethod="copy", catgeom="dbbox", page_size=10000)
    sqlInstr(connection2, dttest, secs_prepare, ioschangesets_ng, mem_type="buffer", mem_access="block", imethod="copy", catgeom="dbbox", page_size=50000)
    sqlInstr(connection2, dttest, secs_prepare, ioschangesets_ng, mem_type="buffer", mem_access="block", imethod="copy", catgeom="dbbox", page_size=100000)

    #=========================================================
    print ("\n--------\t catgeom=dewkt, postgresql generated column with formula ewkt  ", "-"*110)

    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="rec", imethod="base", catgeom="dewkt")

    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="block", imethod="batch", catgeom="dewkt")
    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="block", imethod="batch", catgeom="dewkt", page_size=100)
    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="block", imethod="batch", catgeom="dewkt", page_size=1000)
    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="block", imethod="batch", catgeom="dewkt", page_size=10000)
    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="block", imethod="batch", catgeom="dewkt", page_size=50000)

    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="block", imethod="mogrify", catgeom="dewkt")

    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="block", imethod="values", catgeom="dewkt", page_size=100)
    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="block", imethod="values", catgeom="dewkt", page_size=1000)
    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="block", imethod="values", catgeom="dewkt", page_size=10000)
    sqlInstr(connection2, dttest, secs_prepare, data_arr_ng, mem_type="array", mem_access="block", imethod="values", catgeom="dewkt", page_size=50000)

    sqlInstr(connection2, dttest, secs_prepare, ioschangesets_ng, mem_type="buffer", mem_access="block", imethod="copy", catgeom="dewkt")
    sqlInstr(connection2, dttest, secs_prepare, ioschangesets_ng, mem_type="buffer", mem_access="block", imethod="copy", catgeom="dewkt", page_size=100)
    sqlInstr(connection2, dttest, secs_prepare, ioschangesets_ng, mem_type="buffer", mem_access="block", imethod="copy", catgeom="dewkt", page_size=1000)
    sqlInstr(connection2, dttest, secs_prepare, ioschangesets_ng, mem_type="buffer", mem_access="block", imethod="copy", catgeom="dewkt", page_size=10000)
    sqlInstr(connection2, dttest, secs_prepare, ioschangesets_ng, mem_type="buffer", mem_access="block", imethod="copy", catgeom="dewkt", page_size=50000)
    sqlInstr(connection2, dttest, secs_prepare, ioschangesets_ng, mem_type="buffer", mem_access="block", imethod="copy", catgeom="dewkt", page_size=100000)

    #=========================================================
    del changeset_ng
    del data_arr_ng
    del ioschangesets_ng
    #=========================================================

    # 3. data_arr_ewkt - geography column - data_arr_ewkt [12] == ewkt geometry
    ioschangesets_ewkt = io.StringIO()

    for changeset in data_arr_ewkt:
        row_ewkt="\t".join(map(str,(str(i) for i in changeset)))+"\n"
        ioschangesets_ewkt.write(row_ewkt)

    #=========================================================
    print ("\n--------\t catgeom=ewkt  ", "-"*110)

    sqlInstr(connection2, dttest, secs_prepare, data_arr_ewkt, mem_type="array", mem_access="rec", imethod="base", catgeom="ewkt")

    sqlInstr(connection2, dttest, secs_prepare, data_arr_ewkt, mem_type="array", mem_access="block", imethod="batch", catgeom="ewkt")
    sqlInstr(connection2, dttest, secs_prepare, data_arr_ewkt, mem_type="array", mem_access="block", imethod="batch", catgeom="ewkt", page_size=100)
    sqlInstr(connection2, dttest, secs_prepare, data_arr_ewkt, mem_type="array", mem_access="block", imethod="batch", catgeom="ewkt", page_size=1000)
    sqlInstr(connection2, dttest, secs_prepare, data_arr_ewkt, mem_type="array", mem_access="block", imethod="batch", catgeom="ewkt", page_size=10000)
    sqlInstr(connection2, dttest, secs_prepare, data_arr_ewkt, mem_type="array", mem_access="block", imethod="batch", catgeom="ewkt", page_size=50000)

    sqlInstr(connection2, dttest, secs_prepare, data_arr_ewkt, mem_type="array", mem_access="block", imethod="mogrify", catgeom="ewkt")

    sqlInstr(connection2, dttest, secs_prepare, data_arr_ewkt, mem_type="array", mem_access="block", imethod="values", catgeom="ewkt", page_size=100)
    sqlInstr(connection2, dttest, secs_prepare, data_arr_ewkt, mem_type="array", mem_access="block", imethod="values", catgeom="ewkt", page_size=1000)
    sqlInstr(connection2, dttest, secs_prepare, data_arr_ewkt, mem_type="array", mem_access="block", imethod="values", catgeom="ewkt", page_size=10000)
    sqlInstr(connection2, dttest, secs_prepare, data_arr_ewkt, mem_type="array", mem_access="block", imethod="values", catgeom="ewkt", page_size=50000)

    sqlInstr(connection2, dttest, secs_prepare, ioschangesets_ewkt, mem_type="buffer", mem_access="block", imethod="copy", catgeom="ewkt")
    sqlInstr(connection2, dttest, secs_prepare, ioschangesets_ewkt, mem_type="buffer", mem_access="block", imethod="copy", catgeom="ewkt", page_size=100)
    sqlInstr(connection2, dttest, secs_prepare, ioschangesets_ewkt, mem_type="buffer", mem_access="block", imethod="copy", catgeom="ewkt", page_size=1000)
    sqlInstr(connection2, dttest, secs_prepare, ioschangesets_ewkt, mem_type="buffer", mem_access="block", imethod="copy", catgeom="ewkt", page_size=10000)
    sqlInstr(connection2, dttest, secs_prepare, ioschangesets_ewkt, mem_type="buffer", mem_access="block", imethod="copy", catgeom="ewkt", page_size=50000)
    sqlInstr(connection2, dttest, secs_prepare, ioschangesets_ewkt, mem_type="buffer", mem_access="block", imethod="copy", catgeom="ewkt", page_size=100000)

    return

"""  XML structure
<changeset id="15610060" created_at="2013-04-04T16:39:12Z" ... >
<tag k="created_by" v="Potlatch 2"/>
<tag k="version" v="2.3"/>
<discussion>
  <comment uid="7228700" user="Reinhard12" date="2021-01-02T19:13:57Z">
    <text> ...</text>
  </comment>
</discussion>
</changeset>
"""

def parse_changesets(action, elem, changesets, comments, bulkrows ):
    tags = {}
    for tag in elem.iterchildren(tag='tag'):
        tags[tag.attrib['k']] = tag.attrib['v']

    for discussion in elem.iterchildren(tag='discussion'):
        for commentElement in discussion.iterchildren(tag='comment'):
            for text in commentElement.iterchildren(tag='text'):
               text = text.text
            comment = (elem.attrib['id'], commentElement.attrib.get('uid'),	 commentElement.attrib.get('user'), commentElement.attrib.get('date'), text)
            comments.append(comment)

    changeset=(cint(elem.attrib.get('id')), cint(elem.attrib.get('uid', "0")),	elem.attrib.get('created_at'), cfloat(elem.attrib.get('min_lat', "None")),
     cfloat(elem.attrib.get('max_lat', "None")), cfloat(elem.attrib.get('min_lon', "None")),	cfloat(elem.attrib.get('max_lon', "None")), elem.attrib.get('closed_at', None),
     cboolean(elem.attrib.get('open', "None")), cint(elem.attrib.get('num_changes', "None")), elem.attrib.get('user', "None"), chstore(tags), ewktBBOX(elem.attrib.get('min_lon', None), elem.attrib.get('min_lat', None),
     elem.attrib.get('max_lon', None), elem.attrib.get('max_lat', None)))
    changesets.append(changeset)
    return (changesets, comments)

# memory_profiler instantiating the decorator
#@profile

def parseSample(connection, bulkrows, bz2buffer, filename):
    print("parseSample(connection, bulkrows={0}, bz2buffer={1}, filename={2})".format(bulkrows, bz2buffer, filename))
    dttest=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if(filename[-4:] == '.bz2'):
        if(bz2Support):
            if (bz2buffer):
                changesetFile = BZ2File(filename,'rb',bz2buffer)
                msg_Report('bz2 file buffer : {0}'.format(bz2buffer,))
            else:
                changesetFile = BZ2File(filename)
        else:
            msg_Report('ERROR: bzip2 support not available. Unzip file first or install bz2file')
            sys.exit(1)
    else:
        changesetFile = open(filename, 'rb')
    beginTime = time.perf_counter()
    beginTime_ns = time.perf_counter_ns()
    msg_Report("Begin Parse/Prepare data for {0} bulkrows".format(bulkrows,))
    cursor = connection.cursor()
    #method 1 - toebee
    context = iter(etree.iterparse(changesetFile))
    #method 2 - + events is faster
    #context = iter(etree.iterparse(changesetFile, events=("start", "end")))
    #action, root = next(context)
    changesets=[]
    comments=[]
    parsedFileCount=0
    for action, elem in context:

        if(elem.tag != 'changeset'):
            continue
        (changesets, comments) = parse_changesets(action, elem, changesets, comments, bulkrows)
        # Completed loading
        #clear iterparse lists to free memory, avoid leaking
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
        parsedFileCount=len(changesets)
        if parsedFileCount >= bulkrows:
            break

    # stats from rows parsed (can be less then the planned bulkrows)
    bulkrows=len(changesets)
    endTime = time.perf_counter()
    secs_prepare=endTime - beginTime
    secs_prepare_1M= secs_prepare * (1000000.0/bulkrows)
    print ("bulkrows={0}, secs_prepare={1}, projection ==> prepare 1Million = {2} secs".format(bulkrows, secs_prepare, secs_prepare_1M))

    endTime_ns = time.perf_counter_ns()
    secs_prepare_ns=endTime_ns - beginTime_ns
    secs_prepare_ns_1M= secs_prepare_ns * (1000000.0/(bulkrows*1000000000))
    print ("bulkrows={0}, secs_prepare={1}, projection ==> prepare 1Million = {2} secs".format(bulkrows, secs_prepare, secs_prepare_1M))
    print ("bulkrows={0}, secs_prepare_ns={1}, projection ==> prepare 1Million = {2} secs".format(bulkrows, secs_prepare_ns, secs_prepare_ns_1M))

    print (" ")
    report_header()

    insertTests(connection, dttest, secs_prepare, changesets)

    connection.commit()
    cursor.execute("analyze {0}.osm_changeset".format(schema,))
    cursor.execute("analyze {0}.osm_changeset_comment".format(schema,))
    connection.commit()

    return #('Test completed')


beginTime = datetime.now()
endTime = None
timeCost = None

msg_Report("---------- ChangesetMD Session " + time.strftime('%Y-%m-%d %H:%M:%S') + "	  ----------")

msg_Report("ChangesetMD Tests Bulkrow Insert methods")

cursor = connection2.cursor()
createTables(connection2)

# Trial results for each method will be stored in PostgreSQL Table
for trial in range(5):
    parseSample( connection2, bulkrows=50000, bz2buffer=25000, filename= "planet/changesets_sample_50000.osm.bz2")


sys.exit("\n=========== Test completed, - Results in the changeset_insert_benchmark table  ============")

testsfile="D://OsmContributorStats//changesetMD_p3//github//test/changeset_methods_insert_"+datetime.now().strftime('%Y-%m-%d-%H-%M-%S')+".csv"

# Access resuts from the changeset_insert_benchmark table
sql="""
WITH sel AS (
SELECT dttest, mem_type, mem_access, imethod, catgeom, page_size,
secs_prepare, secs_write_db, recsec_writedb, recs_net, rows
	FROM testfile2.changeset_insert_benchmark
ORDER BY catgeom, imethod, page_size
), grp AS (
SELECT  catgeom, count(*) as options
FROM  testfile2.changeset_insert_benchmark
GROUP BY catgeom
),
stats as (
SELECT dttest, catgeom, imethod, page_size, rows, mem_type, mem_access,
secs_prepare, secs_write_db, recsec_writedb, recs_net,
rank() OVER (PARTITION BY catgeom, imethod, page_size ORDER BY recsec_writedb DESC) as rank,
rank() OVER (PARTITION BY catgeom, imethod ORDER BY recsec_writedb DESC) as best
FROM sel
UNION
SELECT NULL as dttest, catgeom, ' ' as imethod, ' ' as page_size, 0 as rows,
NULL as mem_type, NULL as mem_access,
NULL as secs_prepare, NULL as secs_write_db, NULL as recsec_writedb, NULL as recs_net,
	'1' as rank, '1' as best
FROM sel
)
select * from stats
order by catgeom, imethod, page_size;
""".format(testsfile,)
print (sql)
try:
    #cursor.execute(sql, (dttest, mem_type, mem_access, imethod, catgeom, page_size, secs_prepare, secs_write_db, recsec_writedb, recs_net, rows) )
    cursor.execute(sql)
except psycopg2.OperationalError as err:
    msg_Report("error {0}\n{1}".format(sql, err))
connection.commit()
cursor.close()
