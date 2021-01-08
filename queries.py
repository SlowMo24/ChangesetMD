'''
Just a utility file to store some SQL queries for easy reference

@author: Toby Murray
'''
createChangesetTable = '''CREATE EXTENSION IF NOT EXISTS hstore;
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
  tags hstore
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
'''

initStateTable = '''INSERT INTO {0}.osm_changeset_state VALUES (-1, null, 0)''';

dropIndexes = '''ALTER TABLE {0}.osm_changeset DROP CONSTRAINT IF EXISTS {0}_osm_changeset_pkey CASCADE;
DROP INDEX IF EXISTS {0}_user_name_idx, {0}_user_id_idx, {0}_created_idx, {0}_tags_idx, {0}_changeset_geom_gist ;
'''

createConstraints = '''ALTER TABLE {0}.osm_changeset ADD CONSTRAINT {0}_osm_changeset_pkey PRIMARY KEY(id);'''

createIndexes = '''CREATE INDEX {0}_user_name_idx ON {0}.osm_changeset(user_name);
CREATE INDEX {0}_user_id_idx ON {0}.osm_changeset(user_id);
CREATE INDEX {0}_created_idx ON {0}.osm_changeset(created_at);
CREATE INDEX {0}_tags_idx ON {0}.osm_changeset USING GIN(tags);
'''

createGeometryColumn = '''
CREATE EXTENSION IF NOT EXISTS postgis;
ALTER TABLE {0}.osm_changeset ADD COLUMN IF NOT EXISTS geom geometry(Polygon,4326);
'''

createGeomIndex = '''
CREATE INDEX {0}_changeset_geom_gist ON {0}.osm_changeset USING GIST(geom);
'''
