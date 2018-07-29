----------------------------------------------------------------------------------------------------

-- create triggers for auto fill geometry attributes colums:

CREATE OR REPLACE FUNCTION fill_geometry_attributes() 
RETURNS TRIGGER AS 
	$$ BEGIN
		
		IF ST_GeometryType(NEW.geometry) = 'ST_Point' THEN 
			NEW.point_x:= St_X(NEW.geometry);
			NEW.point_y:= St_Y(NEW.geometry);
			
		ELSIF ST_GeometryType(NEW.geometry) IN ('ST_MultiLineString','ST_LineString') THEN 
			NEW.geo_len:= St_Length(NEW.geometry);
			
		ELSIF ST_GeometryType(NEW.geometry) IN('ST_MultiPolygon','ST_Polygon') THEN
		
			NEW.geo_len:= St_Perimeter(NEW.geometry);
			NEW.geo_area:= St_Area(NEW.geometry);
			
		END IF;
	    RETURN NEW;
	END $$
LANGUAGE plpgsql;

----------------------------------------------------------------------------------------------------

-- table for all found tags
DROP TABLE IF EXISTS osm.osm_tags CASCADE;

CREATE TABLE osm.osm_tags (

	tag_id serial,
	osm_id bigint NOT NULL,
	osm_element character varying(10) NOT NULL,
	tag_key character varying(100),
	tag_value character varying(300),
    CONSTRAINT osm_tags_objectid_pkey PRIMARY KEY (tag_id)
	
);

----------------------------------------------------------------------------------------------------

-- table for found nodes, just the plain geoemtry info and id

DROP TABLE IF EXISTS osm._nodes CASCADE;

CREATE TABLE osm._nodes (

	objectid serial,
	node_id	bigint NOT NULL,
	CONSTRAINT _nodes_objectid_pkey PRIMARY KEY (objectid),
	CONSTRAINT _nodes_node_id_key UNIQUE (node_id)
	
);

SELECT AddGeometryColumn('osm', '_nodes', 'geometry', 32633, 'POINT', 2);

DROP INDEX IF EXISTS osm._nodes_geometry_idx;
CREATE INDEX _nodes_geometry_idx ON osm._nodes USING GIST (geometry);

----------------------------------------------------------------------------------------------------

-- _way_nodes all found ways and their nodes, relation to osm_lines/polygons

DROP TABLE IF EXISTS osm._way_nodes CASCADE;

CREATE TABLE  osm._way_nodes ( 

	objectid serial,
	way_id bigint NOT NULL,
	pos smallint,
	node_id bigint NOT NULL,
	CONSTRAINT _way_nodes_objectid_pkey PRIMARY KEY (objectid),
	CONSTRAINT _way_nodes_way_id_pos_node_id_key UNIQUE (way_id, pos, node_id)
	
);

----------------------------------------------------------------------------------------------------

-- _relations all found relations, relation to _way_nodes\_nodes

DROP TABLE IF EXISTS osm._members CASCADE;

CREATE TABLE  osm._members (

	objectid serial,
	rel_id bigint NOT NULL,
	osm_id bigint NOT NULL,
	osm_element character varying(100),
	pos smallint,
	osm_role character varying(100),
	CONSTRAINT _relations_objectid_pkey PRIMARY KEY (objectid)
	
);

----------------------------------------------------------------------------------------------------

-- table for found nodes, just the plain geoemtry info and id

DROP TABLE IF EXISTS osm._features CASCADE;

CREATE TABLE osm._features (

	objectid serial,
	tag_id bigint NOT NULL,
	osm_id bigint NOT NULL,
	osm_element character varying(10) NOT NULL,
	CONSTRAINT _features_objectid_pkey PRIMARY KEY (objectid)
	
);

----------------------------------------------------------------------------------------------------

-- table for found nodes, just the plain geoemtry info and id

DROP TABLE IF EXISTS osm._ways CASCADE;

CREATE TABLE osm._ways (

	objectid serial,
	way_id	bigint NOT NULL,
	CONSTRAINT _ways_objectid_pkey PRIMARY KEY (objectid),
	CONSTRAINT _ways_node_id_key UNIQUE (way_id)
	
);

SELECT AddGeometryColumn('osm', '_ways', 'geometry', 32633, 'LINESTRING', 2);

DROP INDEX IF EXISTS osm._way_geometry_idx;
CREATE INDEX _way_geometry_idx ON osm._nodes USING GIST (geometry);

----------------------------------------------------------------------------------------------------

-- temp container for inner outer polygons  

DROP TABLE IF EXISTS osm._polygons CASCADE;

CREATE TABLE  osm._polygons (

	objectid serial,
	tag_id bigint NOT NULL,
	osm_id bigint NOT NULL,
	osm_element character varying(10) NOT NULL,
	osm_role character varying(50),
	CONSTRAINT _polygons_objectid_pkey PRIMARY KEY (objectid)
);

SELECT AddGeometryColumn('osm', '_polygons', 'geometry', 32633, 'MULTIPOLYGON', 2);

----------------------------------------------------------------------------------------------------

-- osm points all found nodes and relation nodes with tag infos

DROP TABLE IF EXISTS osm.osm_points CASCADE;

CREATE TABLE osm.osm_points (

	objectid serial,
	tag_id bigint NOT NULL,
	osm_id bigint NOT NULL,
	osm_element character varying(10) NOT NULL,
	osm_role character varying(50),
	point_x numeric(12,4),
	point_y numeric(12,4),
	CONSTRAINT osm_points_objectid_pkey PRIMARY KEY (objectid)

);

SELECT AddGeometryColumn('osm', 'osm_points', 'geometry', 32633, 'POINT', 2);

DROP INDEX IF EXISTS osm.osm_points_geometry_idx;
CREATE INDEX osm_points_geometry_idx ON osm.osm_points USING GIST (geometry);

DROP TRIGGER IF EXISTS osm_points_geom_attr_trigger ON osm.osm_points;
CREATE TRIGGER osm_points_geom_attr_trigger
BEFORE INSERT OR UPDATE ON osm.osm_points
FOR EACH ROW 
EXECUTE PROCEDURE fill_geometry_attributes();

----------------------------------------------------------------------------------------------------

-- osm lines all found ways start != end_point and relation ways without inner/outer role

DROP TABLE IF EXISTS osm.osm_lines CASCADE;

CREATE TABLE  osm.osm_lines (

	objectid serial,
	tag_id bigint NOT NULL,
	osm_id bigint NOT NULL,
	osm_element character varying(10) NOT NULL,
	osm_role character varying(50),
	geo_len numeric(32,6),
	CONSTRAINT osm_lines_objectid_pkey PRIMARY KEY (objectid)
);

SELECT AddGeometryColumn('osm', 'osm_lines', 'geometry', 32633, 'MULTILINESTRING', 2);

DROP INDEX IF EXISTS osm.osm_lines_geometry_idx;
CREATE INDEX osm_lines_geometry_idx ON osm.osm_lines USING GIST (geometry);

DROP TRIGGER IF EXISTS osm_lines_geom_attr_trigger ON osm.osm_lines; 
CREATE TRIGGER osm_lines_geom_attr_trigger
BEFORE INSERT OR UPDATE ON osm.osm_lines
FOR EACH ROW 
EXECUTE PROCEDURE fill_geometry_attributes();

----------------------------------------------------------------------------------------------------

-- osm polygons all found ways and relation ways which are not in osm_lines

DROP TABLE IF EXISTS osm.osm_polygons CASCADE;

CREATE TABLE osm.osm_polygons (

	objectid serial,
	tag_id bigint NOT NULL,
	osm_id bigint NOT NULL,
	osm_element character varying(10) NOT NULL,
	osm_role character varying(50),
	geo_len numeric(32,6),
	geo_area numeric(32,6),
	CONSTRAINT osm_polygons_objectid_pkey PRIMARY KEY (objectid)
);

SELECT AddGeometryColumn('osm', 'osm_polygons', 'geometry', 32633, 'MULTIPOLYGON', 2);

DROP INDEX IF EXISTS osm.osm_polygons_geometry_idx;
CREATE INDEX osm_polygons_geometry_idx ON osm.osm_polygons USING GIST (geometry);

DROP TRIGGER IF EXISTS osm_polygons_geom_attr_trigger ON osm.osm_polygons; 
CREATE TRIGGER osm_polygons_geom_attr_trigger
BEFORE INSERT OR UPDATE ON osm.osm_polygons
FOR EACH ROW 
EXECUTE PROCEDURE fill_geometry_attributes();

----------------------------------------------------------------------------------------------------