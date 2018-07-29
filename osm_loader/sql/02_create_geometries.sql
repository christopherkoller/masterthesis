-----------------------------------------------------
-- ( 1 ) insert tags into _features 
--       by filtering osm_tags with osm_features 
-----------------------------------------------------

INSERT INTO osm._features
(
	tag_id,
	osm_id,
	osm_element
)
(
	SELECT tags.tag_id,
			tags.osm_id,
		   tags.osm_element
		   
	FROM osm.osm_tags as tags, 
		 osm.osm_features as features
		 
	WHERE tags.tag_key = features.featureclass AND
		  tags.tag_value = features.feature
);
-----------------------------------------------------
-- ( 2 ) create _ways geometries by joining 
--       _way_nodes and _nodes
--       ST_MakeLine(geometry) GROUP BY way_id
-----------------------------------------------------

INSERT INTO osm._ways 
(
	way_id,
	geometry
)
(
	SELECT 	way_id,
			ST_MakeLine(geom.geometry) AS geometry
			
	FROM ( 
		SELECT wn.way_id  AS way_id,
		       wn.pos     AS pos,	      
		       wn.node_id AS node_id,
		       n.geometry AS geometry
			   
	    FROM osm._way_nodes AS wn,
		     osm._nodes     AS n
		 
	    WHERE wn.node_id = n.node_id
		
	    ORDER BY wn.way_id, wn.pos
	) as geom
	
	GROUP BY geom.way_id
);

-----------------------------------------------------
-- ( 3 ) insert geometries into osm_points by joining
--        _nodes with _features 
-----------------------------------------------------

INSERT INTO osm.osm_points

(
	tag_id,
	osm_id,
	osm_element,
	geometry
)
(
	SELECT feat.tag_id,
	       feat.osm_id,  
	       feat.osm_element, 
	       node.geometry  
		   
	FROM osm._features AS feat
	
	LEFT JOIN osm._nodes AS node
		ON feat.osm_id = node.node_id
		
	WHERE feat.osm_element = 'node'
);

-----------------------------------------------------------------
-- ( 4 ) insert geometries into osm_points by joining 
--       _members with _nodes and _features 
-----------------------------------------------------------------

INSERT INTO osm.osm_points

(
	tag_id,
	osm_id,
	osm_element,
	osm_role,
	geometry
)
(
SELECT 
	feat.tag_id,
	memb.rel_id,
	'relation',
	memb.osm_role,
	node.geometry

FROM osm._members  AS memb,
     osm._features AS feat,
     osm._nodes AS node
     
WHERE memb.rel_id =  feat.osm_id AND
      memb.osm_id = node.node_id AND
      memb.osm_element = 'node' AND 
      feat.osm_element = 'relation'
);



-----------------------------------------------------------------
-- ( 6 ) insert geometries into osm_lines by joining 
--       _members with _ways and _features, merged by role
-----------------------------------------------------------------

INSERT INTO osm.osm_lines
(
	tag_id,
	osm_id,
	osm_element,
	geometry
)
(
	SELECT tag_id,
	       rel_id,
	       'relation',
	       ST_Multi(ST_LineMerge(ST_Collect(geometry)))

	FROM (	SELECT feat.tag_id,
			memb.rel_id,
			memb.osm_role,
			ways.geometry

		FROM osm._members  AS memb,
		     osm._features AS feat,
		     osm._ways AS ways
		     
		WHERE memb.rel_id =  feat.osm_id AND
		      memb.osm_id = ways.way_id AND
		      memb.osm_element = 'way' AND 
		      feat.osm_element = 'relation' AND
		      memb.osm_role NOT IN ('outer', 'inner') AND
		      NOT ST_Equals(
					ST_StartPoint(ways.geometry),
					ST_EndPoint(ways.geometry)
				  )
	) as rel_lines

	GROUP BY tag_id, rel_id, osm_role
);

-----------------------------------------------------------------
-- ( 7 ) insert geometries into osm_lines by joining 
--       _ways with _features WHERE start point != end point
-----------------------------------------------------------------

INSERT INTO osm.osm_lines

(
	tag_id,
	osm_id,
	osm_element,
	geometry
)
(
	SELECT feat.tag_id,
	       feat.osm_id,  
	       feat.osm_element, 
		   -- ST_Multi creates multilinestring
	       ST_Multi(ways.geometry)  
		   
	FROM osm._features AS feat
	
	LEFT JOIN osm._ways AS ways
		ON feat.osm_id = ways.way_id
		
	WHERE feat.osm_element = 'way' AND
	      -- start point != end point
	      NOT ST_Equals(
			ST_StartPoint(ways.geometry),
			ST_EndPoint(ways.geometry)
		  )
);

-----------------------------------------------------------------
-- ( 8 ) insert geometries into osm_polygons by joining 
--       _members with _nodes and _features 
--       where rol is not outer or inner 
--       and start point = end point
-----------------------------------------------------------------

INSERT INTO osm.osm_polygons 

(
	tag_id,
	osm_id,
	osm_element,
	osm_role,
	geometry
)

(
	SELECT	tag_id,
			rel_id,
			'relation',
			osm_role,
			ST_CollectionExtract(
				ST_Polygonize(geometry),
				3
			) as geometry

	FROM (	SELECT feat.tag_id,
			memb.rel_id,
			memb.osm_role,
			ways.geometry

		FROM osm._members  AS memb,
		     osm._features AS feat,
		     osm._ways AS ways
		     
		WHERE memb.rel_id =  feat.osm_id AND
		      memb.osm_id = ways.way_id AND
		      memb.osm_element = 'way' AND 
		      feat.osm_element = 'relation' AND
		      memb.osm_role NOT IN ('outer', 'inner') AND
		      ST_Equals(
			  	ST_StartPoint(ways.geometry),
			    ST_EndPoint(ways.geometry)
		      )
	) as rel_polygons

	GROUP BY tag_id, rel_id, osm_role
);

-----------------------------------------------------------------
-- ( 9 ) insert geometries into _polygons by joining 
--       _ways with osm_features WHERE start point = end point
-----------------------------------------------------------------

INSERT INTO osm.osm_polygons 

(
	tag_id,
	osm_id,
	osm_element,
	geometry
)
(	
	SELECT	tag_id,
			osm_id,
			'way',
			ST_CollectionExtract(
				ST_Polygonize(geometry),
				3
			) as geometry
	FROM (
		SELECT feat.tag_id,
			   feat.osm_id,  
			   feat.osm_element, 
			   ways.geometry 
			   
		FROM osm._features AS feat
		
		LEFT JOIN osm._ways AS ways
			ON feat.osm_id = ways.way_id
			
		WHERE feat.osm_element = 'way' AND
			  -- start point = end point
			  ST_Equals(
				ST_StartPoint(ways.geometry),
				ST_EndPoint(ways.geometry)
			  )
	)  as multiline
	GROUP BY tag_id, osm_id
);

-----------------------------------------------------------------
-- ( 10 ) insert geometries into osm_polyygons by joining 
--       _members with _nodes and _features 
--       where rol is outer or inner
-----------------------------------------------------------------

INSERT INTO osm._polygons

(
	tag_id,
	osm_id,
	osm_element,
	osm_role,
	geometry
)

(
	SELECT	tag_id,
			rel_id,
			'relation',
			osm_role,
			ST_CollectionExtract(
				ST_Polygonize(geometry),
				3
			) as geometry

	FROM (	SELECT feat.tag_id,
			memb.rel_id,
			memb.osm_role,
			ways.geometry

		FROM osm._members  AS memb,
		     osm._features AS feat,
		     osm._ways AS ways
		     
		WHERE memb.rel_id =  feat.osm_id AND
		      memb.osm_id = ways.way_id AND
		      memb.osm_element = 'way' AND 
		      feat.osm_element = 'relation' AND
		      memb.osm_role IN ('outer', 'inner')
	) as rel_polygons

	GROUP BY tag_id, rel_id, osm_role
);

-----------------------------------------------------------------
-- ( 11 ) insert geometries into osm_polyygons
--        polygons who have only an outer geometry 
-----------------------------------------------------------------

INSERT INTO osm.osm_polygons 

(
	tag_id,
	osm_id,
	osm_element,
	osm_role,
	geometry
)
(
	SELECT	tag_id,
			osm_id,
			osm_element,
			osm_role,
			geometry 
	FROM osm._polygons
	WHERE osm_id in(
		SELECT osm_id
		FROM osm._polygons
		Group BY osm_id
		HAVING COUNT(*) = 1
	)
);

-----------------------------------------------------------------
-- ( 12 ) insert geometries into osm_polyygons 
--        polygons that have a inner and an outer part 
-----------------------------------------------------------------
INSERT INTO osm.osm_polygons 

(
	tag_id,
	osm_id,
	osm_element,
	osm_role,
	geometry
)
(
	SELECT 	out_poly.tag_id,
		out_poly.osm_id,
		out_poly.osm_element,
		'outer-inner',
		St_Multi(
			ST_Difference(
				out_poly.geometry,
				 in_poly.geometry
			)
		)
	
	FROM osm._polygons out_poly,
		 osm._polygons in_poly
	
	WHERE out_poly.osm_id in(
			SELECT osm_id
			FROM osm._polygons
			Group BY osm_id
			HAVING COUNT(*) > 1
			) AND 
	      out_poly.osm_role = 'outer'  AND
	      in_poly.osm_role = 'inner' AND 
	      out_poly.osm_id = in_poly.osm_id
);