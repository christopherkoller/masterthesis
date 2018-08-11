# -------------------------------------------------------------------------------
# Name:        osm_loader
# Purpose:     load osm data into postgis
#
# Author:      Christopher Koller
#
# Created:     20.10.2017
# Copyright:   (c) Christopher Koller 2017
# -------------------------------------------------------------------------------

import sys
import os
import time
import uuid
import tempfile
import struct

import requests

import xml.etree.cElementTree as cElementTree

import psycopg2 as pg

BASE_DIR = os.path.normpath(os.path.dirname(__file__))

sys.path.append(os.path.join(BASE_DIR, r"py\utm-0.4.2"))

import utm

ATTR_TYPES = {"id": int, "lon": float, "lat": float}

TMP_DIR = tempfile.gettempdir()
TMP_GUID = uuid.uuid4().__str__().replace("-", "_")

OP_API_URL = "https://overpass-api.de/api/interpreter"

DB_CON = pg.connect(dbname="homegis", user="ck", password="...")

UTM_ZONE = 33
SRID = 32633

RIGHT = 13.993706353
LEFT = 12.0791287135
TOP = 48.0379534738
BOTTOM = 46.9442948152

# FILTER = "highway=motorway_link"
FILTER = (
    #"highway", "busway", "aerialway", "railway",
    "public_transport",#"sport", "leisure",
    #"tourism", "shop", "amenity", "office"
)

def _type_conversion(attrib_dict):

    for fld, data_type in ATTR_TYPES.items():

        if fld in attrib_dict:
            attrib_dict[fld] = data_type(attrib_dict[fld])

    return attrib_dict


def _chunk_list(l, n):

    for i in xrange(0, len(l), n):
        yield l[i:i + n]


def _get_xml_nodes(xml_file, search_string):

    try:
        xml_tree = cElementTree.ElementTree(file=xml_file)
        xml_root = xml_tree.getroot()
        nodes = xml_root.findall(search_string)
        del xml_tree, xml_root

    except Exception:
        nodes = []

    return nodes


def sink_osm_data(left, bottom, right, top, tag_filter):

    overpass_query = ""

    for osm_element in ("node", "way", "rel"):

        overpass_query += "({osm_element}[{osm_filter}]" \
                          "({bottom},{left},{top},{right});<;>;);".format(
            osm_element=osm_element,
            osm_filter=tag_filter,
            bottom=bottom,
            left=left,
            top=top,
            right=right
        )
    print overpass_query
    response = requests.post(
        OP_API_URL,
        data={"data": "[out:xml];({0});out body;".format(overpass_query)},
        timeout=180,  # seconds
        headers={'Accept-Charset': 'utf-8;q=0.7,*;q=0.7'},
        stream=True
    )

    response.encoding = 'utf-8'

    osm_file = r"{0}\{1}.main.osm.xml".format(
        TMP_DIR,
        TMP_GUID
    )
    print osm_file
    tmp_fobj = open(osm_file, "w")

    for chunk in response.iter_content(chunk_size=1024):
        if chunk:
            tmp_fobj.write(chunk)

    tmp_fobj.close()

    return osm_file


def sink_missing_elements(id_list, osm_type):

    x, y = 0, len(id_list)

    tmp_files = []

    for id_chunk in _chunk_list(id_list, 1000):

        x += len(id_chunk)
        print "Sinking missing {0} elements ({1}/{2})".format(
            osm_type, x, y, id_chunk[:20]
        )

        tmp_file = r"{0}\{1}.{2}.{3}.osm.xml".format(
            TMP_DIR, TMP_GUID, osm_type, x
        )
        tmp_files.append(tmp_file)
        tmp_fobj = open(tmp_file, "w")

        element_query = ""
        for element in id_chunk:
            element_query += "({0}({1});>;);".format(osm_type, element)

        data = {"data": "[out:xml];({0});out body;".format(element_query)}

        while True:

            try:
                request = requests.post(
                    OP_API_URL,
                    data=data,
                    timeout=180,  # seconds
                    headers={'Accept-Charset': 'utf-8;q=0.7,*;q=0.7'},
                    stream=True
                )

            except Exception, e:
                print e, str(e)

            if request.status_code == 200:
                request.encoding = 'utf-8'
                for xml_chunk in request.iter_content(chunk_size=1024):
                    if xml_chunk:
                        tmp_fobj.write(xml_chunk)
                break

            elif request.status_code == 429:
                wait = requests.get(
                    "https://overpass-api.de/api/status"
                ).text.splitlines()[3]
                if "now" in wait:
                    continue
                else:
                    time.sleep(int(wait.split(" ")[-2]))
                    continue

        tmp_fobj.close()

    return tmp_files


def get_tags(osm_id, osm_element, osm_tags):

    tags = []

    for tag in osm_tags:

        if tag.tag == "tag":

            tag_key = tag.attrib["k"]
            tag_val = tag.attrib["v"]

            tags.append(
                {
                    "osm_id": osm_id,
                    "osm_element": osm_element,
                    "tag_key": tag_key,
                    "tag_value": tag_val
                }
            )

    return tags


def get_way_nodes(way_id, tags):
    way_nodes = []
    pos = 0

    for tag in tags:
        if tag.tag == "nd":
            pos += 1
            way_nodes.append(
                {
                    "way_id": way_id,
                    "pos": pos,
                    "node_id": int(tag.attrib["ref"])
                }
            )

    if way_nodes[0]["node_id"] == way_nodes[-1]["node_id"]:
        return "POLYGON", way_nodes

    else:
        return "LINESTRING", way_nodes


def get_members(rel_id, members):

    role_members = []

    pos = {"node": {}, "way": {}, "relation": {}}

    for member in members:

        if member.tag == "member":

            osm_role = member.attrib["role"]
            osm_ele = member.attrib["type"]
            osm_id = int(member.attrib["ref"])

            if osm_role not in pos[osm_ele]:
                pos[osm_ele][osm_role] = 1
            else:
                pos[osm_ele][osm_role] += 1

            role_members.append(
                {
                    "rel_id": rel_id,
                    "osm_id": osm_id,
                    "osm_ele": osm_ele,
                    "pos": pos[osm_ele][osm_role],
                    "osm_role": osm_role
                }
            )

    return role_members


def check_missing_elements():

    print "checking for missing relations, ways, nodes -> check_missing_elements()"

    cursor = DB_CON.cursor()

    missing = {
        "node": [],
        "way": [],
        "rel": []
    }

    sqls = (
        (
            "node",
            '''
            SELECT DISTINCT way.node_id
            FROM osm._way_nodes as way
            LEFT JOIN (
                SELECT DISTINCT node_id 
                FROM osm._nodes
            ) as nod
            ON way.node_id = nod.node_id
            WHERE nod.node_id IS NULL;
            '''
        ),
        (
            "node",
            '''
            SELECT DISTINCT _mem.osm_id 
            FROM osm._members as _mem
            LEFT JOIN (
                SELECT DISTINCT node_id 
                FROM osm._nodes
            ) as nod
            ON _mem.osm_id = nod.node_id
            WHERE _mem.osm_element = 'node' 
                AND nod.node_id IS NULL;
            '''
        ),
        (
            "way",
            '''
            SELECT DISTINCT _mem.osm_id 
            FROM osm._members as _mem
            LEFT JOIN (
                SELECT DISTINCT way_id 
                FROM osm._way_nodes
            ) as way_nod
            ON _mem.osm_id = way_nod.way_id
            WHERE _mem.osm_element = 'way' 
                AND way_nod.way_id IS NULL;
            '''
        ),
        (
            "rel",
            '''
            SELECT DISTINCT mem.osm_id 
            FROM osm._members as mem
            LEFT JOIN (
                SELECT DISTINCT osm_id 
                FROM osm._members
            ) as relchld
            ON mem.osm_id = relchld.osm_id
            WHERE mem.osm_element = 'relation' 
                AND relchld.osm_id IS NULL;
            '''
        )
    )

    for osm_ele, sql in sqls:
        # sum(list_of_tuples, empty_tuple) flattens list
        # -> sum([(1706121979L,), (1706122002L,), (1971432955L,)], ())
        # = (1706121979L, 1706122002L, 1971432955L)
        cursor.execute(sql)
        missing[osm_ele].extend(sum(list(cursor),()))

    return missing


def load_base_elements(xml_files):

    print "Loading base elements"

    if not isinstance(xml_files, (list, tuple)):
        xml_files = [xml_files, ]

    cur = DB_CON.cursor()

    for xml_file in xml_files:

        for node in _get_xml_nodes(xml_file, "node"):

            node.attrib = _type_conversion(node.attrib)
            osm_id = node.attrib["id"]

            x, y, zone = utm.from_latlon(node.attrib["lat"], node.attrib["lon"], UTM_ZONE)
            wkb = struct.pack('>bIdd', 0, 1, x, y).encode("hex")

            cur.execute('''
                INSERT INTO osm._nodes (
                    node_id,
                    geometry
                ) VALUES (
                    %(node_id)s,
                    ST_GeomFromWKB(
                        %(wkb)s::geometry,
                        %(srid)s
                    )
                )
                ON CONFLICT (node_id) DO UPDATE SET 
                    node_id = %(node_id)s,
                    geometry = ST_GeomFromWKB(%(wkb)s::geometry, %(srid)s
                );
                ''',
                {
                    'node_id': osm_id,
                    'wkb': wkb,
                    'srid': SRID
                }
            )

            for tag in  get_tags(osm_id, "node", list(node)):

                cur.execute('''
                    INSERT INTO osm.osm_tags (
                        osm_id,
                        osm_element,
                        tag_key,
                        tag_value
                    ) VALUES (
                        %(osm_id)s,
                        %(osm_element)s,
                        %(tag_key)s,
                        %(tag_value)s
                    );
                    ''',
                    tag
                )

            del node

        DB_CON.commit()

        for way in _get_xml_nodes(xml_file, "way"):

            way.attrib = _type_conversion(way.attrib)
            osm_id = way.attrib["id"]

            geometry_type, way_nodes = get_way_nodes(osm_id, list(way))

            for way_node in way_nodes:
                cur.execute('''
                    INSERT INTO osm._way_nodes (
                        way_id,  
                        pos,
                        node_id
                    ) VALUES (
                        %(way_id)s,   
                        %(pos)s,
                        %(node_id)s          
                    ) ON CONFLICT (way_id, pos, node_id) DO UPDATE SET 
                        way_id = %(way_id)s,
                        pos = %(pos)s,
                        node_id = %(node_id)s
                    ;  
                    ''',
                    way_node
                )

            for tag in get_tags(osm_id, "way", list(way)):

                cur.execute('''
                    INSERT INTO osm.osm_tags (
                        osm_id,
                        osm_element,
                        tag_key,
                        tag_value
                    ) VALUES (
                        %(osm_id)s,
                        %(osm_element)s,
                        %(tag_key)s,
                        %(tag_value)s
                    );
                    ''',
                    tag
                )

        DB_CON.commit()

        for relation in _get_xml_nodes(xml_file, "relation"):

            relation.attrib = _type_conversion(relation.attrib)
            osm_id = int(relation.attrib["id"])

            for member in get_members(osm_id, list(relation)):

                cur.execute('''
                    INSERT INTO osm._members (
                        rel_id,  
                        osm_id,
                        osm_element,  
                        pos,
                        osm_role
                    ) VALUES (
                        %(rel_id)s,   
                        %(osm_id)s,
                        %(osm_ele)s,      
                        %(pos)s,
                        %(osm_role)s                                        
                    );  
                    ''',
                    member
                )

            for tag in get_tags(osm_id, "relation", list(relation)):

                cur.execute('''
                    INSERT INTO osm.osm_tags (
                        osm_id,
                        osm_element,
                        tag_key,
                        tag_value
                    ) VALUES (
                        %(osm_id)s,
                        %(osm_element)s,
                        %(tag_key)s,
                        %(tag_value)s
                    );
                    ''',
                    tag
                )

        DB_CON.commit()


def load_osm():

    global FILTER

    if not isinstance(FILTER, (list, tuple)):
        FILTER = [FILTER,]

    for f in FILTER:

        osm_xml = sink_osm_data(
            left=LEFT,
            bottom=BOTTOM,
            right=RIGHT,
            top=TOP,
            tag_filter=f
        )

        load_base_elements(osm_xml)

        missing = check_missing_elements()

        osm_xmls = []

        for osm_ele in missing:
            osm_xmls.extend(
                sink_missing_elements(
                    missing[osm_ele],
                    osm_ele
                )
            )

        load_base_elements(osm_xmls)


if __name__ == "__main__":

    load_osm()
    #    osm_xml=r"J:\UNIGIS\03__Masterarbeit\03_geodaten\osm\91427ca9_aa02_4b32_ad55_568bb66fba23.osm"
    #)
