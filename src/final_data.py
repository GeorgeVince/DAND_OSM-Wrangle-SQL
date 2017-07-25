#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
The final return value for a "node" element should look something like:

{'node': {'id': 757860928,
          'user': 'uboot',
          'uid': 26299,
       'version': '2',
          'lat': 41.9747374,
          'lon': -87.6920102,
          'timestamp': '2010-07-22T16:16:51Z',
      'changeset': 5288876},
 'node_tags': [{'id': 757860928,
                'key': 'amenity',
                'value': 'fast_food',
                'type': 'regular'},
               {'id': 757860928,
                'key': 'cuisine',
                'value': 'sausage',
                'type': 'regular'},
               {'id': 757860928,
                'key': 'name',
                'value': "Shelly's Tasty Freeze",
                'type': 'regular'}]}



The final return value for a "way" element should look something like:

{'way': {'id': 209809850,
         'user': 'chicago-buildings',
         'uid': 674454,
         'version': '1',
         'timestamp': '2013-03-13T15:58:04Z',
         'changeset': 15353317},
 'way_nodes': [{'id': 209809850, 'node_id': 2199822281, 'position': 0},
               {'id': 209809850, 'node_id': 2199822390, 'position': 1},
               {'id': 209809850, 'node_id': 2199822392, 'position': 2},
               {'id': 209809850, 'node_id': 2199822369, 'position': 3},
               {'id': 209809850, 'node_id': 2199822370, 'position': 4},
               {'id': 209809850, 'node_id': 2199822284, 'position': 5},
               {'id': 209809850, 'node_id': 2199822281, 'position': 6}],
 'way_tags': [{'id': 209809850,
               'key': 'housenumber',
               'type': 'addr',
               'value': '1412'},
              {'id': 209809850,
               'key': 'street',
               'type': 'addr',
               'value': 'West Lexington St.'},
              {'id': 209809850,
               'key': 'street:name',
               'type': 'addr',
               'value': 'Lexington'},
              {'id': 209809850,
               'key': 'building_id',
               'type': 'chicago',
               'value': '366409'}]}
"""

import csv
import codecs
import pprint
import re
import xml.etree.cElementTree as ET
import string

import cerberus

import schema

OSM_PATH = "manchester_england.osm"

NODES_PATH = "nodes.csv"
NODE_TAGS_PATH = "nodes_tags.csv"
WAYS_PATH = "ways.csv"
WAY_NODES_PATH = "ways_nodes.csv"
WAY_TAGS_PATH = "ways_tags.csv"

LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

SCHEMA = schema.schema

# Make sure the fields order in the csvs matches the column order in the sql table schema
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']

expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road", 
            "Trail", "Parkway", "Commons", "West", "Way","Walk","Terrance", "South", "Park", "North", "Hill",
            "Grove", "Gardens", "East", "Crescent", "Close"]

# UPDATE THIS VARIABLE
mapping = { "St": "Street",
            "St.": "Street",
            "Rd.": "Road",
            "Ave": "Avenue",
            "Raod": "Road",
            "N"  : "North",
            "Sq" : "Square",
            "Ln" : "Lane",
            "Rd" : "Road"
            }

invalid_postcode_distance = ['M60 4EP', 'M19 2SY', 'M15 6FD', 'SK5 6XD', 'M17 1TD']
invalid_street_names = ['Avenuehttps://streaming.media.ccc.de/33c3/']

postcode_regex = r'^([A-Za-z]{1,2}[0-9]{1,2}[A-Za-z]?[ ]?)([0-9]{1}[A-Za-z]{2})$'
postcode_re = re.compile(postcode_regex)

street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)

"""Clean and shape node or way XML element to Python dict"""
def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
   
    attribs = {}
    way_nodes = []
    tags = []  # Handle secondary tags the same way for both node and way elements
    
    if element.tag == 'node' or element.tag == 'way':
        
        if element.tag == 'node':
            field_attribs = node_attr_fields
        else:
            field_attribs = way_attr_fields
        #Set field attributes, if not found set to default schema value
        for k in field_attribs:
                val = element.attrib.get(k, None)
                if val:
                    attribs[k] = val
                else:
                    attribs[k] = default_val('node', k)
        #Set tag elements
        _id = element.attrib['id']
        for tag in element.iter("tag"):
            if not problem_chars.search(tag.attrib['k']):
                new_tag = parse_tags(tag.attrib, _id, default_tag_type)
                if new_tag: tags.append(new_tag)
        
        
        if element.tag == 'node':
            return {'node': attribs, 'node_tags': tags}
        
        #Way eelements also have nodes
        elif element.tag == 'way':
            for way_node in element.iter("nd"):
                way_nodes.append({'id': _id,
                                  'node_id' : way_node.attrib['ref'],
                                  'position' : len(way_nodes) })
                        
            return {'way': attribs, 'way_nodes': way_nodes, 'way_tags': tags}



# ================================================== #
#               Data Cleaning Functions              #
# ================================================== #

def format_postcode(postcode):
    postcode_altered = postcode
    postcode_altered = postcode_altered.upper()
    
    if is_valid_post(postcode_altered):
        if postcode_altered in invalid_postcode_distance:
            postcode_altered = None
    else:
        postcode_altered = None
        
    if postcode_altered <> postcode:
        print "Postcode Modified - {} -> {}".format(postcode, postcode_altered)
    
    return postcode_altered

def is_valid_post(postcode):
    m = postcode_re.search(postcode)
    if m:
        return True
    else:
        return False
      

def format_street(street_name):
    street_altered = street_name
    street_altered = string.capwords(street_altered)
    
    m = street_type_re.search(street_altered)
    if m:
        street_type = m.group()
        if street_type not in expected:
            street_altered = update_street_name(street_altered, mapping)
    
    if street_altered in invalid_street_names:
        street_altered = None
    
    if street_altered <> street_name:
        print "Street Modified - {} -> {}".format(street_name, street_altered)

           
def update_street_name(name, mapping):
    
    replace = name.rsplit(None, 1)[-1]
    replace_with = mapping.get(replace)
    
    if replace_with:
        name = name.replace(replace, replace_with)
        
    return name

def is_street_name(elem):
    return (elem['k'] == "addr:street")


def is_post_tag(elem):
    return (elem['k'] == "addr:postcode")


# ================================================== #
#               Helper Functions                     #
# ================================================== #

#Return a default value
def default_val(nodeval, attrib):

    _type = schema.schema[nodeval]['schema'][attrib].get('coerce', "None")
    if _type == int:
        _type = 0
    
    return _type



def parse_tags(tags_dict, node_id, default_tag_type):
    tags = {}

    tags['id'] = node_id
    tags['value'] = tags_dict['v']
    
    key_type = tags_dict['k'].split(":",1)

    if(len(key_type) > 1):
        tags['type'] = key_type[0]
        tags['key'] = key_type[1]
    else:
        tags['type'] = default_tag_type
        tags['key'] = tags_dict['k']

    
    if is_street_name(tags_dict):
        tags['value'] = format_street(tags['value'])

    if is_post_tag(tags_dict):
        tags['value'] = format_postcode(tags['value'])
    
    if tags['value'] == None:
            tags = None
    
    return tags


def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag"""

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


def validate_element(element, validator, schema=SCHEMA):
    """Raise ValidationError if element does not match schema"""
    if validator.validate(element, schema) is not True:
        field, errors = next(validator.errors.iteritems())
        message_string = "\nElement of type '{0}' has the following errors:\n{1}"
        error_string = pprint.pformat(errors)
        
        raise Exception(message_string.format(field, error_string))


class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: (v.encode('utf-8') if isinstance(v, unicode) else v) for k, v in row.iteritems()
        })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


# ================================================== #
#               Main Function                        #
# ================================================== #
def process_map(file_in, validate):
    """Iteratively process each XML element and write to csv(s)"""

    with codecs.open(NODES_PATH, 'wb') as nodes_file, \
         codecs.open(NODE_TAGS_PATH, 'wb') as nodes_tags_file, \
         codecs.open(WAYS_PATH, 'wb') as ways_file, \
         codecs.open(WAY_NODES_PATH, 'wb') as way_nodes_file, \
         codecs.open(WAY_TAGS_PATH, 'wb') as way_tags_file:

        nodes_writer = UnicodeDictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = UnicodeDictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = UnicodeDictWriter(ways_file, WAY_FIELDS)
        way_nodes_writer = UnicodeDictWriter(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = UnicodeDictWriter(way_tags_file, WAY_TAGS_FIELDS)

        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()

        validator = cerberus.Validator()
        
        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element)

            if el:
                
                if validate is True:
                    validate_element(el, validator)

                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])
                elif element.tag == 'way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])
    
    print "Finish processing map"


if __name__ == '__main__':
    # Note: Validation is ~ 10X slower. For the project consider using a small
    # sample of the map when validating.
    process_map(OSM_PATH, validate=False)

