#!/usr/bin/env python
# -*- coding: utf-8 -*-
import xml.etree.cElementTree as ET
import pprint
import re
"""
Your task is to explore the data a bit more.
Before you process the data and add it into your database, you should check the
"k" value for each "<tag>" and see if there are any potential problems.

We have provided you with 3 regular expressions to check for certain patterns
in the tags. As we saw in the quiz earlier, we would like to change the data
model and expand the "addr:street" type of keys to a dictionary like this:
{"address": {"street": "Some value"}}
So, we have to see if we have such tags, and if we have any tags with
problematic characters.

Please complete the function 'key_type', such that we have a count of each of
four tag categories in a dictionary:
  "lower", for tags that contain only lowercase letters and are valid,
  "lower_colon", for otherwise valid tags with a colon in their names,
  "problemchars", for tags with problematic characters, and
  "other", for other tags that do not fall into the other three categories.
See the 'process_map' and 'test' functions for examples of the expected format.
"""

FILENAME = "manchester_england.osm"

lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

expressions = [lower, lower_colon, problemchars]
expression_names = ['lower', 'lower_colon', 'problemchars']

problem_chars = set()

def key_type(element, keys):
    
    if element.tag == "tag":
        elem = element.attrib['k']
        
        for expression, ename in zip(expressions, expression_names):
            if expression.search(elem):
                keys[ename] = keys.get(ename, 0) + 1
                if ename == 'problemchars':
                    problem_chars.add(elem)
                return keys

        keys['other'] = keys.get('other', 0) + 1
        
    return keys

def process_map(filename):
    keys = {"lower": 0, "lower_colon": 0, "problemchars": 0, "other": 0}
    for _, element in ET.iterparse(filename):
        keys = key_type(element, keys)

    return keys



def explore_tags():
    keys = process_map(FILENAME)
    pprint.pprint(keys)
    print problem_chars
  

if __name__ == "__main__":
    explore_tags()