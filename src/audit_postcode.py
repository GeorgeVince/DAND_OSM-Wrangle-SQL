import csv
import gpxpy.geo
import xml.etree.cElementTree as ET
import re
from collections import defaultdict

OSMFILE = "manchester_england.osm"
CORRECTPOSTCODE = "correct_postcodes.csv"
postcode_regex = r'^([A-Za-z]{1,2}[0-9]{1,2}[A-Za-z]?[ ]?)([0-9]{1}[A-Za-z]{2})$'
postcode_re = re.compile(postcode_regex)

good_post = []
bad_post = []

post_area = defaultdict(set)
valid_osm_postcode = defaultdict(list)


def is_valid_post(postcode):
    m = postcode_re.search(postcode)
    if m:
        post_area[postcode[0:2]] = post_area.get(postcode[0:2], 0) + 1
        
        good_post.append(postcode)
        return True
    else:
        bad_post.append(postcode)
        return False


def is_post_tag(elem):
    return (elem.attrib['k'] == "addr:postcode")

def get_lat(elem):
    return (elem.attrib.get('lat'))

def get_lon(elem):
    return (elem.attrib.get('lon'))

def audit(osmfile):
    osm_file = open(osmfile, "r")
    for event, elem in ET.iterparse(osm_file, events=("start",)):

        if elem.tag == "node" or elem.tag == "way":
            postcode = ""
            lon = ""
            lat = ""
           #postcost + streetname
            for tag in elem.iter("tag"):
                if is_post_tag(tag):
                    postcode = tag.attrib['v']

            if postcode:
                if is_valid_post(postcode):
                    lon = get_lon(elem)
                    lat = get_lat(elem)
                    if lon and lat:
                        valid_osm_postcode[postcode].append({"lon":lon, "lat":lat})

    osm_file.close()


#Reads in out CORRECT postcode lat / lon location
#https://data.gov.uk/dataset/national-statistics-postcode-lookup-uk
distances = defaultdict(list)

def compare_correct(datafile):
    
    valid_postcode_data = {}
    with open(datafile,'rb') as f:
         csvreader = csv.reader(f)
         next(csvreader)
         
         for row in csvreader:
             valid_postcode_data[row[0]] = {"lat" : row[1], "lon":row[2]}
             
         for postcode in valid_postcode_data:
             
             lat2 = float(valid_postcode_data[postcode]['lat'])
             lon2 = float(valid_postcode_data[postcode]['lon'])
             
             for elem in valid_osm_postcode[postcode]:                 
                 lat1 = float(elem['lat'])
                 lon1 = float(elem['lon'])
                 dist = gpxpy.geo.haversine_distance(lat1, lon1, lat2, lon2) / 1000
                 distances[postcode].append({"lon":lon1, "lat":lat1, "distance":dist})
    
    avg = sum([dis for dis, pc in _flatten()]) / len([dis for dis, pc in _flatten()])
    min_dist, min_elem = min(_flatten())
    max_dist, max_elem = max(_flatten())      
            
    print "Average distance = {} km".format(round(avg,3))
    print "Minimum distance = {} km for the postcode {}".format(round(min_dist,3), min_elem)
    print "Maximum distance = {} km for the postcode {}".format(round(max_dist,3), max_elem)

#Return a tuple with distance + postcode                
def _flatten():
    for postcode, dlist in distances.items():
        for loc in dlist:
            yield loc['distance'], postcode

def audit_post_code_main():
    audit(OSMFILE)
    
    print "\n"
    print "Correctly for postcodes: {0} - Incorrect postcodess: - {1}".format
    (len(good_post), len(bad_post))
    print "\n"
    
    if True:
        print "Post area stats:"
        print post_area
        
    print "\n"
        
    if True:
        compare_correct(CORRECTPOSTCODE)

if __name__ == '__main__':
    audit_post_code_main()