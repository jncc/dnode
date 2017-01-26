
# This script gets the Sentinel-2 ARD catalogue data for alpha-1 from the S3 bucket.
# It produces products.json containing the product representations for the web deli.

# pip install boto3
# pip install awscli
# pip install pyfunctional
# pip install shapely
# aws configure

from __future__ import print_function

import boto3
import pprint
import uuid
import json
import re

from functional import seq
from shapely.geometry import shape

s3 = boto3.resource('s3')
pp = pprint.PrettyPrinter()

bucket = s3.Bucket('eodip')

# filter the objects in the bucket to /ard "folder"
results = bucket.objects.filter(Prefix='ard', )

# results are S3 keys like 'ard/S2_20160723_94_1/S2_20160723_94_1.tif'
regex = r"^ard/(?P<name>.*)/S2_(?P<year>\d\d\d\d)(?P<month>\d\d)(?P<day>\d\d)_(?P<orbit>\d+)_(?P<row>\d).tif$"

def getFootprintGeojson(orbit, row):
    filepath = "scenes/" + orbit + ".geojson"
    with open(filepath) as jsonFile:
        # load a FeatureCollection geojson object with features[] which have a "properties" object with an "id" row property
        j = json.load(jsonFile)
        feature = seq(j["features"]).filter(lambda f: f["properties"]["id"] == int(row)).head_option()
        if feature == None:
            return None
        else:
            return { "type": "Feature", "properties": {}, "geometry": feature["geometry"] }

def getBBox(geom):
    s = shape(geom)
    # (minx, miny, maxx, maxy)
    bounds = s.bounds
    return bounds
            
def makeProduct(result, match):
    # print(result.key)
    m = match.groupdict()
    guid = uuid.uuid4().urn[9:]
    footprint = getFootprintGeojson(m["orbit"], m["row"])
    bbox = getBBox(footprint["geometry"])
    return { "id"   : guid,
             "title" : m["name"],
             "footprint": footprint,
             "bbox": bbox,
             "properties": {
                 "capturedate": m["year"] + "-" + m["month"] + "-" + m["day"]
             },
             "representations": {
                 "download": {
                     "url": "https://s3-eu-west-1.amazonaws.com/eodip/ard/" + m["name"] + "/" + m["name"] + ".tif",
                     "size": result.size,
                     "type": "Geotiff", 
                 },
                 "wms": {
                     "name": guid,
                     "base_url": "https://eo.jncc.gov.uk/geoserver/ows"
                 }
             }
    }

products, failures = (seq(results)
    .map(lambda r: { "result": r, "match": re.match(regex, r.key) })
    .filter(lambda x: x["match"] != None)
    .distinct_by(lambda x: x["match"].group(0))
    .map(lambda x: makeProduct(x["result"], x["match"]))
    .cache() # force evaluation once
    .partition(lambda p: p['footprint'] != None))

failures.for_each(lambda p: print("Failed to get footprint for " + p["title"]))
products.to_json("products.json")
print("Wrote " + str(products.size()) + " product records to products.json")