
# This script gets the Sentinel-2 ARD catalogue data for alpha-1 from the S3 bucket.
# It produces products.json containing the product representations for the web deli.

# pip install boto3
# pip install awscli
# pip install pyfunctional
# aws configure

from __future__ import print_function

import boto3
import pprint
import uuid
import json
import re
from functional import seq

s3 = boto3.resource('s3')
pp = pprint.PrettyPrinter()

bucket = s3.Bucket('eodip')

# filter the objects in the bucket to /ard "folder"
results = bucket.objects.filter(Prefix='ard', )

# results are S3 keys like 'ard/S2_20160723_94_1/S2_20160723_94_1.tif'
regex = r"ard/(?P<name>.*)/S2_(?P<year>\d\d\d\d)(?P<month>\d\d)(?P<day>\d\d)_(?P<orbit>\d+)_(?P<row>\d).tif"

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
            
def makeProduct(result):
    m = re.search(regex, result.key).groupdict()
    guid = uuid.uuid4().urn[9:]
    return { "id"   : guid,
             "title" : m["name"],
             "footprint": getFootprintGeojson(m["orbit"], m["row"]),
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

output = (seq(results)
    .filter(lambda r: re.match(regex, r.key) != None)
    .map(lambda r: makeProduct(r))
    .partition(lambda p: p["footprint"] == None))

output[0].for_each(lambda p: print("Failed to get footprint for " + p["title"]))
output[1].to_json("products.json")
