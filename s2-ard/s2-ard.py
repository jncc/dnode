
# This script gets the Sentinel-2 ARD catalogue data for alpha-1 from the S3 bucket.
# It produces data.json containing the product representations for the web deli.

# Instal spatial packages from apt:
# sudo add-apt-repository ppa:ubuntugis/ubuntugis-unstable
# sudo apt-get update
## python3-gdal or python-gdal depending on the python env to be used
# sudo apt-get install build-essential libgeos-dev python-dev gdal-bin libgdal-dev python3-gdal python-gdal

# for special people running conda
# conda install -c conda-forge gdal=2.2.0

# pip install gdal
# pip install boto3
# pip install awscli
# pip install pyfunctional
# pip install shapely
# pip install pyproj
# aws configure

from __future__ import print_function

import boto3
import pprint
import uuid
import json
import re
import datetime
import pyproj

from functional import seq
from functools import partial
from shapely.geometry import shape
from shapely.ops import transform

s3 = boto3.resource('s3')
pp = pprint.PrettyPrinter()

bucket = s3.Bucket('eodip')

# filter the objects in the bucket to /ard "folder"
results = bucket.objects.filter(Prefix='ard')

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
    return shape(geom).bounds

def getOsgbBBox(geom):
    s = shape(geom)
    project = partial(
        pyproj.transform,
        pyproj.Proj(init='epsg:4326'), # source coordinate system
        pyproj.Proj(init='epsg:27700')) # destination coordinate system
    s2 = transform(project, s)
    return s2.bounds    
            
def makeProduct(result, match):
    # print(result.key)
    m = match.groupdict()
    guid = uuid.uuid4().urn[9:]
    footprint = getFootprintGeojson(m["orbit"], m["row"])
    bbox = getBBox(footprint["geometry"])
    osgbBBox = getOsgbBBox(footprint["geometry"])
    return { "id"   : guid,
             "title" : m["name"],
             "footprint": footprint,
             "bbox": bbox,
             "osgbBbox": osgbBBox,
             "properties": {
                 "capturedate": m["year"] + "-" + m["month"] + "-" + m["day"],
                 "orbit": m["orbit"],
                 "row": m["row"],
             },
             "data": {
                 "download": {
                     "url": "https://s3-eu-west-1.amazonaws.com/eodip/ard/" + m["name"] + "/" + m["name"] + ".tif",
                     "size": result.size,
                     "type": "Geotiff", 
                 },
                 "wms": {
                     "name": "s2_ard:" + m["name"] + "_rgba",
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
print("Found " + str(products.size()) + " product records")

# there's just one collection, the S2-ARD collection
collections = [{
    "id": "d82f236a-e61d-482d-a581-293ec1b11c3e",
    "metadata": {
        "title": "Sentinel-2 ARD",
        "abstract": "The Sentinel 2 Analysis Ready Data (ARD) products provide an estimate of the surface spectral reflectance measured at ground level in the absence of atmospheric effects. Bands 2-8, 8a, 11 and 12 are provided at 10m spatial resolution. The dataset includes imagery captured over the UK only.",
        "topicCategory": "ImageryBaseMaps EarthCover",
        "keyword": ["Sentinel 2", "S2 level2a", "ARD", "Analysis Ready Data", "Surface Reflectance"],
        "resourceType": "Dataset",
        "datasetReferenceDate": "2017",
        "lineage": "Software versions used are GDAL v2.0, RSGISLib v3.1.0. High level processing steps include: 1.	JPEG2000 converted to GeoTIFF; 2.	Re-projection to the British National Grid; 3.	20m bands (5,6,7,8A,11 and 12) are re-sampled to 10m using a nearest neighbour transformation; 4.	Mosaicing to reconnect granules; 5.	Band stacking - combining all bands associated with an area into one image; 6.	Basic atmospheric correction using a Dark Object Subtraction method; 7.	Improvements to usability such as adding names to reflectance bands. Parameters used in processing are scene specific and will be captured in future iterations through ARCSI software.",
        "responsibleOrganisation": "Joint Nature Conservation Committee",
        "accessLimitations": "None",
        "useConstraints": "Open Government Licence v3",
        "metadataDate": "2017-03-01",
        "metadataPointOfContact": "earthobs@jncc.gov.uk",
        "metadataLanguage": "English",
        "spatialReferenceSystem": "EPSG:4326"
    },
    #"bbox": {},  todo
    "data": { },
    "products": products.list()
}]

seq(collections).to_json("data.json")

print("Wrote " + str(products.size()) + " product records to data.json")
