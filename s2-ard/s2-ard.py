
# This script gets the Sentinel-2 ARD catalogue data for alpha-1 from the S3 bucket.
# It produces json containing the product representations for the web deli.

# pip install boto3
# pip install awscli
# aws configure

import boto3
import pprint
import uuid
import json
from datetime import datetime

s3 = boto3.resource('s3')
pp = pprint.PrettyPrinter()

bucket = s3.Bucket('eodip')

def makeIsoDate(s):
    return datetime.strptime(s, '%Y%m%d').date().strftime('%Y-%m-%d')

# filter the objects in the bucket to /ard "folder"
results = bucket.objects.filter(Prefix='ard', )

# results are S3 keys like 'ard/S2_20160723_94_1/S2_20160723_94_1.tif'
# split on '/' to give us the (distinct) products
# ie the list of product names like 'S2_20160723_94_1'
tiffInfoList = [ {  "name" : r.key.split('/')[1],
                    "size" : r.size } 
                   for r in results if r.key.endswith(".tif") ]

productList = list(map(lambda s:{
                "id": uuid.uuid4().urn[9:],
                "title": s["name"],
                "footprint": "todo",    
                "properties": {
                    "capturedate": makeIsoDate(s["name"].split('_')[1])
                },
                "representations": {
                    "download": {
                        "url": "https://s3-eu-west-1.amazonaws.com/eodip/ard/" + s["name"] + "/" + s["name"] + ".tif",
                        "size": s["size"],
                        "type": "Geotiff", 
                    },
                    "wms": {
                        "name": uuid.uuid4().urn[9:],
                        "base_url": "https://eo.jncc.gov.uk/geoserver/ows"
                    }
                }
            }, tiffInfoList))

print(json.dumps(productList))


