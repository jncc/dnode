
# This script gets the Sentinel-2 ARD catalogue data for alpha-1 from the S3 bucket.
# It produces json containing the product representations for the web deli.

# pip install boto3
# pip install awscli
# pip install pyfunctional 
# aws configure

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

def makeProduct(result):
    m = re.search(regex, result.key).groupdict()
    guid = uuid.uuid4().urn[9:]
    product = { "id"   : guid,
                "title" : m["name"],
                "footprint": "todo",
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
    return product

products = (seq(results)
    .filter(lambda r: re.match(regex, r.key) != None)
    .map(lambda r: makeProduct(r))).to_list()


# pp.pprint(products)
print(json.dumps({"products" : products}))


