
# pip install pyfunctional
# pip install shapely

import json
import pyproj
from pprint import pprint
from functional import seq
from functools import partial
from shapely.geometry import shape
from shapely.ops import transform


with open('products-v1.json') as datafile:
    data = json.load(datafile)

print(len(data)) 

# (minx, miny, maxx, maxy)
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
    
(seq(data)
    .map(lambda p: { "id"   : p["id"],
             "title" : p["title"],
             "footprint": p["footprint"],
             "bbox": getBBox(p["footprint"]["geometry"]),
             "osgbBbox" : getOsgbBBox(p["footprint"]["geometry"]),
             "properties": p["properties"],
             "representations": p["representations"]
    })
.to_json('products-v1-withbbox.json'))


