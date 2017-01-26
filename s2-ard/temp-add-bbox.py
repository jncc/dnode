
# pip install pyfunctional
# pip install shapely

import json
from pprint import pprint
from functional import seq
from shapely.geometry import shape

with open('products-v1.json') as datafile:
    data = json.load(datafile)

pprint(data)
print(len(data))


# (minx, miny, maxx, maxy)
def getBBox(geom):
    return shape(geom).bounds
    
(seq(data)
    .map(lambda p: { "id"   : p["id"],
             "title" : p["title"],
             "footprint": p["footprint"],
             "bbox": getBBox(p["footprint"]["geometry"]),
             "properties": p["properties"],
             "representations": p["representations"]
    })
.to_json('products-v1-withbbox.json'))


