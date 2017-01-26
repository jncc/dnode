

import json
from pprint import pprint
from functional import seq

with open('products-v1.json') as datafile:
    data = json.load(datafile)

pprint(data)
print(len(data))

(seq(data)
    .map(lambda p: { blah: p["id"] })
.to_json('products-v1-withbbox.json'))


