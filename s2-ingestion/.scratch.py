

import os
import json
from functional import seq


def main():

    # path = os.path.join('.', 'output', '20171206-101946.txt')
    
    # with open(path) as f:
    #     lines = (seq(f)
    #         .map(lambda l: l.rstrip('\n')) # remove newline at end of line
            
    #     )
    #     print(lines.first())


    # the file is a dict of arrays
    # assert that every product array has exactly one item with a 'type' of vmsk_sharp_rad_srefdem_stdsref
    # find products which have no item object with a 'type' property of 'vmsk_sharp_rad_srefdem_stdsref'
    path = os.path.join('.', 'output', 'by_product.json')
    with open(path) as f:
        products = json.load(f)
            
    print('We found %s products' % (len(products)))

# from items in products // p is an array
# where items.Any(i => i.type == 'vmsk_sharp_rad_srefdem_stdsref')
# select p

    # x = (seq(products.values())).first()
    # print(x)

    products_without_actual_product = (seq(products.values())
        .filter(lambda items: not seq(items).exists(lambda i: i['type'] == 'vmsk_sharp_rad_srefdem_stdsref'))
    )

    #products_without_actual_product = seq(products.values()).difference(products_with_actual_product)

    # print('We found %s products with actual products' % (products_with_actual_product.len()))
    print('We found %s products without actual products' % (products_without_actual_product.len()))

    # with open(os.path.join('.', 'output', 'missing_products'), 'a') as f:
    #     f.write('%s %s\n' % (o.key, o.size))
    

main()
