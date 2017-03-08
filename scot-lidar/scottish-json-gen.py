import boto
import json
import os
import uuid

def get_bbox(item):
    minx = item['geometry']['coordinates'][0][0][0]
    miny = item['geometry']['coordinates'][0][0][1]
    maxx = item['geometry']['coordinates'][0][0][0]
    maxy = item['geometry']['coordinates'][0][0][1]

    for coord in item['geometry']['coordinates'][0]:
        if minx > coord[0]:
            minx = coord[0]
        if miny > coord[1]:
            miny = coord[1]
        if maxx < coord[0]:
            maxx = coord[0]
        if maxy < coord[1]:
            maxy = coord[1]
    return [minx, miny, maxx, maxy]

def get_products(wgs84_grid_path, osgb_grid_path, s3_path, bucket, s3_region, s3_bucket):
    grids = {}

    with open(wgs84_grid_path) as wgs84_grid_file:
        wgs84_grid_json = json.load(wgs84_grid_file)
        for item in wgs84_grid_json['features']:
            grids[item['properties']['id']] = {'wgs84': {'geojson': item, 'bbox': get_bbox(item)}}

    with open(osgb_grid_path) as osgb_grid_file:
        osgb_grid_json = json.load(osgb_grid_file)
        for item in osgb_grid_json['features']:
            grids[item['properties']['id']]['osgb'] = {'geojson': item, 'bbox': get_bbox(item)}

    products = []

    for key in bucket.list(prefix=s3_path):
        if (key.key.endswith('.tif')):
            print(key.key)
            (product, grid) = os.path.basename(key.key).replace('.tif', '').split('_')
            products.append({
                "id": str(uuid.uuid4()),
                "title": 'Scotland Lidar-1 %s %s' % (product, grid),
                "footprint": grids[grid]['wgs84']['geojson'],
                "bbox": grids[grid]['wgs84']['bbox'],
                "osgbBbox": grids[grid]['osgb']['bbox'],
                "properties": {},
                "representations": {
                    "download": {
                        "url": 'https://s3-%s.amazonaws.com/%s/%s' % (s3_region, s3_bucket, key.key),
                        "size": key.size,
                        "type": 'GeoTIFF'
                    }
                }
            })
    
    return products

if __name__ == '__main__':
    conn = boto.s3.connect_to_region('s3_region', aws_access_key_id='xxx', aws_secret_access_key='xxx', is_secure=True)
    bucket = conn.get_bucket('s3_bucket')

    with open('output.json', 'w') as output:
        json.dump(get_products('wgs84.grid.json', 'osgb.grid.json', 's3-data-path/', bucket, 's3_region', 's3_bucket'), output)
    