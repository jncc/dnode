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

def get_grids(wgs84_grid_path, osgb_grid_path):
    grids = {}

    with open(wgs84_grid_path) as wgs84_grid_file:
        wgs84_grid_json = json.load(wgs84_grid_file)
        for item in wgs84_grid_json['features']:
            grids[item['properties']['id']] = {'wgs84': {'geojson': item, 'bbox': get_bbox(item)}}

    with open(osgb_grid_path) as osgb_grid_file:
        osgb_grid_json = json.load(osgb_grid_file)
        for item in osgb_grid_json['features']:
            grids[item['properties']['id']]['osgb'] = {'geojson': item, 'bbox': get_bbox(item)}
    
    return grids

def get_products(grids, s3_path, bucket, s3_region, s3_bucket):
    products = []

    for key in bucket.list(prefix=s3_path):
        if (key.key.endswith('.tif')):
            print(key.key)
            (product, grid) = os.path.basename(key.key).replace('.tif', '').split('_')
            products.append({
                "id": str(uuid.uuid4()),
                "title": 'LiDAR for Scotland Phase I %s %s' % (product, grid),
                "footprint": grids[grid]['wgs84']['geojson'],
                "bbox": grids[grid]['wgs84']['bbox'],
                "osgbBbox": grids[grid]['osgb']['bbox'],
                "properties": {},
                "data": {
                    "download": {
                        "url": 'https://s3-%s.amazonaws.com/%s/%s' % (s3_region, s3_bucket, key.key),
                        "size": key.size,
                        "type": 'GeoTIFF'
                    }
                }
            })
    
    return products

if __name__ == '__main__':
    s3_region = 'region-name'
    s3_bucket = 'bucket-name'

    conn = boto.s3.connect_to_region(s3_region, aws_access_key_id='xxx', aws_secret_access_key='xxx', is_secure=True)
    bucket = conn.get_bucket(s3_bucket)

    dsm_s3_path = 'gridded-dsm-path'
    dsm_s3_key = bucket.get_key('full-dsm-path')
    dtm_s3_path = 'gridded-dtm-path'
    dtm_s3_key = bucket.get_key('full-dtm-path')

    grids = get_grids('wgs84.grid.json', 'osgb.grid.json')

    with open('output.json', 'w') as output:
        collections = [{
            'id': str(uuid.uuid4()),
            'metadata': {
                'title': 'LiDAR for Scotland Phase I DSM',
                'abstract': 'The Scottish Public Sector LiDAR Phase I dataset was commissioned by the Scottish Government, SEPA and Scottish Water in 2011. This was commissioned in response to the Flood Risk Management Act (2009). The contract was awarded to Atkins, and the LiDAR data was collected and delivered by Blom. Airbourne LiDAR data was collected for 10 collection areas totalling 11,845 km2 between March 2011 and May 2012. A DTM and DSM were produced from the point clouds, with 1m spatial resolution. The data is licenced under an Open Government Licence.',
                'topicCategory': 'Orthoimagery Elevation',
                'keyword': ['Orthoimagery', 'Elevation', 'Society'],
                'resourceType': 'Dataset',
                'datasetReferenceDate': '2016-11-15',
                'lineage': 'The LiDAR data was collected from an aircraft between March 2011 and May 2012 for 10 collection areas. The point density was a minimum of 1 point/sqm, and approximately 2 points/sqm on average between the 10 collection areas. Blom delivered the raw LAS files alongside DTM and DSMs at 1m resolution in ESRI Grid and ASCII format. They also provided reports detailing the height accuracy and point density for each collection area. The results were delivered between 1st July 2011 and 2nd May 2012.',
                'responsibleOrganisation': 'Scottish Government',
                'accessLimitations': '',
                'useConstraints': 'Open Government Licence Version 3',
                'metadataDate': '2017-03-01',
                'metadataPointOfContact': 'Scottish Government, GI-SAT@gov.scot (Geographic Information Science and Analysis Team (GI-SAT), Directorate for Digital), Victoria Quay, Edinburgh, Scotland, EH6 6QQ, United Kingdom',
                'metadataLanguage': '"English',
                'spatialReferenceSystem': 'EPSG:27700'
            },
            'products': get_products(grids, dsm_s3_path, bucket, s3_region, s3_bucket),
            'data': {
                'download': {
                    'url': 'https://s3-%s.amazonaws.com/%s/%s' % (s3_region, s3_bucket, dsm_s3_key.key),
                    'size': dsm_s3_key.size,
                    'type': 'GeoTIFF'
                },
                'wms': {
                    'name': 'scotland:scotland-lidar-1-dsm',
                    'base_url': 'geoserver-url'
                }
            }
        },
        {
            'id': str(uuid.uuid4()),
            'metadata': {
                'title': 'LiDAR for Scotland Phase I DTM',
                'abstract': 'The Scottish Public Sector LiDAR Phase I dataset was commissioned by the Scottish Government, SEPA and Scottish Water in 2011. This was commissioned in response to the Flood Risk Management Act (2009). The contract was awarded to Atkins, and the LiDAR data was collected and delivered by Blom. Airbourne LiDAR data was collected for 10 collection areas totalling 11,845 km2 between March 2011 and May 2012. A DTM and DSM were produced from the point clouds, with 1m spatial resolution. The data is licenced under an Open Government Licence.',
                'topicCategory': 'Orthoimagery Elevation',
                'keyword': ['Orthoimagery', 'Elevation', 'Society'],
                'resourceType': 'Dataset',
                'datasetReferenceDate': '2016-11-15',
                'lineage': 'The LiDAR data was collected from an aircraft between March 2011 and May 2012 for 10 collection areas. The point density was a minimum of 1 point/sqm, and approximately 2 points/sqm on average between the 10 collection areas. Blom delivered the raw LAS files alongside DTM and DSMs at 1m resolution in ESRI Grid and ASCII format. They also provided reports detailing the height accuracy and point density for each collection area. The results were delivered between 1st July 2011 and 2nd May 2012.',
                'responsibleOrganisation': 'Scottish Government',
                'accessLimitations': '',
                'useConstraints': 'Open Government Licence Version 3',
                'metadataDate': '2017-03-01',
                'metadataPointOfContact': 'Scottish Government, GI-SAT@gov.scot (Geographic Information Science and Analysis Team (GI-SAT), Directorate for Digital), Victoria Quay, Edinburgh, Scotland, EH6 6QQ, United Kingdom',
                'metadataLanguage': '"English',
                'spatialReferenceSystem': 'EPSG:27700'
            },
            'products': get_products(grids, dtm_s3_path, bucket, s3_region, s3_bucket),
            'data': {
                'download': {
                    'url': 'https://s3-%s.amazonaws.com/%s/%s' % (s3_region, s3_bucket, dtm_s3_key.key),
                    'size': dtm_s3_key.size,
                    'type': 'GeoTIFF'
                },
                'wms': {
                    'name': 'scotland:scotland-lidar-1-dtm',
                    'base_url': 'geoserver-url'
                }
            }
        }]
        json.dump(collections, output)
    
