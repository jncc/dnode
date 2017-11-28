
print('Hello...')

import boto3
import json
import os
import uuid

grid_dir = 'J:\GISprojects\Generated OSGB Grids'

s3 = boto3.resource('s3')
# sanity check s3 connection
bucket = s3.Bucket('scotland-gov-gi')
print('Bucket name is ' + bucket.name)

# results = bucket.objects.filter(Prefix='lidar-1/processed/DSM/gridded/27700/10000')
# for o in results:
#     print(o.key)
# exit()


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

    with open(os.path.join(grid_dir, wgs84_grid_path)) as wgs84_grid_file:
        wgs84_grid_json = json.load(wgs84_grid_file)
        for item in wgs84_grid_json['features']:
            grids[item['properties']['id']] = {'wgs84': {'geojson': item, 'bbox': get_bbox(item)}}

    with open(os.path.join(grid_dir, osgb_grid_path)) as osgb_grid_file:
        osgb_grid_json = json.load(osgb_grid_file)
        for item in osgb_grid_json['features']:
            grids[item['properties']['id']]['osgb'] = {'geojson': item, 'bbox': get_bbox(item)}
    
    return grids

def get_products(grids, s3_path, bucket, s3_region, s3_bucket, base_title, type):
    products = []

    for o in bucket.objects.filter(Prefix=s3_path): # bucket.list(prefix=s3_path):
        grid = None
        print('Processing %s ...' % (o.key))
        if (type is 'DSM' and o.key.endswith('.tif')):
            grid = os.path.basename(o.key).replace('.tif', '').replace('DSM_', '')
        elif (type is 'DTM' and o.key.endswith('.tif')):
            grid = os.path.basename(o.key).replace('.tif', '').replace('DTM_', '')
        elif (type is 'LAS' and o.key.endswith('.las')):
            grid = os.path.basename(o.key).replace('.las', '').replace('LAS_', '')
        if (grid is not None):
            products.append({
                "id": str(uuid.uuid4()),
                "title": '%s %s %s' % (base_title, type, grid),
                "footprint": grids[grid]['wgs84']['geojson'],
                "bbox": grids[grid]['wgs84']['bbox'],
                "osgbBbox": grids[grid]['osgb']['bbox'],
                "properties": {},
                "data": {
                    "download": {
                        "url": 'https://s3-%s.amazonaws.com/%s/%s' % (s3_region, s3_bucket, o.key),
                        "size": o.size,
                        "type": 'LAS' if type is 'LAS' else 'GeoTIFF'
                    }
                }
            })
    
    return products


s3_region = 'eu-west-1'
s3_bucket = 'scotland-gov-gi'

# conn = boto.s3.connect_to_region(s3_region, aws_access_key_id='xxx', aws_secret_access_key='xxx', is_secure=True)
# bucket = conn.get_bucket(s3_bucket)

phase_1_s3_dsm_path = 'lidar-1/processed/DSM/gridded/27700/10000'
phase_1_s3_dtm_path = 'lidar-1/processed/DTM/gridded/27700/10000'
phase_1_s3_las_path = 'lidar-1/raw/las/gridded/27700/1000'
phase_2_s3_dsm_path = 'lidar-2/processed/DSM/gridded/27700/10000'
phase_2_s3_dtm_path = 'lidar-2/processed/DTM/gridded/27700/10000'
phase_2_s3_las_path = 'lidar-2/raw/las/gridded/27700/5000'

print('Loading grids...')
grids = get_grids('wgs84.1k.grid.scotland.json', 'osgb.1k.grid.scotland.json')
grids5k = get_grids('wgs84.5k.grid.scotland.json', 'osgb.5k.grid.scotland.json')
grids10k = get_grids('wgs84.grid.json', 'osgb.grid.json')
print('Loaded grids!')

#exit()

with open('output.json', 'w') as output:
    collections = {'data': [
    {
        'id': 'b32e4101-6d8a-538b-9c01-a23389acfe35',
        'metadata': {
            'title': 'LiDAR for Scotland Phase I DSM',
            'abstract': 'The Scottish Public Sector LiDAR Phase I dataset was commissioned by the Scottish Government, SEPA and Scottish Water in 2011. This was commissioned in response to the Flood Risk Management Act (2009). The contract was awarded to Atkins, and the LiDAR data was collected and delivered by Blom. Airbourne LiDAR data was collected for 10 collection areas (the dataset does not have full national coverage) totalling 11,845 km2 between March 2011 and May 2012. A DTM and DSM were produced from the point clouds, with 1m spatial resolution.',
            'topicCategory': 'Orthoimagery Elevation',
            'keyword': ['Orthoimagery', 'Elevation', 'Society'],
            'resourceType': 'Dataset',
            'datasetReferenceDate': '2016-11-15',
            'lineage': 'The LiDAR data was collected from an aircraft between March 2011 and May 2012 for 10 collection areas. The point density was a minimum of 1 point/sqm, and approximately 2 points/sqm on average between the 10 collection areas. Blom delivered the raw LAS files alongside DTM and DSMs at 1m resolution in ESRI Grid and ASCII format. They also provided reports detailing the height accuracy and point density for each collection area. The results were delivered between 1st July 2011 and 2nd May 2012.',
            'responsibleOrganisation': 'Scottish Government',
            'accessLimitations': 'No limitations on public access',
            'useConstraints': 'The following attribution statement must be used to acknowledge the source of the information: Crown copyright Scottish Government, SEPA and Scottish Water (2012). Open Government Licence v3',
            'metadataDate': '2017-03-01',
            'metadataPointOfContact': 'Scottish Government, GI-SAT@gov.scot (Geographic Information Science and Analysis Team (GI-SAT), Directorate for Digital), Victoria Quay, Edinburgh, Scotland, EH6 6QQ, United Kingdom',
            'metadataLanguage': 'English',
            'spatialReferenceSystem': 'EPSG:27700'
        },
        'products': get_products(grids10k, phase_1_s3_dsm_path, bucket, s3_region, s3_bucket, 'LiDAR for Scotland Phase I', 'DSM'),
        'data': {
            'wms': {
                'name': 'scotland:scotland-lidar-1-dsm',
                'base_url': 'https://eo.jncc.gov.uk/geoserver/scotland/wms'
            }
        }
    },
    {
        'id': '572c2ead-84bc-5d27-8a2e-8fb1b35e5acc',
        'metadata': {
            'title': 'LiDAR for Scotland Phase I DTM',
            'abstract': 'The Scottish Public Sector LiDAR Phase I dataset was commissioned by the Scottish Government, SEPA and Scottish Water in 2011. This was commissioned in response to the Flood Risk Management Act (2009). The contract was awarded to Atkins, and the LiDAR data was collected and delivered by Blom. Airbourne LiDAR data was collected for 10 collection areas (the dataset does not have full national coverage) totalling 11,845 km2 between March 2011 and May 2012. A DTM and DSM were produced from the point clouds, with 1m spatial resolution.',
            'topicCategory': 'Orthoimagery Elevation',
            'keyword': ['Orthoimagery', 'Elevation', 'Society'],
            'resourceType': 'Dataset',
            'datasetReferenceDate': '2016-11-15',
            'lineage': 'The LiDAR data was collected from an aircraft between March 2011 and May 2012 for 10 collection areas. The point density was a minimum of 1 point/sqm, and approximately 2 points/sqm on average between the 10 collection areas. Blom delivered the raw LAS files alongside DTM and DSMs at 1m resolution in ESRI Grid and ASCII format. They also provided reports detailing the height accuracy and point density for each collection area. The results were delivered between 1st July 2011 and 2nd May 2012.',
            'responsibleOrganisation': 'Scottish Government',
            'accessLimitations': 'No limitations on public access',
            'useConstraints': 'The following attribution statement must be used to acknowledge the source of the information: Crown copyright Scottish Government, SEPA and Scottish Water (2012). Open Government Licence v3',
            'metadataDate': '2017-03-01',
            'metadataPointOfContact': 'Scottish Government, GI-SAT@gov.scot (Geographic Information Science and Analysis Team (GI-SAT), Directorate for Digital), Victoria Quay, Edinburgh, Scotland, EH6 6QQ, United Kingdom',
            'metadataLanguage': 'English',
            'spatialReferenceSystem': 'EPSG:27700'
        },
        'products': get_products(grids10k, phase_1_s3_dtm_path, bucket, s3_region, s3_bucket, 'LiDAR for Scotland Phase I', 'DTM'),
        'data': {
            'wms': {
                'name': 'scotland:scotland-lidar-1-dtm',
                'base_url': 'https://eo.jncc.gov.uk/geoserver/scotland/wms'
            }
        }
    },
    {
        'id': 'ddc9c05b-6060-5abb-92c4-5586ed52ad77',
        'metadata': {
            'title': 'LiDAR for Scotland Phase I LAS',
            'abstract': 'The Scottish Public Sector LiDAR Phase I dataset was commissioned by the Scottish Government, SEPA and Scottish Water in 2011. This was commissioned in response to the Flood Risk Management Act (2009). The contract was awarded to Atkins, and the LiDAR data was collected and delivered by Blom. Airbourne LiDAR data was collected for 10 collection areas (the dataset does not have full national coverage) totalling 11,845 km2 between March 2011 and May 2012. A DTM and DSM were produced from the point clouds, with 1m spatial resolution.',
            'topicCategory': 'Orthoimagery Elevation',
            'keyword': ['Orthoimagery', 'Elevation', 'Society'],
            'resourceType': 'Dataset',
            'datasetReferenceDate': '2016-11-15',
            'lineage': 'The LiDAR data was collected from an aircraft between March 2011 and May 2012 for 10 collection areas. The point density was a minimum of 1 point/sqm, and approximately 2 points/sqm on average between the 10 collection areas. Blom delivered the raw LAS files alongside DTM and DSMs at 1m resolution in ESRI Grid and ASCII format. They also provided reports detailing the height accuracy and point density for each collection area. The results were delivered between 1st July 2011 and 2nd May 2012.',
            'responsibleOrganisation': 'Scottish Government',
            'accessLimitations': 'No limitations on public access',
            'useConstraints': 'The following attribution statement must be used to acknowledge the source of the information: Crown copyright Scottish Government, SEPA and Scottish Water (2012). Open Government Licence v3',
            'metadataDate': '2017-03-01',
            'metadataPointOfContact': 'Scottish Government, GI-SAT@gov.scot (Geographic Information Science and Analysis Team (GI-SAT), Directorate for Digital), Victoria Quay, Edinburgh, Scotland, EH6 6QQ, United Kingdom',
            'metadataLanguage': 'English',
            'spatialReferenceSystem': 'EPSG:27700'
        },
        'products': get_products(grids, phase_1_s3_las_path, bucket, s3_region, s3_bucket, 'LiDAR for Scotland Phase I', 'LAS'),
        'data': {
            'wms': {
                'name': 'scotland:scotland-lidar-1-dsm',
                'base_url': 'https://eo.jncc.gov.uk/geoserver/scotland/wms'
            }
        }
    },
    {
        'id': '227b2528-0c7b-58f1-9e4e-315a1491969c',
        'metadata': {
            'title': 'LiDAR for Scotland Phase II DSM',
            'abstract': 'The Scottish Public Sector LiDAR Phase II dataset was commissioned by the Scottish Government, SEPA, 13 local authorities and sportscotland in 2012. This was commissioned in response to the Flood Risk Management Act (2009). The project was managed by Sniffer and the contract was awarded to Fugro BKS. Airbourne LiDAR data was collected for 66 sites (the dataset does not have full national coverage) totalling 3,516 km2 between 29th November 2012 and 18th April 2014. A DTM and DSM were produced from the point clouds, with 1m spatial resolution.',
            'topicCategory': 'Orthoimagery Elevation',
            'keyword': ['Orthoimagery', 'Elevation', 'Society'],
            'resourceType': 'Dataset',
            'datasetReferenceDate': '2016-11-15',
            'lineage': 'The LiDAR data was collected from an aircraft between 29th November 2012 and 18th April 2014 for 66 sites. The point density was a minimum of 1 point/sqm, and approximately 2 points/sqm on average between the 66 sites. Fugro BKS delivered the raw LAS files alongside DTM and DSMs at 1m resolution in ESRI Grid and ASCII format. They also provided reports detailing the height accuracy for each site. The results were delivered between on the 5th July 2014.',
            'responsibleOrganisation': 'Scottish Government',
            'accessLimitations': '',
            'useConstraints': 'The following attribution statement must be used to acknowledge the source of the information: Copyright Scottish Government and SEPA (2014). Fugro retain the commercial copyright. Open Government Licence v3',
            'metadataDate': '2017-03-01',
            'metadataPointOfContact': 'Scottish Government, GI-SAT@gov.scot (Geographic Information Science and Analysis Team (GI-SAT), Directorate for Digital), Victoria Quay, Edinburgh, Scotland, EH6 6QQ, United Kingdom',
            'metadataLanguage': 'English',
            'spatialReferenceSystem': 'EPSG:27700'
        },
        'products': get_products(grids10k, phase_2_s3_dsm_path, bucket, s3_region, s3_bucket, 'LiDAR for Scotland Phase II', 'DSM'),
        'data': {
            'wms': {
                'name': 'scotland:scotland-lidar-2-dsm',
                'base_url': 'https://eo.jncc.gov.uk/geoserver/scotland/wms'
            }
        }
    },        
    {
        'id': '4bbd5cc3-d879-55e0-a44d-2567697a1471',
        'metadata': {
            'title': 'LiDAR for Scotland Phase II DTM',
            'abstract': 'The Scottish Public Sector LiDAR Phase II dataset was commissioned by the Scottish Government, SEPA, 13 local authorities and sportscotland in 2012. This was commissioned in response to the Flood Risk Management Act (2009). The project was managed by Sniffer and the contract was awarded to Fugro BKS. Airbourne LiDAR data was collected for 66 sites (the dataset does not have full national coverage) totalling 3,516 km2 between 29th November 2012 and 18th April 2014. A DTM and DSM were produced from the point clouds, with 1m spatial resolution.',
            'topicCategory': 'Orthoimagery Elevation',
            'keyword': ['Orthoimagery', 'Elevation', 'Society'],
            'resourceType': 'Dataset',
            'datasetReferenceDate': '2016-11-15',
            'lineage': 'The LiDAR data was collected from an aircraft between 29th November 2012 and 18th April 2014 for 66 sites. The point density was a minimum of 1 point/sqm, and approximately 2 points/sqm on average between the 66 sites. Fugro BKS delivered the raw LAS files alongside DTM and DSMs at 1m resolution in ESRI Grid and ASCII format. They also provided reports detailing the height accuracy for each site. The results were delivered between on the 5th July 2014.',
            'responsibleOrganisation': 'Scottish Government',
            'accessLimitations': '',
            'useConstraints': 'The following attribution statement must be used to acknowledge the source of the information: Copyright Scottish Government and SEPA (2014). Fugro retain the commercial copyright. Open Government Licence v3',
            'metadataDate': '2017-03-01',
            'metadataPointOfContact': 'Scottish Government, GI-SAT@gov.scot (Geographic Information Science and Analysis Team (GI-SAT), Directorate for Digital), Victoria Quay, Edinburgh, Scotland, EH6 6QQ, United Kingdom',
            'metadataLanguage': 'English',
            'spatialReferenceSystem': 'EPSG:27700'
        },
        'products': get_products(grids10k, phase_2_s3_dtm_path, bucket, s3_region, s3_bucket, 'LiDAR for Scotland Phase II', 'DTM'),
        'data': {
            'wms': {
                'name': 'scotland:scotland-lidar-2-dtm',
                'base_url': 'https://eo.jncc.gov.uk/geoserver/scotland/wms'
            }
        }
    },
    {
        'id': 'a4b6e778-0fc6-5fe6-9c70-9721ad9a1ff8',
        'metadata': {
            'title': 'LiDAR for Scotland Phase II LAS',
            'abstract': 'The Scottish Public Sector LiDAR Phase II dataset was commissioned by the Scottish Government, SEPA, 13 local authorities and sportscotland in 2012. This was commissioned in response to the Flood Risk Management Act (2009). The project was managed by Sniffer and the contract was awarded to Fugro BKS. Airbourne LiDAR data was collected for 66 sites (the dataset does not have full national coverage) totalling 3,516 km2 between 29th November 2012 and 18th April 2014. A DTM and DSM were produced from the point clouds, with 1m spatial resolution.',
            'topicCategory': 'Orthoimagery Elevation',
            'keyword': ['Orthoimagery', 'Elevation', 'Society'],
            'resourceType': 'Dataset',
            'datasetReferenceDate': '2016-11-15',
            'lineage': 'The LiDAR data was collected from an aircraft between 29th November 2012 and 18th April 2014 for 66 sites. The point density was a minimum of 1 point/sqm, and approximately 2 points/sqm on average between the 66 sites. Fugro BKS delivered the raw LAS files alongside DTM and DSMs at 1m resolution in ESRI Grid and ASCII format. They also provided reports detailing the height accuracy for each site. The results were delivered between on the 5th July 2014.',
            'responsibleOrganisation': 'Scottish Government',
            'accessLimitations': '',
            'useConstraints': 'The following attribution statement must be used to acknowledge the source of the information: Copyright Scottish Government and SEPA (2014). Fugro retain the commercial copyright. Non-Commercial Government Licence',
            'metadataDate': '2017-03-01',
            'metadataPointOfContact': 'Scottish Government, GI-SAT@gov.scot (Geographic Information Science and Analysis Team (GI-SAT), Directorate for Digital), Victoria Quay, Edinburgh, Scotland, EH6 6QQ, United Kingdom',
            'metadataLanguage': 'English',
            'spatialReferenceSystem': 'EPSG:27700'
        },
        'products': get_products(grids5k, phase_2_s3_las_path, bucket, s3_region, s3_bucket, 'LiDAR for Scotland Phase II', 'LAS'),
        'data': {
            'wms': {
                'name': 'scotland:scotland-lidar-2-dsm',
                'base_url': 'https://eo.jncc.gov.uk/geoserver/scotland/wms'
            }
        }
    }]}
    json.dump(collections, output)

