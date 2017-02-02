import json
import boto
import yaml
import psycopg2
import os
import time
import uuid
import shutil
import re

from helpers import footprint as footprintHelper
from helpers import metadata as metadataHelper
from helpers import verification as verificationHelper
from helpers import s3 as s3Helper
from helpers import database as databaseHelper

class ProductInventoryChecker:
    def __init__(self, config, logger, tempdir):
        self.config = config
        self.logger = logger
        self.temp = tempdir
        self.debug = self.config.get('debug')
        self.s3_conf = self.config.get('s3')
        self.database_conf = self.config.get('database')
        self.db_conn = psycopg2.connect(host=self.database_conf['host'], dbname=self.database_conf['dbname'], user=self.database_conf['username'], password=self.database_conf['password']) 
    
    def getS3Contents(self, path):
        amazon_key_Id = self.s3_conf['access_key']
        amazon_key_secret = self.s3_conf['secret_access_key']

        conn = boto.s3.connect_to_region(self.s3_conf['region'], aws_access_key_id=amazon_key_Id, aws_secret_access_key=amazon_key_secret, is_secure=True)
        bucket = conn.get_bucket(self.s3_conf['bucket'])  

        keys = bucket.get_all_keys(prefix=path)

        exp = re.compile('%s\/(20[0-9]{2}\/[0-9]{2}\/.*\.SAFE\.data)(.*)' % re.escape(path))
        groups = {}

        for key in keys:
            res = exp.findall(key.key)[0]
            if len(res) > 0:
                name = res[0][0]
                if name in groups:
                    groups[name].append(key)
                else:
                    groups[name] = [key]
        
        osni_exp = exp = re.compile('%s\/(20[0-9]{2}\/[0-9]{2}\/.*\.SAFE\.data)\/OSNI1952\/(.*)' % re.escape(path))

        for key in groups.keys():
            fkeys = groups[key]
            representations = {'s3': []}
            osni_representations = {'s3': []}

            (footprint_osgb, footprint_osni) = (None, None)
            (metadata_osgb, metadata_osni) = (None, None)
            
            found_data = {
                'osgb': {
                    'data': False,
                    'metadata': False,
                    'quicklook': False,
                    'footprint': False
                },
                'osni': {
                    'data': False,
                    'metadata': False,
                    'quicklook': False,
                    'footprint': False
                }                
            }

            for fkey in fkeys:
                if osni_exp.match(fkey.key) is not None:
                    # Process OSNI data
                    if fkey.key.endswith('.shp') or fkey.key.endswith('.shx') or fkey.key.endswith('prj') or fkey.key.endswith('dbf'):
                        # Deal with shapefiles (move to Footprint folder or delete?)
                        x = 1
                    elif fkey.key.endswith('.json'):
                        # Deal with geojson file (move to Footprint folder and rename to .geojson)
                        footprint_osni = json.loads(fkey.get_contents_as_string())
                        footprint_osni['crs'] = { "type": "name", "properties": { "name": "urn:ogc:def:crs:EPSG::4326" } }
                        found_data['osni']['footprint'] = True
                    elif fkey.key.endswith('_metadata.xml'):
                        # Deal with metadata file
                        with open(os.path.join(self.temp, 'metadata_osni.xml'), 'wb') as metadata:
                            fkey.get_contents_to_file(metadata)
                        with open(os.path.join(self.temp, 'metadata_osni.xml'), 'r') as metadata:
                            metadata_osni = metadataHelper.xml_to_json(metadata)
                            found_data['osni']['metadata'] = True
                    elif fkey.key.endswith('.tif'):
                        # Found data file
                        found_data['osni']['data'] = True
                    elif fkey.key.endswith('_quicklook.jpg'):
                        # Found quicklook
                        found_data['osni']['quicklook'] = True                            

                    # Extract represenation of the file
                    osni_representations['s3'].append(s3Helper.get_representation(self.s3_conf['bucket'], self.s3_conf['region'], fkey.key, s3Helper.get_file_type(os.path.splitext(fkey.key)[1])))
                else:
                    # Process OSGB data
                    if fkey.key.endswith('.json'):
                        # Deal with geojson file (rename to .geojson)
                        footprint_osgb = json.loads(fkey.get_contents_as_string())
                        footprint_osgb['crs'] = { "type": "name", "properties": { "name": "urn:ogc:def:crs:EPSG::4326" } }
                        found_data['osgb']['footprint'] = True
                    elif fkey.key.endswith('_metadata.xml'):
                        # Deal with metadata file
                        with open(os.path.join(self.temp, 'metadata_osgb.xml'), 'wb') as metadata:
                            fkey.get_contents_to_file(metadata)
                        with open(os.path.join(self.temp, 'metadata_osgb.xml'), 'r') as metadata:                            
                            metadata_osgb = metadataHelper.xml_to_json(metadata)                        
                            found_data['osgb']['metadata'] = True
                    elif fkey.key.endswith('.tif'):
                        # Found data file
                        found_data['osgb']['data'] = True
                    elif fkey.key.endswith('_quicklook.jpg'):
                        # Found quicklook
                        found_data['osgb']['quicklook'] = True
                    
                    # Extract represenation of the file
                    representations['s3'].append(s3Helper.get_representation(self.s3_conf['bucket'], self.s3_conf['region'], fkey.key, s3Helper.get_file_type(os.path.splitext(fkey.key)[1])))
                          # Deal with OSGB product

            # Deal with OSGB product
            if found_data['osgb']['data'] and found_data['osgb']['metadata'] and found_data['osgb']['quicklook'] and found_data['osgb']['footprint']:
                databaseHelper.write_progress_to_database(self.db_conn, self.database_conf['collection_version_uuid'], {'s3imported':True}, metadata_osgb, representations, footprint_osgb)

            if found_data['osni']['data'] and found_data['osni']['metadata'] and found_data['osni']['quicklook'] and found_data['osni']['footprint']:
               databaseHelper.write_progress_to_database(self.db_conn, self.database_conf['collection_version_uuid'], {'s3imported':True}, metadata_osni, osni_representations, footprint_osni, additional={'relatedTo': metadata_osgb['ID']})

if __name__ == '__main__':
    import logging
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger('products_inventory_main')
    logger.setLevel(logging.DEBUG)

    with open('config.yaml', 'r') as config:
            checker = ProductInventoryChecker(yaml.load(config), logger, './temp')
            checker.getS3Contents()
