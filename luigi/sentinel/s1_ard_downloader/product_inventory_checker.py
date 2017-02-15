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
    
    def getS3Contents(self, remote_path, remote_extra_path=None):
        amazon_key_Id = self.s3_conf['access_key']
        amazon_key_secret = self.s3_conf['secret_access_key']

        conn = boto.s3.connect_to_region(self.s3_conf['region'], aws_access_key_id=amazon_key_Id, aws_secret_access_key=amazon_key_secret, is_secure=True)
        bucket = conn.get_bucket(self.s3_conf['bucket'])

        if remote_extra_path is not None:
            prefix = os.path.join(remote_path, remote_extra_path)
        else: 
            prefix = remote_path

        exp = re.compile('(%s\/20[0-9]{2}\/[0-9]{2}\/(.*\.SAFE\.data))(.*)' % re.escape(remote_path))
        groups = {}

        for key in bucket.list(prefix=prefix):
            res = exp.findall(key.key)
            if len(res) > 0:
                name = res[0][1]
                if name in groups:
                    groups[name]['keys'].append(key)
                else:
                    groups[name] = {
                        'path': res[0][0],
                        'keys': [key]
                    }
        
        osni_exp = exp = re.compile('(%s\/20[0-9]{2}\/[0-9]{2}\/.*\.SAFE\.data)\/OSNI1952\/(.*)' % re.escape(remote_path))

        for key in groups.keys():
            fkeys = groups[key]['keys']
            path = groups[key]['path']

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
                    if fkey.key.endswith('.json'):
                        # Deal with geojson file (move to Footprint folder and rename to .geojson)
                        footprint_osni = json.loads(fkey.get_contents_as_string().decode('utf-8'))
                        footprint_osni['crs'] = { "type": "name", "properties": { "name": "urn:ogc:def:crs:EPSG::4326" } }
                        found_data['osni']['footprint'] = True

                        # Write out temporary file for now with corrected header
                        with open(os.path.join(self.temp, 'footprint_osni.geojson'), 'w') as osni_footprint_file:
                            json.dump(footprint_osni, osni_footprint_file)
                        # Upload to correct location
                        dest_remote_path = os.path.join(path, os.path.join('OSNI1952', os.path.join('Footprint', key.replace('.SAFE.data', '_OSNI1952_footprint.geojson'))))
                        s3Helper.copy_file_to_s3(self.logger, amazon_key_Id, amazon_key_secret, self.s3_conf['region'], bucket, '', os.path.join(self.temp, 'footprint_osni.geojson'), dest_remote_path, True, None)
                        osni_representations['s3'].append(s3Helper.get_representation(self.s3_conf['bucket'], self.s3_conf['region'], os.path.join('/%s' % remote_path, dest_remote_path), s3Helper.get_file_type('.geojson')))
                    elif fkey.key.endswith('_metadata.xml'):
                        # Deal with metadata file
                        with open(os.path.join(self.temp, 'metadata_osni.xml'), 'wb') as metadata:
                            fkey.get_contents_to_file(metadata)
                        with open(os.path.join(self.temp, 'metadata_osni.xml'), 'r') as metadata:
                            metadata_osni = metadataHelper.xml_to_json(metadata)
                            found_data['osni']['metadata'] = True
                        osni_representations['s3'].append(s3Helper.get_representation(self.s3_conf['bucket'], self.s3_conf['region'], '/%s' % fkey.key, s3Helper.get_file_type(os.path.splitext(fkey.key)[1])))
                    elif fkey.key.endswith('.tif'):
                        # Found data file
                        found_data['osni']['data'] = True
                        osni_representations['s3'].append(s3Helper.get_representation(self.s3_conf['bucket'], self.s3_conf['region'], '/%s' % fkey.key, s3Helper.get_file_type(os.path.splitext(fkey.key)[1])))
                    elif fkey.key.endswith('_quicklook.jpg'):
                        # Found quicklook
                        found_data['osni']['quicklook'] = True
                        osni_representations['s3'].append(s3Helper.get_representation(self.s3_conf['bucket'], self.s3_conf['region'], '/%s' % fkey.key, s3Helper.get_file_type(os.path.splitext(fkey.key)[1])))
                else:
                    # Process OSGB data
                    if fkey.key.endswith('.json'):
                        # Deal with geojson file (rename to .geojson)
                        footprint_osgb = json.loads(fkey.get_contents_as_string().decode('utf-8'))
                        footprint_osgb['crs'] = { "type": "name", "properties": { "name": "urn:ogc:def:crs:EPSG::4326" } }
                        found_data['osgb']['footprint'] = True
                        # Write out temporary file for now with corrected header
                        with open(os.path.join(self.temp, 'footprint_osgb.geojson'), 'w') as osgb_footprint_file:
                            json.dump(footprint_osgb, osgb_footprint_file)
                        # Upload to correct location
                        dest_remote_path = os.path.join(path, os.path.join('Footprint',key.replace('.SAFE.data', '_footprint.geojson')))
                        s3Helper.copy_file_to_s3(self.logger, amazon_key_Id, amazon_key_secret, self.s3_conf['region'], bucket, '', os.path.join(self.temp, 'footprint_osgb.geojson'), dest_remote_path, True, None)      
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

            # Deal with OSGB product
            if found_data['osgb']['data'] and found_data['osgb']['metadata'] and found_data['osgb']['quicklook'] and found_data['osgb']['footprint']:
                representations = {'s3': [
                    s3Helper.get_representation(self.s3_conf['bucket'], self.s3_conf['region'], os.path.join(path, os.path.join('Footprint', key.replace('.SAFE.data', '_footprint.geojson')), s3Helper.get_file_type('.geojson')),
                    s3Helper.get_representation(self.s3_conf['bucket'], self.s3_conf['region'], os.path.join(path, key.replace('.SAFE.data', '_metadata.xml')), s3Helper.get_file_type('.xml'),
                    s3Helper.get_representation(self.s3_conf['bucket'], self.s3_conf['region'], os.path.join(path, key.replace('.SAFE.data', '_quicklook.jpg')), s3Helper.get_file_type('.jpg'),
                    s3Helper.get_representation(self.s3_conf['bucket'], self.s3_conf['region'], os.path.join(path, key.replace('.SAFE.data', '.tif')), s3Helper.get_file_type('.tif')
                ]}
                databaseHelper.write_progress_to_database(self.db_conn, self.database_conf['collection_version_uuid'], {'s3imported':True, 'filename':key}, metadata_osgb, representations, footprint_osgb, None)
                # Do Cleanup
            
            # Deal with OSNI product
            if found_data['osni']['data'] and found_data['osni']['metadata'] and found_data['osni']['quicklook'] and found_data['osni']['footprint']:
                osni_path = os.path.join(path, 'OSNI1952')
                representations = {'s3': [
                    s3Helper.get_representation(self.s3_conf['bucket'], self.s3_conf['region'], os.path.join(osni_path, os.path.join('Footprint', key.replace('.SAFE.data', '_footprint.geojson')), s3Helper.get_file_type('.geojson')),
                    s3Helper.get_representation(self.s3_conf['bucket'], self.s3_conf['region'], os.path.join(osni_path, key.replace('.SAFE.data', '_metadata.xml')), s3Helper.get_file_type('.xml'),
                    s3Helper.get_representation(self.s3_conf['bucket'], self.s3_conf['region'], os.path.join(osni_path, key.replace('.SAFE.data', '_quicklook.jpg')), s3Helper.get_file_type('.jpg'),
                    s3Helper.get_representation(self.s3_conf['bucket'], self.s3_conf['region'], os.path.join(osni_path, key.replace('.SAFE.data', '.tif')), s3Helper.get_file_type('.tif')
                ]}

               databaseHelper.write_progress_to_database(self.db_conn, self.database_conf['collection_version_uuid'], {'s3imported':True, 'filename':key}, metadata_osni, osni_representations, footprint_osni, {'relatedTo': metadata_osgb['ID']})
               # Do Cleanup

    def cleanupPath(self, path, name, bucket, source_bucket_name, osni):
        if osni:
            bucket.copy_key(os.path.join(path, 'Extras/Footprint/%s.json' % name), source_bucket_name, os.path.join(path, '%s.json' % name))
            bucket.copy_key(os.path.join(path, 'Extras/Footprint/%s.dbf' % name), source_bucket_name, os.path.join(path, '%s.dbf' % name))
            bucket.copy_key(os.path.join(path, 'Extras/Footprint/%s.shp' % name), source_bucket_name, os.path.join(path, '%s.shp' % name))
            bucket.copy_key(os.path.join(path, 'Extras/Footprint/%s.shx' % name), source_bucket_name, os.path.join(path, '%s.shx' % name))
            bucket.copy_key(os.path.join(path, 'Extras/Footprint/%s.prj' % name), source_bucket_name, os.path.join(path, '%s.prj' % name))           
            
        else: 
            bucket.copy_key(os.path.join(path, 'Extras/Footprint/%s.json' % name), source_bucket_name, os.path.join(path, 'Footprint/%s.json' % name))
            bucket.copy_key(os.path.join(path, 'Extras/Footprint/%s.dbf' % name), source_bucket_name, os.path.join(path, 'Footprint/%s.dbf' % name))
            bucket.copy_key(os.path.join(path, 'Extras/Footprint/%s.shp' % name), source_bucket_name, os.path.join(path, 'Footprint/%s.shp' % name))
            bucket.copy_key(os.path.join(path, 'Extras/Footprint/%s.shx' % name), source_bucket_name, os.path.join(path, 'Footprint/%s.shx' % name))
            bucket.copy_key(os.path.join(path, 'Extras/Footprint/%s.prj' % name), source_bucket_name, os.path.join(path, 'Footprint/%s.prj' % name))            

        bucket.copy_key(os.path.join(path, 'Extras/%s_quicklook.jpg.aux.xml' % name), source_bucket_name, os.path.join(path, '%s_quicklook.jpg.aux.xml' % name))


if __name__ == '__main__':
    import logging
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger('products_inventory_main')
    logger.setLevel(logging.DEBUG)

    with open('config.yaml', 'r') as config:
            checker = ProductInventoryChecker(yaml.load(config), logger, './temp')
            checker.getS3Contents('')
