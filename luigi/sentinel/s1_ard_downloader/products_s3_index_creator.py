import json
import boto
import yaml
import os
import time
import uuid
import shutil
import re

from helpers import s3 as s3Helper
from helpers import stat as statHelper

class ProductIndexCreator:
    def __init__(self, config, logger, tempdir, years):
        self.config = config
        self.logger = logger
        self.temp = tempdir
        self.debug = self.config.get('debug')
        self.s3_conf = self.config.get('s3')
        self.years = [2015, 2017]
    
    def getS3Contents(self, remote_path):
        amazon_key_Id = self.s3_conf['access_key']
        amazon_key_secret = self.s3_conf['secret_access_key']

        conn = boto.s3.connect_to_region(self.s3_conf['region'], aws_access_key_id=amazon_key_Id, aws_secret_access_key=amazon_key_secret, is_secure=True)
        bucket = conn.get_bucket(self.s3_conf['bucket'])

        exp = re.compile('(%s\/20[0-9]{2}\/[0-9]{2}\/(.*\.SAFE\.data))(.*)' % re.escape(remote_path))
        osni_exp = re.compile('(%s\/20[0-9]{2}\/[0-9]{2}\/.*\.SAFE\.data)\/OSNI1952\/(.*)' % re.escape(remote_path))

        year_groups = {}

        for year in self.years:
            year_groups[year] = {}
            year_prefix = os.path.join(remote_path, str(year))
            for month in range(1,13):
                prefix = os.path.join(year_prefix, "%02d" % (month))
                group = {}
                for key in bucket.list(prefix=prefix):
                    res = exp.findall(key.key)
                    osni_res = osni_exp.match(key.key)
                    if len(res) > 0:
                        name = res[0][1]
                        if name not in group:
                            group[name] = {
                                'path': res[0][0],
                                'osni': False
                            }
                        if osni_res is not None:
                            group[name]['osni'] = True
                            if key.key.endswith('tif'):
                                group[name]['osni_size'] = statHelper.human_size(key.size)
                        elif key.key.endswith('tif'):
                            group[name]['osgb_size'] = statHelper.human_size(key.size)
                
                if len(group.keys()) > 0:
                    year_groups[year][month] = group

        outputs = {}

        for year in self.years:
            outputs[year] = {}
            for month in outputs[year].keys():
                html_str = ''
                for name in sorted(list(year_groups[year][month].keys())):
                    base_url = 'https://s3-%s.amazonaws.com/%s' % (self.s3_conf['region'], self.s3_conf['bucket'])
                    img_url = '%s/%s/%s' % (base_url, name, name.replace('.SAFE.data', '_quicklook.jpg'))
                    data_url = '%s/%s/%s' % (base_url, name, name.replace('.SAFE.data', '.tif'))
                    data_size = year_groups[year][month][name]['osgb_size']
                    metadata_url = '%s/%s/%s' % (base_url, name, name.replace('.SAFE.data', '_metadata.xml'))
                    footprint_url = '%s/%s/Footprint/%s' % (base_url, name, name.replace('.SAFE.data', '_footprint.geojson'))

                    new_line = '''<tr>
                            <td>
                                <img src="%s" height=100px width=100px />
                            </td>
                            <td>
                                <h3>%s</h3><br/>
                                <a href="%s">Data [GeoTIFF] (%s)</a><br/>
                                <a href="%s">Metadata [XML]</a><br/>
                                <a href="%s">Footprint [GeoJSON]</a>                                
                            </td>
                        </tr>''' % (img_url, name, data_url, data_size, metadata_url, footprint_url)
                    html_str = '%s%s' % (html_str, new_line)
        
                    if year_groups[year][month][name]['osni']:
                        img_url = '%s/%s/OSNI1952/%s' % (base_url, name, name.replace('.SAFE.data', '_OSNI1952_quicklook.jpg'))
                        data_url = '%s/%s/OSNI1952/%s' % (base_url, name, name.replace('.SAFE.data', '_OSNI1952.tif'))
                        data_size = year_groups[year][month][name]['osni_size']
                        metadata_url = '%s/%s/OSNI1952/%s' % (base_url, name, name.replace('.SAFE.data', '_OSNI1952_metadata.xml'))
                        footprint_url = '%s/%s/OSNI1952/Footprint/%s' % (base_url, name, name.replace('.SAFE.data', '_OSNI1952_footprint.geojson'))

                        new_line = '''<tr>
                                <td>
                                    <img src="%s" height=100px width=100px />
                                </td>
                                <td>
                                    <h3>%s [OSNI]</h3><br/>
                                    <a href="%s">Data [GeoTIFF] (%s)</a><br/>
                                    <a href="%s">Metadata [XML]</a><br/>
                                    <a href="%s">Footprint [GeoJSON]</a>                                
                                </td>
                            </tr>''' % (img_url, name, data_url, data_size, metadata_url, footprint_url)
                        html_str = '%s%s' % (html_str, new_line)
                with open(os.path.join(self.temp, '%s-%s.html' % (year, month)), 'w') as out:
                    out.write('<table>%s</table>' % html_str)

if __name__ == '__main__':
    import logging
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger('products_inventory_main')
    logger.setLevel(logging.DEBUG)

    with open('config.yaml', 'r') as config:
        checker = ProductIndexCreator(yaml.load(config), logger, './temp', [2015, 2017])
        checker.getS3Contents('sentinel-1/ard/backscatter')
