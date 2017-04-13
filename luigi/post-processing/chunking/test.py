from osgeo import gdal, ogr, osr
import argparse, logging, os, subprocess, time, sys
import yaml
import shutil
import re
import boto

from helpers import s3 as s3Helper

class product_chunking_simple_s3:
    def __init__(self, config, logger):
        self.temp = config.get('working_dir')
        self.logger = logger

        self.s3_access_key = config.get('s3')['access_key']
        self.s3_secret_access_key = config.get('s3')['secret_access_key']
        self.s3_bucket = config.get('s3')['bucket']
        self.s3_region = config.get('s3')['region']

        self.s3_path = config.get('s3')['path']
        self.s3_public = config.get('s3')['public']

        self.bands = config.get('bands')
        self.lco = config.get('lco')
        self.addo = config.get('addo')

        self.grids = config.get('grids')

        gdal.AllRegister()   
        
    def iterate_products(self):
        output_file_path = os.path.join(self.temp, 'test.txt')
        final_key_path = os.path.join(self.s3_path, 'test.txt')
        with open(output_file_path, 'w') as test:
            test.write('test')
        
        self.logger.info('Uploading to %s' % final_key_path)
        s3Helper.s3().copy_file_to_s3(self.s3_access_key, self.s3_secret_access_key, self.s3_region, self.s3_bucket, output_file_path, final_key_path, self.s3_public, None)        
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cuts a provided Raster image by a provided vector dataset, generally a grid, but can be any data')
    parser.add_argument('-c', '--config', type=str, required=True, help='Path to config.yaml file')
    
    args = parser.parse_args()
    
    with open(args.config, 'r') as conf:
        config = yaml.load(conf)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    logger = logging.getLogger('s3-test-script')
    logger.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)

    fh = logging.FileHandler(os.path.join(config.get('log_dir'), 'product-chunking-simple-s3-%s.log' % time.strftime('%y%m%d-%H%M%S')))
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)

    logger.addHandler(ch)
    logger.addHandler(fh)    

    chunker = product_chunking_simple_s3(config, logger)
    chunker.iterate_products()
