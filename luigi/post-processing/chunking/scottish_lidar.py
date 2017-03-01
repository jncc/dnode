import re
import yaml
import argparse
import logging
import os
import time
import subprocess

from osgeo import gdal
from helpers import s3 as s3Helper

class scottish_lidar_s3_uploader:
    def __init__(self, config, logger):
        self.temp = config.get('working_dir')
        self.logger = logger

        self.s3_access_key = config.get('s3')['access_key']
        self.s3_secret_access_key = config.get('s3')['secret_access_key']
        self.s3_bucket = config.get('s3')['bucket']
        self.s3_region = config.get('s3')['region']

        self.s3_path = config.get('s3')['path']
        self.s3_public = config.get('s3')['public']
        self.lco = config.get('lco')

        self.exp = re.compile('^([A-H,J-Z]{2})([0-9]){1}[0,5]{1}([0-9]{1})[0,5]_(D[S,T]M)\.ASC$')
        self.exp_alt = re.compile('^([A-H,J-Z]{2}[0-9]{2})(NE|NW|SE|SW)_(D[S,T]M)\.ASC$')

    """

    """
    def process_type(self, directory, product, output_directory):
        grids = {}

        for grid in os.listdir(in_dir):
            match = self.exp.match(grid.upper())

            if match is not None:
                grid_100 = match.group(1)
                grid_10_x = match.group(2)
                grid_10_y = match.group(3)

                grid_10k = '%s%s%s' % (grid_100, grid_10_x, grid_10_y)

                if grid_10k in grids:
                    grids[grid_10k].append(grid)
                else:
                    grids[grid_10k] = [grid]
            else:
                match = self.exp_alt.match(grid)
                if match is not None:
                    grid_10k = match.group(1)
                    
                    if grid_10k in grids:
                        grids[grid_10k].append(grid)
                    else:
                        grids[grid_10k] = [grid]

        for grid_10k in grids:
            outputs = []
            self.logger.info('---------------------------------')
            self.logger.info('Working on Grid %s with 5k Grids [%s]' % (grid_10k, ', '.join(grids[grid_10k]))) 
            
            # Change to geotiff to add in missing projection data
            for grid_5k in grids[grid_10k]:
                input_file = os.path.join(in_dir, grid_5k)
                
                input_gdal = gdal.Open(input_file)
                input_gdal_band = input_gdal.GetRasterBand(1)
                #input_nodata = 

                output_file = os.path.join(self.temp, os.path.basename(grid_5k).replace('.asc', '.tif'))
                outputs.append(output_file)
                gdal.Translate(output_file, input_file, options=gdal.TranslateOptions(format = 'GTiff', outputSRS = 'EPSG:27700'))

            # gdal merge
            final_output_file = os.path.join(self.temp, '%s_%s.tif' % (product, grid_10k))

            p = subprocess.Popen('gdal_merge.py -o %s -of GeoTiff -co TILED=YES -co BLOCKXSIZE=256 -co BLOCKYSIZE=256 -co COMPRESS=LZW -a_nodata -9999 -init -9999 %s' % (final_output_file, ' '.join(outputs)), shell=True)
            (output, err) = p.communicate()
            if output is not None:
                logger.debug(output)
            if err is not None:
                logger.error(err)
                sys.exit(1)

            key = '%s/%s/x-%s_%s.tif' % (output_directory, product, product, grid_10k)

            # upload to relevant directory
            s3Helper.copy_file_to_s3(self.access_key, self.secret_access_key, self.region, self.bucket, final_output_file, key, True, None)

            for temp_file in os.listdir(self.temp):
                os.unlink(os.path.join(self.temp, temp_file))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Upload script for scottish lidar DTM and DSM files')
    parser.add_argument('-c', '--config', type=str, required=True, help='Path to config.yaml file')
    
    args = parser.parse_args()
    
    with open(args.config, 'r') as conf:
        config = yaml.load(conf)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    logger = logging.getLogger('scottish_lidar_s3_uploader')
    logger.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)

    fh = logging.FileHandler(os.path.join(config.get('log_dir'), 'scottish_lidar_s3_uploader-%s.log' % time.strftime('%y%m%d-%H%M%S')))
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)

    logger.addHandler(ch)
    logger.addHandler(fh)    

    uploader = scottish_lidar_s3_uploader(config, logger)
    uploader.process_type(os.path.join(os.path.join(config.get('data_directory'), 'DTM'), 'ASCII'), 'DTM', 'x')    
    uploader.process_type(os.path.join(os.path.join(config.get('data_directory'), 'DSM'), 'ASCII'), 'DSM', 'x')