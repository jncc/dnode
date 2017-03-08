import argparse, boto, logging, os, shutil, subprocess, time, yaml

from osgeo import gdal

class wms_mosaic_creator:
    def __init__(self, temp_directory, logger, s3_region=None, s3_bucket=None, s3_access_key=None, s3_secret_access_key=None, s3_path=None):
        if s3_region is not None and s3_bucket is not None and s3_access_key is not None and s3_secret_access_key is not None and s3_path is not None:
            self.s3 = True
            self.s3_region = s3_region
            self.s3_bucket = s3_bucket
            self.s3_access_key = s3_access_key
            self.s3_secret_access_key = s3_secret_access_key
            self.s3_path = s3_path
        else:
            self.s3 = False

        self.temp_directory = temp_directory
        self.logger = logger
        
    def is_s3_configured(self):
        return self.s3

    def collect_source_data(self, path):
        if self.s3:
            # S3 data source
            conn = boto.s3.connect_to_region(self.s3_region, aws_access_key_id=self.s3_access_key, aws_secret_access_key=self.s3_secret_access_key, is_secure=True)
            bucket = conn.get_bucket(self.s3_bucket)
            source_data = os.path.join(self.temp_directory, 'source')

            # Make temp storage directory if one does not exist
            if not os.path.isdir(source_data):
                os.makedirs(source_data)
            
            self.logger.info('Downloading source data from S3')
            # Get product list, no glob search so manual search is needed, exclude any item with a gridded/osgb folder as we have already done that one
            for key in bucket.list(prefix=self.s3_path):
                self.logger.info('- Downloading %s' % key.key)
                if key.key.endswith('.tif'):
                    file_path = os.path.join(source_data, os.path.basename(key.key))
                    with open(file_path, 'wb') as output:
                        key.get_contents_to_file(output)
        else:
            # Local data source
            source_data = self.path
        
        return source_data

    def create_vrt(self, source_data_directory):
        self.logger.info('Creating VRT from source data directory [%s]' % source_data_directory)
        inputs = [os.path.join(source_data_directory, item) for item in os.listdir(source_data_directory)]
        vrt = os.path.join(self.temp_directory, 'source.vrt')
        gdal.BuildVRT(vrt, inputs)

        return vrt

    def create_mosaic_tiles(self, vrt, output_directory, lco, tilesize=102400, addo='2 4 8 16 32 64 128 256'):
        self.logger.info('Creating mosaic tiles from source VRT [%s]' % vrt)
        dset = gdal.Open(vrt)

        width = dset.RasterXSize
        height = dset.RasterYSize

        self.logger.info('Input dataset is %dx%d, Tilesize is %d' % (width, height, tilesize))

        for i in range(0, width, tilesize):
            for j in range(0, height, tilesize):
                w = min(i+tilesize, width) - i
                h = min(j+tilesize, height) - j

                output_file_path = os.path.join(self.temp_directory, '%d_%d.tif' % (i, j))
                final_output_file_path = os.path.join(output_directory, '%d_%d.tif' % (i, j))
                self.logger.info('Creating file [%s]' % output_file_path)
                gdal.Translate(output_file_path, vrt, options=gdal.TranslateOptions(srcWin=[i, j, w, h], format='GTiff', creationOptions=lco, stats=True))

                self.logger.info('Adding Overviews for file [%s]' % output_file_path)
                ## Add Overviews - Can't find a way to do this without the subproccess call right now
                p = subprocess.Popen('gdaladdo %s %s' % (output_file_path, addo), shell=True)
                (output, err) = p.communicate()
                if output is not None:
                    self.logger.debug(output)
                if err is not None:
                    raise RuntimeError(err)
                
                self.logger.info('Copying file [%s] to final location [%s]' % (output_file_path, final_output_file_path))
                ## Copy to final location
                shutil.move(output_file_path, final_output_file_path)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Creates a mosaic for use in a geoserver layer from a large input file / directory of input files')
    parser.add_argument('-c', '--config', type=str, required=True, help='Path to config.yaml file')
    
    args = parser.parse_args()   

    with open(args.config, 'r') as conf:
        config = yaml.load(conf)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    logger = logging.getLogger('wms-mosaic-creator')
    logger.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)

    fh = logging.FileHandler(os.path.join(config.get('log_dir'), 'wms-mosaic-creator-%s.log' % time.strftime('%y%m%d-%H%M%S')))
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)

    logger.addHandler(ch)
    logger.addHandler(fh)        
    
    mosaicer = wms_mosaic_creator(config.get('working_dir'), logger, s3_region=config.get('s3')['region'], s3_bucket=config.get('s3')['bucket'], s3_access_key=config.get('s3')['access_key'], s3_secret_access_key=config.get('s3')['secret_access_key'], s3_path=config.get('s3')['path'])

    if mosaicer.is_s3_configured:
        source_dir = mosaicer.collect_source_data(config.get('s3')['path'])
    else:
        source_dir = mosaicer.collect_source_data(config.get('data_directory'))
    
    vrt = mosaicer.create_vrt(source_dir)
    mosaicer.create_mosaic_tiles(vrt, config.get('output_data_directory'), config.get('lco'))