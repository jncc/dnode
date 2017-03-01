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
        conn = boto.s3.connect_to_region(self.s3_region, aws_access_key_id=self.s3_access_key, aws_secret_access_key=self.s3_secret_access_key, is_secure=True)
        bucket = conn.get_bucket(self.s3_bucket)

        products = []
        exp = re.compile('S2_20[0-9]{6}_[0-9]{2,3}_[0-9]{1}.tif$')

        # Get product list, no glob search so manual search is needed, exclude any item with a gridded/osgb folder as we have already done that one
        for key in bucket.list(prefix=os.path.join(self.s3_path, 'S2_20')):
            if exp.search(key.key) is not None:
                product_path = os.path.split(key.key)[0]
                if len(bucket.get_all_keys(prefix=os.path.join(product_path, 'gridded/27700'), max_keys=1)) == 0:
                    products.append(key)

        for product in products:
            try:
                self.logger.info('----------------------------------')
                self.logger.info('Working on %s' % product.key)
                self.chunk_product(product, bucket)
            except Exception as ex:
                self.logger.error(repr(ex))
            finally:
                self.logger.info('Cleaning temp directory')
                for item in os.listdir(self.temp):
                    file_path = os.path.join(self.temp, item)
                    if os.path.isfile(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path): 
                        shutil.rmtree(file_path)


    def chunk_product(self, key, bucket):
        ####################################################################################
        # Download product from S3 to local storage for processing
        ####################################################################################
        input = os.path.join(self.temp, 'current.tif')

        with open(input, 'wb') as output:
            key.get_contents_to_file(output)

        ####################################################################################
        # Check Grid Projection matches file projection
        ####################################################################################
        inputData = gdal.Open(input)
        inputDataSRS = int(osr.SpatialReference(inputData.GetProjection()).GetAuthorityCode(None))

        ####################################################################################
        # Extract footprint of product
        ####################################################################################
        self.logger.info('Getting Image footprint - Step 1 - Extract Single Colour Band')
        
        bands = ''
        for band in self.bands:
            bands = '%s -b %d' % (bands, band)
        
        p = subprocess.Popen('gdal_translate %s %s %s' % (bands, input, os.path.join(self.temp, 'band.tif')), shell=True)
        (output, err) = p.communicate()
        if output is not None:
            self.logger.debug(output)
        if err is not None:
            raise RuntimeError(err)

        self.logger.info('Getting Image footprint - Step 2 - Extract Alpha Band')
        p = subprocess.Popen('gdalwarp -srcnodata 0 -dstalpha %s %s' % (os.path.join(self.temp, 'band.tif'), os.path.join(self.temp, 'alpha.tif')), shell=True)
        (output, err) = p.communicate()
        if output is not None:
            self.logger.debug(output)
        if err is not None:
            raise RuntimeError(err)
        
        self.logger.info('Getting Image footprint - Step 3 - Extract Footprint from Alpha Band')
        p = subprocess.Popen('gdal_polygonize.py %s -b %d -f GeoJSON %s' % (os.path.join(self.temp, 'alpha.tif'), (len(self.bands) + 1), os.path.join(self.temp, 'outline.geojson')), shell=True)
        (output, err) = p.communicate()
        if output is not None:
            self.logger.debug(output)
        if err is not None:
            raise RuntimeError(err)
            
        self.logger.info('Getting Image footprint - Step 4 - Grab output footprint for use and collect multiple geometries into a single geometry if needed')
        outline_file = ogr.Open(os.path.join(self.temp, 'outline.geojson'))
        outline_layer = outline_file.GetLayer(0)
        # Collect all features in the polygonized output in the case of multiple geometries created and
        # create a convex hull of that joined polygon
        geomcol = ogr.Geometry(ogr.wkbGeometryCollection)
        for feature in outline_layer:
            geomcol.AddGeometry(feature.GetGeometryRef())
        #footprint = geomcol.ConvexHull()   
        footprint = geomcol

        for grid in self.grids:
            self.logger.info('Collecting grid system to cut with [%s]' % grid)
        
            grid_file = ogr.Open(self.grids[grid]['source_grid'])
            grid_features = grid_file.GetLayer(0)
            self.logger.info('Found %d features in gridfile' % grid_features.GetFeatureCount())             
            self.logger.info(self.grids[grid])
            if 'addo' in self.grids[grid]:
                self.logger.debug('Overiews to be generated for this grid type')
            else:
                self.logger.debug('Overiews not generated, as not requested for this grid type')        

            gridDataSRS = int(grid_features.GetSpatialRef().GetAuthorityCode(None))
        
            if inputDataSRS != gridDataSRS:
                # TODO Reproject code, shouldn't be needed right now however
                self.logger.warn('Could not process %s with %s, projection did not match supplied grid file' % (key.key, self.grids[grid]['source_grid']))
            else:
                ####################################################################################
                # Iterate over overlapping grid and cut the source product into grids as required
                ####################################################################################
                self.logger.info('Find overlaps in provided grid system and create outputs')

                [key_path, key_file] = os.path.split(key.key)
                dest_path = os.path.join(key_path, self.grids[grid]['dest_path'])

                for feature in grid_features:
                    geom = feature.GetGeometryRef()
                    if geom.Intersects(footprint):
                        ## Do Cut
                        output_name = feature.GetField(0)
                        intersection = geom.Intersection(footprint)
                        
                        ## Output cut to external file for use in gdalwarp
                        srs = osr.SpatialReference()
                        srs.ImportFromEPSG(27700)
                        
                        driver = ogr.GetDriverByName('GeoJSON')
                        cd = driver.CreateDataSource(os.path.join(self.temp, 'cutline.geojson'))
                        
                        cl = cd.CreateLayer('cutline', srs, ogr.wkbPolygon)
                        field = ogr.FieldDefn('id', ogr.OFTString)
                        cl.CreateField(field)
                        
                        cf = ogr.Feature(cl.GetLayerDefn())
                        cf.SetField('id', output_name)
                        cf.SetGeometry(intersection.ConvexHull())
                        cl.CreateFeature(cf)
                        
                        cf = None
                        cl = None
                        cd = None
                        
                        ## Create cut file based on intersection
                        lco = ''
                        for co in self.lco:
                            lco = '%s -co "%s"' % (lco, co)
                        
                        output_file_path = os.path.join(self.temp, '%s.tif' % (output_name))

                        p = subprocess.Popen('gdalwarp %s -crop_to_cutline -cutline %s %s %s' % (lco, os.path.join(self.temp, 'cutline.geojson'), input, output_file_path), shell=True)
                        (output, err) = p.communicate()
                        if output is not None:
                            self.logger.debug(output)
                        if err is not None:
                            raise RuntimeError(err)

                        ## Add Overviews
                        if 'addo' in self.grids[grid]:
                            p = subprocess.Popen('gdaladdo %s %s' % (output_file_path, self.grids[grid]['addo']), shell=True)
                            (output, err) = p.communicate()
                            if output is not None:
                                self.logger.debug(output)
                            if err is not None:
                                raise RuntimeError(err)                            
                        
                        ####################################################################################
                        # Upload results back to S3
                        ####################################################################################
                        final_key_path = os.path.join(dest_path, key_file.replace('.tif', '_%s.tif' % output_name))
                        self.logger.info('Uploading to %s' % final_key_path)
                        s3Helper.copy_file_to_s3(self.s3_access_key, self.s3_secret_access_key, self.s3_region, self.s3_bucket, output_file_path, final_key_path, self.s3_public, None)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cuts a provided Raster image by a provided vector dataset, generally a grid, but can be any data')
    parser.add_argument('-c', '--config', type=str, required=True, help='Path to config.yaml file')
    
    args = parser.parse_args()
    
    with open(args.config, 'r') as conf:
        config = yaml.load(conf)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    logger = logging.getLogger('product-chunking-simple-s3')
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
           
        