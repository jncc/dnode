from osgeo import gdal, ogr, osr
import argparse, glob, logging, os, subprocess, time, sys

class ProductChunker:
    
    def __init__(self, temp, logger):
        self.logger = logger
        self.temp = temp

        gdal.AllRegister()
        gdal.UseExceptions()
    
    def setGridSystem():
        logger.info('-----------------------------')
        logger.info('Collecting grid system to cut with')
        grids = ogr.Open(args.grid)
        grid_features = grids.GetLayer(0)
        logger.info('Found %d features in gridfile' % grid_features.GetFeatureCount())        


    band_output = os.path.join(self.temp, 'band.tif')
    gdal.Translate(band_output, input, gdal.TranslateOptions(format = 'GTiff', bandList = [band]))

    alpha_output = os.path.join(self.temp, 'alpha.tif')
    gdal.Warp(alpha_output, band_output, gdal.WarpOptions(format = 'GTiff', srcNodata=0, dstAlpha=True))

    footprint_output = os.path.join(self.temp, 'outline.geojson')
    alpha_src = gdal.Open(alpha_output)
    alpha_src_band = alpha_src.GetRasterBand(2)

    drv = ogr.GetDriverByName('GeoJSON')
    dst_ds = drv.CreateDataSource(footprint_output)
    dst_layer = dst_ds.CreateLayer('outline', srs=None)

    gdal.Polygonize(alpha_src_band, None, dst_layer, -1, [], callback=None)

    outline_file = ogr.Open(footprint_output)
    outline_layer = outline_file.GetLayer(0)
    # Collect all features in the polygonized output in the case of multiple geometries created and
    # create a convex hull of that joined polygon
    geomcol = ogr.Geometry(ogr.wkbGeometryCollection)
    for feature in outline_layer:
        geomcol.AddGeometry(feature.GetGeometryRef())
    footprint = geomcol.ConvexHull()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cuts a provided Raster image by a provided vector dataset, generally a grid, but can be any data')
    parser.add_argument('-i', '--input', type=str, required=False, help='Single file to process')
    parser.add_argument('-r', '--root', type=str, required=False, help='Root of directory containing scenes')
    parser.add_argument('--pattern', type=str, required=False, default='*/*.tif', help='Pattern of files to copy (unix glob) defaults to */*.tif')
    parser.add_argument('-t', '--temp', type=str, required=False, default='./temp', help='Path to temp file directory')
    parser.add_argument('-g', '--grid', type=str, required=True, help='Path to grid file used to cut raster file')
    parser.add_argument('-b', '--band', type=int, nargs='+', required=False, default=[1, 2, 3], help='Band(s) containing data for cutting input file footprint [Default 1,2,3]')
    parser.add_argument('-f', '--footprint', type=str, required=False, help='Footprint file to use (path taken from base of input file path)')
    parser.add_argument('-c', '--lco', type=str, nargs='+', default=['TILED=YES', 'BLOCKXSIZE=256', 'BLOCKYSIZE=256', 'BIGTIFF=YES', 'COMPRESS=LZW'], help='Layer creation options for output raster files')
    parser.add_argument('-a', '--addo', type=str, default='2 4 8 16 32', help='Overviews to add to the finished images')
    
    args = parser.parse_args()
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    logger = logging.getLogger('grid-choppa')
    logger.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)

    fh = logging.FileHandler('grid-choppa-%s.log' % time.strftime('%y%m%d-%H%M%S'))
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)

    logger.addHandler(ch)
    logger.addHandler(fh)    
    
    gdal.AllRegister()
    
    logger.info('-----------------------------')
    logger.info('Collecting grid system to cut with')
    grids = ogr.Open(args.grid)
    grid_features = grids.GetLayer(0)
    logger.info('Found %d features in gridfile' % grid_features.GetFeatureCount())
    
    logger.info('-----------------------------')
    logger.info('Generating file list ot iterate over')
    
    inputList = []
    
    if args.input is not None:
        inputList.append(args.input)
        logger.info('Operating on single file - %s' % args.input)
    elif args.root is not None:
        inputList = glob.glob(os.path.join(args.root, args.pattern))
        logger.info('Operating on globbed list [%s] from root %s' % (args.pattern, args.root))
    else:
        logger.severe('Single file input and root are not defined, must define at least one argument')
        sys.exit(1)
    
    for input in inputList:
        logger.info('=============================')
        logger.info('Working on %s' % input)
        
        inputData = gdal.Open(input)
        inputDataSRS = int(osr.SpatialReference(inputData.GetProjection()).GetAuthorityCode(None))
        gridDataSRS = int(grid_features.GetSpatialRef().GetAuthorityCode(None))
        
        if inputDataSRS != gridDataSRS:
            ## TODO: Needs testing and fixing, should keep track of potentially many different reprojections
            ## of the input grid if needs be
            driver = ogr.GetDriverByName('GeoJSON')
            srs = osr.SpatialReference()
            srs.ImportFromEPSG(inputDataSRS)
            
            coordTrans = osr.CoordinateTransformation(grid_features.GetSpatialRef(), osr.SpatialReference(inputData.GetProjection()))

            rpData = driver.CreateDataSource(os.path.join(args.temp, 'grid_%d.geojson' % (inputDataSRS)))
            rpDataLayer = rpData.CreateLayer('grid_%d' % inputDataSRS, srs, ogr.wkbPolygon)
            
            # Copy field definitions
            inLayerDefn = grid_features.GetLayerDefn()
            for i in range(0, inLayerDefn.GetFieldCount()):
                fieldDefn = inLayerDefn.GetFieldDefn(i)
                rpDataLayer.CreateField(fieldDefn)
            
            rpLayerDefn = rpDataLayer.GetLayerDefn()
            
            feature = grid_features.GetNextFeature()
            while feature:
                geom = feature.GetGeometryRef()
                geom.Transform(coordTrans)
                outFeature = ogr.Feature(rpLayerDefn)
                outFeature.SetGeometry(geom)
                for i in range(0, rpLayerDefn.GetFieldCount()):
                    outFeature.SetField(outLayerDefn.GetFieldDefn(i).GetNameRef(), feature.GetField(i))
                rpDataLayer.CreateFeature(outFeature)
                outFeature = None
                feature = grid_features.GetNextFeature()
                
            rpDataLayer = None
            rpData = None
            
            grid_features = None
            grid = None
            
            grid = ogr.Open(os.path.join(args.temp, 'grid_%d.geojson' % (inputDataSRS)))
            grid_features = grid.GetLayer(0)
        
        if args.footprint is None:
            
            logger.info('-----------------------------')
            logger.info('Getting Image footprint - Step 1 - Extract Single Colour Band')
            
            bands = ''
            for band in args.band:
                bands = '%s -b %d' % (bands, band)
            
            p = subprocess.Popen('gdal_translate %s %s %s' % (bands, input, os.path.join(args.temp, 'band.tif')), shell=True)
            (output, err) = p.communicate()
            if output is not None:
                logger.debug(output)
            if err is not None:
                logger.error(err)
                sys.exit(1)

            logger.info('-----------------------------')
            logger.info('Getting Image footprint - Step 2 - Extract Alpha Band')
            p = subprocess.Popen('gdalwarp -srcnodata 0 -dstalpha %s %s' % (os.path.join(args.temp, 'band.tif'), os.path.join(args.temp, 'alpha.tif')), shell=True)
            (output, err) = p.communicate()
            if output is not None:
                logger.debug(output)
            if err is not None:
                logger.error(err)
                sys.exit(1)
            
            logger.info('-----------------------------')
            logger.info('Getting Image footprint - Step 3 - Extract Footprint from Alpha Band')
            p = subprocess.Popen('gdal_polygonize.py %s -b %d -f GeoJSON %s' % (os.path.join(args.temp, 'alpha.tif'), (len(args.band) + 1), os.path.join(args.temp, 'outline.geojson')), shell=True)
            (output, err) = p.communicate()
            if output is not None:
                logger.debug(output)
            if err is not None:
                logger.error(err)
                sys.exit(1)
                
            logger.info('-----------------------------')
            logger.info('Getting Image footprint - Step 4 - Simplify with Convex Hull for final output')
            outline_file = ogr.Open(os.path.join(args.temp, 'outline.geojson'))
            outline_layer = outline_file.GetLayer(0)
            # Collect all features in the polygonized output in the case of multiple geometries created and
            # create a convex hull of that joined polygon
            geomcol = ogr.Geometry(ogr.wkbGeometryCollection)
            for feature in outline_layer:
                geomcol.AddGeometry(feature.GetGeometryRef())
            footprint = geomcol.ConvexHull()   
        else:
            footprintPath = os.path.join(os.path.split(input)[0], args.footprint)
            footprintData = ogr.Open(footprintPath)
            footprintLayer = footprintData.GetLayer(0)
            footprint = ogr.Geometry(ogr.wkbMultiPolygon)
            for feature in footprintLayer:
                footprint.AddGeometry(feature.GetGeometryRef())
        
        logger.info('-----------------------------')
        logger.info('Find overlaps in provided grid system and create outputs')
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
                cd = driver.CreateDataSource(os.path.join(args.temp, 'cutline.geojson'))
                
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
                for co in args.lco:
                    lco = '%s -co "%s"' % (lco, co)
                
                p = subprocess.Popen('gdalwarp %s -crop_to_cutline -cutline %s %s %s' % (lco, os.path.join(args.temp, 'cutline.geojson'), input, os.path.join(args.temp, '%s.tif' % (output_name))), shell=True)
                (output, err) = p.communicate()
                if output is not None:
                    logger.debug(output)
                if err is not None:
                    logger.error(err)
                    sys.exit(1)
                    
                ## Add Overviews
                p = subprocess.Popen('gdaladdo %s %s' % (os.path.join(args.temp, '%s.tif' % (output_name)), args.addo), shell=True)
                (output, err) = p.communicate()
                if output is not None:
                    logger.debug(output)
                if err is not None:
                    logger.error(err)
                    sys.exit(1)