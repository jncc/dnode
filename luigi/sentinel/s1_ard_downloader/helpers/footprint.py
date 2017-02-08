import json, os
from osgeo import ogr, osr

"""
Reprojects a given file into a GeoJSON file of the provided projection (default: EPSG:4326)

:param inFile: Path to the input file to reproject
:param outFile: Path to the output file
:param toProjection: EPSG number to reproject to (defualt: 4326 [WGS84])
"""
def reproject_footprint(inFile, outFile, toProjection=4326):
    outDriver = ogr.GetDriverByName('GeoJSON')

    if os.path.splitext(outFile)[1] == '.shp':
        outDriver = ogr.GetDriverByName('ESRI Shapefile')            

    # get the input layer
    inDataSet = ogr.Open(inFile)
    inLayer = inDataSet.GetLayer()

    # input SpatialReference
    inSpatialRef = inLayer.GetSpatialRef()

    # output SpatialReference
    outSpatialRef = osr.SpatialReference()
    outSpatialRef.ImportFromEPSG(toProjection)

    # create the CoordinateTransformation
    coordTrans = osr.CoordinateTransformation(inSpatialRef, outSpatialRef)        

    # create the output layer
    if os.path.exists(outFile):
        outDriver.DeleteDataSource(outFile)
    outDataSet = outDriver.CreateDataSource(outFile)
    outLayer = outDataSet.CreateLayer('footprint_4326', outSpatialRef, geom_type=ogr.wkbMultiPolygon)

    # add fields
    inLayerDefn = inLayer.GetLayerDefn()
    for i in range(0, inLayerDefn.GetFieldCount()):
        fieldDefn = inLayerDefn.GetFieldDefn(i)
        outLayer.CreateField(fieldDefn)

    # get the output layer's feature definition
    outLayerDefn = outLayer.GetLayerDefn()

    # loop through the input features
    inFeature = inLayer.GetNextFeature()
    while inFeature:
        # get the input geometry
        geom = inFeature.GetGeometryRef()
        # reproject the geometry
        geom.Transform(coordTrans)
        # create a new feature
        outFeature = ogr.Feature(outLayerDefn)
        # set the geometry and attribute
        outFeature.SetGeometry(geom)
        for i in range(0, outLayerDefn.GetFieldCount()):
            outFeature.SetField(outLayerDefn.GetFieldDefn(i).GetNameRef(), inFeature.GetField(i))
        # add the feature to the shapefile
        outLayer.CreateFeature(outFeature)
        # dereference the features and get the next input feature
        outFeature = None
        inFeature = inLayer.GetNextFeature()

    # Save and close the shapefiles
    inDataSet = None
    outDataSet = None

"""
Extract WGS84 footprints from the given footprints, looks for an OSNI folder to extract any additional footprints from, will save the outputs as 
*.geojson

:param item: The item that we are downloading (sourced from the available products list)
:param path: The path to base our footprint paths on
:return: Returns the raw GeoJSON as a tuple (osgb, osni), osni will be None if no OSNI data exists
"""
def extract_footprints_wgs84(item, path):
    # Grab footprints and create wgs84 geojson for upload to the catalog / s3
    std_osgb_path = os.path.join(os.path.join(os.path.join(path, item['filename']), 'Footprint'), item['filename'].replace('.SAFE.data', '_footprint'))
    footprint_osgb_path = std_osgb_path
    footprint_osgb_output_path = ''
    osgb = None

    # if os.path.isfile('%s.shp' % footprint_osgb_path):
    #     footprint_osgb_path = '%s.shp' % footprint_osgb_path
    #     footprint_osgb_output_path = footprint_osgb_path.replace('.shp', '.geojson')
    #     # TODO: Turn into geojson

    if os.path.isfile('%s.json' % footprint_osgb_path):
        footprint_osgb_path = '%s.json' % footprint_osgb_path
        footprint_osgb_output_path = footprint_osgb_path.replace('.json', '.geojson')
    elif os.path.isfile('%s.geojson' % footprint_osgb_path):
        footprint_osgb_path = '%s.geojson' % footprint_osgb_path
        footprint_osgb_output_path = footprint_osgb_path

    osgb = rewrite_outputs(footprint_osgb_path, footprint_osgb_output_path)        

    # Attempt to extract any potential OSNI geometry
    std_osni_path = os.path.join(os.path.join(path, item['filename'], 'OSNI1952'), item['filename'].replace('.SAFE.data', '_OSNI1952_footprint'))
    footprint_osni_path = std_osni_path
    footprint_osni_output_path = os.path.join(os.path.join(os.path.join(path, item['filename'], 'OSNI1952'), 'Footprint'), item['filename'].replace('.SAFE.data', '_OSNI1952_footprint.geojson'))
    osni = None
    found_osni = False

    # if os.path.isfile('%s.shp' % footprint_osni_path):
    #     footprint_osni_path = '%s.shp' % footprint_osni_path
    #     footprint_osni_output_path = footprint_osni_path.replace('.shp', '.geojson')
    #     # TODO Write to geojson

    if os.path.isfile('%s.json' % footprint_osni_path):
        found_osni = True
        footprint_osni_path = '%s.json' % footprint_osni_path
    elif os.path.isfile('%s.geojson' % footprint_osni_path):
        found_osni = True
        footprint_osni_path = '%s.geojson' % footprint_osni_path

    if found_osni:
        if not os.path.isdir(os.path.join(os.path.join(path, item['filename'], 'OSNI1952'), 'Footprint')):
            os.makedirs(os.path.join(os.path.join(path, item['filename'], 'OSNI1952'), 'Footprint'))
        osni = rewrite_outputs(footprint_osni_path, footprint_osni_output_path)

    # Remove extra uneeded files
    remove_file('%s.dbf' % std_osgb_path)
    remove_file('%s.prj' % std_osgb_path)
    remove_file('%s.shp' % std_osgb_path)
    remove_file('%s.shx' % std_osgb_path)
    remove_file('%s.json' % std_osgb_path)
    if found_osni:
        remove_file('%s.dbf' % std_osni_path)
        remove_file('%s.prj' % std_osni_path)
        remove_file('%s.shp' % std_osni_path)
        remove_file('%s.shx' % std_osni_path)
        remove_file('%s.json' % std_osni_path)

    return (osgb, osni)

"""

"""
def remove_file(filepath):
    if os.path.isfile(filepath):
        os.unlink(filepath) 

"""
Opens a GeoJSON file, optionally reprojects it if there is a CRS (non WGS84, so assume if CRS block exists then need
to reproject) and returns the content of that file (or the reprojected one)

:param input_path: The path to an input GeoJSON file
:param output_path: The path to the output GeoJSON file
"""
def rewrite_outputs(input_path, output_path):
    with open(input_path, 'r') as input_file:
        data = json.load(input_file)

    if 'crs' in data:
        # If a CRS block exsists, assume this is not a WGS84 block and reproject, saving the original
        splitext = os.path.splitext(output_path)
        
        with open('%s.original.geojson' % splitext[0], 'w') as output_file:
            json.dump(data, output_file)

        reproject_footprint('%s.original.geojson' % splitext[0], output_path)

        with open(output_path) as output_final:
            data = json.load(output_final)

    elif input_path != output_path:
        with open(output_path, 'w') as output:
            json.dump(data, output)
    
    return data
    