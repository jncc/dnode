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
*.wgs84.geojson

:param item: The item that we are downloading (sourced from the available products list)
:param path: The path to base our footprint paths on
:return: Returns the raw GeoJSON as a tuple (osgb, osni), osni will be None if no OSNI data exists
"""
def extract_footprints_wgs84(item, path):
    # Grab footprints and create wgs84 geojson for upload to the catalog / s3
    footprint_osgb_path = os.path.join(os.path.join(os.path.join(path, item['filename']), 'Footprint'), item['filename'].replace('.SAFE.data', '_footprint'))
    footprint_osgb_output_path = ''

    if os.path.isfile('%s.shp' % footprint_osgb_path):
        footprint_osgb_path = '%s.shp' % footprint_osgb_path
        footprint_osgb_output_path = footprint_osgb_path.replace('.shp', '_wgs84.geojson')
    elif os.path.isfile('%s.geojson' % footprint_osgb_path):
        footprint_osgb_path = '%s.geojson' % footprint_osgb_path
        footprint_osgb_output_path = footprint_osgb_path.replace('.geojson', '_wgs84.geojson')
    else:
        raise RuntimeError('No footprint found for %s, halting' % item['filename'])
    
    reproject_footprint(footprint_osgb_path, footprint_osgb_output_path)

    with open(footprint_osgb_output_path) as osgb_output:
        osgb = json.load(osgb_output)

    # Attempt to extract any potential OSNI geometry
    footprint_osni_path = os.path.join(os.path.join(os.path.join(os.path.join(path, item['filename']), 'OSNI1952'), 'Footprint'), item['filename'].replace('.SAFE.data', '_OSNI1952_footprint'))
    footprint_osni_output_path = None
    osni = None

    if os.path.isfile('%s.shp' % footprint_osni_path):
        footprint_osni_path = '%s.shp' % footprint_osni_path
        footprint_osni_output_path = footprint_osni_path.replace('.shp', '_wgs84.geojson')
        reproject_footprint(footprint_osni_path, footprint_osni_output_path)
    elif os.path.isfile('%s.geojson' % footprint_osni_path):
        footprint_osni_path = '%s.geojson' % footprint_osni_path
        footprint_osni_output_path = footprint_osni_path.replace('.geojson', '_wgs84.geojson')            
        reproject_footprint(footprint_osni_path, footprint_osni_output_path)
    
    if footprint_osni_output_path is not None:
        with open(footprint_osni_output_path) as osni_out:
            osni = json.load(osni_out)

    return (osgb, osni)    