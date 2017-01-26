import json
import boto
import yaml
import psycopg2
import os
import hashlib
import zipfile
import time
import ogr
import uuid
from osgeo import ogr, osr
from lxml import etree
from datahub_client import DatahubClient

def calculate_checksum(filename):
    """ 
    Calculate checksum of a given file

    :param filename: The filename of the downloaded dataset
    :return: The checksum of the file specifed by the filename
    """
    hasher = hashlib.md5()
    with open(filename, 'r') as stream:
        for chunk in iter(lambda: stream.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

class ProductDownloader:
    def __init__(self, config_file, logger):
        # Setup Config from config file
        self.logger = logger

        # Check config file to make sure it looks sane before use
        if os.path.isfile(config_file):
            with open("config.yaml", 'r') as conf:
                self.config = yaml.load(conf)

                datahub_conf = self.config.get('datahub')
                if datahub_conf is not None \
                    and 'search_zone_id' in datahub_conf \
                    and 'username' in datahub_conf \
                    and 'password' in datahub_conf \
                    and 'base_url' in datahub_conf \
                    and 'download_chunk_size' in datahub_conf: 
                    self.client = DatahubClient(datahub_conf['base_url'], datahub_conf['download_chunk_size'], datahub_conf['search_zone_id'], datahub_conf['username'], datahub_conf['password'], None)
                else:
                    raise RuntimeError('Config file has invalid datahub entries')

                self.s3_conf = self.config.get('s3')
                if not (self.s3_conf is not None \
                    and 'access_key' in self.s3_conf \
                    and 'secret_access_key' in self.s3_conf):
                    raise RuntimeError('Config file has invalid s3 entries')
                
                self.database_conf = self.config.get('database')
                if self.database_conf is not None \
                    and 'host' in self.database_conf \
                    and 'dbname' in self.database_conf \
                    and 'username' in self.database_conf \
                    and 'password' in self.database_conf \
                    and 'table' in self.database_conf:
                    self.db_conn = psycopg2.connect(host=self.database_conf['host'], dbname=self.database_conf['dbname'], user=self.database_conf['username'], password=self.database_conf['password'])
                else:
                    raise RuntimeError('Config file has missing database config entires')

        else:
            raise RuntimeError('Config file at [%s] was not found' % config_file)

    def destroy():
        self.db_conn.close()

    """
    Download from the supplied list of available products, upload the results to S3 and write the progress to a
    database table

    :param available: JSON list of available products for download
    :param downloaded: Ouput file stream to record completed downloads to
    """
    def downloadProducts(self, available, downloaded):  
        available_list = json.load(available)
        available_product_ids = [str(item.product_id) for item in available_list['available_products']]

        # Compare to already aquired products
        cur = self.conn.cursor()
        cur.execute("SELECT properties->>'product_id' FROM sentinel_ard_backscatter WHERE properties->>'product_id' not in %s AND properties->>'downloaded' != 'true';", (tuple(available_product_ids),))
        wanted_list_rows = cur.fetchall()
        cur.close()
        wanted_list = [item[0] for item in wanted_list_rows]

        extracted_path = os.path.join(self.temp, 'extracted')
        downloaded = []

        # Pass over list of available items and look for an non downloaded ID
        for item in available_list['available_products']:
            if item.product_id in wanted_list:
                filename = os.path.join(self.temp, '%s.zip' % item)
                client.download_product(item[0], filename)

                # Extract all files from source
                with zipfile.ZipFile(filename, 'r') as product_zip:
                    os.path.makedirs(extracted_path)
                    product_zip.extractall(extracted_path)
                tif_file = item.name.replace('.SAFE.data', '.tif')
                
                remote_checksum = client.get_checksum(item[0])
                local_checksum = calculate_checksum(tif_file)

                if remote_checksum == local_checksum:
                    # Extract footprints from downloaded files
                    osgb_geojson = None
                    osni_geojson = None
                    (osgb_geojson, osni_geojson) = __extract_footprints_wgs84(item, path)

                    # Extract Metadata for the OSGB side and a potential OSNI partition
                    osgb_metadata = xml_to_json(os.path.join(os.path.join(extracted_path, item.name), item.name.replace('.SAFE.data', '_metadata.xml')))
                    osni_metadata = None

                    if os.path.isfile(os.path.join(os.path.join(os.path.join(extracted_path, item.name), 'OSNI1952'), item.name.replace('.SAFE.data', '_metadata.xml'))):
                        osni_metadata = xml_to_json(os.path.join(os.path.join(os.path.join(extracted_path, item.name), 'OSNI1952'), item.name.replace('.SAFE.data', '_metadata.xml')))
                    
                    # Upload all files to S3
                    representations = self.__upload_dir_to_s3(extracted_path, '%s/%s' % (self.s3_conf['bucket_dest_path'], item.name))
                    representations = self.__extract_representations(representations, item.name)
                    
                    id = self.__write_progress_to_database(item, metadata=osgb_metadata, representations=representations['osgb'], success=True, geom=osgb_geojson)

                    if len(representations['osni']) > 0:
                        if osni_geojson is None:
                            self.__write_progress_to_database(item, metadata=osni_metadata, representations=representations['osni'], success=True, additional={'relatedTo': id}, geom=osgb_geojson)
                        else:
                            self.__write_progress_to_database(item, metadata=osni_metadata, representations=representations['osni'], success=True, additional={'relatedTo': id}, geom=osni_geojson)
                else:
                    self.__write_progress_to_database(item, success=False)
            else:
                ## TODO: Increment attempt record / error recovery
                self.__write_progress_to_database(item, success=False)
        
        self.client.logout()
        downloaded.write(json.dumps(downloaded)) 
        
    """

    """
    def __extract_footprints_wgs84(self, item, path):
        # Grab footprints and create wgs84 geojson for upload to the catalog / s3
        footprint_osgb_path = os.path.join(os.path.join(os.path.join(path, item.name), 'Footprint'), item.name.replace('.SAFE.data', '_footprint'))
        footprint_osgb_output_path = ''

        if os.path.isfile('%s.shp' % footprint_osgb_path):
            footprint_osgb_path = '%s.shp' % footprint_osgb_path
            footprint_osgb_output_path = footprint_osgb_path.replace('.shp', 'wgs84.geojson')
        elif os.path.isfile('%s.geojson' % footprint_osgb_path):
            footprint_osgb_path = '%s.geojson' % footprint_osgb_path
            footprint_osgb_output_path = footprint_osgb_path.replace('.geojson', 'wgs84.geojson')
        else:
            raise RuntimeError('No footprint found for %s, halting' % item.name)
        
        reproject_footprint(footprint_osgb_path, footprint_osgb_output_path)
        osgb = json.load(footprint_osgb_output_path)

        footprint_osni_path = os.path.join(os.path.join(os.path.join(os.path.join(path, item.name), 'OSNI1952'), 'Footprint'), item.name.replace('.SAFE.data', '_OSNI1952_footprint'))
        footprint_osni_output_path = ''

        osni = None

        if os.path.isfile('%s.shp' % footprint_osgb_path):
            footprint_osni_path = '%s.shp' % footprint_osni_path
            footprint_osni_output_path = footprint_osni_path.replace('.shp', 'wgs84.geojson')
            
            reproject_footprint(footprint_osni_path, footprint_osni_output_path)

            osni = json.load(footprint_osni_output_path)
        elif os.path.isfile('%s.geojson' % footprint_osgb_path):
            footprint_osni_path = '%s.geojson' % footprint_osni_path
            footprint_osni_output_path = footprint_osni_path.replace('.geojson', 'wgs84.geojson')
            
            reproject_footprint(footprint_osni_path, footprint_osni_output_path)

            osni = json.load(footprint_osni_output_path)
        else:
            raise RuntimeError('No footprint found for %s, halting' % item.name)        
        

        return (osgb, osni)

    """

    :param representations:
    :return: An array of representations for each non standard projection system (denoted by folder structure)
    """
    def __extract_representations(self, representations, name):
        outputs = {
            'osgb': [],
            'osni': []
        }
        for r in representations:
            if r.path.startswith('%s/%s/OSNI1952' % (name, self.s3_conf['bucket_dest_path'])):
                outputs['osni'].append(r)
            else:
                outputs['osgb'].append(r)
        return outputs

    """

    :param item:
    :param representations:
    :param success:
    """
    def __write_progress_to_database(self, item, metadata={}, representations={}, success=True, additional=None, geom=None):
        cur = self.conn.cursor()
        cur.execute("SELECT properties->>'product_id' FROM sentinel_ard_backscatter WHERE properties->>'product_id' = %s;", ((item.product_id)),)
        existing = cur.fetchone()

        retVal = None

        # Grab the UUID from the metadata if possible, if not create one
        uuid_str = metadata['ID']
        try:
            val = uuid.UUID(uuid_str, version=4)
        except ValueError:
            uuid_str = str(uuid.uuid4())
            metadata['ID'] = uuid_str

        # If UUID is equal to the optionally provided relatedTo UUID then generate a new one and replace the 
        # one in the metadata with it 
        if metadata['ID'] == additional['realtedTo']:
            uuid_str = str(uuid.uuid4())
            metadata['ID'] = uuid_str

        if existing is not None or additional is not None:
            # Entry exists
            props = json.loads(existing[3])
            props['downloaded'] = success
            props['attempts'] = int(props['attempts']) + 1 if props['attempts'] else 1

            if geom is None:
                if additional is not None:
                    # If we are adding an extra record with the same ID i.e. OSNI projection
                    props['relatedTo'] = additional['relatedTo']
                    cur.execute("INSERT INTO sentinel_ard_backscatter VALUES (%s, %s, %s, %s, %s, null) RETURNS id", (uuid_str,
                        self.database_conf.collection_version_uuid, json.dumps(metadata), json.dumps(props), json.dumps(representations), geom, ))
                    retVal = cur.fetchone()[0]
                else:
                    cur.execute("UPDATE sentinel_ard_backscatter SET properties = %s, representations = %s, footprint = null WHERE id = %s", (
                        json.dumps(props), json.dumps(representations), geom, existing(0), ))
            else:
                if additional is not None:
                    # If we are adding an extra record with the same ID i.e. OSNI projection
                    props['relatedTo'] = additional['relatedTo']
                    cur.execute("INSERT INTO sentinel_ard_backscatter VALUES (%s, %s, %s, %s, %s, ST_GeomFromGeoJSON(%s)) RETURNS id", (uuid_str, 
                        self.database_conf['collection_version_uuid'], json.dumps(metadata), json.dumps(props), json.dumps(representations), geom, ))
                    retVal = cur.fetchone()[0]
                else:
                    cur.execute("UPDATE sentinel_ard_backscatter SET properties = %s, representations = %s, footprint = ST_GeomFromGeoJSON(%s) WHERE id = %s", (
                        json.dumps(props), json.dumps(representations), geom, existing(0), ))
                    
                    retVal = existing(0)
        else:
            # Entry does not exist
            props = {
                "downloaded": success,
                "attempts": 1,
                "product_id": item.product_id,
                "name": item.name
            }

            if geom is None:
                cur.execute("INSERT INTO sentinel_ard_backscatter VALUES (%s, %s, '{}', %s, %s, null) RETURNS id", (uuid_str,
                    self.database_conf.collection_version_uuid, json.dumps(props), json.dumps(representations), geom, ))
            else:
                cur.execute("INSERT INTO sentinel_ard_backscatter VALUES (%s, %s, '{}', %s, %s, ST_GeomFromGeoJSON(%s)) RETURNS id", (uuid_str,
                    self.database_conf.collection_version_uuid, json.dumps(props), json.dumps(representations), geom, ))
            
            retVal = cur.fetchone()[0]
        
        cur.close()
        return retVal

    """

    :param sourcedir:
    :param destpath:
    :param representations:
    :return:
    """
    def __upload_dir_to_s3(self, sourcedir, destpath, representations={'s3': []}):
        for item in os.listdir(sourcedir):
            item_path = os.path.join(sourcedir, item)
            if os.path.isdir(item_path):
                representations = self.__upload_dir_to_s3(item_path, '%s/%s' % (destpath, item), representations)
            else:
                path = '%s/%s' % (destpath, item)
                self.__copy_file_to_s3(item_path, path)
                representations['s3'].append({
                    'bucket': self.s3_conf.bucket,
                    'region': self.s3_conf.region,
                    'path': path,
                    'url': 'https://s3-%s.amazonaws.com/%s/%s' % (self.s3_conf.region, self.s3_conf.bucket, path),
                    'type': self.__get_file_type(os.path.splitext(item))
                })
        return representations

    """

    :param ext:
    :return:
    """
    def __get_file_type(self, ext):
        if ext == '.tif':
            return 'data'
        elif ext == '.geojson':
            return 'footprint'
        elif ext == '.xml':
            return 'metadata'
        elif ext == '.jpeg':
            return 'preview'
        else:
            return 'unknown'

    """

    :param sourcepath:
    :param filename:
    """
    def __copy_file_to_s3(self, sourcepath, filename):
        #max size in bytes before uploading in parts. between 1 and 5 GB recommended
        MAX_SIZE = 5000000000
        #size of parts when uploading in parts
        PART_SIZE = 100000000        

        amazon_key_Id = self.s3_conf['access_key']
        amazon_key_secret = self.s3_conf['secret_access_key']

        conn = boto.s3.connect_to_region(self.s3_conf['region'], is_secure=True)

        bucket_name = self.s3_conf['bucket']
        amazonDestPath = self.s3_conf['bucket_dest_path']
        bucket = conn.get_bucket(bucket_name)

        destpath = os.path.join(amazonDestPath, filename)

        metadata = {'md5': calculate_checksum(sourcepath), 'uploaded': time.strftime('%Y-%m-%d %H:%M')}

        if self.debug:
            self.log("DEBUG: Would copy %s to %s", sourcepath, amazonDestPath)
        else:
            if bucket.get_key(destpath) != None:
                bucket.delete_key(destpath)            

            filesize = os.path.getsize(sourcepath)
            if filesize > MAX_SIZE:
                
                mp = None

                if self.s3_conf['public']:
                    mp = bucket.initiate_multipart_upload(destpath, metadata=metadata, policy='public-read')
                else:
                    mp = bucket.initiate_multipart_upload(destpath, metadata=metadata)
                fp = open(sourcepath,'rb')
                fp_num = 0
                while (fp.tell() < filesize):
                    fp_num += 1
                    mp.upload_part_from_file(fp, fp_num, num_cb=10, size=PART_SIZE)

                mp.complete_upload()

            else:
                k = boto.s3.key.Key(bucket)
                k.key = destpath
                k.setMetadata(metadata)
                if self.s3_conf['public']:
                    k.set_acl('public-read')
                k.set_contents_from_filename(sourcepath, num_cb=10)

    """

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
        #inSpatialRef = osr.SpatialReference()
        #inSpatialRef.ImportFromEPSG(fromProjection)
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
        outLayer = outDataSet.CreateLayer('footprint_4326', geom_type=ogr.wkbMultiPolygon)

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

    :param xml_file: Path to an xml file containing the Gemini metadata to be translated into a json blob
    :return: A JSON representation of the provided Gemini XML file
    """
    def xml_to_json(self, xml_file):
        t = etree.parse(xml_file)
        r = t.getroot()

        # Setup some common id strings
        characterString = '{%s}%s' % (r.nsmap['gco'], 'CharacterString')
        dateTimeString = '{%s}%s' % (r.nsmap['gco'], 'DateTime')
        dateString = '{%s}%s' % (r.nsmap['gco'], 'Date')
        distanceString = '{%}s%s' % (r.nsmap['gco'], 'Distance')
        decimalString = '{%s}%s' % (r.nsmap['gco'], 'Decimal')

        limitationsOnPublicAccess = ''
        useConstraints = ''

        for c in r.find('%s%s', (r.nsmap['gmd'], 'identificationInfo')).find('%s%s', (r.nsmap['gmd'], 'MD_DataIdentification')).findall('%s%s', (r.nsmap['gmd'], 'resourceConstraints')):
            if c.find('%s%s', (r.nsmap['gmd'], 'MD_LegalConstraints')) is not None:
                limitationsOnPublicAccess = c.find('%s%s', (r.nsmap['gmd'], 'MD_LegalConstraints')).find('%s%s', (r.nsmap['gmd'], 'otherConstraints')).find(characterString).text
            elif c.find('%s%s', (r.nsmap['gmd'], 'MD_Constraints')) is not None:
                useConstraints = c.find('%s%s', (r.nsmap['gmd'], 'MD_Constraints')).find('%s%s', (r.nsmap['gmd'], 'useLimitation')).find(characterString).text

        stripParser = etree.XMLParser(remove_blank_text=True)  
        
        return {
            'ID': r.find('{%s}%s' % (r.nsmap['gmd'], 'fileIdentifier')).find(characterString).text,
            'Title': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'citation')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Citation')).find('{%s}%s' % (r.nsmap['gmd'], 'title')).find(characterString).text,
            'Abstract': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'abstract')).find(characterString).text,
            'TopicCategory': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'topicCategory')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_TopicCategoryCode')).text,
            'Keywords': [
                {
                    'Value': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'descriptiveKeywords')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_Keywords')).find('{%s}%s' % (r.nsmap['gmd'], 'keyword')).find(characterString).text,
                    'Vocab': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'descriptiveKeywords')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_Keywords')).find('{%s}%s' % (r.nsmap['gmd'], 'thesaurusName')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Citation')).find('{%s}%s' % (r.nsmap['gmd'], 'title')).find(characterString).text
                }
            ],
            'TemporalExtent': {
                'Begin': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'extent')).find('{%s}%s' % (r.nsmap['gmd'], 'EX_Extent')).find('{%s}%s' % (r.nsmap['gmd'], 'temporalElement')).find('{%s}%s' % (r.nsmap['gmd'], 'EX_TemporalExtent')).find('{%s}%s' % (r.nsmap['gmd'], 'extent')).find('{%s}%s' % (r.nsmap['gml'], 'TimePeriod')).find('{%s}%s' % (r.nsmap['gml'], 'beginPosition')).items()[0][1],
                'End': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'extent')).find('{%s}%s' % (r.nsmap['gmd'], 'EX_Extent')).find('{%s}%s' % (r.nsmap['gmd'], 'temporalElement')).find('{%s}%s' % (r.nsmap['gmd'], 'EX_TemporalExtent')).find('{%s}%s' % (r.nsmap['gmd'], 'extent')).find('{%s}%s' % (r.nsmap['gml'], 'TimePeriod')).find('{%s}%s' % (r.nsmap['gml'], 'endPosition')).items()[0][1]
            },
            'DatasetReferenceDate': time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'Lineage': r.find('{%s}%s' % (r.nsmap['gmd'], 'dataQualityInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'DQ_DataQuality')).find('{%s}%s' % (r.nsmap['gmd'], 'lineage')).find('{%s}%s' % (r.nsmap['gmd'], 'LI_Lineage')).find('{%s}%s' % (r.nsmap['gmd'], 'statement')).find(characterString).text,
            'SpatialResolution': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'spatialResolution')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_Resolution')).find('{%s}%s' % (r.nsmap['gmd'], 'distance')).find(distanceString).text,
            'ResourceLocator': '', 
            'AdditionalInformationSource': '', 
            'DataFormat': r.find('{%s}%s' % (r.nsmap['gmd'], 'distributionInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_Distribution')).find('{%s}%s' % (r.nsmap['gmd'], 'distributionFormat')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_Format')).find('{%s}%s' % (r.nsmap['gmd'], 'name')).find(characterString).text,
            'ResponsibleOrganisation': {
                'Name': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'pointOfContact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'organisationName')).find(characterString).text,
                'Role': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'pointOfContact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'role')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_RoleCode')).text,
                'Email': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'pointOfContact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'contactInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Contact')).find('{%s}%s' % (r.nsmap['gmd'], 'address')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Address')).find('{%s}%s' % (r.nsmap['gmd'], 'electronicMailAddress')).find(characterString).text,
                'Telephone': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'pointOfContact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'contactInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Contact')).find('{%s}%s' % (r.nsmap['gmd'], 'phone')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Telephone')).find('{%s}%s' % (r.nsmap['gmd'], 'voice')).find(characterString).text,
                'Website': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'pointOfContact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'contactInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Contact')).find('{%s}%s' % (r.nsmap['gmd'], 'onlineResource')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_OnlineResource')).find('{%s}%s' % (r.nsmap['gmd'], 'linkage')).find('{%s}%s' % (r.nsmap['gmd'], 'URL')).text,
                'Address': {
                    'DeliveryPoint': r.find('{%s}%s' % (r.nsmap['gmd'],'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'pointOfContact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'contactInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Contact')).find('{%s}%s' % (r.nsmap['gmd'], 'address')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Address')).find('{%s}%s' % (r.nsmap['gmd'], 'deliveryPoint')).find(characterString).text,
                    'City': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'pointOfContact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'contactInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Contact')).find('{%s}%s' % (r.nsmap['gmd'], 'address')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Address')).find('{%s}%s' % (r.nsmap['gmd'], 'city')).find(characterString).text,
                    'PostalCode': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'pointOfContact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'contactInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Contact')).find('{%s}%s' % (r.nsmap['gmd'], 'address')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Address')).find('{%s}%s' % (r.nsmap['gmd'], 'postalCode')).find(characterString).text,
                    'Country': r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'pointOfContact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'contactInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Contact')).find('{%s}%s' % (r.nsmap['gmd'], 'address')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Address')).find('{%s}%s' % (r.nsmap['gmd'], 'country')).find(characterString).text
                }
            },
            'LimitationsOnPublicAccess': limitationsOnPublicAccess,
            'UseConstraints': useConstraints,
            'Copyright': '',
            'SpatialReferenceSystem': r.find('{%s}%s' % (r.nsmap['gmd'], 'referenceSystemInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_ReferenceSystem')).find('{%s}%s' % (r.nsmap['gmd'], 'referenceSystemIdentifier')).find('{%s}%s' % (r.nsmap['gmd'], 'RS_Identifier')).find('{%s}%s' % (r.nsmap['gmd'], 'code')).find(characterString).text,
            'Extent': {
                'Value': 'urn:ogc:def:crs:EPSG::4326',
                'Vocab': 'http://www.epsg-registry.org/'
            },
            'MetadataDate': time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'MetadataPointOfContact': {
                'Name': r.find('{%s}%s' % (r.nsmap['gmd'], 'contact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'organisationName')).find(characterString).text,
                'Role': r.find('{%s}%s' % (r.nsmap['gmd'], 'contact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'role')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_RoleCode')).text,
                'Email': r.find('{%s}%s' % (r.nsmap['gmd'], 'contact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'contactInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Contact')).find('{%s}%s' % (r.nsmap['gmd'], 'address')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Address')).find('{%s}%s' % (r.nsmap['gmd'], 'electronicMailAddress')).find(characterString).text,
                'Telephone': r.find('{%s}%s' % (r.nsmap['gmd'], 'contact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'contactInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Contact')).find('{%s}%s' % (r.nsmap['gmd'], 'phone')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Telephone')).find('{%s}%s' % (r.nsmap['gmd'], 'voice')).find(characterString).text,
                'Website': r.find('{%s}%s' % (r.nsmap['gmd'], 'contact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'contactInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Contact')).find('{%s}%s' % (r.nsmap['gmd'], 'onlineResource')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_OnlineResource')).find('{%s}%s' % (r.nsmap['gmd'], 'linkage')).find('{%s}%s' % (r.nsmap['gmd'], 'URL')).text,
                'Address': {
                    'DeliveryPoint': r.find('{%s}%s' % (r.nsmap['gmd'], 'contact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'contactInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Contact')).find('{%s}%s' % (r.nsmap['gmd'], 'address')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Address')).find('{%s}%s' % (r.nsmap['gmd'], 'deliveryPoint')).find(characterString).text,
                    'City': r.find('{%s}%s' % (r.nsmap['gmd'], 'contact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'contactInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Contact')).find('{%s}%s' % (r.nsmap['gmd'], 'address')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Address')).find('{%s}%s' % (r.nsmap['gmd'], 'city')).find(characterString).text,
                    'PostalCode': r.find('{%s}%s' % (r.nsmap['gmd'], 'contact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'contactInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Contact')).find('{%s}%s' % (r.nsmap['gmd'], 'address')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Address')).find('{%s}%s' % (r.nsmap['gmd'], 'postalCode')).find(characterString).text,
                    'Country': r.find('{%s}%s' % (r.nsmap['gmd'], 'contact')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_ResponsibleParty')).find('{%s}%s' % (r.nsmap['gmd'], 'contactInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Contact')).find('{%s}%s' % (r.nsmap['gmd'], 'address')).find('{%s}%s' % (r.nsmap['gmd'], 'CI_Address')).find('{%s}%s' % (r.nsmap['gmd'], 'country')).find(characterString).text
                }
            },
            'ResourceType': r.find('{%s}%s' % (r.nsmap['gmd'], 'hierarchyLevel')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_ScopeCode')).text,
            'BoundingBox': {
                'North': float(r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'extent')).find('{%s}%s' % (r.nsmap['gmd'], 'EX_Extent')).find('{%s}%s' % (r.nsmap['gmd'], 'geographicElement')).find('{%s}%s' % (r.nsmap['gmd'], 'EX_GeographicBoundingBox')).find('{%s}%s' % (r.nsmap['gmd'], 'northBoundLatitude')).find(decimalString).text),
                'South': float(r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'extent')).find('{%s}%s' % (r.nsmap['gmd'], 'EX_Extent')).find('{%s}%s' % (r.nsmap['gmd'], 'geographicElement')).find('{%s}%s' % (r.nsmap['gmd'], 'EX_GeographicBoundingBox')).find('{%s}%s' % (r.nsmap['gmd'], 'southBoundLatitude')).find(decimalString).text),
                'East': float(r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'extent')).find('{%s}%s' % (r.nsmap['gmd'], 'EX_Extent')).find('{%s}%s' % (r.nsmap['gmd'], 'geographicElement')).find('{%s}%s' % (r.nsmap['gmd'], 'EX_GeographicBoundingBox')).find('{%s}%s' % (r.nsmap['gmd'], 'eastBoundLongitude')).find(decimalString).text),
                'West': float(r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).find('{%s}%s' % (r.nsmap['gmd'], 'extent')).find('{%s}%s' % (r.nsmap['gmd'], 'EX_Extent')).find('{%s}%s' % (r.nsmap['gmd'], 'geographicElement')).find('{%s}%s' % (r.nsmap['gmd'], 'EX_GeographicBoundingBox')).find('{%s}%s' % (r.nsmap['gmd'], 'westBoundLongitude')).find(decimalString).text)
            },
            'RawMetadata': etree.tostring(etree.XML(etree.tostring(r), stripParser))
        }

if __name__ == "__main__":
    downloader = ProductDownloader('config.yaml', None)
    downloader.destroy()