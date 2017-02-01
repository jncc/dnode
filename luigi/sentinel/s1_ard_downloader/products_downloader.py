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
import shutil
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
    with open(filename, 'rb') as stream:
        for chunk in iter(lambda: stream.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

class ProductDownloader:
    def __init__(self, config, logger, tempdir):
        self.config = config
        self.logger = logger

        self.temp = tempdir
        self.debug = self.config.get('debug')

        datahub_conf = self.config.get('datahub')
        self.client = DatahubClient(datahub_conf['base_url'], datahub_conf['download_chunk_size'], datahub_conf['search_zone_id'], datahub_conf['username'], datahub_conf['password'], None)
        self.s3_conf = self.config.get('s3')

        self.database_conf = self.config.get('database')
        self.db_conn = psycopg2.connect(host=self.database_conf['host'], dbname=self.database_conf['dbname'], user=self.database_conf['username'], password=self.database_conf['password']) 
    
    """
    Cleanup task
    """
    def destroy(self):
        self.db_conn.close()

    """
    Attaches a failure message to the failure output stream
    """
    def __attach_failure(failures, item, reason):
        item['reason'] = reason
        failures.append(item)

    """
    Download from the supplied list of available products, upload the results to S3 and write the progress to a
    database table

    :param available: JSON list of available products for download
    :param downloaded: Ouput file stream to record completed downloads to
    """
    def downloadProducts(self, available, downloaded, failures):
        available_list = json.load(available)
        downloaded = []
        failed = []
        extracted_path = os.path.join(self.temp, 'extracted')

        # Pass over list of available items and look for an non downloaded ID
        for item in available_list:
            # Download item from the remote repo
            filename = os.path.join(self.temp, '%s.zip' % item['filename'])
            self.logger.info('Downloading %s from API' % filename)
            self.client.download_product(item['product_id'], filename)

            self.logger.info('Extracting downloaded file %s' % filename)
            # Extract all files from source
            with zipfile.ZipFile(filename, 'r') as product_zip:
                # Remove existing extracted directory if it exists
                if os.path.isdir(extracted_path):
                    shutil.rmtree(extracted_path)
                os.makedirs(extracted_path)
                product_zip.extractall(extracted_path)
            tif_file = item['filename'].replace('.SAFE.data', '.tif')
            
            remote_checksum = client.get_checksum(item['product_id'])
            local_checksum = calculate_checksum(os.path.join(os.path.join(extracted_path, item['filename']), tif_file))

            if self.debug:
                self.logger.debug('Remote Checksum is %s | Local Checksum is %s | Checksums %s' % (remote_checksum, local_checksum, 'Match' if remote_checksum == local_checksum else 'Don\'t Match'))

            if remote_checksum == local_checksum:
                try:
                    # Extract footprints from downloaded files
                    (osgb_geojson, osni_geojson) = self.extract_footprints_wgs84(item, extracted_path)
                    # Extract Metadata
                    (osgb_metadata, osni_metadata) = self.extract_metadata(item, extracted_path)
                    # Upload all files to S3 and get paths to uploaded data, optionally extract OSNI data to save as a seperate product

                    beginStamp = time.strptime(osgb_metadata['TemporalExtent']['Begin'], '%Y-%m-%dT%H:%M:%s')

                    destPath = '%d/%02d/%s' % (beginStamp.tm_year, beginStamp.tm_mon, self.s3_conf['bucket_dest_path'])

                    representations = self.upload_dir_to_s3(extracted_path, destPath)
                    representations = self.extract_representations(representations, item['filename'])
                    
                    # Write the progress to the catalog table
                    id = self.__write_progress_to_database(item, metadata=osgb_metadata, representations=representations['osgb'], success=True, geom=osgb_geojson)
                    # If we have more tha one representation (i.e. OSNI data exists) then add an additional record to the catalog for that data
                    if len(representations['osni']) > 0:
                        if self.debug:
                            self.logger.debug('OSNI representation exists for %s' % item['filename'])
                            
                        if osni_geojson is None:
                            self.__write_progress_to_database(item, metadata=osni_metadata, representations=representations['osni'], success=True, additional={'relatedTo': id}, geom=osgb_geojson)
                        else:
                            self.__write_progress_to_database(item, metadata=osni_metadata, representations=representations['osni'], success=True, additional={'relatedTo': id}, geom=osni_geojson)                    
                except RuntimeError as ex:
                    logger.error(repr(ex))
                    __attach_failure(failed, item, repr(ex))    
            else:
                __attach_failure(failed, item, 'Remote Checksum did not match Local Checksum')
            
            # Cleanup temp extracted directory
            shutil.rmtree(extracted)
            # Cleanup download file
            os.unlink(filename)

        self.client.logout()

        # Dump out failures if any exist
        if len(failed) > 1:
            failures.write(json.dumps(failed))
        else:
            ## TODO Should remove failures if empty
            failures.close()
        # Dump out the downloaded update
        downloaded.write(json.dumps(downloaded)) 
        
    """
    Extract metadata from the provided XML file(s), looks for an optional OSNI folder

    :param item: The item that we are downloading (sourced from the available products list)
    :param path: The path to base our xml paths on
    :return: A tuple with (osgb, osni) TopCat standard metadata JSON, OSNI will be None if no OSNI folder exists on the base path
    """
    def extract_metadata(self, item, path):
        osgb = self.xml_to_json(os.path.join(os.path.join(path, item['filename']), item['filename'].replace('.SAFE.data', '_metadata.xml')))
        osni = None

        if os.path.isfile(os.path.join(os.path.join(os.path.join(path, item['filename']), 'OSNI1952'), item['filename'].replace('.SAFE.data', '_OSNI1952_metadata.xml'))):
            osni = self.xml_to_json(os.path.join(os.path.join(os.path.join(path, item['filename']), 'OSNI1952'), item['filename'].replace('.SAFE.data', '_OSNI1952_metadata.xml')))
        
        return (osgb, osni)


    """
    Extract WGS84 footprints from the given footprints, looks for an OSNI folder to extract any additional footprints from, will save the outputs as 
    *.wgs84.geojson

    :param item: The item that we are downloading (sourced from the available products list)
    :param path: The path to base our footprint paths on
    :return: Returns the raw GeoJSON as a tuple (osgb, osni), osni will be None if no OSNI data exists
    """
    def extract_footprints_wgs84(self, item, path):
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
        
        self.reproject_footprint(footprint_osgb_path, footprint_osgb_output_path)

        with open(footprint_osgb_output_path) as osgb_output:
            osgb = json.load(osgb_output)

        # Attempt to extract any potential OSNI geometry
        footprint_osni_path = os.path.join(os.path.join(os.path.join(os.path.join(path, item['filename']), 'OSNI1952'), 'Footprint'), item['filename'].replace('.SAFE.data', '_OSNI1952_footprint'))
        footprint_osni_output_path = None
        osni = None

        if os.path.isfile('%s.shp' % footprint_osni_path):
            footprint_osni_path = '%s.shp' % footprint_osni_path
            footprint_osni_output_path = footprint_osni_path.replace('.shp', '_wgs84.geojson')
            self.reproject_footprint(footprint_osni_path, footprint_osni_output_path)
        elif os.path.isfile('%s.geojson' % footprint_osni_path):
            footprint_osni_path = '%s.geojson' % footprint_osni_path
            footprint_osni_output_path = footprint_osni_path.replace('.geojson', '_wgs84.geojson')            
            self.reproject_footprint(footprint_osni_path, footprint_osni_output_path)
        
        if footprint_osni_output_path is not None:
            with open(footprint_osni_output_path) as osni_out:
                osni = json.load(osni_out)

        return (osgb, osni)

    """
    Extract OSGB and OSNI S3 metadata from a give represenations block created by the __upload_dir_to_s3 function

    :param representations: Represenations block generated by __upload_dir_to_s3
    :return: An array of representations for each non standard projection system (denoted by folder structure)
    """
    def extract_representations(self, representations, name):
        outputs = {
            'osgb': [],
            'osni': []
        }
        for r in representations:
            if r['path'].startswith('%s/%s/OSNI1952' % (self.s3_conf['bucket_dest_path'], name)):
                outputs['osni'].append(r)
            else:
                outputs['osgb'].append(r)
        return outputs

    """
    Write the progress of this download to the database (i.e. failure, etc...)

    :param item: The item that we want to record progress against (item pulled from API)
    :param representations: The files that we have uploaded to S3 and some basic metadata about them
    :param success: If the download / upload was successfull or not
    :param additional: Any additional metadata that we need (realtedTo uuid for OSNI uploads)
    :param geom: GeoJSON represenation of the footprint of the data we are recording progress against
    """
    def __write_progress_to_database(self, item, metadata={}, representations={}, success=True, additional=None, geom=None):
        cur = self.db_conn.cursor()
        cur.execute("SELECT properties->>'product_id' FROM sentinel_ard_backscatter WHERE properties->>'product_id' = %s;", ((str(item['product_id']))),)
        existing = cur.fetchone()

        retVal = None

        if 'id' in metadata:
            # Grab the UUID from the metadata if possible, if not create one
            uuid_str = metadata['ID']
            try:
                val = uuid.UUID(uuid_str, version=4)
            except ValueError:
                uuid_str = str(uuid.uuid4())
                metadata['ID'] = uuid_str
        else:
            # Possibly blank metadata, success is very false here so set it that way
            success = False
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
                "product_id": item['product_id'],
                "name": item['filename']
            }

            if geom is None:
                cur.execute("INSERT INTO sentinel_ard_backscatter VALUES (%s, %s, %s, %s, %s, null) RETURNS id", (uuid_str,
                    self.database_conf.collection_version_uuid, json.dumps(metadata), json.dumps(props), json.dumps(representations), ))
            else:
                cur.execute("INSERT INTO sentinel_ard_backscatter VALUES (%s, %s, %s, %s, %s, ST_GeomFromGeoJSON(%s)) RETURNS id", (uuid_str,
                    self.database_conf.collection_version_uuid, json.dumps(metadata), json.dumps(props), json.dumps(representations), geom, ))
            
            retVal = cur.fetchone()[0]
        
        cur.close()
        return retVal

    """
    Upload a directory to S3 (recursive)

    :param sourcedir: The source directory to start uploading files from
    :param destpath: The destination path of files on S3
    :param representations: A list of files that have been uploaded, where they are and what sort of type that file is
    :return: The generated list of the represenations from this upload
    """
    def upload_dir_to_s3(self, sourcedir, destpath, representations={'s3': []}, additionalMetadata=None):
        for item in os.listdir(sourcedir):
            item_path = os.path.join(sourcedir, item)
            if os.path.isdir(item_path):
                representations = self.upload_dir_to_s3(item_path, '%s/%s' % (destpath, item), representations)
            else:
                path = '%s/%s' % (destpath, item)
                representations['s3'].append({
                    'bucket': self.s3_conf['bucket'],
                    'region': self.s3_conf['region'],
                    'path': path,
                    'url': 'https://s3-%s.amazonaws.com/%s%s' % (self.s3_conf['region'], self.s3_conf['bucket'], path),
                    'type': self.__get_file_type(os.path.splitext(item)[1])
                })

                # If we aren't in debug mode, upload file to S3
                if not self.debug:
                    self.__copy_file_to_s3(item_path, path, additionalMetadata)
                else:
                    self.logger.debug('Would upload %s to %s' % (item_path, path))

        return representations

    """
    Based on a file extension return a vague type of file to be added as metadata in the database catalog

    :param ext: The extension of a file you wanted to label
    :return: A vague type for that sort of file
    """
    def __get_file_type(self, ext):
        if ext == '.tif':
            return 'data'
        elif ext == '.geojson':
            return 'footprint'
        elif ext == '.cpg' or ext == '.dbf' or ext == '.prj' or ext =='.qpj' or ext == '.shp' or ext == '.shx':
            return 'footprint-shapefile'
        elif ext == '.xml':
            return 'metadata'
        elif ext == '.jpeg' or ext == '.jpg' or ext == '.png':
            return 'preview'
        else:
            return 'unknown'

    """
    Copy a file up to a S3 from the sourcepath to a given filename (full path name), will
    calculate md5 checksums and upload with the file

    :param sourcepath: The source path of the file to upload
    :param filename: The destination path of the file being uploaded
    """
    def __copy_file_to_s3(self, sourcepath, filename, additionalMetadata=None):
        #max size in bytes before uploading in parts. between 1 and 5 GB recommended
        MAX_SIZE = 5000000000
        #size of parts when uploading in parts
        PART_SIZE = 100000000        

        amazon_key_Id = self.s3_conf['access_key']
        amazon_key_secret = self.s3_conf['secret_access_key']

        conn = boto.s3.connect_to_region(self.s3_conf['region'], aws_access_key_id=amazon_key_Id, aws_secret_access_key=amazon_key_secret, is_secure=True)

        bucket_name = self.s3_conf['bucket']
        amazonDestPath = self.s3_conf['bucket_dest_path']
        bucket = conn.get_bucket(bucket_name)

        destpath = os.path.join(amazonDestPath, filename)
        
        if additionalMetadata is not None:
            metadata = additionalMetadata
        else:
            metadata = {}

        metadata['md5'] = calculate_checksum(sourcepath)
        metadata['uploaded'] = time.strftime('%Y-%m-%dT%H:%M:%SZ')

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
                k.set_metadata('md5', metadata['md5'])
                k.set_metadata('uploaded', metadata['uploaded'])
                if self.s3_conf['public']:
                    k.set_acl('public-read')
                k.set_contents_from_filename(sourcepath, num_cb=10)

    """
    Reprojects a given file into a GeoJSON file of the provided projection (default: EPSG:4326)

    :param inFile: Path to the input file to reproject
    :param outFile: Path to the output file
    :param toProjection: EPSG number to reproject to (defualt: 4326 [WGS84])
    """
    def reproject_footprint(self, inFile, outFile, toProjection=4326):
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
    Parse the standard Gemini metadata coming from the API and the downloads into a Topcat Standard JSON representation
    for stowing in the database, includes a raw representation of the data for reprocessing reasons at a later date

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
        distanceString = '{%s}%s' % (r.nsmap['gco'], 'Distance')
        decimalString = '{%s}%s' % (r.nsmap['gco'], 'Decimal')

        limitationsOnPublicAccess = ''
        useConstraints = ''

        for c in r.find('{%s}%s' % (r.nsmap['gmd'], 'identificationInfo')).find('{%s}%s' % (r.nsmap['gmd'], 'MD_DataIdentification')).findall('{%s}%s' % (r.nsmap['gmd'], 'resourceConstraints')):
            if c.find('{%s}%s' % (r.nsmap['gmd'], 'MD_LegalConstraints')) is not None:
                limitationsOnPublicAccess = c.find('{%s}%s' % (r.nsmap['gmd'], 'MD_LegalConstraints')).find('{%s}%s' % (r.nsmap['gmd'], 'otherConstraints')).find(characterString).text
            elif c.find('{%s}%s' % (r.nsmap['gmd'], 'MD_Constraints')) is not None:
                useConstraints = c.find('{%s}%s' % (r.nsmap['gmd'], 'MD_Constraints')).find('{%s}%s' % (r.nsmap['gmd'], 'useLimitation')).find(characterString).text

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
    import logging
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger('products_downloader_main')
    logger.setLevel(logging.DEBUG)

    with open('config.yaml', 'r') as config, open('list.json', 'r') as available, open('output.json', 'w') as output, open('_failures.json', 'w') as failures:
            downloader = ProductDownloader(yaml.load(config), logger, './temp')
            downloader.downloadProducts(available, output, failures)
            downloader.destroy()
