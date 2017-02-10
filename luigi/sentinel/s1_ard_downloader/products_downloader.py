import json
import boto
import yaml
import psycopg2
import os
import zipfile
import time
import ogr
import uuid
import shutil

from datahub_client import DatahubClient
from helpers import metadata as metadataHelper
from helpers import verification as verificationHelper
from helpers import footprint as footprintHelper
from helpers import database as databaseHelper
from helpers import s3 as s3Helper
 
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
    def __attach_failure(self, failures, item, reason):
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
        downloaded_list = []
        failed = []
        extracted_path = os.path.join(self.temp, 'extracted')

        # Pass over list of available items and look for an non downloaded ID
        for item in available_list:
            # Download item from the remote repo
            filename = os.path.join(self.temp, '%s.zip' % item['filename'])
            self.logger.info('-----------------------------------')
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
            
            remote_checksum = self.client.get_checksum(item['product_id'])
            local_checksum = verificationHelper.calculate_checksum(os.path.join(os.path.join(extracted_path, item['filename']), tif_file))

            if self.debug:
                self.logger.debug('Remote Checksum is %s | Local Checksum is %s | Checksums %s' % (remote_checksum, local_checksum, 'Match' if remote_checksum == local_checksum else 'Don\'t Match'))

            try:
                if remote_checksum == local_checksum:
                    # Extract footprints from downloaded files
                    (osgb_geojson, osni_geojson) = footprintHelper.extract_footprints_wgs84(item, extracted_path)
                    # Extract Metadata
                    (osgb_metadata, osni_metadata) = metadataHelper.extract_metadata(item, extracted_path)
                   
                    # Remove some known unwanted files before upload, i.e. aux.xml file from quicklook
                    if os.path.isfile(os.path.join(os.path.join(extracted_path, item['filename']), '%s' % item['filename'].replace('.SAFE.data', '_quicklook.jpg.aux.xml'))):
                        os.unlink(os.path.join(os.path.join(extracted_path, item['filename']), '%s' % item['filename'].replace('.SAFE.data', '_quicklook.jpg.aux.xml')))
                    
                    # Upload all files to S3 and get paths to uploaded data, optionally extract OSNI data to save as a seperate product
                    beginStamp = time.strptime(osgb_metadata['TemporalExtent']['Begin'], '%Y-%m-%dT%H:%M:%S')

                    destPath = os.path.join(self.s3_conf['bucket_dest_path'], '%d/%02d' % (beginStamp.tm_year, beginStamp.tm_mon))

                    self.logger.info('Uploading to destination path: %s' % destPath)

                    representations = self.upload_dir_to_s3(extracted_path, destPath, {'s3': []}, {'productid': item['product_id']})
                    representations['region_split'] = self.extract_representations(representations['s3'], os.path.join(destPath, item['filename']))

                    self.logger.info('Uploaded to destination path, writing progress to database')
                    
                    # Write the progress to the catalog table
                    id = databaseHelper.write_progress_to_database(self.db_conn, self.database_conf['collection_version_uuid'], item, osgb_metadata, representations['region_split']['osgb'], osgb_geojson)
                    # If we have more tha one representation (i.e. OSNI data exists) then add an additional record to the catalog for that data
                    if len(representations['region_split']['osni']['s3']) > 0:
                        self.logger.info('OSNI representation exists for %s' % item['filename'])

                        additionalMetadata = {'relatedTo': osgb_metadata['ID']}

                        if osni_metadata is None:
                            # If no OSNI metadata exists copy the OSGB metadata, change its ID to a newly generated one and mark this in the additional metadata
                            osni_metadata = osgb_metadata
                            
                            # Attempt to bodge as much of the osgb metadata into the osni metadata, will still be gaps however
                            osni_metadata['ID'] = str(uuid.uuid4())
                            osni_metadata['Title'] = item['filename'].replace('.SAFE.data', '_OSNI1952.tif')
                            osni_metadata['SpatialReferenceSystem'] = 'http://www.opengis.net/def/crs/EPSG/0/29901'
                            # Pop the raw metadata as this is now generated metadata
                            osni_metadata.pop('RawMetadata', None)

                            additionalMetadata['osni_metadata_not_available'] = True

                        if osni_geojson is None:
                            # If no OSNI footprint exists copy the OSGB footprint and mark this in the additional metadata
                            osni_geojson = osgb_geojson
                            additionalMetadata['osni_footprint_not_available'] = True

                        if os.path.isfile(os.path.join(os.path.join(os.path.join(extracted_path, item['filename']), 'OSNI1952'), '%s' % item['filename'].replace('.SAFE.data', '_quicklook.jpg.aux.xml'))):
                            os.unlink(os.path.join(os.path.join(os.path.join(extracted_path, item['filename']), 'OSNI1952'), '%s' % item['filename'].replace('.SAFE.data', '_quicklook.jpg.aux.xml')))
                            
                        databaseHelper.write_progress_to_database(self.db_conn, self.database_conf['collection_version_uuid'], item, osni_metadata, representations['region_split']['osni'], osni_geojson, additional=additionalMetadata)
                    
                    item['representations'] = representations['region_split']
                    
                    downloaded_list.append(item)
                else:
                    self.__attach_failure(failed, item, 'Remote Checksum did not match Local Checksum')                    
            except RuntimeError as ex:
                logger.error(repr(ex))
                self.__attach_failure(failed, item, repr(ex))
            finally:                
                # Cleanup temp extracted directory
                shutil.rmtree(extracted_path)
                # Cleanup download file
                os.unlink(filename)
        
        self.logger.info('-----------------------------------')
        self.logger.info('Reached end of supplied products list, downloaded %d of %d [%d failures]' % (len(downloaded_list), len(available_list), len(failed)))
        self.logger.info('-----------------------------------')

        # Dump out failures if any exist
        if len(failed) > 0:
            failures.write(json.dumps(failed))
        else:
            ## TODO Should remove failures if empty
            failures.close()
        # Dump out the downloaded update
        downloaded.write(json.dumps(downloaded_list)) 
        
    """
    Extract OSGB and OSNI S3 metadata from a give represenations block created by the upload_dir_to_s3 function

    :param representations: Represenations block generated by upload_dir_to_s3
    :return: An array of representations for each non standard projection system (denoted by folder structure)
    """
    def extract_representations(self, representations, destpath):
        outputs = {
            'osgb': {
                's3': []
            },
            'osni': {
                's3': []
            }
        }
        for r in representations:
            if r['path'].startswith(os.path.join(destpath, 'OSNI1952')):
                outputs['osni']['s3'].append(r)
            else:
                outputs['osgb']['s3'].append(r)
        return outputs

    """
    Upload a directory to S3 (recursive)

    :param sourcedir: The source directory to start uploading files from
    :param destpath: The destination path of files on S3
    :param representations: A list of files that have been uploaded, where they are and what sort of type that file is
    :return: The generated list of the represenations from this upload
    """
    def upload_dir_to_s3(self, sourcedir, destpath, representations, additionalMetadata):
        for item in os.listdir(sourcedir):
            item_path = os.path.join(sourcedir, item)
            if os.path.isdir(item_path):
                representations = self.upload_dir_to_s3(item_path, '%s/%s' % (destpath, item), representations, additionalMetadata)
            else:
                path = '%s/%s' % (destpath, item)
                representations['s3'].append(s3Helper.get_representation(self.s3_conf['bucket'], self.s3_conf['region'], path, s3Helper.get_file_type(os.path.splitext(item)[1])))

                # If we aren't in debug mode, upload file to S3
                if not self.debug:
                    s3Helper.copy_file_to_s3(self.logger, self.s3_conf['access_key'], self.s3_conf['secret_access_key'], self.s3_conf['region'], self.s3_conf['bucket'], self.s3_conf['bucket_dest_path'], item_path, path, self.s3_conf['public'], additionalMetadata)
                else:
                    self.logger.debug('Would upload %s to %s' % (item_path, path))

        return representations

if __name__ == "__main__":
    import logging
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger('products_downloader_main')
    logger.setLevel(logging.DEBUG)

    with open('config.yaml', 'r') as config, open('list.json', 'r') as available, open('output.json', 'w') as output, open('_failures.json', 'w') as failures:
        downloader = ProductDownloader(yaml.load(config), logger, './temp')
        downloader.downloadProducts(available, output, failures)
        downloader.destroy()
