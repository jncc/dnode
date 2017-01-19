import json
import boto
import yaml
import psycopg2
import os
import hashlib
import zipfile
import time
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

        if os.path.isfile(config_file):
            with open("config.yaml", 'r') as conf:
                self.config = yaml.load(conf)

                datahub_conf = self.config.get('datahub')
                if datahub_conf is not None \
                    and 'search_zone_id' in datahub_conf \
                    and 'username' in datahub_conf \
                    and 'password' in datahub_conf 
                    self.client = new new DatahubClient(datahub_conf['search_zone_id'], datahub_conf['username'], datahub_conf['password'], None)
                else:
                    raise RuntimeError('Config file has invalid datahub entries')

                self.s3_conf = self.config.get('s3')
                if !(self.s3_conf is not None \
                    and 'access_key' in self.s3_conf \
                    and 'secret_access_key' in self.s3_conf)
                    raise RuntimeError('Config file has invalid s3 entries')
                
                database_conf = self.config.get('database')
                if self.database_conf is not None \
                    and 'host' in database_conf \
                    and 'dbname' in database_conf \
                    and 'username' in database_conf \
                    and 'password' in database_conf \
                    and 'table' in database_conf
                    self.db_conn = psycopg2.connect(host=database_conf['host'] dbname=database_conf['dbname'] user=database_conf['username'] password=database_conf['password'])
                else:
                    raise RuntimeError('Config file has missing database config entires')

        else:
            raise RuntimeError('Config file at [%s] was not found' % config_file)

    def downloadProducts(self, available, downloaded):  
        available_list = json.load(available)
        available_product_ids = [str(item.product_id) for item in available_list['available_products']]

        # Compare to already aquired products
        cur = self.conn.cursor()
        cur.execute("SELECT properties->>'product_id' FROM sentinel_ard_backscatter WHERE properties->>'product_id' not in %s AND properties->>'downloaded' != 'true';", (tuple(available_product_ids),)))
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
                    # Upload all files to S3
                    self.uploadDir(extracted_path, '%s/%s' % (self.s3_conf['bucket_dest_path'], item.name))
                    
                    item.databaseID = 
            else:
                ## TODO: Increment attempt record / error recovery
                self.__write_progress_to_database(item, False)

        downloaded.write(json.dumps(downloaded)) 

    def __write_progress_to_database(self, item, success=True):
        cur = self.conn.cursor()
        cur.execute("SELECT properties->>'product_id' FROM sentinel_ard_backscatter WHERE properties->>'product_id' = %s;", ((item.product_id)),))))
        existing = cur.fetchone()

        if existing is not None:
            # Entry exists
            
        else:
            # Entry does not exist
            


    def __upload_dir_to_s3(self, sourcedir, destpath):
        for item in os.listdir(sourcedir):
            item_path = os.path.join(sourcedir, item)
            if os.path.isdir(item_path):
                self.__upload_dir_to_s3(item_path, '%s/%s' % (destpath, item))
            else:
                self.__copy_file_to_s3(item_path, '%s/%s' % (destpath, item))

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

            
                

    # - Read available.json
    # - Log into the api
    # - for each product in available products list
    # 	- Download product to temporary area
    # 	- Get checksum for product from api get_checksum 
    # 	- if checksum ok
    # 		Move file to s3 repository for s1 ard products
    # 		write product id to downloded products list
    # 	  else
    # 	  	Discard
    # 		log?
    # 	- Write out 