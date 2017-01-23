import json
import os
import zipfile
import pycurl
import boto
import boto.s3
import log_helper

from config_manager import ConfigManager
from catalog_manager import CatalogManager

class ProductDownloader:
    DOWNLOAD_URL_BASE = 'https://scihub.copernicus.eu/apihub/odata/v1'

    def __init__(self, debug, workPath):
        self.config = ConfigManager("cfg.ini")
        self.debug = debug
        self.workPath = workPath
        self.log = log_helper.setup_logging(self.workPath, 'DownloadAvailableProducts')


    def __download_product(self, product):
        uniqueId = product["uniqueId"]
        name = product["title"]

        url = "%s/Products('%s')/$value" % (self.DOWNLOAD_URL_BASE,uniqueId)
        
        tempPath = os.path.join(self.workPath, 'temp')
        if not os.path.isdir(tempPath):
            os.makedirs(tempPath)

        zipname = "%s.zip" % name
        tempFilename = os.path.join(tempPath,zipname)
        
        if self.debug:
            self.log.info("DEBUG: download url: %s, would create %s", url, tempFilename)
            return None

        try: 
            with open(tempFilename, 'wb') as f:
                c = pycurl.Curl()
                c.setopt(c.URL,url)
                c.setopt(c.FOLLOWLOCATION, True)
                c.setopt(c.SSL_VERIFYPEER, False)
                c.setopt(c.USERPWD, self.config.get_esa_credentials())
                c.setopt(c.WRITEFUNCTION,f.write)
                c.perform()
                c.close()

        except pycurl.error, e:
            msg = "%s product %s resulted in download error %s" % (downloadType, name, e.args[0])
            raise Exception(msg)
        
        return tempFilename

    def __verify_zip_file(self, productZipFile):
        if not zipfile.is_zipfile(productZipFile):
            return False

        with zipfile.ZipFile(productZipFile, 'r') as archive:
            if archive.testzip() is not None:
                return False

        return True

    def __copy_product_to_s3(self, sourcepath, filename):
        #max size in bytes before uploading in parts. between 1 and 5 GB recommended
        MAX_SIZE = 5000000000
        #size of parts when uploading in parts
        PART_SIZE = 100000000

        amazon_key_Id = accessKeyId
        amazon_key_secret = accessKeySecret

        conn = boto.s3.connect_to_region('eu-west-1', is_secure=True)

        bucket_name = self.config.getAmazonBucketName()
        amazonDestPath = self.config.getAmazonDestPath()
        bucket = conn.get_bucket(bucket_name)

        destpath = os.path.join(amazonDestPath, filename)

        if self.debug:
            self.log("DEBUG: Would copy %s to %s", sourcepath, amazonDestPath)
        else:
            if bucket.get_key(destpath) != None:
                bucket.delete_key(destpath)

            filesize = os.path.getsize(sourcepath)
            if filesize > MAX_SIZE:
                mp = bucket.initiate_multipart_upload(destpath)
                fp = open(sourcepath,'rb')
                fp_num = 0
                while (fp.tell() < filesize):
                    fp_num += 1
                    mp.upload_part_from_file(fp, fp_num, num_cb=10, size=PART_SIZE)

                mp.complete_upload()

            else:
                k = boto.s3.key.Key(bucket)
                k.key = destpath
                k.set_contents_from_filename(sourcepath, num_cb=10)
        
        return destpath

    def download_products(self, productListFile):
        productList = json.load(productListFile)

        downloadedProductCount = 0
        errorCount = 0

        with CatalogManager() as cat:
            for product in productList["products"]:
                # download product
                productZipFile = None
                try:
                    productZipFile = self.__download_product(product)
                except Exception as e: 
                    self.log.warn("Failed to download product %s with error %s ", product["title"], e)
                    continue

                if productZipFile is None and not self.debug:
                    continue

                # verify product
                if not self.debug:
                    verified = self.__verify_zip_file(productZipFile)
                    if not verified:
                        self.log.warn("Failed to download product %s with error invalid zip file", product["title"])
                        continue
                
                # transfer to s3
                if not self.debug:
                    product["location"] = self.__copy_product_to_s3(productZipFile, product["title"])
                    if product["location"] == '': 
                        continue
                else:
                    product["location"] = "DEBUG, NO PRODUCT DOWNLOADED"
                
                # add metadata to catalog
                cat.addProduct(product)

                downloadedProductCount = downloadedProductCount + 1

        return downloadedProductCount
        