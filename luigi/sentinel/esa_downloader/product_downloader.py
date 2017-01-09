import json
import os
import zipfile
import pycurl
import boto
import boto.s3

from config_manager import ConfigManager
from catalog_manager import CatalogManager

class ProductDownloader:
    DOWNLOAD_URL_BASE = 'https://scihub.copernicus.eu/apihub/odata/v1'

    def __init__(self):
        self.config = ConfigManager("cfg.ini")

    def __download_product(self, product):
        uniqueId = product["uniqueId"]
        name = product["title"]

        url = "%s/Products('%s')/$value" % (self.DOWNLOAD_URL_BASE,uniqueId)
        
        tempPath = self.config.get_temp_path()
        zipname = "%s.zip" % name
        tempFilename = os.path.join(tempPath,zipname)
        
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

    def __verify_zip_file(self, productZipFile):
        if not zipfile.is_zipfile(productZipFile):
            return False

        with zipfile.ZipFile(productZipFile, 'r') as archive:
            if archive.testzip() is not None:
                return False

        return True

    def __copy_product_to_s3(self, sourcepath, filename):
        bucketName = 'filestore-eodip'
        AmazonDestPath = 'sentinel'

        #max size in bytes before uploading in parts. between 1 and 5 GB recommended
        MAX_SIZE = 5000000000
        #size of parts when uploading in parts
        PART_SIZE = 100000000

        amazon_key_Id = accessKeyId
        amazon_key_secret = accessKeySecret

        conn = boto.s3.connect_to_region('eu-west-1', is_secure=True)

        bucket_name = self.config.getAmazonBucketName()

        bucket = conn.get_bucket(bucket_name)

        destpath = os.path.join(AmazonDestPath, filename)

        if bucket.get_key(destpath) != None:
            bucket.delete_key(destpath)

        filesize = os.path.getsize(sourcepath)
        if filesize > MAX_SIZE:
            # if self.args.verbose:
            #     print 'multipart upload'
            mp = bucket.initiate_multipart_upload(destpath)
            fp = open(sourcepath,'rb')
            fp_num = 0
            while (fp.tell() < filesize):
                fp_num += 1
                #print "uploading part %i" %fp_num
                mp.upload_part_from_file(fp, fp_num, num_cb=10, size=PART_SIZE)

            #sys.stdout.write('\n')
            #sys.stdout.flush()
            mp.complete_upload()

        else:
            k = boto.s3.key.Key(bucket)
            k.key = destpath
            k.set_contents_from_filename(sourcepath, num_cb=10)
        
        return destpath

    def download_products(self, productListFile):
        productList = json.load(productListFile)

        downloadedProductCount = None
        errorCount = 0

        with CatalogManager() as cat:
            for product in productList["products"]:
                # download product
                productZipFile = None
                try:
                    productZipFile = self.__download_product(product)
                except Exception as e: 
                    message = e.message
                    print message
                    #TODO: log silently without fialing, downloads will frequently fail
                    continue

                if productZipFile is None:
                    continue
                # verify product
                verified = self.__verify_zip_file(productZipFile)
                if not verified:
                    #TODO: log invalid zip file.
                    continue
                
                # transfer to s3
                product["location"] = self.__copy_product_to_s3(sourcepath, product["title"])
                if product["location"] == '': 
                    continue
                
                # add metadata to catalog
                cat.addProduct(product)

                downloadedProductCount = downloadedProductCount + 1

        return downloadedProductCount
        