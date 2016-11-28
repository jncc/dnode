import json
import os
import zipfile
import pycurl

from config_manager import ConfigManager

class ProductDownloader:
    DOWNLOAD_URL_BASE = 'https://scihub.copernicus.eu/apihub/odata/v1'

    def __init__(self):
        self.config = ConfigManager("cfg.ini")

    def download_product(product):
        uniqueId = product["uniqueId"]
        title = product["title"]

        tempPath = ConfigManager.get_temp_path()
        url = "%s/Products('%s')/$value" % (self.DOWNLOAD_URL_BASE,uniqueId)
        zipname = "%s.zip" % title

        try: 
            with open(tempFilename, 'wb') as f:
                c = pycurl.Curl()
                c.setopt(c.URL,url)
                c.setopt(c.FOLLOWLOCATION, True)
                c.setopt(c.SSL_VERIFYPEER, False)
                c.setopt(c.USERPWD,auth)
                c.setopt(c.WRITEFUNCTION,f.write)
                c.perform()
                c.close()

            if self.isValidZip(tempFilename):
                if self.args.verbose:
                    print "Moving product %s to output folder" % name
                self.archiveProduct(tempFilename, zipname, id)
            elif self.args.keepErroredFiles:
                if self.args.verbose:
                    print "Moving product %s to errored files it is not a valid zip" % name
                if os.path.exists(errorFileName):
                    os.remove(errorFileName)
                shutil.move(tempFilename,errorFileName)
            else:
                if self.args.verbose:
                    print "Removing product %s as it is not a valid zip" % name
                os.remove(tempFilename)

        except pycurl.error, e:
            msg = "%s product %s resulted in download error %s" % (downloadType, name, e.args[0])
            raise Exception(msg)

    def download_products(self, productListFile):
        productList = json.load(productListFile)

        downloadedProductCount = 0
        errorCount = 0
        for product in productList["products"]:
            # download product
            productZipFile = ''
            try:
                productZipFile = self.download_product(product)
            except e: 
                msg = e.msg
                #TODO: log silently without fialing, downloads will frequently fail

            # verify product
            verified = self.verify_product_file()
            # transfer to s3
            # add metadata to catalog


        return downloadedProductCount
        