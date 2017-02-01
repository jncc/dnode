import luigi
import datetime
import os
from product_list_manager import ProductListManager
from product_downloader import ProductDownloader
from luigi.s3 import S3Target
from luigi.util import requires
from datetime import timedelta

FILE_ROOT = 's3://jncc-data/workflows/sentinel-download/'
#FILE_ROOT = '/home/felix/temp/esadownloader'

def getWorkPath(date):
    return os.path.join(FILE_ROOT, date.strftime("%Y-%m-%d"))

class LastAvailableProductsList(luigi.ExternalTask):
    debug = luigi.BooleanParameter()
    seeding = luigi.BooleanParameter()
    runDate = luigi.DateParameter(default=datetime.datetime.now())

    def output(self):
        d = self.runDate - timedelta(days=1)
        filePath = os.path.join(os.path.join(getWorkPath(d), 'available.json')
        
        #return luigi.LocalTarget(filePath)
        return S3Target(filePath)
        

@requires(LastAvailableProductsList)
class CreateAvailableProductsList(luigi.Task):

    def run(self):
        workPath = getWorkPath(self.runDate)

        with self.input().open() as lastList, self.output().open('w') as productList:
            listManager = ProductListManager(self.debug)
            listManager.create_list(self.runDate,lastList, productList, self.seeding)

    def output(self):
        filePath = os.path.join(os.path.join(FILE_ROOT, self.runDate.strftime("%Y-%m-%d")),'available.json')
        
        #return luigi.LocalTarget(filePath)
        return S3Target(filePath)

@requires(CreateAvailableProductsList)
class DownloadAvailableProducts(luigi.Task):
  
    def run(self):
        workPath = getWorkPath(self.runDate)

        downloader = ProductDownloader(self.debug, runDate)
        result = None

        with self.input().open() as productList: 
            result = downloader.download_products(productList)

        if not result is None:
            with self.output().open('w') as successFile:
                if self.debug:
                    msg = "DEBUG MODE: virtual download test succeded for %s files -See DownloadAvailableProducts-log for details" % (result,)
                else:
                    msg = "Downloaded %s products" % (result,)
                successFile.write(msg)

    def output(self):
        filePath = os.path.join(os.path.join(FILE_ROOT, self.runDate.strftime("%Y-%m-%d")), '_success.json')
        
        #return luigi.LocalTarget(filePath)
        return S3Target(filePath)
        
if __name__ == '__main__':
    luigi.run()
