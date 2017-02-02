import luigi
import datetime
import os
import json
from product_list_manager import ProductListManager
from product_downloader import ProductDownloader
from luigi.s3 import S3Target
from datetime import timedelta

FILE_ROOT = 's3://jncc-data/luigi/sentinel/esa_downloader'

def getWorkPath(date):
    return os.path.join(FILE_ROOT, date.strftime("%Y-%m-%d"))

class parameters(luigi.Config)
    debug = luigi.BooleanParameter()
    seedDate = luigi.DateParameter()
    runDate = luigi.DateParameter(default=datetime.datetime.now())
    workPath = getWorkPath(runDate)

class LastAvailableProductsList(parameters, luigi.ExternalTask):

    def output(self):
        d = self.runDate - timedelta(days=1)
        filePath = os.path.join(os.path.join(getWorkPath(d), 'available.json')
        
        return S3Target(filePath)
        

class CreateAvailableProductsList(parameters, luigi.Task):

    def run(self):
        lastList = {}

        # If not seeding get last ingestion list
        if seedDate is None:
            lastListTarget = yield LastAvailableProductsList()
            with self.input().open() as l:
                lastList = json.load(l)

        with self.output().open('w') as productList:
            listManager = ProductListManager(self.debug)
            listManager.create_list(self.workPath,lastList, productList, self.seeding)

    def output(self):
        filePath = os.path.join(self.workPath,'available.json')
        
        return S3Target(filePath)


class DownloadAvailableProducts(parameters, luigi.Task):
  
    def requires(self):
            return CreateAvailableProductsList()

    def run(self):

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
        filePath = os.path.join(self.workPath, '_success.json')
        
        return S3Target(filePath)
        
if __name__ == '__main__':
    luigi.run()
