import luigi
import datetime
import os
import json
import constants
from product_list_manager import ProductListManager
from product_downloader import ProductDownloader
from luigi.util import inherits
from luigi.util import requires
from luigi.s3 import S3Target
from datetime import timedelta

FILE_ROOT = 's3://jncc-data/luigi/sentinel/esa_downloader'

def getWorkPath(date):
    return os.path.join(FILE_ROOT, date.strftime("%Y-%m-%d"))

class LastAvailableProductsList(luigi.ExternalTask):
    debug = luigi.BooleanParameter()
    seedDate = luigi.DateParameter(default=DEFAULT_DATE)
    runDate = luigi.DateParameter(default=datetime.datetime.now())

    def output(self):
        d = self.runDate - timedelta(days=1)
        filePath = os.path.join(getWorkPath(d), 'available.json')
        
        return S3Target(filePath)
        
@inherits(LastAvailableProductsList)
class CreateAvailableProductsList(luigi.Task):

    def run(self):
        lastList = {"products":[]}
        workPath = getWorkPath(self.runDate)

        # If not seeding get last ingestion list from LastAvailableProductsList task
        if self.seedDate == DEFAULT_DATE:
            lastListTarget = yield LastAvailableProductsList()
            with self.input().open() as l:
                lastList = json.load(l)

        with self.output().open('w') as productList:
            listManager = ProductListManager(self.debug)
            listManager.create_list(workPath,lastList, productList, self.seedDate)

    def output(self):
        workPath = getWorkPath(self.runDate)
        filePath = os.path.join(workPath,'available.json')
        
        return S3Target(filePath)

@requires(CreateAvailableProductsList)
class DownloadAvailableProducts(luigi.Task):

    def run(self):

        downloader = ProductDownloader(self.debug)
        result = None

        with self.input().open() as productList: 
            result = downloader.download_products(productList, self.runDate)

        if not result is None:
            with self.output().open('w') as successFile:
                if self.debug:
                    msg = "DEBUG MODE: virtual download test succeded for %s files -See DownloadAvailableProducts-log for details" % (result,)
                else:
                    msg = "Downloaded %s products" % (result,)
                successFile.write(msg)

    def output(self):
        workPath = getWorkPath(self.runDate)
        filePath = os.path.join(workPath, '_success.json')
        
        return S3Target(filePath)
        
if __name__ == '__main__':
    luigi.run()
