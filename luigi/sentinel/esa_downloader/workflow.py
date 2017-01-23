import luigi
import datetime
import os
from product_list_manager import ProductListManager
from product_downloader import ProductDownloader
from luigi.s3 import S3Target
from luigi.util import requires
from datetime import timedelta

#S3_ROOT = 's3://jncc-data/workflows/sentinel-download/'
FILE_ROOT = '/home/felix/temp/'

class LastAvailableProductsList(luigi.ExternalTask):
    debug = luigi.BooleanParameter()
    seeding = luigi.BooleanParameter()
    runDate = luigi.DateParameter(default=datetime.datetime.now())

    def output(self):
        d = self.runDate - timedelta(days=1)
        # s3Path = S3_ROOT +  d.strftime("%Y-%m-%d") + '/available.json'
        filePath = os.path.join(os.path.join(FILE_ROOT, d.strftime("%Y-%m-%d")), 'available.json')

        # return S3Target(s3Path)
        return luigi.LocalTarget(filePath)

@requires(LastAvailableProductsList)
class CreateAvailableProductsList(luigi.Task):

    def run(self):
        workPath = os.path.join(FILE_ROOT, self.runDate.strftime("%Y-%m-%d"))

        with self.input().open() as lastList, self.output().open('w') as productList:
            listManager = ProductListManager(workPath, self.debug)
            listManager.create_list(self.runDate,lastList, productList, self.seeding)

    def output(self):
        workPath = os.path.join(FILE_ROOT, self.runDate.strftime("%Y-%m-%d"))
        filePath = workPath + '/available.json'
        return luigi.LocalTarget(filePath)
        #s3Path = S3_ROOT + runDate.strftime("%Y-%m-%d") + '/available.json'
        # return S3Target(s3Path)

@requires(CreateAvailableProductsList)
class DownloadAvailableProducts(luigi.Task):
  
    def run(self):
        workPath = os.path.join(FILE_ROOT, self.runDate.strftime("%Y-%m-%d"))

        downloader = ProductDownloader(self.debug, workPath)
        result = None

        with self.input().open() as productList:
            result = downloader.download_products(productList)

        with self.output().open('w') as successFile:
            if self.debug:
                msg = "DEBUG MODE: virtual download test succeded for %s files -See DownloadAvailableProducts-log for details" % (result,)
            else:
                msg = "Downloaded %s products" % (result,)
            successFile.write(msg)

    def output(self):
        filePath = os.path.join(os.path.join(FILE_ROOT, self.runDate.strftime("%Y-%m-%d")), '_success.json')
        return luigi.LocalTarget(filePath)
        # s3Path = S3_ROOT + runDate.strftime("%Y-%m-%d") + '/_success.json'
        # return S3Target(s3Path)
        
if __name__ == '__main__':
    luigi.run()
