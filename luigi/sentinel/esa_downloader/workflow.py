import luigi
import datetime
import log_helper
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
    runDate = luigi.DateParameter(default=datetime.datetime.now())

    def output(self):
        d = self.runDate - timedelta(days=1)
        # s3Path = S3_ROOT +  d.strftime("%Y-%m-%d") + '/available.json'
        filePath = FILE_ROOT +  d.strftime("%Y-%m-%d") + '/available.json'

        # return S3Target(s3Path)
        return luigi.LocalTarget(filePath)

@requires(LastAvailableProductsList)
class CreateAvailableProductsList(luigi.Task):
    workPath = os.path.join(FILE_ROOT, self.runDate.strftime("%Y-%m-%d"))
    log = log_helper.setup_logging(workPath, 'CreateAvailableProductsList')

    def run(self):
        listManager = ProductListManager(log, debug)
        lastIngestionDate = None

        with self.input().open() as lastList, self.output().open('w') as productList:
            listManager.create_list(self.runDate,lastList, productList)

    def output(self):
        filePath = workPath + '/available.json'
        return luigi.LocalTarget(filePath)
        #s3Path = S3_ROOT + runDate.strftime("%Y-%m-%d") + '/available.json'
        # return S3Target(s3Path)

@requires(CreateAvailableProductsList)
class DownloadAvailableProducts(luigi.Task):
    workPath =  os.path.join(FILE_ROOT, self.runDate.strftime("%Y-%m-%d"))
    log = log_helper.setup_logging(workPath, 'DownloadAvailableProducts')
    
    def run(self):
        downloader = ProductDownloader(log, debug)
        result = None

        with self.input().open() as productList:
            result = downloader.download_products(productList, debug, workPath)
        
        if result is None:
            raise Exception("Download Failure")

        with self.output().open('w') as successFile:
            successFile.write('Success\n')

    def output(self):
        filePath = FILE_ROOT + self.runDate.strftime("%Y-%m-%d") + '/_success.json'
        return luigi.LocalTarget(filePath)
        # s3Path = S3_ROOT + runDate.strftime("%Y-%m-%d") + '/_success.json'
        # return S3Target(s3Path)
        
if __name__ == '__main__':
    luigi.run()
