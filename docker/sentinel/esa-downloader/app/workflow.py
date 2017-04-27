import luigi
import datetime
import os
import json
import constants
import logging

from product_list_manager import ProductListManager
from product_downloader import ProductDownloader
from luigi.util import inherits
from luigi.util import requires
from luigi.s3 import S3Target, S3Client
from datetime import timedelta
from luigi import LocalTarget


FILE_ROOT = 's3://jncc-data/luigi/sentinel/esa_downloader'
LOCAL_TARGET = '/mnt/state/luigi_debug/sentinel/esa_downloader'

logger = logging.getLogger('luigi-interface')    

def getTarget(fileName, date, debug, awsAccessKeyId, awsSecretKey):
    workPath = ''
    if debug:
        workPath = os.path.join(LOCAL_TARGET, date.strftime("%Y-%m-%d"))
    else:
        workPath = os.path.join(FILE_ROOT, date.strftime("%Y-%m-%d"))

    filePath = os.path.join(workPath, fileName)

    if debug:
        logger.info("Debug - writing to %s", filePath)
        return LocalTarget(filePath)
    else:
        client = S3Client(awsAccessKeyId, awsSecretKey)
        return S3Target(path=filePath, client=client)

def getDbConnectionString(host, name, user, password):
    return "host=%s name dbname=%s user=%s name password=%s" % (host, name, user, password)
        
class LastAvailableProductsList(luigi.ExternalTask):
    debug = luigi.BoolParameter()
    seedDate = luigi.DateParameter(default=constants.DEFAULT_DATE)
    runDate = luigi.DateParameter(default=datetime.datetime.now())
    awsAccessKeyId = luigi.Parameter()
    awsSecretKey = luigi.Parameter()

    def output(self):
        d = self.runDate - timedelta(days=1)
        
        return getTarget('available.json', d, self.debug, self.awsAccessKeyId, self.awsSecretKey)    
        
@inherits(LastAvailableProductsList)
class CreateAvailableProductsList(luigi.Task):
    esaUsername = luigi.Parameter()
    esaPassword = luigi.Parameter()
    dbHost = luigi.Parameter()
    dbName = luigi.Parameter()
    dbUser = luigi.Parameter()
    dbPassword = luigi.Parameter()

    def run(self):
        lastList = {"products":[]}

        # If not seeding get last ingestion list from LastAvailableProductsList task
        if self.seedDate == constants.DEFAULT_DATE:
            lastListTarget = yield LastAvailableProductsList()
            with lastListTarget.open() as l:
                lastList = json.load(l)

        with self.output().open('w') as productList:
            listManager = ProductListManager(self.debug)
            
            esaCredentials = self.esaUsername + ':' + self.esaPassword
            dbConn = getDbConnectionString(self.dbHost, self.dbName, self.dbUser, self.dbPassword)
            listManager.create_list(self.runDate ,lastList, productList, self.seedDate, esaCredentials, dbConn)

    def output(self):        
        return getTarget('available.json', self.runDate, self.debug, self.awsAccessKeyId, self.awsSecretKey)

@requires(CreateAvailableProductsList)
class DownloadAvailableProducts(luigi.Task):

    def run(self):

        downloader = ProductDownloader(self.debug)
        result = None

        with self.input().open() as productList: 
            dbConn = getDbConnectionString(self.dbHost, self.dbName, self.dbUser, self.dbPassword)
            result = downloader.download_products(productList, self.runDate, self.awsAccessKeyId, self.awsSecretKey, dbConn)

        if not result is None:
            with self.output().open('w') as successFile:
                if self.debug:
                    msg = "DEBUG MODE: virtual download test succeded for %s files -See DownloadAvailableProducts-log for details" % (result,)
                else:
                    msg = "Downloaded %s products" % (result,)
                successFile.write(msg)

    def output(self):        
        return getTarget('_success.json', self.runDate, self.debug, self.awsAccessKeyId, self.awsSecretKey)
        
if __name__ == '__main__':
    luigi.run()
