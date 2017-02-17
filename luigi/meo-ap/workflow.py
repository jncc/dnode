import luigi
import docker
import datetime
import os
import json

from ftp_client import FTPClient
from config_manager import ConfigManager
from catalog_manager import CatalogManager
from luigi.util import requires
from luigi.s3 import S3Target

from helpers import s3 as s3Helper

S3_FILE_ROOT = 's3://jncc-data/meo-ap/chlor_a/'
FILE_ROOT = '/tmp/meo-ap'
#DOCKER_IMAGE = '914910572686.dkr.ecr.eu-west-1.amazonaws.com/process-test'
PRODUCTLIST = ['daily', '5day', 'monthly']

class CreateWorkOrder(luigi.Task):
    runDate = luigi.DateParameter(default=datetime.datetime.now())
    ftp = FTPClient()

    def run(self):
        with self.output().open('w') as wddump:
            plist = {}

            for p in PRODUCTLIST:
                print('Getting file list for: ' + p)
                plist[p] = self.ftp.listProductFiles(p)

            print('Writing file list')
            json.dump(plist, wddump, indent=4, sort_keys=True, separators=(',', ':'))    
    
    def output(self):
       filePath = os.path.join(S3_FILE_ROOT, 'todo.json')  

       return S3Target(filePath)

class TransformSrcFileToTiff(luigi.Task):
    runDate = luigi.DateParameter(default=datetime.datetime.now())
    product = luigi.Parameter()
    srcFile = luigi.Parameter()
    fileDate = luigi.Parameter()

    ftp = FTPClient()
    catalog = CatalogManager()
    config = ConfigManager('cfg.ini')

    def run(self):
        ncFile = os.path.join(os.path.join(FILE_ROOT, self.runDate.strftime("%Y-%m-%d")), self.product + '-' + self.fileDate + '.nc')
        tiffFile = os.path.join(os.path.join(FILE_ROOT, self.runDate.strftime("%Y-%m-%d")), 'UK-' + self.product + '-' + self.fileDate + '.tif')
        s3DstFile = os.path.join(os.path.join(os.path.join(os.path.join('meo-ap/chlor_a', self.product), self.fileDate[:4]), self.fileDate[5:6]), 'UK-' + self.product + '-' + self.fileDate + '.tif')
        httpLoc = 'http://jnnc-data.s3-eu-west-1.amazonaws.com/' + s3DstFile

        print('Retrieving ' + self.srcFile)
        self.ftp.getFile(self.product, self.srcFile, ncFile)

        os.system('gdal_translate NETCDF:' + ncFile + ':chlor_a -projwin -24 63 6 48 ' + tiffFile)

        s3Helper.copy_file_to_s3(self.config.getAmazonKeyId(), self.config.getAmazonKeySecret(), self.config.getAmazonRegion(), self.config.getAmazonBucketName(), tiffFile, s3DstFile, True, None)

        self.catalog.addEntry(self.product, 'Chlorophyll-A Density for UK Waters - ' + self.product + ' - ' + self.fileDate, self.srcFile, httpLoc, datetime.datetime.now().strftime("%Y-%m-%d"))

        return

    def output(self):
        filePath = os.path.join(os.path.join(os.path.join(os.path.join(S3_FILE_ROOT, self.product), self.fileDate[:4]), self.fileDate[5:6]), 'UK-' + self.product + '-' + self.fileDate + '.tif')

        return luigi.LocalTarget(filePath) 

    
class ProcessFiles(luigi.Task):
    runDate = luigi.DateParameter(default=datetime.datetime.now())

    def requires(self):
        return CreateWorkOrder(self.runDate)

    def run(self):
        with self.input().open('r') as inp:
            data = json.load(inp)

            for p in PRODUCTLIST:
                for k, v in data[p].items():
                    yield TransformSrcFileToTiff(self.runDate, p, v, k)
            
        with self.output().open('w') as outp:
            outp.write('Test\n')

    def output(self):
       filePath = os.path.join(os.path.join(FILE_ROOT, self.runDate.strftime("%Y-%m-%d")), 'first.txt')  

       return luigi.LocalTarget(filePath)
        
