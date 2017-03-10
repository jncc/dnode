import luigi
import docker
import datetime
import os
import json

from ftp_client import FTPClient
from folder_client import FolderClient
from config_manager import ConfigManager
from catalog_manager import CatalogManager
from luigi.util import requires
from luigi.s3 import S3Target
from shutil import copyfile
from ncMetadata import NetCDFMetadata

from helpers import s3 as s3Helper

S3_FILE_ROOT = 's3://jncc-data/meo-ap/chlor_a/'
FILE_ROOT = '/tmp/meo-ap'
PRODUCTLIST = ['monthly', '5day', 'daily']

#####
# Author: Paul Gilbertson
#
# Creates a list of products present on the PML ftp site without entries in the catalog. These files need
# processing.
#####
class CreateWorkOrder(luigi.Task):
    runDate = luigi.DateParameter(default=datetime.datetime.now())
    ftp = FTPClient()
    folder = FolderClient()
    config = ConfigManager('cfg.ini')

    def run(self):
        with self.output().open('w') as wddump:
            plist = {}

            for p in PRODUCTLIST:
                print('Getting file list for: ' + p)
                plist[p] = self.ftp.listProductFiles(p)
            
            plist['yearly'] = self.folder.listProductFiles('yearly', self.config.getAnnualFolder())

            print('Writing file list')
            json.dump(plist, wddump)    
    
    def output(self):
       filePath = os.path.join(os.path.join(S3_FILE_ROOT, self.runDate.strftime("%Y-%m-%d")), 'todo.json')  

       return S3Target(filePath)

#####
# Author: Paul Gilbertson
#
# Retrieves a Chloro_a density file from PML, transforms to GeoTIFF, trims to UK waters and pushes the
# output to S3.
#####
class ProcessNetCDFFile(luigi.Task):
    runDate = luigi.DateParameter(default=datetime.datetime.now())
    product = luigi.Parameter()
    srcFile = luigi.Parameter()
    fileDate = luigi.Parameter()

    ftp = FTPClient()
    catalog = CatalogManager()
    config = ConfigManager('cfg.ini')
    metadata = NetCDFMetadata()

    def run(self):
        ncFile = os.path.join(os.path.join(FILE_ROOT, self.runDate.strftime("%Y-%m-%d")), self.product + '-' + self.fileDate + '.nc')
        tiffFile = os.path.join(os.path.join(FILE_ROOT, self.runDate.strftime("%Y-%m-%d")), 'UK-' + self.product + '-' + self.fileDate + '.tif')
        s3DstFile = os.path.join(os.path.join(os.path.join(os.path.join('meo-ap/chlor_a', self.product), self.fileDate[:4]), self.fileDate[4:6]), 'UK-' + self.product + '-' + self.fileDate + '.tif')
        httpLoc = 'http://jncc-data.s3-eu-west-1.amazonaws.com/' + s3DstFile

        # Get NetCDF file from FTP site
        print('Retrieving ' + self.srcFile)
        self.ftp.getFile(self.product, self.srcFile, ncFile)

        # Get metadata from NetCDF (also acts as a validation check)
        tc = self.metadata.getTimeCoverage(ncFile)

        # Use GDAL to translate NetCDF to GeoTIFF and trim file to UK waters
        os.system('gdal_translate NETCDF:' + ncFile + ':chlor_a -projwin -24 63 6 48 ' + tiffFile)

        # Push file to S3
        s3Helper.copy_file_to_s3(self.config.getAmazonKeyId(), self.config.getAmazonKeySecret(), self.config.getAmazonRegion(), self.config.getAmazonBucketName(), tiffFile, s3DstFile, True, None)

        # Add entry to catalog
        self.catalog.addEntry(self.product, 'Chlorophyll-a Concentration for UK Waters - ' + self.product + ' - ' + self.fileDate, self.srcFile, httpLoc, tc['start'][:8], tc['end'][:8], datetime.datetime.now().strftime("%Y-%m-%d"))

        # Clean up intermediate files so we don't flood out /tmp
        os.remove(ncFile)
        os.remove(tiffFile)

        return

    def output(self):
        filePath = os.path.join(os.path.join(os.path.join(os.path.join(S3_FILE_ROOT, self.product), self.fileDate[:4]), self.fileDate[4:6]), 'UK-' + self.product + '-' + self.fileDate + '.tif')

        return S3Target(filePath) 

#####
# Author: Paul Gilbertson
#
# Retrieves a Chloro_a density file from a local folder, transforms to GeoTIFF, trims to UK waters and pushes the
# output to S3.
#####
class ProcessAnnualNetCDFFile(luigi.Task):
    runDate = luigi.DateParameter(default=datetime.datetime.now())
    product = luigi.Parameter()
    srcFile = luigi.Parameter()
    fileDate = luigi.Parameter()

    ftp = FTPClient()
    catalog = CatalogManager()
    config = ConfigManager('cfg.ini')
    metadata = NetCDFMetadata()

    def run(self):
        ncFile = os.path.join(os.path.join(FILE_ROOT, self.runDate.strftime("%Y-%m-%d")), self.product + '-' + self.fileDate + '.nc')
        tiffFile = os.path.join(os.path.join(FILE_ROOT, self.runDate.strftime("%Y-%m-%d")), 'UK-' + self.product + '-' + self.fileDate + '.tif')
        s3DstFile = os.path.join(os.path.join(os.path.join('meo-ap/chlor_a', self.product), self.fileDate[:4]), 'UK-' + self.product + '-' + self.fileDate + '.tif')
        httpLoc = 'http://jncc-data.s3-eu-west-1.amazonaws.com/' + s3DstFile

        # Get NetCDF file from FTP site
        print('Retrieving ' + self.srcFile)
        if not os.path.exists(os.path.dirname(os.path.join(FILE_ROOT, self.runDate.strftime("%Y-%m-%d")))):
            os.makedirs(os.path.dirname(os.path.join(FILE_ROOT, self.runDate.strftime("%Y-%m-%d"))))

        copyfile(self.srcFile, ncFile)

        # Get metadata from NetCDF (also acts as a validation check)
        tc = self.metadata.getTimeCoverage(ncFile)

        # Use GDAL to translate NetCDF to GeoTIFF and trim file to UK waters
        os.system('gdal_translate NETCDF:' + ncFile + ':chlor_a -projwin -24 63 6 48 ' + tiffFile)

        # Push file to S3
        s3Helper.copy_file_to_s3(self.config.getAmazonKeyId(), self.config.getAmazonKeySecret(), self.config.getAmazonRegion(), self.config.getAmazonBucketName(), tiffFile, s3DstFile, True, None)

        # Add entry to catalog
        self.catalog.addEntry(self.product, 'Chlorophyll-a Concentration for UK Waters - ' + self.product + ' - ' + self.fileDate, self.srcFile, httpLoc, tc['start'][:8], tc['end'][:8], datetime.datetime.now().strftime("%Y-%m-%d"))

        # Clean up intermediate files so we don't flood out /tmp
        os.remove(ncFile)
        os.remove(tiffFile)

        return

    def output(self):
        filePath = os.path.join(os.path.join(os.path.join(S3_FILE_ROOT, self.product), self.fileDate[:4]), 'UK-' + self.product + '-' + self.fileDate + '.tif')

        return S3Target(filePath) 

#####
# Author: Paul Gilbertson
#
# Create a work order and run it
#####    
class ProcessFiles(luigi.Task):
    runDate = luigi.DateParameter(default=datetime.datetime.now())

    def requires(self):
        return CreateWorkOrder(self.runDate)

    def run(self):
        with self.input().open('r') as inp:
            data = json.load(inp)

            for p in PRODUCTLIST:
                for k, v in data[p].items():
                    yield ProcessNetCDFFile(self.runDate, p, v, k)
            
            for k, v in data['yearly'].items():
                yield ProcessAnnualNetCDFFile(self.runDate, 'yearly', v, k)

        with self.output().open('w') as outp:
            outp.write('Test\n')

    def output(self):
       filePath = os.path.join(os.path.join(FILE_ROOT, self.runDate.strftime("%Y-%m-%d")), '_completed.txt')  

       return luigi.LocalTarget(filePath)
