import luigi
import products_list_manager
import products_downloader
import products_metadata
import os
import logging
import time
import yaml

from luigi.s3 import S3Target
from luigi.util import requires
import datetime

from products_list_manager import ProductsListManager
from products_downloader import ProductDownloader

def getFilePath(root, filename):
    return os.path.join(os.path.join(root, datetime.datetime.now().strftime('%Y-%m-%d')), filename)

def getLogger(folder, name):
    if not os.path.isdir(folder):
        os.makedirs(folder)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler(os.path.join(folder, '%s-%s.log' % (name, time.strftime('%y%m%d-%H%M%S'))))
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)

    logger.addHandler(fh)    
    return logger

def runtimeErrorLog(logger, message):
    logger.severe(message)
    raise RuntimeError(message)

# Create new products list
class CreateProductsList(luigi.ExternalTask):
    debug = luigi.BooleanParameter()
    runDate = luigi.DateParameter(default=datetime.datetime.now())
    config = luigi.Parameter(default='config.yaml')
    working_dir = ''
    s3_working_path = ''
    
    def run(self):
        with open(self.config, 'r') as conf:
            config = yaml.load(conf)
            logger = getLogger(config.get('log_dir'), 'CreateProductsList')
            self.working_dir = config.get('working_dir')
            self.s3_working_path = config.get('s3_working_path')

            datahub_conf = config.get('datahub')
            if not (datahub_conf is not None \
                and 'search_zone_id' in datahub_conf \
                and 'username' in datahub_conf \
                and 'password' in datahub_conf \
                and 'base_url' in datahub_conf \
                and 'download_chunk_size' in datahub_conf): 
                runtimeErrorLog('Config file has invalid datahub entries')

            s3_conf = config.get('s3')
            if not (s3_conf is not None \
                and 'access_key' in s3_conf \
                and 'secret_access_key' in s3_conf):
                runtimeErrorLog('Config file has invalid s3 entries')        

            database_conf = config.get('database')
            if not (database_conf is not None \
                and 'host' in database_conf \
                and 'dbname' in database_conf \
                and 'username' in database_conf \
                and 'password' in database_conf \
                and 'table' in database_conf):
                runtimeErrorLog(logger, 'Config file has missing database config entires')                

            with self.output().open('w') as output:
                products_list_manager = ProductsListManager(config, logger, output)
                products_list_manager.getDownloadableProductsFromDataCatalog()

    def output(self):
        with open(self.config, 'r') as conf:
            config = yaml.load(conf)
        # Local Target
        #return luigi.LocalTarget(getFilePath(self.working_dir, 'available.json'))

        # S3 Target
        return S3Target(getFilePath(config['s3_working_path'], 'available.json'))
        

# Download new products
class DownloadProducts(luigi.Task):
    debug = luigi.BooleanParameter()
    runDate = luigi.DateParameter(default=datetime.datetime.now())
    config = luigi.Parameter(default='config.yaml')
    working_dir = ''

    def requires(self):
        return CreateProductsList()

    def run(self):
        with open(self.config, 'r') as conf:
            config = yaml.load(conf)
            
        self.working_dir = config.get('working_dir')
        logger = getLogger(config.get('log_dir'), 'DownloadProducts')

        with self.input().open() as available, self.output().open('w') as downloaded, self.failures().open('w') as failures:
            datahub_conf = config.get('datahub')
            if not (datahub_conf is not None \
                and 'search_zone_id' in datahub_conf \
                and 'username' in datahub_conf \
                and 'password' in datahub_conf \
                and 'base_url' in datahub_conf \
                and 'download_chunk_size' in datahub_conf): 
                runtimeErrorLog(logger, 'Config file has invalid datahub entries')

            s3_conf = config.get('s3')
            if not (s3_conf is not None \
                and 'access_key' in s3_conf \
                and 'secret_access_key' in s3_conf):
                runtimeErrorLog(logger, 'Config file has invalid s3 entries')
            
            database_conf = config.get('database')
            if not (database_conf is not None \
                and 'host' in database_conf \
                and 'dbname' in database_conf \
                and 'username' in database_conf \
                and 'password' in database_conf \
                and 'table' in database_conf):
                runtimeErrorLog(logger, 'Config file has missing database config entires')

            tempdir = os.path.join(self.working_dir, 'temp')
            if not os.path.isdir(tempdir):
                os.makedirs(tempdir)

            downloader = ProductDownloader(config, logger, tempdir)
            downloader.downloadProducts(available, downloaded, failures)

    def failures(self):
        with open(self.config, 'r') as conf:
            config = yaml.load(conf)
        # Local Target
        # Local Target
        #return luigi.LocalTarget(getFilePath(self.working_dir, '_failures.json'))
        # S3 Target
        return S3Target(getFilePath(config['s3_working_path'], '_failures.json'))

    def output(self):
        with open(self.config, 'r') as conf:
            config = yaml.load(conf)
        # Local Target
        # Local Target
        #return luigi.LocalTarget(getFilePath(self.working_dir, '_success.json'))
        # S3 Target
        return S3Target(getFilePath(config['s3_working_path'], '_success.json'))        

if __name__ == '__main__':
    luigi.run()
