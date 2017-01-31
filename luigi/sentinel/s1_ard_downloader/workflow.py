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

def runtimeErrorLog(logger, message):
    logger.severe(message)
    raise RuntimeError(message)

# Create new products list
class CreateProductsList(luigi.Task):
    debug = luigi.BooleanParameter()
    runDate = luigi.DateParameter(default=datetime.datetime.now())
    config = luigi.Parameter(default='config.yaml')

    cnf = yaml.load(config)
    logger = getLogger(cnf.get('log_dir'), 'CreateProductsList')
    
    def output(self):
        d = self.runDate - timedelta(days=1)
        filePath = os.path.join(os.path.join(cnf.get('working_dir'), d.strftime("%Y-%m-%d")), 'available.json')

    def run(self):
        with open("config.yaml", 'r') as conf:
            config = yaml.load(conf)
            working_dir = getFilePath(config.get('working_dir'), 'working')

            datahub_conf = self.config.get('datahub')
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

            with self.output().open('w') as output:
                products_list_manager = ProductsListManager(config_file, logger, output)
                products_list_manager.getDownloadableProductsFromDataCatalog()

    def output(self):
        return luigi.LocalTarget(getFilePath('available.json'))


# Download new products
class DownloadProducts(luigi.Task):
    debug = luigi.BooleanParameter()
    runDate = luigi.DateParameter(default=datetime.datetime.now())
    config = luigi.Parameter(default='config.yaml')

    def requires(self):
        return CreateProductsList()

    def run(self):
        with open("config.yaml", 'r') as conf:
            config = yaml.load(conf)
            working_dir = getFilePath(config.get('working_dir'), 'working')
            logger = getLogger(cnf.get('log_dir'), 'DownloadProducts')
            
            with self.input().open() as available, self.output(working_dir).open('w') as downloaded:
                if os.path.isfile(config):
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

                    downloader = ProductDownloader(config, logger, working_dir)
                    products_downloader.downloadProducts(available, downloaded)
                else:
                    runtimeErrorLog(logger, 'Config file at [%s] was not found' % config_file)

    def output(self, working_dir):
        return luigi.LocalTarget(getFilePath(working_dir, 'downloaded.json'))

if __name__ == '__main__':
    luigi.run()
