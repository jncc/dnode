import json
import os
import psycopg2
import yaml
from datahub_client import DatahubClient

class createList:
    # - Log into the api
    # - Get Products list from api list_products
    # - Check list against catalog 
    # - Subtract downloded products from list
    # - Create todays-date/available.json in lugi s3 folder
    def __init__(self, config_file, logger, outputfile):
        # Setup Config from config file
        self.logger = logger
        self.outputFile = outputfile

        if os.path.isfile(config_file):
            with open("config.yaml", 'r') as conf:
                self.config = yaml.load(conf)

                datahub_conf = self.config.get('datahub')
                if datahub_conf is not None \
                    and 'search_zone_id' in datahub_conf \
                    and 'username' in datahub_conf \
                    and 'password' in datahub_conf \
                    and 'base_url' in datahub_conf \
                    and 'download_chunk_size' in datahub_conf: 
                    self.client = DatahubClient(datahub_conf['base_url'], datahub_conf['download_chunk_size'], datahub_conf['search_zone_id'], datahub_conf['username'], datahub_conf['password'], None)
                else:
                    raise RuntimeError('Config file has invalid datahub entries')

                self.s3_conf = self.config.get('s3')
                if not (self.s3_conf is not None \
                    and 'access_key' in self.s3_conf \
                    and 'secret_access_key' in self.s3_conf):
                    raise RuntimeError('Config file has invalid s3 entries')

    def getDownloadableProductsFromDataCatalog(self):
        productList = self.client.get_product_list()
        self.outputFile.write(json.dumps(productList))

if __name__ == "__main__":
    with open('list.json', 'w') as output:
        lister = createList('config.yaml', None, output)
        lister.getDownloadableProductsFromDataCatalog()