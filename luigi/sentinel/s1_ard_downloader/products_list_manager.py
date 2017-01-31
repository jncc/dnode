import json
import os
import psycopg2
import yaml
from datahub_client import DatahubClient

class ProductsListManager:
    # - Log into the api
    # - Get Products list from api list_products
    # - Check list against catalog 
    # - Subtract downloded products from list
    # - Create todays-date/available.json in lugi s3 folder
    def __init__(self, config_file, logger, outputfile):
        # Setup Config from config file
        self.logger = logger
        self.outputFile = outputfile

        datahub_conf = self.config.get('datahub')
        self.client = DatahubClient(datahub_conf['base_url'], datahub_conf['download_chunk_size'], datahub_conf['search_zone_id'], datahub_conf['username'], datahub_conf['password'], None)
        self.s3_conf = self.config.get('s3')

    def getDownloadableProductsFromDataCatalog(self):
        productList = self.client.get_product_list()
        self.outputFile.write(json.dumps(productList))

if __name__ == "__main__":
    with open('list.json', 'w') as output:
        lister = createList('config.yaml', None, output)
        lister.getDownloadableProductsFromDataCatalog()