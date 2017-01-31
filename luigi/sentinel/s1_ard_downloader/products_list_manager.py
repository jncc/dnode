import json
import os
import psycopg2
import yaml
from datahub_client import DatahubClient

class ProductsListManager:
    def __init__(self, config, logger, outputfile):
        # Setup Config from config file
        self.logger = logger
        self.outputFile = outputfile

        datahub_conf = config.get('datahub')
        self.client = DatahubClient(datahub_conf['base_url'], datahub_conf['download_chunk_size'], datahub_conf['search_zone_id'], datahub_conf['username'], datahub_conf['password'], None)
        self.s3_conf = config.get('s3')

        self.database_conf = self.config.get('database')
        self.db_conn = psycopg2.connect(host=self.database_conf['host'], dbname=self.database_conf['dbname'], user=self.database_conf['username'], password=self.database_conf['password'])

    def getDownloadableProductsFromDataCatalog(self):
        productList = self.client.get_product_list()

        available_product_ids = [str(item['product_id']) for item in productList]

        extracted_path = os.path.join(self.temp, 'extracted')

        # Compare to already aquired products
        cur = self.db_conn.cursor()
        
        ## Get products which we know about that are in the available list and have downloaded
        cur.execute("SELECT (properties->>'product_id')::integer AS id FROM sentinel_ard_backscatter WHERE properties->>'product_id' in %s ORDER BY id;", (tuple(available_product_ids),))
        known_list = [int(item(0)) for item in cur.fetchall()]
        cur.close()

        wanted_list = []

        for item in productList:
            downloaded = False
            if item['product_id'] in known_list:
                downloaded = True                    
            if not downloaded:
                wanted_list.append(item)

        self.outputFile.write(json.dumps(wanted_list))

if __name__ == "__main__":
    with open('list.json', 'w') as output:
        lister = createList('config.yaml', None, output)
        lister.getDownloadableProductsFromDataCatalog()