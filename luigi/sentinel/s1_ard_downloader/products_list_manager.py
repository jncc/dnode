import json
from datahub_client import DatahubClient
import psycopg2

class createList:
# - Log into the api
# - Get Products list from api list_products
# - Check list against catalog 
# - Subtract downloded products from list
# - Create todays-date/available.json in lugi s3 folder
    def init(self, config, logger, outputfile):
        # Setup Config from config file
        self.logger = logger

        if os.path.isfile(config_file):
            with open("config.yaml", 'r') as conf:
                self.config = yaml.load(conf)

                datahub_conf = self.config.get('datahub')
                if datahub_conf is not None \
                    and 'search_zone_id' in datahub_conf \
                    and 'username' in datahub_conf \
                    and 'password' in datahub_conf 
                    self.client = new new DatahubClient(datahub_conf['search_zone_id'], datahub_conf['username'], datahub_conf['password'], None)
                else:
                    raise RuntimeError('Config file has invalid datahub entries')

                self.s3_conf = self.config.get('s3')
                if !(self.s3_conf is not None \
                    and 'access_key' in self.s3_conf \
                    and 'secret_access_key' in self.s3_conf)
                    raise RuntimeError('Config file has invalid s3 entries')

def getDownloadableProductsFromDataCatalog():
    productList = client.get_product_list()
    outputFile.write(json.dumps(productList))