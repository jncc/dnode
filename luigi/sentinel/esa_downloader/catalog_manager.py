import psycopg2
import logging
import sys
import shapely.wkt

from config_manager import ConfigManager

class CatalogManager:
    DOWNLOAD_URL_BASE = 'https://scihub.copernicus.eu/apihub/odata/v1'

    def __init__(self):
        self.config = ConfigManager("cfg.ini")

    def __getConnection(self):
        connection = self.config.getDatabaseConnectionString()
        db =  psycopg2.connect(connection)
        return db

    def exists(self, productId):
        db = self.__getConnection()
        cur = db.cursor()
        cur.execute("SELECT uniqueid FROM sentinel_l WHERE uniqueid = %s", (productId,))
        return cur.fetchone() is not None
