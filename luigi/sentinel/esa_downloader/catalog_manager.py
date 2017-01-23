import psycopg2
import logging
import sys
import json

from config_manager import ConfigManager

class CatalogManager:
    DOWNLOAD_URL_BASE = 'https://scihub.copernicus.eu/apihub/odata/v1'

    db = None

    def __init__(self):
        self.config = ConfigManager("cfg.ini")

    def __enter__(self):
        connection = self.config.getDatabaseConnectionString()
        self.db = psycopg2.connect(connection)
        return self

    def __exit__(self ,type, value, traceback):
        self.db.close()

    def exists(self, productId):
        cur = self.db.cursor()
        cur.execute("SELECT uniqueid FROM sentinel_l WHERE uniqueid = %s", (productId,))
        return cur.fetchone() is not None

    def addProduct(self, product):
        cur = self.db.cursor()

        centroid = json.dumps(product["centroid"])
        footprint = json.dumps(product["footprint"])

        cur.execute('''INSERT INTO sentinel_l
            (uniqueId,title,ingestiondate,footprint,centroid,beginposition,endposition,orbitdirection,producttype,orbitno,relorbitno,platform,location)
            VALUES (%s,%s,to_date(%s,'YYYY-MM-DD'),ST_GeomFromGeoJSON(%s),ST_GeomFromGeoJSON(%s),%s,%s,%s,%s,%s,%s,%s,%s)''',
            (product["uniqueId"],
            product["title"],
            product["ingestionDate"],
            footprint,
            centroid,
            product["beginPosition"],
            product["endPosition"],
            product["orbitDirection"],
            product["productType"],
            product["orbitNo"],
            product["relOrbitNo"],
            product["platform"],
            product["location"]))
        self.db.commit()

