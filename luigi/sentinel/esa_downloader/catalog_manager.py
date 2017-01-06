import psycopg2
import logging
import sys
import shapely.wkt

from config_manager import ConfigManager

class CatalogManager:
    DOWNLOAD_URL_BASE = 'https://scihub.copernicus.eu/apihub/odata/v1'

    self.db = None

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

        simple = shapely.wkt.loads(product["footprint"])
        centroid = shapely.wkt.dumps(simple.centroid)
        cur.execute('''INSERT INTO sentinel
            (uniqueId,title,ingestiondate,footprint,centroid,beginposition,endposition,orbitdirection,producttype,orbitno,relorbitno,platform)
            VALUES (%s,%s,to_date(%s,'YYYY-MM-DD'),ST_GeomFromText(%s,4326),ST_GeomFromText(%s,4326),%s,%s,%s,%s,%s,%s,%s)''',
            (product["uniqueId"],
            product["title"],
            product["ingestionDate"],
            product["footprint"],
            centroid,
            product["beginPosition"],
            product["endPosition"],
            product["orbitDirection"],
            product["productType"],
            product["orbitNo"],
            product["relOrbitNo"],
            product["platform"],))
        self.db.commit()

