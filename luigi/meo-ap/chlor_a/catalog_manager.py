import psycopg2
import logging
import sys
import json

from config_manager import ConfigManager

class CatalogManager:
    def __init__(self):
        self.config = ConfigManager("cfg.ini")
        connection = self.config.getDatabaseConnectionString()
        self.db = psycopg2.connect(connection)

    def __exit__(self ,type, value, traceback):
        self.db.close()

    def exists(self, product, srcFile):
        cur = self.db.cursor()
        cur.execute("SELECT title FROM chlor_a WHERE srcFile = %s AND product = %s", (srcFile,product))
        return cur.fetchone() is not None

    def addEntry(self, product, title, srcFile, location, ingestionDate):
        cur = self.db.cursor()

        cur.execute('''INSERT INTO chlor_a (product, title, srcFile, location, ingestiondate) VALUES (%s, %s, %s, %s, to_date(%s,'YYYY-MM-DD'))''', (product, title, srcFile, location, ingestionDate))
        self.db.commit()
