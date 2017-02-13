import psycopg2
import logging
import sys
import json

from config_manager import ConfigManager

class CatalogManager:
    config = ConfigManager("cfg.ini")
    connection = config.getDatabaseConnectionString()
    db = psycopg2.connect(connection)

    def __exit__(self ,type, value, traceback):
        self.db.close()

    def exists(self, product, srcFile):
        cur = self.db.cursor()
        cur.execute("SELECT title FROM chloro_a WHERE title = '%s' AND product = '%s'", (srcFile,product))
        return cur.fetchone() is not None

    def addEntry(self, product, title, srcFile, location, ingestionDate):
        cur = self.db.cursor()

        cur.execute('''INSERT INTO chloro_a (product, title, srcFile, location, ingestiondate) VALUES (%s, %s, %s, %s, to_date(%s,'YYYY-MM-DD'))''', (product, title, srcFile, location, ingestionDate))
        self.db.commit()
        
#   product character varying(7) NOT NULL,
#  title character varying(500) NOT NULL,
#  srcFile character varying(500) NOT NULL,
#  location character varying(500) NOT NULL,
#  ingestiondate date NOT NULL
