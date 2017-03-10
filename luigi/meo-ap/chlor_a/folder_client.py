import datetime
import os
import fnmatch
import json
import re

from catalog_manager import CatalogManager

class FolderClient:
    catalog = CatalogManager()

    def listProductFiles(self, product, folder):
        files = [f for f in os.listdir(folder) if (os.path.isfile(os.path.join(folder, f)) and fnmatch.fnmatch(f, '*.nc'))]

        res = {}

        for f in files:
            m = re.search('OCx_QAA-([0-9]{4})-fv', f)
            y = m.group(1)
            
            if not self.catalog.exists(product, y + '/' + f):
                res[y] = folder + '/' + f
        
        return res
